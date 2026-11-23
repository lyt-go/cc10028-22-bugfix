"""向量嵌入服务：文本分块的向量嵌入模型选择、批次写入与向量索引地址登记。

从切分代次清单读取文本分块快照，执行向量嵌入计算并把向量写入按代次命名的索引文件。

与切分产物的关系：
- 嵌入消费的是切分代次清单里的分块快照，而不是正在被改写的活动文件，
  因此切分正在扫描上一代时，本次嵌入仍然看到一致的旧分块，不会混入追加到一半的新文本；
- 每条索引记录带上实际使用的页和切分代次编号，可反查它对应的分块成品；
- 同一分块集合与模型重复嵌入会落到同一个向量代次，chunk_id 每个只计一次，
  不会产生额外的索引副本。
"""
from __future__ import annotations

import time
import hashlib
from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class VectorEmbedService(BaseModuleService):
    MODULE = "vector_embed"
    ENTITY = "向量化任务"
    OPERATION_TERM = "写入"
    LIST_FIELDS = ["任务编号", "分块编号", "嵌入模型", "向量维度", "批次大小", "向量条数", "索引地址", "模型版本"]
    REQUIRED_FIELDS = ["任务编号", "分块编号", "嵌入模型"]
    STATUS_ORDER = ["草稿", "待嵌入", "嵌入中", "已完成"]
    ACTION_RULES = {"启动嵌入": "待嵌入", "标记完成": "已完成", "标记废弃": "已完成"}
    NEGATIVE_ACTIONS = []

    GENERATION_KIND = "vectors"

    MODEL_PRESETS = {
        "text2vec-large": {"dimension": 1024, "batch_size": 32},
        "bge-large": {"dimension": 1024, "batch_size": 64},
        "m3e-base": {"dimension": 768, "batch_size": 32},
    }
# 数据操作：写入文本分块向量化生成向量索引数据集

    def embed_from_chunk(
        self,
        chunk_set_id: str,
        *,
        model: str = "text2vec-large",
        generation: str | int | None = None,
    ) -> dict[str, Any]:
        """按切分代次的分块清单执行向量嵌入，并持久化为可追溯的向量索引文件。

        `generation` 为空时使用该页组当前确认的切分代次；显式指定则嵌入历史代次，
        便于在并发重跑后核对"嵌入到底用了哪一批分块"。
        """
        chunk_entry, chunks, error = self._load_chunk_generation(chunk_set_id, generation)
        if error:
            return {"error": error}
        assert chunk_entry is not None

        chunk_rows = self._dedupe_chunks(chunks)
        declared = int(chunk_entry.get("records", len(chunks)) or 0)
        if len(chunk_rows) != declared:
            return {
                "error": (
                    f"切分代次 {chunk_entry.get('generation_id')} 分块明细不一致："
                    f"清单声明 {declared} 条，去重后 {len(chunk_rows)} 条"
                ),
                "chunk_set_id": chunk_set_id,
                "chunk_generation": chunk_entry.get("generation_id"),
            }

        model_info = self.MODEL_PRESETS.get(model, {"dimension": 1024, "batch_size": 32})
        dimension = model_info["dimension"]
        batch_size = model_info["batch_size"]

        vectors = []
        for chunk in chunk_rows:
            text = chunk.get("text", "")
            if not text:
                continue
            emb = self._compute_embedding(text, dimension)
            chunk_id = chunk.get("chunk_id")
            vectors.append({
                "chunk_id": chunk_id,
# 数据产物：向量索引数据集写入vectors目录持久化文件
                "doc_id": chunk.get("doc_id"),
                "page": chunk.get("page", 1),
                "chunk_set_id": chunk_set_id,
                "chunk_generation_id": chunk_entry.get("generation_id"),
                "chunk_generation_index": chunk_entry.get("index"),
                "chunk_manifest": chunk_entry.get("manifest"),
                "chunk_set_digest": chunk_entry.get("digest"),
                "vector_dim": dimension,
                "vector": emb,
                "model": model,
                "batch_size": batch_size,
                "text_length": len(text),
                "index": f"vector_index/{chunk_id}",
                "embedded_at": time.time(),
            })

        vector_digest = self._digest_payload({
            "chunk_generation": chunk_entry.get("generation_id"),
            "chunk_digest": chunk_entry.get("digest"),
            "model": model,
            "dimension": dimension,
            "chunk_ids": [v.get("chunk_id") for v in vectors],
        })
        generation_path = (
            self._data_dir("vectors")
            / f"{chunk_set_id}__{(vector_digest or '')[:16]}_vectors.jsonl"
        )
        active_path = self._data_dir("vectors") / f"{chunk_set_id}_vectors.jsonl"
        lock_target = self._data_dir("manifests") / "locks" / f"{self._safe_name(chunk_set_id)}.embed"

        reused = False
        with self._file_lock(lock_target):
            existing = next(
                (e for e in self._generations_for(self.GENERATION_KIND, chunk_set_id)
                 if e.get("digest") == vector_digest),
                None,
            )
            reused = existing is not None
            written = self._write_jsonl(generation_path, vectors)
            generation = self._publish_generation(
                kind=self.GENERATION_KIND,
                page_group=chunk_set_id,
                digest=vector_digest,
                records=vectors,
                meta={
                    "chunk_generation_id": chunk_entry.get("generation_id"),
                    "chunk_generation_index": chunk_entry.get("index"),
                    "chunk_manifest": chunk_entry.get("manifest"),
                    "model": model,
                    "dimension": dimension,
                    "pages": sorted({v.get("page", 1) for v in vectors}),
                    "artifact": self._artifact_ref(generation_path),
                },
            )
            if generation.get("status") == "current" and int(generation.get("index", 0)) == self._active_vector_index(chunk_set_id):
                self._write_jsonl(active_path, vectors)

        self._write_checkpoint(f"vector_{chunk_set_id}", {
            "chunk_set_id": chunk_set_id,
            "chunk_generation_id": chunk_entry.get("generation_id"),
            "vector_generation_id": generation.get("generation_id"),
            "vector_generation_index": generation.get("index"),
            "vector_digest": vector_digest,
            "model": model,
            "dimension": dimension,
            "chunks_embedded": len(vectors),
            "unique_chunk_ids": len({v.get("chunk_id") for v in vectors}),
            "pages": sorted({v.get("page", 1) for v in vectors}),
            "total_dimension": len(vectors) * dimension,
        })

        return {
            "chunk_set_id": chunk_set_id,
            "chunk_generation_id": chunk_entry.get("generation_id"),
            "chunk_generation_index": chunk_entry.get("index"),
            "vector_generation_id": generation.get("generation_id"),
            "vector_generation_index": generation.get("index"),
            "reused_generation": reused,
            "model": model,
            "dimension": dimension,
            "chunks": len(vectors),
            "unique_chunk_ids": len({v.get("chunk_id") for v in vectors}),
            "pages": sorted({v.get("page", 1) for v in vectors}),
            "records_written": written,
            "output": str(generation_path),
            "active_output": str(active_path),
        }

    def embed_generation(self, chunk_set_id: str, generation: str | int) -> dict[str, Any]:
        """显式嵌入某个切分代次，用于并发重跑后核对历史代次。"""
        return self.embed_from_chunk(chunk_set_id, generation=generation)

    def generation_status(self, chunk_set_id: str) -> dict[str, Any]:
        """查明某页组的切分代次与向量代次：正在消费的代次、下一次可用索引。"""
        return {
            "chunk_set_id": chunk_set_id,
            "chunk_generations": self._generation_report("chunks", chunk_set_id),
            "vector_generations": self._generation_report(self.GENERATION_KIND, chunk_set_id),
        }

    def compare_generations(self, chunk_set_id: str, ref_a: str | int, ref_b: str | int) -> dict[str, Any]:
        """比较两个向量代次的真实分块编号与来源摘要，检验同页没有生成额外向量。"""
        return self._compare_generation_refs(
            kind=self.GENERATION_KIND,
            page_group=chunk_set_id,
            ref_a=ref_a,
            ref_b=ref_b,
        )

    def verify_against_chunks(
        self,
        chunk_set_id: str,
        vector_ref: str | int | None = None,
        chunk_ref: str | int | None = None,
    ) -> dict[str, Any]:
        """按切分代次产物核对分块明细，确认向量与分块一一对应。"""
        chunk_entry, chunks, error = self._load_chunk_generation(chunk_set_id, chunk_ref)
        if error:
            return {"error": error}
        vector_entry = self._vector_generation(chunk_set_id, vector_ref)
        if vector_entry is None:
            return {"error": f"向量代次不存在：{chunk_set_id}/{vector_ref or 'current'}"}
        vectors = self._read_generation_manifest(vector_entry) or []
        chunk_ids = [str(c.get("chunk_id")) for c in chunks if c.get("text")]
        vector_ids = [str(v.get("chunk_id")) for v in vectors]
        return {
            "chunk_set_id": chunk_set_id,
            "chunk_generation_id": None if chunk_entry is None else chunk_entry.get("generation_id"),
            "vector_generation_id": vector_entry.get("generation_id"),
            "chunk_records": len(chunks),
            "vector_records": len(vectors),
            "duplicate_chunk_ids": self._duplicate_ids(vector_ids),
            "missing_vectors": sorted(set(chunk_ids) - set(vector_ids)),
            "orphan_vectors": sorted(set(vector_ids) - set(chunk_ids)),
            "consistent": sorted(set(chunk_ids)) == sorted(set(vector_ids))
            and not self._duplicate_ids(vector_ids),
        }

    # -- 内部实现 --

    def _load_chunk_generation(
        self,
        chunk_set_id: str,
        generation: str | int | None,
    ) -> tuple[dict[str, Any] | None, list[dict[str, Any]], str | None]:
        """定位切分代次并读出清单快照；没有代次账本时退回读取活动产物。"""
        kind = "chunks"
        entry: dict[str, Any] | None
        if generation is None or str(generation).strip() == "":
            entry = self._active_generation(kind, chunk_set_id)
        else:
            entry = self._generation_by_ref(kind, chunk_set_id, generation)

        if entry is not None:
            chunks = self._read_generation_manifest(entry)
            if chunks is None:
                return entry, [], f"切分代次 {entry.get('generation_id')} 的清单缺失或损坏"
            return entry, chunks, None

        if generation not in (None, ""):
            return None, [], f"切分代次 {chunk_set_id}/{generation} 不存在"

        # 兼容尚未登记代次的旧产物：作为一次临时快照消费，不写入账本。
        # 旧版追加写可能留下同名副本，按 chunk_id 去重后再作为嵌入输入。
        legacy_path = self._data_dir("chunks") / f"{chunk_set_id}_chunks.jsonl"
        chunks = self._read_jsonl(legacy_path)
        if not chunks:
            return None, [], f"分块产物 {chunk_set_id} 不存在或为空，请先执行分块切分"
        deduped = self._dedupe_chunks(chunks)
        snapshot = {
            "page_group": chunk_set_id,
            "generation_id": f"{self._safe_name(chunk_set_id)}__legacy",
            "index": 0,
            "digest": self._digest_payload(deduped)[:16],
            "manifest": self._artifact_ref(legacy_path),
            "records": len(deduped),
            "legacy_rows": len(chunks),
            "legacy_duplicates": len(chunks) - len(deduped),
        }
        return snapshot, deduped, None

    def _vector_generation(self, chunk_set_id: str, ref: str | int | None) -> dict[str, Any] | None:
        if ref is None or str(ref).strip() == "":
            return self._active_generation(self.GENERATION_KIND, chunk_set_id)
        return self._generation_by_ref(self.GENERATION_KIND, chunk_set_id, ref)

    def _active_vector_index(self, chunk_set_id: str) -> int:
        active = self._active_generation(self.GENERATION_KIND, chunk_set_id)
        return int(active.get("index", 0)) if active else 0

    def _dedupe_chunks(self, chunks: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """每个 chunk_id 只计一次，避免历史残留副本进入嵌入输入。"""
        return self._dedupe_records(chunks, "chunk_id")

    def _compute_embedding(self, text: str, dimension: int) -> list[float]:
        """模拟向量嵌入计算，基于文本哈希生成伪嵌入向量用于测试数据流。"""
        hash_bytes = hashlib.sha256(text.encode("utf-8")).digest()
        vector = []
        for i in range(dimension):
            byte_idx = i % len(hash_bytes)
            value = (hash_bytes[byte_idx] / 255.0) - 0.5
            if i % 3 == 0:
                value *= 1.5
            elif i % 5 == 0:
                value *= 0.8
            vector.append(round(value, 6))
        return vector

    def batch_embed_documents(self, doc_ids: list[str]) -> dict[str, Any]:
        """批量对多个文档执行向量嵌入，返回汇总统计。"""
        results = []
        for doc_id in doc_ids:
            try:
                r = self.embed_from_chunk(f"{doc_id}_parsed")
                if "error" not in r:
                    results.append(r)
            except Exception:
                pass
        return {
            "documents": len(doc_ids),
            "success": len(results),
            "total_vectors": sum(r.get("chunks", 0) for r in results),
        }
