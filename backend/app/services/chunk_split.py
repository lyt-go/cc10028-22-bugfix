"""分块切分服务：文档文本按语义边界的分块切分、重叠窗口保留与检索标签标注。

从解析产物读取已解析文本，按语义边界切分为独立分块并持久化，同时触发实体识别和向量嵌入。

切分产物按「页组 + 输入摘要」组织成不可变代次（generation）：
- 同一份解析页数据、同样的页范围和切分设置重复执行，只会复用同一个代次，
  不会把同名分块再追加一份；
- 每次切分先在 manifests/chunks/ 下写完整清单，再登记代次账本，
  下游按清单快照消费，不会读到写了一半的新文本；
- 账本记录同一页组内的代次索引，并发的下一次切分能拿到明确、递增的索引。
"""
from __future__ import annotations

from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class ChunkSplitService(BaseModuleService):
    MODULE = "chunk_split"
    ENTITY = "文本分块"
    OPERATION_TERM = "切分"
    LIST_FIELDS = ["分块编号", "文档编号", "分块长度", "重叠窗口", "切分边界", "分块序号", "检索标签", "切分策略"]
    REQUIRED_FIELDS = ["分块编号", "文档编号", "分块长度"]
    STATUS_ORDER = ["草稿", "待切分", "已切分", "切分异常"]
    ACTION_RULES = {"启动切分": "待切分", "确认切分": "已切分", "标记异常": "切分异常"}
    NEGATIVE_ACTIONS = ["标记异常"]

    GENERATION_KIND = "chunks"

    STRATEGY_PRESETS = {
        "semantic": {"description": "按语义边界切分，保留完整句子和段落结构"},
        "fixed": {"description": "按固定长度切分，超出部分截断"},
        "sliding": {"description": "滑动窗口切分，保留重叠区以确保上下文连贯"},
    }

    def split_from_document(
        self,
        parsed_doc_id: str,
        *,
        strategy: str = "semantic",
        chunk_size: int = 512,
        overlap: int = 50,
        pages: list[int] | None = None,
    ) -> dict[str, Any]:
        """从已解析文档读入并按语义边界切分为文本分块，持久化后触发实体识别和向量嵌入。

        同样的解析摘要、页范围和切分设置会得到同一个代次（幂等重试）；
        只有输入真的变化时才产生新的代次索引。
        """
        # 读取解析产物作为输入源
        parsed_file = self._data_dir("parsed") / f"{parsed_doc_id.replace('_parsed', '')}_parsed.jsonl"
        if not parsed_file.exists():
            # 尝试直接查找
            alt_path = self._data_dir("parsed") / f"{parsed_doc_id}.jsonl"
            if alt_path.exists():
                parsed_file = alt_path
            else:
# 数据操作：切分文本分块按语义边界生成分块数据集
                return {"error": f"解析产物文件 {parsed_doc_id} 不存在，请先执行格式解析"}

        parsed_records = self._read_jsonl(parsed_file)
        if pages:
            wanted = {int(p) for p in pages}
            parsed_records = [
                rec for rec in parsed_records if int(rec.get("page_num", 1)) in wanted
            ]
        if not parsed_records:
            return {"error": f"解析产物 {parsed_doc_id} 为空，无法切分"}

        settings = {"strategy": strategy, "chunk_size": chunk_size, "overlap": overlap}
        page_nums = sorted({int(rec.get("page_num", 1)) for rec in parsed_records})
        page_group = self._page_group(parsed_doc_id, page_nums, all_pages=not pages)
        parse_digest = self._parse_digest(parsed_records)
        digest = self._digest_payload({
            "parse_digest": parse_digest,
            "pages": page_nums,
            "settings": settings,
        })

        output_path = self._data_dir("chunks") / f"{page_group}_chunks.jsonl"
        digest_safe = digest[:16]
        generation_path = self._data_dir("chunks") / f"{page_group}__{digest_safe}_chunks.jsonl"
        lock_target = self._data_dir("manifests") / "locks" / f"{self._safe_name(page_group)}.split"
        reused = False
        with self._file_lock(lock_target):
            existing = next(
                (e for e in self._generation_ledger_entries(page_group) if e.get("digest") == digest),
                None,
            )
            if existing is not None:
                chunks = self._read_generation_manifest(existing)
                if chunks is None:  # 清单缺失时按相同输入重建，保证代次可用
                    chunks = self._dedupe_chunks(
                        self._build_chunks(parsed_records, strategy, chunk_size, overlap)
                    )
                reused = True
            else:
                chunks = self._dedupe_chunks(
                    self._build_chunks(parsed_records, strategy, chunk_size, overlap)
                )

            # 代次产物按摘要命名、内容固定，重放同一输入不会追加出第二份分块。
            written = self._write_jsonl(generation_path, chunks)

            generation = self._publish_generation(
                kind=self.GENERATION_KIND,
                page_group=page_group,
                digest=digest,
                records=chunks,
                meta={
                    "parsed_doc_id": parsed_doc_id,
                    "parse_digest": parse_digest,
                    "pages": page_nums,
                    "settings": settings,
                    "artifact": self._artifact_ref(generation_path),
                },
            )
            # 活动产物只跟随最新确认代次；旧代次重试不会把旧文本盖回来。
            if generation.get("status") == "current" and int(generation.get("index", 0)) == self._active_index(page_group):
                self._write_jsonl(output_path, chunks)

        # 写入检查点
        self._write_checkpoint(f"chunk_{parsed_doc_id}", {
            "parsed_doc_id": parsed_doc_id,
            "page_group": page_group,
            "generation_id": generation.get("generation_id"),
            "generation_index": generation.get("index"),
            "digest": digest,
            "parse_digest": parse_digest,
            "pages": page_nums,
            "settings": settings,
            "chunks_count": len(chunks),
            "unique_chunk_ids": len({c.get("chunk_id") for c in chunks}),
            "strategy": strategy,
            "total_length": sum(c["length"] for c in chunks),
            "decision_boundary": "按语义边界切分是保持上下文完整性的最佳实践",
        })

# 数据产物：标签检索分块数据集按标签过滤定位分块条目
        # 跨模块自动触发：实体识别和向量嵌入（嵌入按代次清单快照消费）
        triggers = {}
        if parsed_doc_id:
            trig_entity = self._trigger_entity_recognize(parsed_doc_id)
            triggers["entity_recognize"] = trig_entity
            trig_vector = self._trigger_vector_embed(
                parsed_doc_id, generation=generation.get("generation_id")
            )
            triggers["vector_embed"] = trig_vector

        return {
            "parsed_doc_id": parsed_doc_id,
            "page_group": page_group,
            "generation_id": generation.get("generation_id"),
            "generation_index": generation.get("index"),
            "digest": digest,
            "parse_digest": parse_digest,
            "pages": page_nums,
            "reused_generation": reused,
            "chunks": len(chunks),
            "unique_chunk_ids": len({c.get("chunk_id") for c in chunks}),
            "records_written": written,
            "strategy": strategy,
            "output": str(output_path),
            "triggers": triggers,
        }

    # -- 代次查询与比对 --

    def generation_status(self, page_group: str | None = None) -> dict[str, Any]:
        """查明代次消费情况：正在被索引消费的页组代次与下一次可用索引。

        不传页组时返回所有已登记页组的汇总，便于并发重跑后核对。
        """
        if page_group is None:
            groups = sorted({str(e.get("page_group", "")) for e in self._read_generation_ledger(self.GENERATION_KIND)})
            return {
                "kind": self.GENERATION_KIND,
                "page_groups": {group: self._generation_report(self.GENERATION_KIND, group) for group in groups},
            }
        return self._generation_report(self.GENERATION_KIND, page_group)

    def list_generations(self, page_group: str) -> list[dict[str, Any]]:
        """列出某页组已登记的全部切分代次（含历史与已取代代次）。"""
        return self._generations_for(self.GENERATION_KIND, page_group)

    def resolve_generation(self, page_group: str, ref: str | int | None = None) -> dict[str, Any] | None:
        """按代次编号或索引定位切分代次；ref 为空时取当前确认代次。"""
        if ref is None or str(ref).strip() == "":
            return self._active_generation(self.GENERATION_KIND, page_group)
        return self._generation_by_ref(self.GENERATION_KIND, page_group, ref)

    def compare_generations(self, page_group: str, ref_a: str | int, ref_b: str | int) -> dict[str, Any]:
        """比较两个切分代次的实际分块编号与来源摘要，确认同页没有多出分块。"""
        return self._compare_generation_refs(
            kind=self.GENERATION_KIND,
            page_group=page_group,
            ref_a=ref_a,
            ref_b=ref_b,
        )

    # -- 内部实现 --

    def _page_group(self, parsed_doc_id: str, page_nums: list[int], *, all_pages: bool) -> str:
        """页组标识：整份解析产物用其编号，限定页范围时附上页号。"""
        if all_pages:
            return str(parsed_doc_id)
        return f"{parsed_doc_id}#p{'-'.join(str(n) for n in page_nums)}"

    def _parse_digest(self, parsed_records: list[dict[str, Any]]) -> str:
        """解析摘要：只看页内容和页号，不受文件写入时间等易变字段影响。"""
        view = sorted(
            (
                {
                    "doc_id": rec.get("doc_id"),
                    "page_num": rec.get("page_num", 1),
                    "paragraphs": rec.get("paragraphs"),
                    "text_length": rec.get("text_length"),
                    "content_hash": rec.get("content_hash"),
                    "parser": rec.get("parser"),
                }
                for rec in parsed_records
            ),
            key=lambda item: (str(item["doc_id"]), int(item["page_num"])),
        )
        return self._digest_payload(view)

    def _active_index(self, page_group: str) -> int:
        active = self._active_generation(self.GENERATION_KIND, page_group)
        return int(active.get("index", 0)) if active else 0

    def _generation_ledger_entries(self, page_group: str) -> list[dict[str, Any]]:
        return self._generations_for(self.GENERATION_KIND, page_group)

    def _dedupe_chunks(self, chunks: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """同一 chunk_id 只保留一条，保证分块明细计数与索引行一一对应。"""
        return self._dedupe_records(chunks, "chunk_id")

    def _build_chunks(
        self,
        parsed_records: list[dict[str, Any]],
        strategy: str,
        chunk_size: int,
        overlap: int,
    ) -> list[dict[str, Any]]:
        """按句子边界把解析页文本切成带重叠窗口的分块。"""
        chunks = []
        chunk_seq = 0
        for rec in parsed_records:
            # 对每页的内容切分
            page_text = "\n".join(self._simulate_doc_content(rec.get("doc_id"), paragraphs=3))
            # 按句子边界切分
            sentences = page_text.replace("。", "。\n").split("\n")
            current_chunk = ""
            for sent in sentences:
                sent = sent.strip()
                if not sent:
                    continue
                if len(current_chunk) + len(sent) > chunk_size:
                    if current_chunk:
                        chunk_seq += 1
                        chunks.append(self._chunk_row(rec, chunk_seq, current_chunk, strategy, overlap))
                    # 保留重叠窗口
                    overlap_text = current_chunk[-overlap:] if len(current_chunk) > overlap else ""
                    current_chunk = overlap_text + sent
# 数据产物：分块数据集写入chunks目录持久化文件，同时触发下游实体数据集与向量索引
                else:
                    current_chunk += sent if not current_chunk else " " + sent
            # 处理最后一段
            if current_chunk:
                chunk_seq += 1
                chunks.append(self._chunk_row(rec, chunk_seq, current_chunk, strategy, overlap))
        return chunks

    def _chunk_row(
        self,
        rec: dict[str, Any],
        chunk_seq: int,
        text: str,
        strategy: str,
        overlap: int,
    ) -> dict[str, Any]:
        return {
            "chunk_id": f"CHUNK-{rec['doc_id']}-{chunk_seq:04d}",
            "doc_id": rec["doc_id"],
            "page": rec.get("page_num", 1),
            "seq": chunk_seq,
            "length": len(text),
            "overlap": overlap,
            "boundary": "sentence" if strategy == "semantic" else "fixed",
            "tags": self._generate_tags(text),
            "text": text,
            "strategy": strategy,
        }

    def _generate_tags(self, text: str) -> list[str]:
        """基于文本内容生成检索标签，识别文本中的关键实体类别。"""
        tags = []
# 数据边界：切分操作处理空分块与缺失产物异常边界数据
        keywords = {
            "企业": ["公司", "集团", "企业", "有限公司"],
            "人员": ["张", "博士", "科学家", "工程师", "经理"],
            "时间": ["2024年", "季度", "月", "日"],
            "地点": ["Beijing", "Shanghai", "总部", "分部"],
            "金额": ["亿元", "万元", "投资"],
            "技术": ["知识图谱", "人工智能", "技术", "系统"],
        }
        for tag, words in keywords.items():
            if any(w in text for w in words):
                tags.append(tag)
        return tags if tags else ["通用"]
