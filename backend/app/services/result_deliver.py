"""结果交付服务：抽取结果按交付格式向目标系统的分片导出与交付记录登记。

从版本快照读取产物，按指定格式和分片策略导出到目标系统。
"""
from __future__ import annotations

import time
from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class ResultDeliverService(BaseModuleService):
    MODULE = "result_deliver"
    ENTITY = "交付任务"
    OPERATION_TERM = "导出"
    LIST_FIELDS = ["交付编号", "快照编号", "交付格式", "目标系统", "字段范围", "记录条数", "分片大小", "交付时间"]
    REQUIRED_FIELDS = ["交付编号", "快照编号", "交付格式"]
    STATUS_ORDER = ["草稿", "待导出", "导出中", "已交付"]
    ACTION_RULES = {"启动导出": "待导出", "确认交付": "已交付", "标记失败": "已交付"}
    NEGATIVE_ACTIONS = []

    SUPPORTED_FORMATS = {
        "jsonl": {"mime": "application/x-ndjson", "description": "每行一条JSON记录的文本格式"},
        "csv": {"mime": "text/csv", "description": "逗号分隔值表格格式"},
        "parquet": {"mime": "application/octet-stream", "description": "列式存储格式"},
        "xlsx": {"mime": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", "description": "Excel表格格式"},
    }
# 数据操作：导出交付任务数据将版本快照产物分发到目标系统

    def deliver_from_snapshot(self, snapshot_id: str) -> dict[str, Any]:
        """从版本快照导出抽取产物到目标系统，支持分片分批交付。"""
        # 查找快照文件
        version_files = sorted(self._data_dir("versions").glob("*.jsonl"))
        snapshot_data = None
        for vf in version_files:
            data = self._read_jsonl(vf)
            for snap in data:
                if snap.get("快照编号") == snapshot_id:
                    snapshot_data = snap
                    break
            if snapshot_data:
                break

        if not snapshot_data:
            return {"error": f"版本快照 {snapshot_id} 不存在，请先创建版本快照"}

        doc_id = snapshot_data.get("文档编号", "")
        fmt = "jsonl"
        target = "下游知识库"

        # 收集所有可交付的抽取产物
        deliverables = self._collect_deliverables(doc_id)

        if not deliverables:
            return {"error": f"快照 {snapshot_id} 无可交付的抽取产物"}

        shard_size = 500
        shards = []
# 数据产物：交付分片文件写入deliveries目录持久化文件
        batch = []
        for item in deliverables:
            batch.append(item)
            if len(batch) >= shard_size:
                shards.append(batch)
                batch = []
        if batch:
            shards.append(batch)

        # 写入交付产物文件
        output_files = []
        for i, shard in enumerate(shards):
            output_path = self._data_dir("deliveries") / f"{snapshot_id}_shard_{i+1:03d}.jsonl"
            self._write_jsonl(output_path, shard)
            output_files.append(str(output_path))

        delivery = {
            "交付编号": f"DELIVER-{snapshot_id}",
            "快照编号": snapshot_id,
            "交付格式": fmt,
            "目标系统": target,
            "字段范围": f"实体+关系+三元组+向量",
            "记录条数": len(deliverables),
            "分片大小": shard_size,
            "交付时间": time.strftime("%Y-%m-%d %H:%M:%S"),
        }

        self._write_jsonl(
            self._data_dir("deliveries") / f"{snapshot_id}_delivery.jsonl",
            [delivery]
        )

        self._write_checkpoint(f"deliver_{snapshot_id}", {
            "snapshot_id": snapshot_id,
            "format": fmt,
            "target": target,
            "total_records": len(deliverables),
            "shards": len(shards),
        })

        return {
            "snapshot_id": snapshot_id,
            "delivery_id": delivery["交付编号"],
            "format": fmt,
            "target": target,
            "total_records": len(deliverables),
            "shards": len(shards),
            "output_files": output_files,
        }

    def _collect_deliverables(self, doc_id: str) -> list[dict[str, Any]]:
        """收集指定文档的所有可交付抽取产物。"""
        items = []

        # 收集实体
        entity_files = sorted(self._data_dir("entities").glob(f"{doc_id}*.jsonl"))
        for ef in entity_files:
            for e in self._read_jsonl(ef):
                items.append({"type": "entity", "source": str(ef), "data": e})

        # 收集关系
        relation_files = sorted(self._data_dir("relations").glob(f"{doc_id}*.jsonl"))
        for rf in relation_files:
            for r in self._read_jsonl(rf):
                items.append({"type": "relation", "source": str(rf), "data": r})

        # 收集向量：优先按当前向量代次清单取，避免把历史代次文件重复算进交付
        vector_files = sorted(self._data_dir("vectors").glob(f"{doc_id}*_vectors.jsonl"))
        seen_vectors: set[str] = set()
        active_entry = self._active_generation("vectors", f"{doc_id}_parsed")
        active_rows = self._read_generation_manifest(active_entry) if active_entry else None
        if active_rows is not None:
            vector_sources = [active_rows]
        else:
            vector_sources = [self._read_jsonl(vf) for vf in vector_files]
        for rows in vector_sources:
            for v in rows:
                chunk_id = str(v.get("chunk_id", ""))
                if chunk_id and chunk_id in seen_vectors:
                    continue
                seen_vectors.add(chunk_id)
                items.append({"type": "vector", "source": "vectors", "data": {
                    "chunk_id": v.get("chunk_id"),
                    "doc_id": v.get("doc_id"),
                    "page": v.get("page"),
                    "chunk_generation_id": v.get("chunk_generation_id"),
                    "dimension": v.get("vector_dim"),
                    "index": v.get("index"),
                }})

        return items

    def list_formats(self) -> list[dict[str, Any]]:
        """列出所有支持的交付格式。"""
        return [
            {"format": k, "mime": v["mime"], "description": v["description"]}
            for k, v in self.SUPPORTED_FORMATS.items()
        ]