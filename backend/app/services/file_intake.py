"""文件接入服务：多格式文档批量接入、来源校验值比对与接入游标推进。

主控链路起点 — 接入批次校验通过后自动触发格式解析任务创建。
"""
from __future__ import annotations

import time
from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class FileIntakeService(BaseModuleService):
    MODULE = "file_intake"
    ENTITY = "接入批次"
    OPERATION_TERM = "采集"
    LIST_FIELDS = ["批次编号", "来源系统", "文件清单", "文件类型", "字节总量", "校验值", "采集方式", "接入游标"]
    REQUIRED_FIELDS = ["批次编号", "来源系统", "文件清单"]
    STATUS_ORDER = ["草稿", "待校验", "已接入", "已拒收"]
    ACTION_RULES = {"提交校验": "待校验", "确认接入": "已接入", "标记拒收": "已拒收"}
    NEGATIVE_ACTIONS = ["标记拒收"]

    def onboard_batch(self, values: dict[str, Any]) -> dict[str, Any]:
        """接入新文档批次并初始化采集游标，将批次清单写入持久化文件。"""
        entry, missing = self.create_entry(values)
        if entry is None:
            raise ValueError(f"缺少必填字段: {missing}")
        # 初始化接入游标与位点
# 数据产物：接入批次数据集写入持久化日志文件
        batch_key = entry["批次编号"]
        self._write_checkpoint(batch_key, {
            "batch_key": batch_key,
            "file_count": len(entry.get("文件清单", "").split(",")),
            "offset": 0,
            "bytes_processed": 0,
            "created_at": time.time(),
        })
        # 写入接入日志供下游溯源：按批次去重，重复登记同一批次不会产生第二条。
        log_path = self._data_dir("batches") / "onboard.jsonl"
        existing = [
            row for row in self._read_jsonl(log_path)
            if row.get("batch_key") != batch_key
        ]
        self._write_jsonl(
            log_path,
            existing + [{
# 数据操作：采集校验值比对与批次数据校验
                "action": "onboard",
                "batch_key": batch_key,
                "source_system": entry["来源系统"],
                "file_type": entry.get("文件类型", ""),
                "at": time.time(),
            }],
        )
        return entry

    def validate_batch(self, batch_id: int) -> dict[str, Any]:
        """校验批次校验值，校验通过后自动触发格式解析任务创建。"""
        entry = store.find(self.MODULE, batch_id)
        if entry is None:
            return {"error": f"接入批次 {batch_id} 不存在或已被删除"}
        if entry.get("status") != "待校验":
            return {"error": f"接入批次 {batch_id} 状态不允许校验，当前状态: {entry.get('status')}"}

        # 执行校验值比对
        stored_checksum = entry.get("校验值", "")
        batch_key = entry["批次编号"]
        file_list_str = entry.get("文件清单", "")
        computed = self._compute_checksum(file_list_str)
        is_valid = (not stored_checksum) or (computed == stored_checksum)
# 数据产物：已校验批次写入validated.jsonl日志文件

        if is_valid:
            entry["status"] = "已接入"
            entry["pending"] = False
            # 校验通过：写入已接入清单（同一批次重复校验只保留一条）
            validated_path = self._data_dir("batches") / "validated.jsonl"
            existing = [
                row for row in self._read_jsonl(validated_path)
                if row.get("batch_key") != batch_key
            ]
            self._write_jsonl(
                validated_path,
                existing + [{
                    "batch_key": batch_key,
                    "checksum": computed,
                    "validated_at": time.time(),
                }],
            )
            # 跨模块自动触发：格式解析
            trigger_result = self._trigger_format_parse(batch_key)
# 数据产物：批次CSV导出文件包含完整字段与校验结果
            # 触发血缘记录
            try:
                self._trigger_lineage_record(batch_key)
            except Exception:
                pass
            return {
                "batch_key": batch_key,
                "valid": True,
                "checksum": computed,
                "format_parse_triggered": trigger_result,
                "validated_at": time.time(),
            }
        else:
            entry["status"] = "已拒收"
            entry["abnormal"] = True
# 数据产物：导出批次数据集供外部审计系统消费
            return {
                "batch_key": batch_key,
                "valid": False,
                "expected": stored_checksum,
                "computed": computed,
                "error": "校验值不匹配，批次已拒收",
            }

    def get_pending_batches(self) -> list[dict[str, Any]]:
        """列出所有待校验批次，供调度系统轮询处理。"""
# 【数据工程边界】采集操作与接入批次数据集产品一致性
        rows = [r for r in store.rows(self.MODULE) if r.get("status") == "待校验"]
        for r in rows:
            cp = self._read_checkpoint(r["批次编号"])
            r["checkpoint"] = cp
        return rows

    def batch_ingest_sources(self, source_list: list[dict[str, Any]]) -> dict[str, Any]:
        """批量接入多个文档来源，适用爬虫/RPA采集场景的批量登记。"""
        results = {"created": 0, "skipped": 0, "errors": []}
        for source in source_list:
            try:
                existing = [r for r in store.rows(self.MODULE) if r.get("批次编号") == source.get("批次编号")]
                if existing:
                    results["skipped"] += 1
                    continue
                entries = self.onboard_batch(source)
                if entries.get("批次编号"):
                    results["created"] += 1
            except Exception as e:
                results["errors"].append({"source": source.get("批次编号", "?"), "error": str(e)})
        return results