"""敏感词过滤服务：违禁词条的匹配模式、替换策略与规则级别命中统计。

在分块文本中扫描命中词条并执行替换/脱敏策略，用于知识抽取产物的安全合规过滤。
"""
from __future__ import annotations

import time
import re
from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class SensitiveFilterService(BaseModuleService):
    MODULE = "sensitive_filter"
    ENTITY = "过滤规则"
    OPERATION_TERM = "过滤"
    LIST_FIELDS = ["规则编号", "匹配模式", "命中词条", "替换策略", "规则级别", "命中次数", "生效范围", "规则版本"]
# 数据操作：清洗过滤文本数据执行敏感词脱敏处理
    REQUIRED_FIELDS = ["规则编号", "匹配模式", "命中词条"]
    STATUS_ORDER = ["草稿", "待生效", "已生效", "已停用"]
    ACTION_RULES = {"提交生效": "待生效", "确认生效": "已生效", "标记停用": "已停用"}
    NEGATIVE_ACTIONS = ["标记停用"]

    FILTER_PRESETS = [
        {"keywords": ["身份证", "手机号", "银行卡"], "pattern": r"(\d{11,19})", "strategy": "mask", "level": "high"},
        {"keywords": ["邮箱", "电话"], "pattern": r"([\w\.-]+@[\w\.-]+\.\w+)", "strategy": "replace", "level": "medium"},
        {"keywords": ["地址"], "pattern": r"(\w+市\w+区\w+路\d+号)", "strategy": "mask", "level": "high"},
        {"keywords": ["密码", "密钥"], "pattern": r"(password|secret|token)[:=]\s*\S+", "strategy": "remove", "level": "critical"},
    ]

    def apply_to_text(self, text: str, scope: str = "all") -> dict[str, Any]:
        """对文本应用敏感词过滤规则，返回过滤后文本和命中统计。"""
        # 始终加载默认规则作为基础
        active_rules = self._default_rules()
        # 追加存储中的已生效规则
        store_rules = [r for r in store.rows(self.MODULE) if r.get("status") == "已生效"]
        for sr in store_rules:
            # 避免重复规则
# 数据产物：过滤后文本数据集脱敏后数据写入持久化文件
            if not any(r.get("规则编号") == sr.get("规则编号") for r in active_rules):
                active_rules.append(sr)

        hits = []
        filtered_text = text
        for rule in active_rules:
            pattern = rule.get("匹配模式", "")
            if not pattern:
                continue
            try:
                for match in re.finditer(pattern, filtered_text, re.IGNORECASE):
                    keyword = rule.get("命中词条", "未知")
                    strategy = rule.get("替换策略", "mask")
                    hits.append({
                        "rule_id": rule.get("规则编号", ""),
                        "keyword": keyword,
                        "strategy": strategy,
                        "level": rule.get("规则级别", "medium"),
                        "match_text": match.group(0),
                        "position": match.span(),
                    })
                    # 执行替换策略
                    if strategy == "mask":
                        masked = match.group(0)[:2] + "***" + match.group(0)[-2:]
                        filtered_text = filtered_text[:match.start()] + masked + filtered_text[match.end():]
                    elif strategy == "remove":
                        filtered_text = filtered_text[:match.start()] + "[已脱敏]" + filtered_text[match.end():]
                    elif strategy == "replace":
                        filtered_text = filtered_text[:match.start()] + "[已替换]" + filtered_text[match.end():]

                    # 更新规则的命中次数
                    rule["命中次数"] = str(int(rule.get("命中次数", 0)) + 1)
            except re.error:
                continue

        # 写入过滤日志（追加型审计日志，保留每次命中历史）
        if hits:
            self._append_jsonl(
                self._data_dir("filters") / "hit_log.jsonl",
                [{"text_hash": self._compute_checksum(text), "hit_count": len(hits), "at": time.time()}]
            )

        return {
            "original_length": len(text),
            "filtered_length": len(filtered_text),
            "hit_count": len(hits),
            "hits": hits,
            "filtered_text": filtered_text,
            "critical_hits": sum(1 for h in hits if h["level"] == "critical"),
        }

    def filter_chunks(self, chunk_set_id: str) -> dict[str, Any]:
        """对分块集中所有文本分块执行敏感词过滤。"""
        chunk_file = self._data_dir("chunks") / f"{chunk_set_id}_chunks.jsonl"
        chunks = self._read_jsonl(chunk_file)
        if not chunks:
            return {"error": f"分块产物 {chunk_set_id} 不存在"}

        results = []
        total_hits = 0
        for chunk in chunks:
            text = chunk.get("text", "")
            if text:
                result = self.apply_to_text(text, scope="chunk")
                total_hits += result["hit_count"]
                results.append({
                    "chunk_id": chunk.get("chunk_id"),
                    "hit_count": result["hit_count"],
                    "filtered": result["hit_count"] > 0,
                })

        output_path = self._data_dir("filters") / f"{chunk_set_id}_filtered.jsonl"
        written = self._write_jsonl(output_path, results)

        self._write_checkpoint(f"filter_{chunk_set_id}", {
            "chunk_set_id": chunk_set_id,
            "total_chunks": len(chunks),
            "total_hits": total_hits,
            "chunks_affected": sum(1 for r in results if r["filtered"]),
        })

        return {
            "chunk_set_id": chunk_set_id,
            "chunks": len(chunks),
            "total_hits": total_hits,
            "records_written": written,
            "output": str(output_path),
        }

    def _default_rules(self) -> list[dict[str, Any]]:
        """生成默认过滤规则集。"""
        rules = []
        for i, preset in enumerate(self.FILTER_PRESETS):
            rules.append({
                "规则编号": f"AUTO-RULE-{i+1:03d}",
                "匹配模式": preset["pattern"],
                "命中词条": ", ".join(preset["keywords"]),
                "替换策略": preset["strategy"],
                "规则级别": preset["level"],
                "命中次数": "0",
                "生效范围": "all",
                "规则版本": "v1.0",
                "status": "已生效",
            })
        return rules