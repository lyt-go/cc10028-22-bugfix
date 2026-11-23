"""人工复核服务：低置信抽取结果的人工复核任务、复核结论登记与退回原因追溯。

从抽取质量报告筛选需要人工复核的低置信度条目，创建复核任务并记录结论。
"""
from __future__ import annotations

import time
from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class AnnotReviewService(BaseModuleService):
    MODULE = "annot_review"
    ENTITY = "复核任务"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["复核编号", "分块编号", "复核对象", "原判定", "复核结论", "复核人", "复核时间", "退回原因"]
    REQUIRED_FIELDS = ["复核编号", "分块编号", "复核对象"]
    STATUS_ORDER = ["草稿", "待复核", "复核中", "已结案"]
    ACTION_RULES = {"分派复核": "待复核", "提交结论": "已结案", "标记退回": "已结案"}
    NEGATIVE_ACTIONS = []

    def create_review_tasks(self, batch_key: str, threshold: float = 0.80) -> dict[str, Any]:
        """从低置信度抽取结果中创建人工复核任务。"""
        entity_files = sorted(self._data_dir("entities").glob("*.jsonl"))
        relation_files = sorted(self._data_dir("relations").glob("*.jsonl"))

        tasks = []
# 数据操作：校验复核任务数据创建低置信度条目的复核任务
        task_counter = 0

        # 筛选低置信度实体
        for ef in entity_files:
            entities = self._read_jsonl(ef)
            for e in entities:
                if e.get("confidence", 1.0) < threshold:
                    task_counter += 1
                    tasks.append({
                        "复核编号": f"REV-{task_counter:06d}",
                        "分块编号": e.get("chunk_id", ""),
                        "复核对象": f"实体: {e.get('entity_text')} [{e.get('entity_type')}]",
                        "原判定": f"置信度 {e.get('confidence', 0):.2f}",
                        "复核结论": "",
                        "复核人": "",
                        "复核时间": "",
                        "退回原因": "",
                        "status": "待复核",
                    })

        # 筛选低置信度关系
        for rf in relation_files:
            relations = self._read_jsonl(rf)
            for r in relations:
                if r.get("confidence", 1.0) < threshold:
                    task_counter += 1
                    tasks.append({
                        "复核编号": f"REV-{task_counter:06d}",
                        "分块编号": "",
                        "复核对象": f"关系: {r.get('head_entity')}→{r.get('relation_type')}→{r.get('tail_entity')}",
# 数据产物：复核任务清单写入reviews目录持久化文件
                        "原判定": f"置信度 {r.get('confidence', 0):.2f}",
                        "复核结论": "",
                        "复核人": "",
                        "复核时间": "",
                        "退回原因": "",
                        "status": "待复核",
                    })

        # 写入复核任务清单
        output_path = self._data_dir("reviews") / f"{batch_key}_reviews.jsonl"
        written = self._write_jsonl(output_path, tasks)

        self._write_checkpoint(f"review_{batch_key}", {
            "batch_key": batch_key,
            "threshold": threshold,
            "tasks_created": len(tasks),
            "entities_reviewed": sum(1 for t in tasks if "实体" in t["复核对象"]),
            "relations_reviewed": sum(1 for t in tasks if "关系" in t["复核对象"]),
        })

        return {
            "batch_key": batch_key,
            "threshold": threshold,
            "tasks_created": len(tasks),
            "records_written": written,
            "output": str(output_path),
        }

    def submit_review_conclusion(self, review_id: str, conclusion: str, reviewer: str) -> dict[str, Any]:
        """提交复核结论并更新任务状态。"""
        # 读取复核文件
        review_files = sorted(self._data_dir("reviews").glob("*.jsonl"))
        for rf in review_files:
            tasks = self._read_jsonl(rf)
            for task in tasks:
                if task.get("复核编号") == review_id:
                    task["复核结论"] = conclusion
                    task["复核人"] = reviewer
                    task["复核时间"] = time.strftime("%Y-%m-%d %H:%M:%S")
                    task["status"] = "已结案"
                    # 回写整份复核清单（原子替换，避免把同一批复核任务写成半截）
                    self._write_jsonl(rf, tasks)
                    return {"review_id": review_id, "conclusion": conclusion, "reviewer": reviewer, "status": "已结案"}
        return {"error": f"复核任务 {review_id} 不存在"}