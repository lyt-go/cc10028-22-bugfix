"""切分代次与嵌入一致性测试：重复切分不叠加分块、并发重跑可查明代次、嵌入输入一致。

对应线上问题：
1. 同一份解析页数据执行两次切分会留下同名分块，分块明细数量对不上；
2. 向量嵌入入口读取的索引行多出副本；
3. 有人因切分响应丢失重试同一页组时与正在扫描上一代分块的嵌入并发；
4. 跨进程重启后仍要能读到已确认的页组和索引引用；
5. 追加到一半的新文本不能混进旧向量任务。
"""
import json
import os
import subprocess
import sys
import textwrap
import threading
import uuid

import pytest

BACKEND_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BACKEND_DIR)

from app.services.base import DATA_ROOT
from app.services.chunk_split import ChunkSplitService
from app.services.format_parse import FormatParseService
from app.services.vector_embed import VectorEmbedService


def _uid(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:8].upper()}"


def _prepare_doc() -> str:
    """跑一次格式解析，返回页组编号（解析产物键）。"""
    doc_id = _uid("GEN")
    parse_svc = FormatParseService()
    task, _ = parse_svc.create_entry({
        "任务编号": _uid("PR"), "文档编号": doc_id, "解析器类型": "PDF",
    })
    result = parse_svc.execute_parse(task["id"])
    assert "error" not in result, result
    return f"{doc_id}_parsed"


def _lines(path) -> list[dict]:
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


class TestSplitGenerationPersistence:
    """重复切分只留一份同名分块"""

    def test_repeat_split_is_idempotent(self):
        page_group = _prepare_doc()
        svc = ChunkSplitService()
        # execute_parse 已自动切过一次，这里模拟"响应丢失后重试同一页组"的两次请求
        baseline = svc.split_from_document(page_group)
        retried = svc.split_from_document(page_group)
        assert "error" not in baseline and "error" not in retried

        assert baseline["reused_generation"] is True
        assert retried["reused_generation"] is True
        assert baseline["generation_id"] == retried["generation_id"]
        assert baseline["generation_index"] == retried["generation_index"] == 1
        assert baseline["chunks"] == baseline["unique_chunk_ids"] == retried["chunks"]

        rows = _lines(DATA_ROOT / "chunks" / f"{page_group}_chunks.jsonl")
        ids = [row["chunk_id"] for row in rows]
        assert len(ids) == len(set(ids)) == baseline["chunks"], "同名分块不能出现第二份"

        generations = svc.list_generations(page_group)
        assert len(generations) == 1, "同一输入重试不应新增代次"
        assert generations[0]["records"] == baseline["chunks"]

    def test_first_split_registers_generation(self):
        page_group = _prepare_doc()
        # 只保留解析产物，清掉自动切分留下的代次，模拟首次切分
        for path in (DATA_ROOT / "chunks").glob(f"{page_group}*"):
            path.unlink()
        for path in (DATA_ROOT / "manifests").glob(f"chunks/{page_group}*"):
            path.unlink()
        ledger = DATA_ROOT / "manifests" / "chunks_generations.jsonl"
        if ledger.exists():
            kept = [row for row in _lines(ledger) if row.get("page_group") != page_group]
            ledger.write_text(
                "".join(json.dumps(row, ensure_ascii=False) + "\n" for row in kept),
                encoding="utf-8",
            )

        first = ChunkSplitService().split_from_document(page_group)
        assert "error" not in first
        assert first["reused_generation"] is False
        assert first["generation_index"] == 1

    def test_changed_settings_create_new_generation(self):
        page_group = _prepare_doc()
        svc = ChunkSplitService()
        base = svc.split_from_document(page_group, chunk_size=512)
        finer = svc.split_from_document(page_group, chunk_size=200)

        assert finer["generation_index"] == base["generation_index"] + 1
        assert finer["chunks"] > base["chunks"]

        status = svc.generation_status(page_group)
        assert status["active_index"] == finer["generation_index"]
        assert status["next_index"] == finer["generation_index"] + 1
        assert [entry["index"] for entry in status["consuming"]] == [finer["generation_index"]]

        # 旧代次的成品仍然可读，不会被新活动的产物覆盖
        replay = svc.split_from_document(page_group, chunk_size=512)
        assert replay["generation_id"] == base["generation_id"]
        active_rows = _lines(DATA_ROOT / "chunks" / f"{page_group}_chunks.jsonl")
        assert len(active_rows) == finer["chunks"], "旧代次重试不能把活动产物改回去"


class TestEmbedConsumesGeneration:
    """嵌入按代次清单消费，输入与索引行一一对应"""

    def test_repeat_embed_dedupes_index_rows(self):
        page_group = _prepare_doc()
        ChunkSplitService().split_from_document(page_group)
        vec_svc = VectorEmbedService()

        # 解析触发的自动切分已经顺带嵌入过一次；这里模拟重试入口
        first = vec_svc.embed_from_chunk(page_group)
        second = vec_svc.embed_from_chunk(page_group)
        assert "error" not in first and "error" not in second
        assert first["reused_generation"] is True
        assert second["reused_generation"] is True
        assert first["vector_generation_id"] == second["vector_generation_id"]
        assert first["vector_generation_index"] == second["vector_generation_index"] == 1
        assert first["unique_chunk_ids"] == first["chunks"] == second["chunks"]

        rows = _lines(DATA_ROOT / "vectors" / f"{page_group}_vectors.jsonl")
        ids = [row["chunk_id"] for row in rows]
        assert len(ids) == len(set(ids)) == first["chunks"]

        report = vec_svc.verify_against_chunks(page_group)
        assert report["consistent"] is True
        assert report["duplicate_chunk_ids"] == {}
        assert report["missing_vectors"] == [] and report["orphan_vectors"] == []

    def test_index_records_trace_page_and_generation(self):
        page_group = _prepare_doc()
        split = ChunkSplitService().split_from_document(page_group)
        vec_svc = VectorEmbedService()
        vec_svc.embed_from_chunk(page_group)

        rows = _lines(DATA_ROOT / "vectors" / f"{page_group}_vectors.jsonl")
        assert rows
        chunk_rows = _lines(DATA_ROOT / "chunks" / f"{page_group}_chunks.jsonl")
        page_by_chunk = {row["chunk_id"]: row["page"] for row in chunk_rows}
        for row in rows:
            assert row["page"] == page_by_chunk[row["chunk_id"]]
            assert row["chunk_generation_id"] == split["generation_id"]
            assert row["chunk_generation_index"] == split["generation_index"]
            assert row["chunk_manifest"], "索引记录须能反查分块成品"

    def test_embedding_running_while_split_reruns(self):
        """嵌入扫描上次分块的过程中重试切分，两次结果都不能多出分块或向量。"""
        page_group = _prepare_doc()
        split_svc = ChunkSplitService()
        vec_svc = VectorEmbedService()
        base = split_svc.split_from_document(page_group)

        # 上一代产物的消费快照
        baseline = vec_svc.embed_from_chunk(page_group, generation=base["generation_id"])
        assert "error" not in baseline

        errors: list[str] = []
        results: dict[str, dict] = {}

        def embed_old():
            try:
                results["embed"] = vec_svc.embed_from_chunk(
                    page_group, generation=base["generation_id"]
                )
            except Exception as exc:  # pragma: no cover - 失败即视为回归
                errors.append(str(exc))

        def rerun_split():
            try:
                results["split"] = split_svc.split_from_document(page_group, chunk_size=256)
            except Exception as exc:  # pragma: no cover
                errors.append(str(exc))

        threads = [threading.Thread(target=embed_old), threading.Thread(target=rerun_split)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        assert not errors, errors
        embed_result = results["embed"]
        split_result = results["split"]
        assert embed_result["chunks"] == baseline["chunks"], "旧向量任务不能混入新切分的文本"
        assert embed_result["unique_chunk_ids"] == embed_result["chunks"]
        assert split_result["unique_chunk_ids"] == split_result["chunks"]

        comparison = vec_svc.compare_generations(
            page_group,
            result_generation(results, "embed"),
            result_generation(results, "embed"),
        )
        assert comparison["extra_records"] == 0
        assert comparison["a"]["duplicate_chunk_ids"] == {}

        chunk_comparison = split_svc.compare_generations(
            page_group, base["generation_id"], split_result["generation_id"]
        )
        assert chunk_comparison["a"]["duplicate_chunk_ids"] == {}
        assert chunk_comparison["b"]["duplicate_chunk_ids"] == {}
        assert chunk_comparison["a"]["records"] == chunk_comparison["a"]["unique_records"]
        assert chunk_comparison["b"]["records"] == chunk_comparison["b"]["unique_records"]

        status = split_svc.generation_status(page_group)
        assert status["consuming"], "并发重跑后仍要能查明索引正在消费的页组代次"
        assert status["next_index"] > status["active_index"]
        assert len(status["generations"]) == 2


def result_generation(results: dict, key: str) -> str:
    payload = results[key]
    return payload.get("vector_generation_id") or payload["generation_id"]


class TestCrossProcessDurability:
    """跨进程重启后仍能读到已确认的页组与索引引用"""

    def test_new_process_reads_generation(self):
        page_group = _prepare_doc()
        split = ChunkSplitService().split_from_document(page_group)
        VectorEmbedService().embed_from_chunk(page_group, generation=split["generation_id"])

        script = textwrap.dedent(
            """
            import json, sys
            sys.path.insert(0, sys.argv[1])
            from app.services.chunk_split import ChunkSplitService
            from app.services.vector_embed import VectorEmbedService

            split_svc = ChunkSplitService()
            vec_svc = VectorEmbedService()
            print(json.dumps({
                "chunk_status": split_svc.generation_status(sys.argv[2]),
                "vector_status": vec_svc.generation_status(sys.argv[2])["vector_generations"],
                "verified": vec_svc.verify_against_chunks(sys.argv[2]),
            }, ensure_ascii=False))
            """
        )
        completed = subprocess.run(
            [sys.executable, "-I", "-c", script, BACKEND_DIR, page_group],
            capture_output=True, text=True, check=True,
        )
        payload = json.loads(completed.stdout.strip().splitlines()[-1])

        assert payload["chunk_status"]["active"]["generation_id"] == split["generation_id"]
        assert payload["chunk_status"]["active_index"] == split["generation_index"]
        assert payload["vector_status"]["active"]["meta"]["chunk_generation_id"] == split["generation_id"]
        assert payload["verified"]["consistent"] is True

    def test_partial_text_never_reaches_active_artifact(self):
        """模拟写入中断：遗留半截临时文件时，活动产物与代次清单仍是完整的旧版本。"""
        page_group = _prepare_doc()
        split = ChunkSplitService().split_from_document(page_group)
        active = DATA_ROOT / "chunks" / f"{page_group}_chunks.jsonl"
        before = active.read_text(encoding="utf-8")

        partial = active.with_name(f".{active.name}.99999.deadbeef.tmp")
        partial.write_text('{"chunk_id": "CHUNK-BROKEN-9999", "text": "追加到一半', encoding="utf-8")
        try:
            # 读取方只看活动文件与代次清单，不会碰到临时文件
            rows = _lines(active)
            assert active.read_text(encoding="utf-8") == before
            assert len(rows) == split["chunks"]
            assert all("BROKEN" not in row["chunk_id"] for row in rows)

            vector = VectorEmbedService().embed_from_chunk(page_group)
            assert vector["chunks"] == split["chunks"]

            # 重试时若输入未变，仍是同一代次，不会把半截内容登记成新代次
            replay = ChunkSplitService().split_from_document(page_group)
            assert replay["generation_id"] == split["generation_id"]
            assert len(ChunkSplitService().list_generations(page_group)) == 1
            assert active.read_text(encoding="utf-8") == before
        finally:
            partial.unlink(missing_ok=True)


class TestGenerationEndpoints:
    """HTTP 层可查明代次与索引引用"""

    def test_ops_routes_expose_status(self):
        from app.main import app
        from app.routers import ops

        page_group = _prepare_doc()
        split = ChunkSplitService().split_from_document(page_group)
        VectorEmbedService().embed_from_chunk(page_group)

        paths = app.openapi()["paths"]
        for path in [
            "/api/ops/chunks/{page_group}/generations",
            "/api/ops/chunks/{page_group}/generations/compare",
            "/api/ops/vectors/{chunk_set_id}/generations",
            "/api/ops/vectors/{chunk_set_id}/verify",
        ]:
            assert path in paths, f"缺少代次运维接口 {path}"

        status = ops.chunk_generations(page_group)
        assert status["active"]["generation_id"] == split["generation_id"]
        assert status["next_index"] == split["generation_index"] + 1

        vector_status = ops.vector_generations(page_group)
        assert vector_status["vector_generations"]["active"], "应能查到向量代次"
        assert vector_status["chunk_generations"]["active"]["generation_id"] == split["generation_id"]

        verify = ops.verify_embedding(page_group, vector_ref=None, chunk_ref=None)
        assert verify["consistent"] is True

        compared = ops.compare_chunk_generations(
            page_group, a=str(split["generation_index"]), b=split["generation_id"]
        )
        assert compared["extra_records"] == 0
        assert compared["same_chunk_ids"] is True

        rerun = ops.run_split(page_group, pages=None, strategy="semantic", chunk_size=512, overlap=50)
        assert rerun["generation_id"] == split["generation_id"]
        assert rerun["reused_generation"] is True
