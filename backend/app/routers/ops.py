"""代次运维接口：切分/嵌入代次的运行、消费状态与产物比对。

业务数据仍由通用资源路由承载；这里只暴露代次相关的操作，便于并发重跑后
查明"索引正在消费哪个代次""下一次允许使用哪个索引"，并核对两份产物的分块编号。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Body, Query

from app.services.chunk_split import ChunkSplitService
from app.services.vector_embed import VectorEmbedService

router = APIRouter(prefix="/api/ops", tags=["代次运维"])


@router.get("/generations")
def generation_overview(kind: str = Query(default="chunks", pattern="^(chunks|vectors)$")) -> dict[str, Any]:
    """列出某类产物的全部页组代次状态（当前消费代次、下一次可用索引）。"""
    if kind == "chunks":
        return ChunkSplitService().generation_status()
    return {
        "kind": "vectors",
        "page_groups": {
            group: VectorEmbedService().generation_status(group)["vector_generations"]
            for group in sorted({
                str(entry.get("page_group", ""))
                for entry in VectorEmbedService()._read_generation_ledger("vectors")
            })
        },
    }


@router.get("/chunks/{page_group}/generations")
def chunk_generations(page_group: str) -> dict[str, Any]:
    """查明某页组正在被索引消费的切分代次与下一次允许使用的代次索引。"""
    return ChunkSplitService().generation_status(page_group)


@router.get("/chunks/{page_group}/generations/compare")
def compare_chunk_generations(
    page_group: str,
    a: str = Query(description="代次编号或索引"),
    b: str = Query(description="代次编号或索引"),
) -> dict[str, Any]:
    """比较两个切分代次的实际分块编号与来源摘要，确认同页没有多出分块。"""
    return ChunkSplitService().compare_generations(page_group, a, b)


@router.post("/chunks/{page_group}/split")
def run_split(
    page_group: str,
    pages: list[int] | None = Body(default=None, embed=True),
    strategy: str = Body(default="semantic", embed=True),
    chunk_size: int = Body(default=512, embed=True),
    overlap: int = Body(default=50, embed=True),
) -> dict[str, Any]:
    """对页组执行一次切分（幂等：相同输入复用同一个代次）。"""
    return ChunkSplitService().split_from_document(
        page_group, strategy=strategy, chunk_size=chunk_size, overlap=overlap, pages=pages
    )


@router.get("/vectors/{chunk_set_id}/generations")
def vector_generations(chunk_set_id: str) -> dict[str, Any]:
    """查明某页组的切分代次与向量代次，以及下一次可用索引。"""
    return VectorEmbedService().generation_status(chunk_set_id)


@router.post("/vectors/{chunk_set_id}/embed")
def run_embed(
    chunk_set_id: str,
    generation: str | int | None = Body(default=None, embed=True),
    model: str = Body(default="text2vec-large", embed=True),
) -> dict[str, Any]:
    """按切分代次清单执行嵌入；generation 为空时消费当前确认代次。"""
    return VectorEmbedService().embed_from_chunk(chunk_set_id, model=model, generation=generation)


@router.get("/vectors/{chunk_set_id}/verify")
def verify_embedding(
    chunk_set_id: str,
    vector_ref: str | None = Query(default=None),
    chunk_ref: str | None = Query(default=None),
) -> dict[str, Any]:
    """按切分代次产物核对向量分块明细，确认一一对应且没有重复。"""
    return VectorEmbedService().verify_against_chunks(
        chunk_set_id, vector_ref=vector_ref, chunk_ref=chunk_ref
    )


@router.get("/vectors/{chunk_set_id}/generations/compare")
def compare_vector_generations(
    chunk_set_id: str,
    a: str = Query(description="代次编号或索引"),
    b: str = Query(description="代次编号或索引"),
) -> dict[str, Any]:
    """比较两个向量代次的真实分块编号与来源摘要，检验同页没有生成额外向量。"""
    return VectorEmbedService().compare_generations(chunk_set_id, a, b)
