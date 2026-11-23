"""模块服务基类：共享查询、登记、动作、文件IO和跨模块数据流骨架。

为文档解析与知识抽取平台18个领域服务提供统一的CRUD、持久化和
跨模块自动触发能力。各业务模块只需声明字段/状态/动作元数据，
真正的数据流在增强方法中实现。
"""
from __future__ import annotations

import json
import os
import hashlib
import time
import uuid
import importlib
import threading
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

try:  # POSIX 平台用 flock 做跨进程互斥；其他平台退化为进程内互斥。
    import fcntl
except ImportError:  # pragma: no cover - 仅非 POSIX 环境
    fcntl = None  # type: ignore[assignment]

from app.store import store

DATA_ROOT = Path(__file__).resolve().parent.parent.parent.parent / "data"

_THREAD_LOCKS: dict[str, threading.Lock] = {}
_THREAD_LOCKS_GUARD = threading.Lock()


def _thread_lock_for(key: str) -> threading.Lock:
    """同一进程内按路径复用锁，避免两个线程各自 flock 时的竞态。"""
    with _THREAD_LOCKS_GUARD:
        lock = _THREAD_LOCKS.get(key)
        if lock is None:
            lock = threading.Lock()
            _THREAD_LOCKS[key] = lock
        return lock


def _ensure_dir(path: Path) -> None:
    """创建目录（含所有父目录），已有则跳过，确保数据产物写入路径存在。"""
    path.mkdir(parents=True, exist_ok=True)


def _fsync_dir(path: Path) -> None:
    """尽最大努力刷盘目录项，保证 rename 结果在进程重启后仍可读取。"""
    try:
        fd = os.open(str(path), os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(fd)
    except OSError:
        pass
    finally:
        os.close(fd)


class BaseModuleService:
    MODULE = ""
    ENTITY = ""
    LIST_FIELDS: list[str] = []
    REQUIRED_FIELDS: list[str] = []
    STATUS_ORDER: list[str] = []
    ACTION_RULES: dict[str, str] = {}
    NEGATIVE_ACTIONS: list[str] = []

    # -- 基础 CRUD --

    def list_entries(
        self,
        *,
        filters: dict[str, str] | None = None,
        keyword: str | None = None,
        status: str | None = None,
        page: int = 1,
        size: int = 20,
    ) -> tuple[list[dict[str, Any]], int]:
        rows = list(store.rows(self.MODULE))
        for field, expected in (filters or {}).items():
            if field in self.LIST_FIELDS:
                rows = [
                    row for row in rows
                    if expected.casefold() in str(row.get(field, "")).casefold()
                ]
        if keyword and self.LIST_FIELDS:
            field = self.LIST_FIELDS[0]
            rows = [row for row in rows if keyword.casefold() in str(row.get(field, "")).casefold()]
        if status:
            rows = [row for row in rows if row.get("status") == status]
        total = len(rows)
        start = (page - 1) * size
        return rows[start:start + size], total

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        return store.find(self.MODULE, entry_id)

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        missing = [
            field for field in self.REQUIRED_FIELDS
            if not str(values.get(field) or "").strip()
        ]
        if missing:
            return None, missing
        rows = store.rows(self.MODULE)
        entry = {"id": max((int(row.get("id", 0)) for row in rows), default=0) + 1}
        entry.update({field: values.get(field) for field in self.LIST_FIELDS})
        entry["status"] = self.STATUS_ORDER[0]
        entry["pending"] = True
        entry["abnormal"] = False
        rows.append(entry)
        return entry, []

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None, f"{self.ENTITY} {entry_id} 不存在或已归档"
        target = self.ACTION_RULES.get(action)
        if target is None:
            return None, f"动作「{action}」不属于{self.MODULE}可执行范围"
        entry["status"] = target
        entry["pending"] = target != self.STATUS_ORDER[-1]
        entry["abnormal"] = action in self.NEGATIVE_ACTIONS
        return entry, f"{self.ENTITY}已{action}"

    def domain_snapshot(self) -> dict[str, Any]:
        """输出模块级数据质量快照，供跨模块消费和题位源码锚点使用。"""
        rows = store.rows(self.MODULE)
        by_status = {
            status: sum(1 for row in rows if row.get("status") == status)
            for status in self.STATUS_ORDER
# 数据产物：基础工具数据集持久化读写方法写入jsonl文件
        }
        return {
            "module": self.MODULE,
            "entity": self.ENTITY,
            "artifact": f"{self.ENTITY}数据集",
            "count": len(rows),
            "by_status": by_status,
            "abnormal": sum(1 for row in rows if row.get("abnormal")),
        }

    # -- 文件 IO 持久化工具 --

    def _data_dir(self, sub: str) -> Path:
        """获取数据子目录并确保目录存在，用于持久化解析产物、分块、实体等文件。"""
        p = DATA_ROOT / sub
        _ensure_dir(p)
        return p

    def _write_jsonl(self, filepath: Path, records: list[dict[str, Any]]) -> int:
        """把记录整体落盘为一个JSONL文件（临时文件+原子替换），返回写入条数。

        同名产物只保留本次写出的记录，重复执行不会追加出双份行；
        写入过程对同路径加文件锁，读侧只会看到完整的旧版本或完整的新版本，
        不会读到追加到一半的新文本。
        """
        with self._file_lock(filepath):
            return self._replace_jsonl_locked(filepath, records)

    def _replace_jsonl(self, filepath: Path, records: list[dict[str, Any]]) -> int:
        """显式语义的原子覆盖写，供代次产物与清单文件使用。"""
        with self._file_lock(filepath):
            return self._replace_jsonl_locked(filepath, records)

    def _replace_jsonl_locked(self, filepath: Path, records: list[dict[str, Any]]) -> int:
        _ensure_dir(filepath.parent)
        tmp = filepath.with_name(f".{filepath.name}.{os.getpid()}.{uuid.uuid4().hex[:8]}.tmp")
        written = 0
        try:
            with open(tmp, "w", encoding="utf-8") as f:
                for rec in records:
                    f.write(json.dumps(rec, ensure_ascii=False, default=str) + "\n")
                    written += 1
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp, filepath)
            _fsync_dir(filepath.parent)
        finally:
            if tmp.exists():
                try:
                    tmp.unlink()
                except OSError:
                    pass
        return written

    def _append_jsonl(self, filepath: Path, records: list[dict[str, Any]]) -> int:
        """只在真正的追加型日志（如命中日志）上使用；产物文件请改用 _write_jsonl。"""
        with self._file_lock(filepath):
            _ensure_dir(filepath.parent)
            written = 0
            with open(filepath, "a", encoding="utf-8") as f:
                for rec in records:
                    f.write(json.dumps(rec, ensure_ascii=False, default=str) + "\n")
                    written += 1
                f.flush()
                os.fsync(f.fileno())
            return written

    @contextmanager
    def _file_lock(self, target: Path, *, blocking: bool = True) -> Iterator[bool]:
        """对产物路径加跨进程互斥锁，加锁范围是 `<target>.lock` 旁路文件。

        同一进程内先用线程锁串行化，再用 flock 保证多进程（多次启动的服务、
        并发的两个切分/嵌入）不会同时改写同一份产物。返回是否真正拿到锁，
        便于调用方在 `blocking=False` 时做"已被他人持有"的判断。
        """
        lock_path = target.with_name(f".{target.name}.lock")
        _ensure_dir(lock_path.parent)
        thread_lock = _thread_lock_for(str(lock_path))
        if not thread_lock.acquire(blocking=blocking):
            yield False
            return
        handle = None
        flock_held = False
        try:
            handle = open(lock_path, "a+", encoding="utf-8")
            if fcntl is not None:
                flags = fcntl.LOCK_EX if blocking else fcntl.LOCK_EX | fcntl.LOCK_NB
                try:
                    fcntl.flock(handle.fileno(), flags)
                    flock_held = True
                except BlockingIOError:
                    yield False
                    return
            yield True
        finally:
            if handle is not None:
                try:
                    if flock_held and fcntl is not None:
                        fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
                finally:
                    handle.close()
            thread_lock.release()

    def _write_json_atomic(self, filepath: Path, payload: dict[str, Any]) -> None:
        """原子写入单个JSON对象（清单/检查点），避免重启时读到半截内容。"""
        with self._file_lock(filepath):
            _ensure_dir(filepath.parent)
            tmp = filepath.with_name(f".{filepath.name}.{os.getpid()}.{uuid.uuid4().hex[:8]}.tmp")
            try:
                with open(tmp, "w", encoding="utf-8") as f:
                    json.dump(payload, f, ensure_ascii=False, indent=2, default=str)
                    f.flush()
                    os.fsync(f.fileno())
                os.replace(tmp, filepath)
                _fsync_dir(filepath.parent)
            finally:
                if tmp.exists():
                    try:
                        tmp.unlink()
                    except OSError:
                        pass

    def _read_jsonl(self, filepath: Path) -> list[dict[str, Any]]:
        """从JSONL文件读取全部记录，文件不存在返回空列表，脏行自动跳过。"""
        if not filepath.exists():
            return []
        records = []
        with open(filepath, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        records.append(json.loads(line))
                    except json.JSONDecodeError:
                        continue
        return records

    def _write_checkpoint(self, name: str, state: dict[str, Any]) -> Path:
        """写入检查点文件，用于中断恢复与游标推进。检查点记录当前处理偏移和状态。"""
# 数据操作：存储检查点数据管理中断恢复与游标推进
        p = self._data_dir("checkpoints")
        cp_file = p / f"{name}.json"
        state["_updated_at"] = time.time()
        # 检查点会被重启后的进程直接读取，必须整文件原子替换，不能留下半截JSON。
        self._write_json_atomic(cp_file, state)
        return cp_file

    def _read_checkpoint(self, name: str) -> dict[str, Any] | None:
        """读取检查点文件，不存在返回None。用于中断恢复时定位上次处理进度。"""
        cp_file = self._data_dir("checkpoints") / f"{name}.json"
        if not cp_file.exists():
            return None
        try:
            with open(cp_file, "r", encoding="utf-8") as f:
                return json.load(f)
        except (json.JSONDecodeError, OSError):
            return None

    # -- 代次产物与分块清单 --
    #
    # 切分和向量嵌入都按"代次(generation)"组织产物：
    # - 一次切分把本次的全部行原子写入分块产物，并在 manifests/ 下留一份不可变清单；
    # - 清单账本 manifests/<kind>_generations.jsonl 记录每个页组的代次索引，
    #   索引只增不减，因此并发的下一次写入能拿到一个明确且不冲突的索引；
    # - 下游（向量嵌入）消费清单里的分块快照而不是活动文件，跨进程重启后仍读到同一代次。

    GENERATION_LEDGER_DIR = "manifests"

    @staticmethod
    def _canonical_json(payload: Any) -> str:
        return json.dumps(
            payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str
        )

    def _digest_payload(self, payload: Any) -> str:
        """对确定性输入（解析摘要、页、切分设置、分块明细）取摘要，作为代次身份。"""
        return hashlib.sha256(self._canonical_json(payload).encode("utf-8")).hexdigest()

    @staticmethod
    def _safe_name(value: str) -> str:
        return "".join(ch if ch.isalnum() or ch in "._-" else "_" for ch in str(value)) or "unknown"

    def _generation_ledger_path(self, kind: str) -> Path:
        return self._data_dir(self.GENERATION_LEDGER_DIR) / f"{kind}_generations.jsonl"

    def _generation_manifest_path(self, kind: str, generation_id: str) -> Path:
        return self._data_dir(self.GENERATION_LEDGER_DIR) / kind / f"{generation_id}.json"

    def _artifact_ref(self, path: Path) -> str:
        """把产物路径记录成相对 DATA_ROOT 的引用，换机器/重启后仍能定位。"""
        try:
            return path.resolve().relative_to(DATA_ROOT.resolve()).as_posix()
        except ValueError:
            return str(path)

    def _read_generation_ledger(self, kind: str) -> list[dict[str, Any]]:
        ledger = self._read_jsonl(self._generation_ledger_path(kind))
        # 账本按 (page_group, index) 唯一；重复行取最后一条，防止历史追加残留干扰。
        unique: dict[tuple[str, int], dict[str, Any]] = {}
        for entry in ledger:
            try:
                key = (str(entry.get("page_group", "")), int(entry.get("index", 0)))
            except (TypeError, ValueError):
                continue
            unique[key] = entry
        return list(unique.values())

    def _generations_for(self, kind: str, page_group: str) -> list[dict[str, Any]]:
        entries = [
            e for e in self._read_generation_ledger(kind)
            if str(e.get("page_group", "")) == str(page_group)
        ]
        return sorted(entries, key=lambda e: int(e.get("index", 0)))

    def _active_generation(self, kind: str, page_group: str) -> dict[str, Any] | None:
        """当前有效的代次（最新且已完整落盘的清单）。"""
        candidates = [
            e for e in self._generations_for(kind, page_group)
            if e.get("status") == "current" and e.get("complete")
        ]
        if not candidates:
            return None
        return max(candidates, key=lambda e: int(e.get("index", 0)))

    def _generation_by_ref(self, kind: str, page_group: str, ref: str | int) -> dict[str, Any] | None:
        for entry in self._generations_for(kind, page_group):
            if str(entry.get("generation_id")) == str(ref) or str(entry.get("index")) == str(ref):
                return entry
        return None

    def _publish_generation(
        self,
        *,
        kind: str,
        page_group: str,
        digest: str,
        records: list[dict[str, Any]],
        meta: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """登记一代产物：复用同摘要代次，否则追加新索引；返回账本条目。

        调用方负责先把对应产物文件原子落盘。账本写入自身加锁，两个进程并发登记时
        由文件锁串行化，索引在同一页组内严格递增。
        """
        ledger_path = self._generation_ledger_path(kind)
        generation_id = f"{self._safe_name(page_group)}__{digest[:16]}"
        manifest_path = self._generation_manifest_path(kind, generation_id)
        manifest = {
            "kind": kind,
            "page_group": page_group,
            "generation_id": generation_id,
            "digest": digest,
            "records": len(records),
            "chunk_ids": [str(r.get("chunk_id", "")) for r in records],
            "chunks": records,
            "complete": True,
            "written_at": time.time(),
            "meta": meta or {},
        }
        self._write_json_atomic(manifest_path, manifest)

        with self._file_lock(ledger_path):
            entries = self._read_generation_ledger(kind)
            same_group = [e for e in entries if str(e.get("page_group", "")) == str(page_group)]
            existing = next(
                (e for e in same_group if str(e.get("digest")) == digest),
                None,
            )
            if existing is not None:
                index = int(existing.get("index", 1))
                generation_id = str(existing.get("generation_id", generation_id))
            else:
                index = max((int(e.get("index", 0)) for e in same_group), default=0) + 1

            merged: list[dict[str, Any]] = []
            seen_keys: set[tuple[str, int]] = set()
            for entry in entries:
                group = str(entry.get("page_group", ""))
                entry_index = int(entry.get("index", 0))
                key = (group, entry_index)
                if key in seen_keys:
                    continue
                seen_keys.add(key)
                merged.append(entry)

            record = {
                "kind": kind,
                "page_group": page_group,
                "generation_id": generation_id,
                "index": index,
                "digest": digest,
                "manifest": self._artifact_ref(manifest_path),
                "records": len(records),
                "complete": True,
                "status": "current",
                "written_at": time.time(),
                "meta": meta or {},
            }
            merged.append(record)
            normalized = self._normalize_generation_status(merged)
            persisted = next(
                (
                    entry for entry in normalized
                    if str(entry.get("page_group", "")) == str(page_group)
                    and str(entry.get("generation_id", "")) == str(generation_id)
                ),
                record,
            )
            self._replace_jsonl_locked(ledger_path, normalized)
        return persisted

    @staticmethod
    def _normalize_generation_status(entries: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """每个页组只有索引最大的完整代次是 current，其余标为 superseded。

        这样并发重跑里对旧输入的"重试"不会把已确认的新代次顶掉，
        "正在消费的代次"也始终只有一个确定答案。
        """
        latest: dict[str, int] = {}
        for entry in entries:
            if not entry.get("complete"):
                continue
            group = str(entry.get("page_group", ""))
            latest[group] = max(latest.get(group, 0), int(entry.get("index", 0)))
        normalized: list[dict[str, Any]] = []
        for entry in entries:
            group = str(entry.get("page_group", ""))
            current = (
                entry.get("complete")
                and int(entry.get("index", 0)) == latest.get(group)
            )
            normalized.append({**entry, "status": "current" if current else "superseded"})
        return normalized

    def _generation_report(self, kind: str, page_group: str) -> dict[str, Any]:
        """说明一个页组的代次状态：当前索引、下一个可用索引、历史代次清单。"""
        entries = self._generations_for(kind, page_group)
        active = self._active_generation(kind, page_group)
        max_index = max((int(e.get("index", 0)) for e in entries), default=0)
        # 下一次登记一定是新索引；同一输入重放则复用当前代次，不再占号。
        next_index = max_index + 1
        consuming = [
            {
                "generation_id": e.get("generation_id"),
                "index": e.get("index"),
                "digest": e.get("digest"),
                "records": e.get("records"),
            }
            for e in entries if e.get("status") == "current" and e.get("complete")
        ]
        return {
            "kind": kind,
            "page_group": page_group,
            "generations": [
                {
                    "generation_id": e.get("generation_id"),
                    "index": e.get("index"),
                    "digest": e.get("digest"),
                    "records": e.get("records"),
                    "status": e.get("status"),
                    "manifest": e.get("manifest"),
                    "written_at": e.get("written_at"),
                }
                for e in entries
            ],
            "active": active,
            "active_index": None if active is None else active.get("index"),
            "consuming": consuming,
            "last_index": max_index,
            "next_index": next_index,
            "replay_reuses_index": None if active is None else active.get("index"),
        }

    def _read_generation_manifest(self, entry: dict[str, Any]) -> list[dict[str, Any]] | None:
        """读取代次清单中的分块明细；缺失或损坏返回 None。"""
        ref = entry.get("manifest")
        if not ref:
            return None
        path = Path(ref)
        if not path.is_absolute():
            path = DATA_ROOT / path
        if not path.exists():
            return None
        try:
            with open(path, "r", encoding="utf-8") as f:
                manifest = json.load(f)
        except (json.JSONDecodeError, OSError):
            return None
        chunks = manifest.get("chunks")
        return chunks if isinstance(chunks, list) else None

    @staticmethod
    def _dedupe_records(records: list[dict[str, Any]], key: str = "chunk_id") -> list[dict[str, Any]]:
        """按业务编号去重，同一编号只保留首次出现的记录。"""
        unique: dict[str, dict[str, Any]] = {}
        for record in records:
            value = str(record.get(key, ""))
            if value and value not in unique:
                unique[value] = record
        return list(unique.values())

    @staticmethod
    def _duplicate_ids(ids: list[Any]) -> dict[str, int]:
        """返回出现多次的编号及次数，用于识别重复副本。"""
        counts: dict[str, int] = {}
        for item in ids:
            key = str(item)
            counts[key] = counts.get(key, 0) + 1
        return {key: count for key, count in counts.items() if count > 1}

    def _compare_generation_refs(
        self,
        *,
        kind: str,
        page_group: str,
        ref_a: str | int,
        ref_b: str | int,
    ) -> dict[str, Any]:
        """比较两个代次清单的真实记录编号与来源摘要，核对是否多出记录。"""
        entry_a = self._generation_by_ref(kind, page_group, ref_a)
        entry_b = self._generation_by_ref(kind, page_group, ref_b)
        if entry_a is None or entry_b is None:
            return {"error": f"页组 {page_group} 的代次不存在或引用未确认"}

        rows_a = self._read_generation_manifest(entry_a)
        rows_b = self._read_generation_manifest(entry_b)
        if rows_a is None or rows_b is None:
            return {"error": f"页组 {page_group} 的代次清单缺失或损坏"}

        ids_a = [str(r.get("chunk_id", "")) for r in rows_a]
        ids_b = [str(r.get("chunk_id", "")) for r in rows_b]
        only_a = sorted(set(ids_a) - set(ids_b))
        only_b = sorted(set(ids_b) - set(ids_a))
        duplicates_a = self._duplicate_ids(ids_a)
        duplicates_b = self._duplicate_ids(ids_b)

        def _by_page(rows: list[dict[str, Any]]) -> dict[str, int]:
            counts: dict[str, int] = {}
            for row in rows:
                counts[str(row.get("page", 1))] = counts.get(str(row.get("page", 1)), 0) + 1
            return counts

        pages_a = _by_page(rows_a)
        pages_b = _by_page(rows_b)
        page_deltas = {
            page: pages_b.get(page, 0) - pages_a.get(page, 0)
            for page in sorted(set(pages_a) | set(pages_b), key=str)
            if pages_b.get(page, 0) != pages_a.get(page, 0)
        }
        return {
            "kind": kind,
            "page_group": page_group,
            "a": {
                "generation_id": entry_a.get("generation_id"),
                "index": entry_a.get("index"),
                "digest": entry_a.get("digest"),
                "source_digest": (entry_a.get("meta") or {}).get("chunk_generation_id")
                or (entry_a.get("meta") or {}).get("parse_digest")
                or entry_a.get("digest"),
                "records": len(rows_a),
                "unique_records": len(set(ids_a)),
                "records_by_page": pages_a,
                "duplicate_chunk_ids": duplicates_a,
            },
            "b": {
                "generation_id": entry_b.get("generation_id"),
                "index": entry_b.get("index"),
                "digest": entry_b.get("digest"),
                "source_digest": (entry_b.get("meta") or {}).get("chunk_generation_id")
                or (entry_b.get("meta") or {}).get("parse_digest")
                or entry_b.get("digest"),
                "records": len(rows_b),
                "unique_records": len(set(ids_b)),
                "records_by_page": pages_b,
                "duplicate_chunk_ids": duplicates_b,
            },
            "same_digest": entry_a.get("digest") == entry_b.get("digest"),
            "same_chunk_ids": set(ids_a) == set(ids_b),
            "only_in_a": only_a,
            "only_in_b": only_b,
            "page_deltas": page_deltas,
            "no_extra_per_page": not page_deltas,
            "extra_records": (
                len(only_a) + len(only_b)
                + sum(count - 1 for count in duplicates_a.values())
                + sum(count - 1 for count in duplicates_b.values())
            ),
        }

    def _compute_checksum(self, content: str) -> str:
        """计算内容SHA256校验值（取前16位），用于文件完整性比对和版本校验。"""
        return hashlib.sha256(content.encode("utf-8")).hexdigest()[:16]

    def _simulate_doc_content(self, doc_id: str, paragraphs: int = 5) -> list[str]:
        """模拟生成文档段落内容用于测试数据流，每条段落包含企业、人员、时间、地点实体。"""
        lines = []
        for i in range(paragraphs):
            lines.append(f"【第{i+1}段】本文档编号为{doc_id}，包含企业、人员、时间、地点等多类实体信息。")
            lines.append(f"  2024年第三季度，某科技集团与生态合作伙伴签订战略协议，总投资额超过5亿元。")
            lines.append(f"  首席科学家张三博士在会议上指出，知识图谱技术将重塑信息抽取的行业标准。")
            lines.append(f"  Beijing总部及Shanghai分部共500名工程师参与核心系统研发。")
        return lines

    # -- 跨模块自动触发帮助器 --

    @staticmethod
    def _safe_import(module_path: str, class_name: str):
        """延迟导入避免循环依赖，module_path如'app.services.format_parse'。"""
        mod = importlib.import_module(module_path)
        return getattr(mod, class_name)

    def _auto_create_seed(self, module: str, overrides: dict[str, Any]) -> dict[str, Any] | None:
        """跨模块自动创建配套配置条目，默认为'已生效'状态。仅追加不覆盖已有配置。"""
        rows = store.rows(module)
        new_id = max((int(r.get("id", 0)) for r in rows), default=0) + 1
        entry = {"id": new_id}
        for field, default_val in overrides.items():
            entry[field] = default_val
        entry["status"] = "已生效"
        entry["pending"] = False
        entry["abnormal"] = False
        rows.append(entry)
        return entry

# 数据操作：存储跨模块延迟导入工具管理导入数据
    def _has_auto_triggers(self) -> bool:
        """子类覆盖为True以声明支持自动触发下游模块。"""
        return False

    # -- 本域专用跨模块触发方法 --

    def _trigger_format_parse(self, batch_id: str) -> dict[str, Any]:
        """接入批次校验通过后自动触发格式解析任务创建。"""
        try:
            cls = self._safe_import("app.services.format_parse", "FormatParseService")
            return cls().create_from_batch(batch_id)
        except Exception as e:
            return {"auto_trigger": "format_parse", "batch_id": batch_id, "error": str(e)}

    def _trigger_chunk_split(self, doc_id: str) -> dict[str, Any]:
        """解析完成后自动触发文档分块切分，按语义边界将解析产物切分为文本分块。"""
        try:
            cls = self._safe_import("app.services.chunk_split", "ChunkSplitService")
            return cls().split_from_document(doc_id)
        except Exception as e:
            return {"auto_trigger": "chunk_split", "doc_id": doc_id, "error": str(e)}

    def _trigger_entity_recognize(self, chunk_id: str) -> dict[str, Any]:
        """分块完成后自动触发命名实体识别，从分块文本中提取人名、机构、时间、地名等实体。"""
        try:
            cls = self._safe_import("app.services.entity_recognize", "EntityRecognizeService")
            return cls().recognize_from_chunk(chunk_id)
        except Exception as e:
            return {"auto_trigger": "entity_recognize", "chunk_id": chunk_id, "error": str(e)}

    def _trigger_relation_extract(self, entity_set_id: str) -> dict[str, Any]:
        """实体识别完成后自动触发关系抽取，从实体对中抽取语义关系。"""
        try:
            cls = self._safe_import("app.services.relation_extract", "RelationExtractService")
            return cls().extract_from_entities(entity_set_id)
        except Exception as e:
            return {"auto_trigger": "relation_extract", "entity_set_id": entity_set_id, "error": str(e)}

    def _trigger_kg_build(self, relation_set_id: str) -> dict[str, Any]:
        """关系抽取完成后自动触发知识图谱构建，将三元组入图。"""
        try:
            cls = self._safe_import("app.services.kg_build", "KgBuildService")
            return cls().build_from_relations(relation_set_id)
        except Exception as e:
            return {"auto_trigger": "kg_build", "relation_set_id": relation_set_id, "error": str(e)}

    def _trigger_layout_restore(self, doc_id: str) -> dict[str, Any]:
        """解析完成后自动触发版式还原，恢复段落层级和表格图片区域边界。"""
        try:
            cls = self._safe_import("app.services.layout_restore", "LayoutRestoreService")
            return cls().restore_from_document(doc_id)
        except Exception as e:
            return {"auto_trigger": "layout_restore", "doc_id": doc_id, "error": str(e)}

    def _trigger_table_extract(self, layout_id: str) -> dict[str, Any]:
        """版式还原后自动触发表格抽取，提取表格区域的行列结构化数据。"""
        try:
            cls = self._safe_import("app.services.table_extract", "TableExtractService")
            return cls().extract_from_layout(layout_id)
        except Exception as e:
            return {"auto_trigger": "table_extract", "layout_id": layout_id, "error": str(e)}

    def _trigger_ocr(self, image_area_id: str) -> dict[str, Any]:
        """版式还原后自动触发OCR识别，对图片区域进行文字识别。"""
        try:
            cls = self._safe_import("app.services.ocr_engine", "OcrEngineService")
            return cls().recognize_from_area(image_area_id)
        except Exception as e:
            return {"auto_trigger": "ocr_engine", "image_area_id": image_area_id, "error": str(e)}

    def _trigger_vector_embed(
        self, chunk_id: str, *, generation: str | int | None = None
    ) -> dict[str, Any]:
        """分块完成后自动触发向量嵌入，按刚确认的切分代次快照转换成向量索引。"""
        try:
            cls = self._safe_import("app.services.vector_embed", "VectorEmbedService")
            return cls().embed_from_chunk(chunk_id, generation=generation)
        except Exception as e:
            return {"auto_trigger": "vector_embed", "chunk_id": chunk_id, "error": str(e)}

    def _trigger_quality_check(self, batch_id: str) -> dict[str, Any]:
        """解析完成后自动触发抽取质量评估，对抽取结果进行抽样质检。"""
        try:
            cls = self._safe_import("app.services.extract_quality", "ExtractQualityService")
            return cls().check_from_batch(batch_id)
        except Exception as e:
            return {"auto_trigger": "extract_quality", "batch_id": batch_id, "error": str(e)}

    def _trigger_lineage_record(self, parse_task_id: str) -> dict[str, Any]:
        """解析完成后自动触发血缘链路记录，登记源文件到解析产物的全链路血缘。"""
        try:
            cls = self._safe_import("app.services.extract_lineage", "ExtractLineageService")
            return cls().record_from_parse(parse_task_id)
        except Exception as e:
            return {"auto_trigger": "extract_lineage", "parse_task_id": parse_task_id, "error": str(e)}

    def _trigger_result_deliver(self, snapshot_id: str) -> dict[str, Any]:
        """版本快照发布后自动触发结果交付，将抽取结果导出到目标系统。"""
        try:
            cls = self._safe_import("app.services.result_deliver", "ResultDeliverService")
            return cls().deliver_from_snapshot(snapshot_id)
        except Exception as e:
            return {"auto_trigger": "result_deliver", "snapshot_id": snapshot_id, "error": str(e)}