from __future__ import annotations

import json
import posixpath
import shutil
import uuid
from collections.abc import Callable, Iterable
from pathlib import Path
from typing import Any, Protocol

from citations_pipeline.telemetry import ProgressTracker


class BucketPersistenceError(RuntimeError):
    pass


class BucketStore(Protocol):
    def put_file(
        self,
        source: Path,
        destination: Path,
        *,
        progress: Callable[[dict[str, object]], None] | None = None,
        progress_interval: float = 60.0,
    ) -> None: ...

    def put_json(self, payload: dict[str, Any], destination: Path) -> None: ...

    def put_tree(
        self,
        source: Path,
        destination: Path,
        *,
        progress: Callable[[dict[str, object]], None] | None = None,
        progress_interval: float = 60.0,
    ) -> None: ...

    def delete(self, destination: Path) -> None: ...

    def delete_tree(self, destination: Path) -> None: ...


def _json_bytes(payload: dict[str, Any]) -> bytes:
    return (
        json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=False) + "\n"
    ).encode("utf-8")


def _source_size(source: Path | bytes) -> int:
    return len(source) if isinstance(source, bytes) else source.stat().st_size


class HfBucketStore:
    def __init__(
        self,
        *,
        api: Any,
        bucket_id: str,
        mount_root: Path,
        token: str,
    ) -> None:
        self._api = api
        self._bucket_id = bucket_id
        self._mount_root = mount_root
        self._token = token

    def put_file(
        self,
        source: Path,
        destination: Path,
        *,
        progress: Callable[[dict[str, object]], None] | None = None,
        progress_interval: float = 60.0,
    ) -> None:
        size = source.stat().st_size
        with ProgressTracker(
            phase="upload_raw_archive",
            total_bytes=size,
            progress=progress,
            interval=progress_interval,
        ) as tracker:
            self._apply(add=[(source, self._remote_path(destination))])
            tracker.advance(size)

    def put_json(self, payload: dict[str, Any], destination: Path) -> None:
        self._apply(add=[(_json_bytes(payload), self._remote_path(destination))])

    def put_tree(
        self,
        source: Path,
        destination: Path,
        *,
        progress: Callable[[dict[str, object]], None] | None = None,
        progress_interval: float = 60.0,
    ) -> None:
        remote_prefix = self._remote_path(destination).rstrip("/")
        if self._entries_under(remote_prefix):
            raise BucketPersistenceError(
                f"bucket generation already exists: {remote_prefix}"
            )
        additions = [
            (path, f"{remote_prefix}/{path.relative_to(source).as_posix()}")
            for path in sorted(source.rglob("*"))
            if path.is_file()
        ]
        total_size = sum(_source_size(path) for path, _remote in additions)
        with ProgressTracker(
            phase="upload_derived",
            total_bytes=total_size,
            progress=progress,
            interval=progress_interval,
        ) as tracker:
            self._apply(add=additions)
            tracker.advance(total_size)

    def delete(self, destination: Path) -> None:
        self._apply(delete=[self._remote_path(destination)])

    def delete_tree(self, destination: Path) -> None:
        remote_prefix = self._remote_path(destination).rstrip("/")
        files = [
            entry.path
            for entry in self._entries_under(remote_prefix)
            if getattr(entry, "type", None) == "file"
        ]
        self._apply(delete=files)

    def _remote_path(self, destination: Path) -> str:
        try:
            return destination.relative_to(self._mount_root).as_posix()
        except ValueError as error:
            raise BucketPersistenceError(
                f"bucket destination is outside mount root: {destination}"
            ) from error

    def _list(self, prefix: str) -> list[Any]:
        try:
            return list(
                self._api.list_bucket_tree(
                    self._bucket_id,
                    prefix=prefix,
                    recursive=True,
                    token=self._token,
                )
            )
        except Exception as error:
            raise BucketPersistenceError(
                f"bucket listing failed for prefix: {prefix}"
            ) from error

    def _entries_under(self, prefix: str) -> list[Any]:
        boundary = prefix.rstrip("/") + "/"
        return [
            entry
            for entry in self._list(prefix)
            if entry.path == prefix or entry.path.startswith(boundary)
        ]

    def _apply(
        self,
        *,
        add: Iterable[tuple[Path | bytes, str]] = (),
        delete: Iterable[str] = (),
    ) -> None:
        additions = list(add)
        deletions = list(delete)
        try:
            self._api.batch_bucket_files(
                self._bucket_id,
                add=additions,
                delete=deletions,
                token=self._token,
            )
            expected_sizes = {
                destination: _source_size(source)
                for source, destination in additions
            }
            prefixes = sorted({*expected_sizes, *deletions})
            observed: dict[str, int] = {}
            if prefixes:
                common_prefix = posixpath.commonpath(prefixes)
                observed.update(
                    {
                        entry.path: entry.size
                        for entry in self._list(common_prefix)
                        if getattr(entry, "type", None) == "file"
                    }
                )
            for destination, expected_size in expected_sizes.items():
                actual_size = observed.get(destination)
                if actual_size is None:
                    raise BucketPersistenceError(
                        f"bucket object is missing after upload: {destination}"
                    )
                if actual_size != expected_size:
                    raise BucketPersistenceError(
                        "bucket object size mismatch after upload: "
                        f"{destination}: {actual_size} != {expected_size}"
                    )
            retained = sorted(path for path in deletions if path in observed)
            if retained:
                raise BucketPersistenceError(
                    f"bucket objects remain after deletion: {retained}"
                )
        except BucketPersistenceError:
            raise
        except Exception as error:
            raise BucketPersistenceError("bucket persistence operation failed") from error


class LocalBucketStore:
    def __init__(self, *, mount_root: Path) -> None:
        self._mount_root = mount_root

    def put_file(
        self,
        source: Path,
        destination: Path,
        *,
        progress: Callable[[dict[str, object]], None] | None = None,
        progress_interval: float = 60.0,
    ) -> None:
        self._validate_destination(destination)
        destination.parent.mkdir(parents=True, exist_ok=True)
        temporary = destination.with_name(
            f"{destination.name}.{uuid.uuid4().hex}.part"
        )
        size = source.stat().st_size
        try:
            with ProgressTracker(
                phase="upload_raw_archive",
                total_bytes=size,
                progress=progress,
                interval=progress_interval,
            ) as tracker:
                shutil.copy2(source, temporary)
                temporary.replace(destination)
                tracker.advance(size)
        finally:
            temporary.unlink(missing_ok=True)

    def put_json(self, payload: dict[str, Any], destination: Path) -> None:
        self._put_bytes(_json_bytes(payload), destination)

    def put_tree(
        self,
        source: Path,
        destination: Path,
        *,
        progress: Callable[[dict[str, object]], None] | None = None,
        progress_interval: float = 60.0,
    ) -> None:
        self._validate_destination(destination)
        if destination.exists():
            raise BucketPersistenceError(
                f"bucket generation already exists: {destination}"
            )
        destination.parent.mkdir(parents=True, exist_ok=True)
        staged = destination.with_name(
            f"{destination.name}.{uuid.uuid4().hex}.stage"
        )
        total_size = sum(
            path.stat().st_size for path in source.rglob("*") if path.is_file()
        )
        try:
            with ProgressTracker(
                phase="upload_derived",
                total_bytes=total_size,
                progress=progress,
                interval=progress_interval,
            ) as tracker:
                shutil.copytree(source, staged)
                staged.replace(destination)
                tracker.advance(total_size)
        finally:
            if staged.exists():
                shutil.rmtree(staged)

    def delete(self, destination: Path) -> None:
        self._validate_destination(destination)
        destination.unlink(missing_ok=True)

    def delete_tree(self, destination: Path) -> None:
        self._validate_destination(destination)
        if not destination.exists():
            return
        if not destination.is_dir() or destination.is_symlink():
            raise BucketPersistenceError(
                f"bucket tree destination is not a directory: {destination}"
            )
        shutil.rmtree(destination)

    def _put_bytes(self, content: bytes, destination: Path) -> None:
        self._validate_destination(destination)
        destination.parent.mkdir(parents=True, exist_ok=True)
        temporary = destination.with_name(
            f"{destination.name}.{uuid.uuid4().hex}.part"
        )
        try:
            temporary.write_bytes(content)
            temporary.replace(destination)
        finally:
            temporary.unlink(missing_ok=True)

    def _validate_destination(self, destination: Path) -> None:
        try:
            destination.relative_to(self._mount_root)
        except ValueError as error:
            raise BucketPersistenceError(
                f"bucket destination is outside mount root: {destination}"
            ) from error
