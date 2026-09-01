from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest

from citations_pipeline.bucket import (
    BucketPersistenceError,
    HfBucketStore,
    LocalBucketStore,
)


class InMemoryBucketApi:
    def __init__(self) -> None:
        self.files: dict[str, bytes] = {}
        self.reported_size_delta = 0
        self.list_calls: list[str | None] = []
        self.fail_after_additions: int | None = None
        self.list_error: BaseException | None = None

    def batch_bucket_files(
        self,
        _bucket_id: str,
        *,
        add=None,
        delete=None,
        **_kwargs,
    ) -> None:
        for index, (source, destination) in enumerate(add or [], start=1):
            self.files[destination] = (
                source if isinstance(source, bytes) else Path(source).read_bytes()
            )
            if self.fail_after_additions == index:
                raise RuntimeError("partial batch failure")
        for path in delete or []:
            self.files.pop(path, None)

    def list_bucket_tree(
        self,
        _bucket_id: str,
        prefix: str | None = None,
        **_kwargs,
    ):
        self.list_calls.append(prefix)
        if self.list_error is not None:
            raise self.list_error
        for path, content in sorted(self.files.items()):
            if prefix is None or path.startswith(prefix):
                yield SimpleNamespace(
                    type="file",
                    path=path,
                    size=len(content) + self.reported_size_delta,
                    xet_hash="xet-hash",
                    mtime=None,
                    uploaded_at=None,
                )


def test_hf_store_returns_only_after_exact_remote_size_is_visible(tmp_path) -> None:
    api = InMemoryBucketApi()
    source = tmp_path / "archive.bin"
    source.write_bytes(b"verified archive")
    store = HfBucketStore(
        api=api,
        bucket_id="cometadata/shared-storage",
        mount_root=tmp_path / "mount",
        token="hf-test-token",
    )

    store.put_file(source, tmp_path / "mount" / "prefix" / "archive.bin")

    assert api.files == {"prefix/archive.bin": b"verified archive"}


def test_hf_store_reports_upload_start_and_verified_completion(tmp_path) -> None:
    api = InMemoryBucketApi()
    source = tmp_path / "archive.bin"
    source.write_bytes(b"verified archive")
    progress: list[dict[str, object]] = []
    store = HfBucketStore(
        api=api,
        bucket_id="cometadata/shared-storage",
        mount_root=tmp_path / "mount",
        token="hf-test-token",
    )

    store.put_file(
        source,
        tmp_path / "mount" / "prefix" / "archive.bin",
        progress=progress.append,
        progress_interval=0,
    )

    assert [event["bytes_completed"] for event in progress] == [0, 16]
    assert [event["percent"] for event in progress] == [0.0, 100.0]
    assert all(event["phase"] == "upload_raw_archive" for event in progress)


def test_hf_store_rejects_a_remote_size_mismatch(tmp_path) -> None:
    api = InMemoryBucketApi()
    api.reported_size_delta = 1
    source = tmp_path / "archive.bin"
    source.write_bytes(b"verified archive")
    store = HfBucketStore(
        api=api,
        bucket_id="cometadata/shared-storage",
        mount_root=tmp_path / "mount",
        token="hf-test-token",
    )

    with pytest.raises(BucketPersistenceError, match="size mismatch"):
        store.put_file(source, tmp_path / "mount" / "prefix" / "archive.bin")


def test_hf_store_uploads_a_new_tree_without_deletions(tmp_path) -> None:
    api = InMemoryBucketApi()
    api.files = {
        "unmanaged/keep.txt": b"keep",
    }
    source = tmp_path / "derived"
    (source / "data").mkdir(parents=True)
    (source / "data" / "new.parquet").write_bytes(b"new")
    (source / "metadata.json").write_bytes(b"new metadata")
    store = HfBucketStore(
        api=api,
        bucket_id="cometadata/shared-storage",
        mount_root=tmp_path / "mount",
        token="hf-test-token",
    )

    store.put_tree(source, tmp_path / "mount" / "prefix" / "derived")

    assert api.files == {
        "prefix/derived/data/new.parquet": b"new",
        "prefix/derived/metadata.json": b"new metadata",
        "unmanaged/keep.txt": b"keep",
    }
    assert api.list_calls == ["prefix/derived", "prefix/derived"]


def test_hf_store_reports_derived_tree_upload_completion(tmp_path) -> None:
    api = InMemoryBucketApi()
    source = tmp_path / "derived"
    source.mkdir()
    (source / "metadata.json").write_bytes(b"metadata")
    progress: list[dict[str, object]] = []
    store = HfBucketStore(
        api=api,
        bucket_id="cometadata/shared-storage",
        mount_root=tmp_path / "mount",
        token="hf-test-token",
    )

    store.put_tree(
        source,
        tmp_path / "mount" / "prefix" / "derived",
        progress=progress.append,
        progress_interval=0,
    )

    assert [event["bytes_completed"] for event in progress] == [0, 8]
    assert all(event["phase"] == "upload_derived" for event in progress)


def test_partial_tree_upload_cannot_mutate_the_active_generation(
    tmp_path,
) -> None:
    api = InMemoryBucketApi()
    active = "prefix/derived-generations/active/metadata.json"
    api.files[active] = b"active"
    api.fail_after_additions = 1
    source = tmp_path / "derived"
    source.mkdir()
    (source / "metadata.json").write_bytes(b"new")
    (source / "data.parquet").write_bytes(b"rows")
    store = HfBucketStore(
        api=api,
        bucket_id="cometadata/shared-storage",
        mount_root=tmp_path / "mount",
        token="hf-test-token",
    )

    with pytest.raises(BucketPersistenceError, match="persistence operation"):
        store.put_tree(
            source,
            tmp_path
            / "mount"
            / "prefix"
            / "derived-generations"
            / "candidate",
        )

    assert api.files[active] == b"active"


def test_hf_put_tree_refuses_an_existing_generation(tmp_path) -> None:
    api = InMemoryBucketApi()
    api.files[
        "prefix/derived-generations/candidate/orphan"
    ] = b"partial"
    source = tmp_path / "derived"
    source.mkdir()
    (source / "metadata.json").write_bytes(b"new")
    store = HfBucketStore(
        api=api,
        bucket_id="cometadata/shared-storage",
        mount_root=tmp_path / "mount",
        token="hf-test-token",
    )

    with pytest.raises(BucketPersistenceError, match="already exists"):
        store.put_tree(
            source,
            tmp_path
            / "mount"
            / "prefix"
            / "derived-generations"
            / "candidate",
        )


def test_hf_delete_tree_removes_only_the_exact_generation_prefix(
    tmp_path,
) -> None:
    api = InMemoryBucketApi()
    api.files = {
        "prefix/derived-generations/old/data/a.parquet": b"old",
        "prefix/derived-generations/older/data/a.parquet": b"keep",
        "prefix/operator-note.txt": b"keep",
    }
    store = HfBucketStore(
        api=api,
        bucket_id="cometadata/shared-storage",
        mount_root=tmp_path / "mount",
        token="hf-test-token",
    )

    store.delete_tree(
        tmp_path / "mount" / "prefix" / "derived-generations" / "old"
    )

    assert api.files == {
        "prefix/derived-generations/older/data/a.parquet": b"keep",
        "prefix/operator-note.txt": b"keep",
    }


def test_hf_delete_tree_wraps_listing_failures(tmp_path) -> None:
    api = InMemoryBucketApi()
    api.list_error = RuntimeError("listing unavailable")
    store = HfBucketStore(
        api=api,
        bucket_id="cometadata/shared-storage",
        mount_root=tmp_path / "mount",
        token="hf-test-token",
    )

    with pytest.raises(BucketPersistenceError, match="listing failed"):
        store.delete_tree(
            tmp_path / "mount" / "prefix" / "derived-generations" / "old"
        )


def test_local_tree_operations_reject_replacement_and_delete_exactly(
    tmp_path,
) -> None:
    mount = tmp_path / "mount"
    source = tmp_path / "derived"
    source.mkdir()
    (source / "metadata.json").write_bytes(b"new")
    candidate = mount / "prefix" / "derived-generations" / "candidate"
    sibling = mount / "prefix" / "derived-generations" / "candidate-old"
    sibling.mkdir(parents=True)
    (sibling / "keep.txt").write_bytes(b"keep")
    store = LocalBucketStore(mount_root=mount)

    store.put_tree(source, candidate)
    with pytest.raises(BucketPersistenceError, match="already exists"):
        store.put_tree(source, candidate)
    store.delete_tree(candidate)

    assert not candidate.exists()
    assert (sibling / "keep.txt").read_bytes() == b"keep"


def test_local_store_preserves_the_same_observable_contract(tmp_path) -> None:
    mount = tmp_path / "mount"
    source = tmp_path / "archive.bin"
    source.write_bytes(b"verified archive")
    tree = tmp_path / "derived"
    (tree / "data").mkdir(parents=True)
    (tree / "data" / "new.parquet").write_bytes(b"new")
    (tree / "metadata.json").write_text("new\n")
    store = LocalBucketStore(mount_root=mount)

    store.put_file(source, mount / "prefix" / "archive.bin")
    store.put_json({"complete": True}, mount / "prefix" / "marker.json")
    store.put_tree(tree, mount / "prefix" / "derived")

    assert (mount / "prefix" / "archive.bin").read_bytes() == b"verified archive"
    assert (mount / "prefix" / "marker.json").read_text() == (
        '{\n  "complete": true\n}\n'
    )
    assert (mount / "prefix" / "derived" / "metadata.json").read_text() == "new\n"
