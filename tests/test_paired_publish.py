"""Paired publication: prepared datasets, guarded Hub commits, recovery, markers.

Every test runs against `FakeHubApi`, an in-memory Hub that stores files per
revision, so tree listings, LFS metadata, downloads, and parent-guarded
commits behave like the real service without any network access.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable
from dataclasses import asdict, replace
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from huggingface_hub import CommitOperationAdd, CommitOperationDelete

from citations_pipeline import paths
from citations_pipeline.datacite_publish import (
    ARXIV_CONFIGS,
    DATACITE_CONFIGS,
    DatasetTransform,
)
from citations_pipeline.full_state import (
    FULL_PIPELINE_KIND,
    FullPipelineMarker,
    PublicationReceipt,
    RunIdentity,
)
from citations_pipeline.paired_publish import (
    CARD_PATH,
    MANIFEST_KIND,
    MANIFEST_PATH,
    MANIFEST_VERSION,
    PreparedDataset,
    canonical_manifest_bytes,
    prepare_dataset,
    publish_pair,
    publish_prepared,
    render_datacite_card,
    validate_manifest,
    verify_current,
    verify_manifest_files,
)
from citations_pipeline.publish import ConfigStats, PublishError, render_dataset_card
from citations_pipeline.state import PipelineMarker, StateError
from citations_pipeline.telemetry import EventLogger

DATACITE_REPO = "cometadata/crossref-datacite-citations"
ARXIV_REPO = "cometadata/crossref-arxiv-citations"
TOKEN = "hf_secret_token"
IDENTITY = RunIdentity(
    "a" * 64, "2026-07", "b" * 64, "c" * 64, "d" * 64,
    "datacite", 2, 1, 2, "fnv1a64-v1", 256, 2, 1,
)
STATS = {
    "all": ConfigStats(works=923302, citations=5121642, references=5200000),
    "asserted": ConfigStats(works=110030, citations=308474, references=310000),
    "mined": ConfigStats(works=891035, citations=4813168, references=4890000),
}
DATACITE_FILES = {
    "data/all/part-00000.parquet": b"all-shard-0",
    "data/all/part-00001.parquet": b"all-shard-1",
    "data/asserted/part-00000.parquet": b"asserted-shard-0",
    "data/mined/part-00000.parquet": b"mined-shard-0",
    "data/ambiguous_urls/part-00000.parquet": b"ambiguous-shard-0",
}
ARXIV_FILES = {
    "data/all.parquet": b"arxiv-all",
    "data/asserted.parquet": b"arxiv-asserted",
    "data/mined.parquet": b"arxiv-mined",
}


def sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


# --- the verbatim orchestration test -----------------------------------------


def test_retry_publishes_only_the_missing_destination(tmp_path):
    prepared = {
        "datacite": PreparedDataset("cometadata/crossref-datacite-citations", {}, "a"*64),
        "arxiv": PreparedDataset("cometadata/crossref-arxiv-citations", {}, "b"*64),
    }
    calls, receipts = [], {}
    fail_arxiv = True

    def commit_one(dataset):
        calls.append(dataset.repo_id)
        if dataset.repo_id.endswith("arxiv-citations") and fail_arxiv:
            raise RuntimeError("injected publication failure")
        return PublicationReceipt(dataset.repo_id, "c"*40,
                                  "https://huggingface.co/commit/c", dataset.artifact_digest)

    def save(kind, receipt):
        receipts[kind] = receipt

    with pytest.raises(RuntimeError, match="injected"):
        publish_pair(prepared, receipts, commit_one, save, lambda dataset, receipt: None)
    assert set(receipts) == {"datacite"}
    fail_arxiv = False
    result = publish_pair(prepared, receipts, commit_one, save, lambda dataset, receipt: None)
    assert set(result) == {"datacite", "arxiv"}
    assert calls.count(prepared["datacite"].repo_id) == 1
    assert calls.count(prepared["arxiv"].repo_id) == 2


# --- fixtures and helpers ----------------------------------------------------


def make_transform(
    root: Path, kind: str, *, contents: dict[str, bytes] | None = None
) -> DatasetTransform:
    contents = dict(contents or (DATACITE_FILES if kind == "datacite" else ARXIV_FILES))
    files: dict[str, Path] = {}
    for name, data in contents.items():
        path = root / kind / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)
        files[name] = path
    if kind == "datacite":
        config_files = {
            config: tuple(name for name in contents if name.startswith(f"data/{config}/"))
            for config in DATACITE_CONFIGS
        }
    else:
        config_files = {config: (f"data/{config}.parquet",) for config in ARXIV_CONFIGS}
    return DatasetTransform(
        files=files,
        config_files=config_files,
        stats=dict(STATS),
        reference_keys={},
        dropped_reference_keys=(),
        ambiguity_rows=3 if kind == "datacite" else 0,
        sizes={name: len(data) for name, data in contents.items()},
    )


def prepare(
    tmp_path: Path,
    kind: str,
    *,
    extractor_version: str = "2.1.0",
    contents: dict[str, bytes] | None = None,
    label: str = "prepared",
) -> PreparedDataset:
    transform = make_transform(tmp_path / "transform" / label, kind, contents=contents)
    return prepare_dataset(
        kind, transform, IDENTITY, "2026-07", extractor_version, tmp_path / label / kind
    )


def local_manifest(prepared: PreparedDataset) -> dict[str, Any]:
    return json.loads(prepared.files[MANIFEST_PATH].read_bytes())


def collect_events() -> tuple[EventLogger, list[str]]:
    lines: list[str] = []
    return EventLogger(output=lines.append), lines


class FakeHubApi:
    """In-memory Hugging Face Hub with per-revision file storage.

    `lfs` decides which paths report LFS metadata (size + sha256) in tree
    listings; other files expose only a Git blob id, which is a SHA-1 and must
    never be compared with a SHA-256. `before_commit` and `before_tree` hooks
    let tests inject concurrent changes at precise moments.
    """

    def __init__(self) -> None:
        self.revisions: dict[str, dict[str, dict[str, bytes]]] = {}
        self.heads: dict[str, str] = {}
        self.history: dict[str, list[str]] = {}
        self.create_repo_calls: list[dict[str, Any]] = []
        self.commit_calls: list[dict[str, Any]] = []
        self.downloads: list[tuple[str, str, str]] = []
        self.tree_calls: list[tuple[str, str]] = []
        self.tokens: set[Any] = set()
        self.lfs: Callable[[str], bool] = lambda path: path.endswith(".parquet")
        self.before_commit: Callable[[str], None] | None = None
        self.before_tree: Callable[[str, str], None] | None = None
        self.fail_commit_for: set[str] = set()

    # -- test helpers ------------------------------------------------------

    def seed(self, repo_id: str, files: dict[str, bytes] | None = None) -> str:
        assert repo_id not in self.heads
        self.revisions[repo_id] = {}
        self.history[repo_id] = []
        return self._store(repo_id, {".gitattributes": b"*.parquet filter=lfs\n", **(files or {})})

    def external_commit(
        self, repo_id: str, adds: dict[str, bytes] | None = None, deletes: tuple[str, ...] = ()
    ) -> str:
        files = dict(self.files_at(repo_id))
        for path in deletes:
            del files[path]
        files.update(adds or {})
        return self._store(repo_id, files)

    def files_at(self, repo_id: str, revision: str | None = None) -> dict[str, bytes]:
        revision = self.heads[repo_id] if revision is None else revision
        try:
            return self.revisions[repo_id][revision]
        except KeyError as error:
            raise RuntimeError(f"unknown revision {revision} of {repo_id}") from error

    def commits_to(self, repo_id: str) -> list[dict[str, Any]]:
        return [call for call in self.commit_calls if call["repo_id"] == repo_id]

    def _store(self, repo_id: str, files: dict[str, bytes]) -> str:
        seed = f"{repo_id}:{len(self.history[repo_id])}:{sorted(files.items())!r}"
        sha = hashlib.sha1(seed.encode()).hexdigest()
        self.revisions[repo_id][sha] = dict(files)
        self.history[repo_id].append(sha)
        self.heads[repo_id] = sha
        return sha

    # -- Hub API -----------------------------------------------------------

    def create_repo(self, repo_id, *, repo_type, private, exist_ok, token):
        self.tokens.add(token)
        self.create_repo_calls.append(
            {"repo_id": repo_id, "repo_type": repo_type, "private": private, "exist_ok": exist_ok}
        )
        assert repo_type == "dataset"
        if repo_id in self.heads:
            if not exist_ok:
                raise RuntimeError(f"repository {repo_id} already exists")
            return SimpleNamespace(repo_id=repo_id)
        self.seed(repo_id)
        return SimpleNamespace(repo_id=repo_id)

    def dataset_info(self, repo_id, revision=None, token=None):
        self.tokens.add(token)
        if repo_id not in self.heads:
            raise RuntimeError(f"repository {repo_id} not found")
        if revision is None:
            return SimpleNamespace(sha=self.heads[repo_id])
        self.files_at(repo_id, revision)
        return SimpleNamespace(sha=revision)

    def create_commit(self, *, repo_id, repo_type, operations, commit_message, parent_commit, token):
        self.tokens.add(token)
        assert repo_type == "dataset"
        if self.before_commit is not None:
            self.before_commit(repo_id)
        record = {
            "repo_id": repo_id,
            "operations": list(operations),
            "commit_message": commit_message,
            "parent_commit": parent_commit,
        }
        if repo_id in self.fail_commit_for:
            self.fail_commit_for.discard(repo_id)
            raise RuntimeError("injected commit failure")
        if parent_commit != self.heads[repo_id]:
            raise RuntimeError(
                f"412 parent commit {parent_commit} is not the head of {repo_id}"
            )
        files = dict(self.files_at(repo_id))
        for operation in record["operations"]:
            if isinstance(operation, CommitOperationDelete):
                if operation.path_in_repo not in files:
                    raise RuntimeError(f"cannot delete missing {operation.path_in_repo}")
                del files[operation.path_in_repo]
            elif isinstance(operation, CommitOperationAdd):
                source = operation.path_or_fileobj
                files[operation.path_in_repo] = (
                    source if isinstance(source, bytes) else Path(source).read_bytes()
                )
            else:
                raise RuntimeError(f"unsupported operation {operation!r}")
        sha = self._store(repo_id, files)
        record["oid"] = sha
        self.commit_calls.append(record)
        return SimpleNamespace(
            oid=sha, commit_url=f"https://huggingface.co/datasets/{repo_id}/commit/{sha}"
        )

    def list_repo_tree(self, repo_id, *, revision, repo_type, recursive, token):
        self.tokens.add(token)
        assert repo_type == "dataset" and recursive
        if self.before_tree is not None:
            self.before_tree(repo_id, revision)
        self.tree_calls.append((repo_id, revision))
        files = self.files_at(repo_id, revision)
        folders = sorted({path.rsplit("/", 1)[0] for path in files if "/" in path})
        for folder in folders:
            yield SimpleNamespace(path=folder, tree_id="f" * 40)
        for path, data in sorted(files.items()):
            # A Git blob id is a SHA-1; make it look plausible but unrelated.
            blob_id = hashlib.sha1(b"blob " + data).hexdigest()
            lfs = (
                SimpleNamespace(size=len(data), sha256=sha256(data), pointer_size=120)
                if self.lfs(path)
                else None
            )
            yield SimpleNamespace(path=path, size=len(data), blob_id=blob_id, lfs=lfs)

    def download(self, repo_id, filename, *, repo_type, revision, token, cache_dir):
        self.tokens.add(token)
        assert repo_type == "dataset"
        self.downloads.append((repo_id, filename, revision))
        files = self.files_at(repo_id, revision)
        if filename not in files:
            raise RuntimeError(f"{filename} not found in {repo_id} at {revision}")
        target = Path(cache_dir) / revision / filename
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(files[filename])
        return str(target)


def hub_commit_one(api: FakeHubApi, events: EventLogger | None = None):
    events = events or collect_events()[0]

    def commit_one(dataset: PreparedDataset) -> PublicationReceipt:
        return publish_prepared(
            api=api, token=TOKEN, prepared=dataset, events=events, download=api.download
        )

    return commit_one


def hub_check_current(api: FakeHubApi):
    def check_current(dataset: PreparedDataset, receipt: PublicationReceipt) -> None:
        verify_current(
            api=api, token=TOKEN, prepared=dataset, receipt=receipt, download=api.download
        )

    return check_current


def run_and_activate(
    marker: Path,
    prepared: dict[str, PreparedDataset],
    receipts: dict[str, PublicationReceipt],
    commit_one,
    save,
    check_current,
) -> dict[str, PublicationReceipt]:
    """The caller's contract: the pipeline marker is written only after
    `publish_pair` returns."""
    result = publish_pair(prepared, receipts, commit_one, save, check_current)
    marker.write_text("activated")
    return result


def managed(files: dict[str, bytes]) -> set[str]:
    return {
        path
        for path in files
        if path in (CARD_PATH, MANIFEST_PATH) or path.startswith("data/")
    }


# --- publish_pair orchestration ----------------------------------------------


def test_publish_pair_rechecks_existing_receipts_and_never_recommits() -> None:
    prepared = {
        "datacite": PreparedDataset(DATACITE_REPO, {}, "a" * 64),
        "arxiv": PreparedDataset(ARXIV_REPO, {}, "b" * 64),
    }
    receipts = {
        kind: PublicationReceipt(item.repo_id, "c" * 40, "u", item.artifact_digest)
        for kind, item in prepared.items()
    }
    checks: list[tuple[str, str]] = []

    def check(dataset, receipt):
        checks.append((dataset.repo_id, receipt.commit_sha))

    def commit_one(dataset):
        raise AssertionError("nothing should be committed")

    saved: dict[str, PublicationReceipt] = {}
    result = publish_pair(prepared, receipts, commit_one, saved.__setitem__, check)
    assert result == receipts
    assert saved == {}
    # Both destinations are checked before the loop and again before returning.
    assert checks == [
        (DATACITE_REPO, "c" * 40),
        (ARXIV_REPO, "c" * 40),
        (DATACITE_REPO, "c" * 40),
        (ARXIV_REPO, "c" * 40),
    ]


def test_first_commit_failure_saves_nothing_and_writes_no_marker(tmp_path: Path) -> None:
    prepared = {
        "datacite": PreparedDataset(DATACITE_REPO, {}, "a" * 64),
        "arxiv": PreparedDataset(ARXIV_REPO, {}, "b" * 64),
    }
    calls: list[str] = []
    receipts: dict[str, PublicationReceipt] = {}

    def commit_one(dataset):
        calls.append(dataset.repo_id)
        raise RuntimeError("injected datacite failure")

    marker = tmp_path / "_PIPELINE_COMPLETE.json"
    with pytest.raises(RuntimeError, match="datacite failure"):
        run_and_activate(
            marker, prepared, receipts, commit_one, receipts.__setitem__, lambda d, r: None
        )
    assert calls == [DATACITE_REPO]
    assert receipts == {}
    assert not marker.exists()


def test_last_verification_failure_keeps_receipts_but_writes_no_marker(
    tmp_path: Path,
) -> None:
    prepared = {
        "datacite": PreparedDataset(DATACITE_REPO, {}, "a" * 64),
        "arxiv": PreparedDataset(ARXIV_REPO, {}, "b" * 64),
    }
    receipts: dict[str, PublicationReceipt] = {}
    checks: list[str] = []

    def commit_one(dataset):
        return PublicationReceipt(dataset.repo_id, "c" * 40, "u", dataset.artifact_digest)

    def check(dataset, receipt):
        checks.append(dataset.repo_id)
        if dataset.repo_id == ARXIV_REPO:
            raise PublishError("concurrent change in arxiv during the final pass")

    marker = tmp_path / "_PIPELINE_COMPLETE.json"
    with pytest.raises(PublishError, match="final pass"):
        run_and_activate(marker, prepared, receipts, commit_one, receipts.__setitem__, check)
    # Both commits happened and were journaled; only the final check failed.
    assert set(receipts) == {"datacite", "arxiv"}
    assert checks == [DATACITE_REPO, ARXIV_REPO]
    assert not marker.exists()


# --- prepare_dataset ---------------------------------------------------------


def test_prepare_dataset_writes_canonical_manifest_and_digest(tmp_path: Path) -> None:
    prepared = prepare(tmp_path, "datacite")
    assert prepared.repo_id == DATACITE_REPO
    assert set(prepared.files) == set(DATACITE_FILES) | {CARD_PATH, MANIFEST_PATH}
    payload = prepared.files[MANIFEST_PATH].read_bytes()
    manifest = json.loads(payload)
    assert payload == canonical_manifest_bytes(manifest)
    assert not payload.endswith(b"\n")
    assert prepared.artifact_digest == sha256(payload)
    assert manifest["kind"] == MANIFEST_KIND == "citation-dataset"
    assert manifest["manifest_version"] == MANIFEST_VERSION == 2
    assert manifest["dataset_kind"] == "datacite"
    assert manifest["publication_schema_version"] == paths.DATACITE_PUBLISH_SCHEMA_VERSION
    assert manifest["identity"] == IDENTITY.to_dict()
    assert manifest["snapshot_id"] == "2026-07"
    assert manifest["extractor_version"] == "2.1.0"
    assert manifest["config_files"] == {
        "all": ["data/all/part-00000.parquet", "data/all/part-00001.parquet"],
        "asserted": ["data/asserted/part-00000.parquet"],
        "mined": ["data/mined/part-00000.parquet"],
        "ambiguous_urls": ["data/ambiguous_urls/part-00000.parquet"],
    }
    assert manifest["counts"] == {name: asdict(stats) for name, stats in STATS.items()}
    assert manifest["ambiguity_rows"] == 3
    # The manifest lists every data file and the card, never itself.
    assert set(manifest["files"]) == set(DATACITE_FILES) | {CARD_PATH}
    for name, data in DATACITE_FILES.items():
        assert manifest["files"][name] == {"size": len(data), "sha256": sha256(data)}
    card = prepared.files[CARD_PATH].read_bytes()
    assert manifest["files"][CARD_PATH] == {"size": len(card), "sha256": sha256(card)}
    assert validate_manifest(manifest).keys() == manifest["files"].keys()


def test_prepare_dataset_is_deterministic_and_sensitive_to_inputs(tmp_path: Path) -> None:
    first = prepare(tmp_path, "datacite", label="first")
    second = prepare(tmp_path, "datacite", label="second")
    assert first.files[MANIFEST_PATH].read_bytes() == second.files[MANIFEST_PATH].read_bytes()
    assert first.artifact_digest == second.artifact_digest
    other_version = prepare(tmp_path, "datacite", extractor_version="2.2.0", label="third")
    assert other_version.artifact_digest != first.artifact_digest
    other_data = prepare(
        tmp_path,
        "datacite",
        contents={**DATACITE_FILES, "data/mined/part-00000.parquet": b"mined-changed"},
        label="fourth",
    )
    assert other_data.artifact_digest != first.artifact_digest


def test_prepare_arxiv_dataset_uses_legacy_paths_and_card(tmp_path: Path) -> None:
    prepared = prepare(tmp_path, "arxiv")
    assert prepared.repo_id == ARXIV_REPO
    assert set(prepared.files) == {
        "data/all.parquet",
        "data/asserted.parquet",
        "data/mined.parquet",
        CARD_PATH,
        MANIFEST_PATH,
    }
    manifest = local_manifest(prepared)
    assert manifest["dataset_kind"] == "arxiv"
    assert manifest["publication_schema_version"] == paths.ARXIV_PUBLISH_SCHEMA_VERSION == 1
    assert manifest["ambiguity_rows"] == 0
    assert manifest["config_files"] == {
        "all": ["data/all.parquet"],
        "asserted": ["data/asserted.parquet"],
        "mined": ["data/mined.parquet"],
    }
    card = prepared.files[CARD_PATH].read_text(encoding="utf-8")
    assert card == render_dataset_card(
        STATS, snapshot_id="2026-07", datacite_release="2026-07", extractor_version="2.1.0"
    )
    assert "path: data/all.parquet" in card and "ambiguous_urls" not in card


@pytest.mark.parametrize(
    ("kind", "mutate", "message"),
    [
        ("smoke", lambda t: t, "dataset kind"),
        (
            "datacite",
            lambda t: replace(t, config_files={**t.config_files, "mined": ()}),
            "mined",
        ),
        (
            "datacite",
            lambda t: replace(t, config_files={k: v for k, v in t.config_files.items() if k != "ambiguous_urls"}),
            "ambiguous_urls",
        ),
        (
            "datacite",
            lambda t: replace(t, config_files={**t.config_files, "all": t.config_files["all"][:1]}),
            "part-00001",
        ),
        (
            "datacite",
            lambda t: replace(t, config_files={**t.config_files, "all": ("data/all/nested/part-00000.parquet",)}),
            "nested",
        ),
        (
            "datacite",
            lambda t: replace(t, stats={k: v for k, v in t.stats.items() if k != "asserted"}),
            "asserted",
        ),
        ("datacite", lambda t: replace(t, ambiguity_rows=-1), "ambiguity_rows"),
        ("arxiv", lambda t: replace(t, ambiguity_rows=2), "ambiguity_rows"),
        (
            "arxiv",
            lambda t: replace(t, sizes={**t.sizes, "data/all.parquet": 1}),
            "data/all.parquet",
        ),
    ],
)
def test_prepare_dataset_rejects_inconsistent_transforms(
    tmp_path: Path, kind: str, mutate, message: str
) -> None:
    base_kind = "datacite" if kind == "smoke" else kind
    transform = mutate(make_transform(tmp_path / "transform", base_kind))
    with pytest.raises(PublishError, match=message):
        prepare_dataset(kind, transform, IDENTITY, "2026-07", "2.1.0", tmp_path / "out")


def test_prepare_dataset_requires_files_on_disk(tmp_path: Path) -> None:
    transform = make_transform(tmp_path / "transform", "datacite")
    transform.files["data/mined/part-00000.parquet"].unlink()
    with pytest.raises(PublishError, match="data/mined/part-00000.parquet"):
        prepare_dataset(
            "datacite", transform, IDENTITY, "2026-07", "2.1.0", tmp_path / "out"
        )


# --- dataset cards -----------------------------------------------------------


def test_datacite_card_declares_configs_license_counts_and_policy() -> None:
    card = render_datacite_card(
        STATS,
        3,
        snapshot_id="2026-07",
        datacite_release="2026-06",
        identity_fingerprint=IDENTITY.fingerprint(),
        extractor_version="2.1.0",
    )
    assert card.startswith("---\nlicense: cc0-1.0\n")
    header = card.split("---\n")[1]
    for config in ("all", "asserted", "mined", "ambiguous_urls"):
        assert f"config_name: {config}\n" in header
        assert f"path: data/{config}/*.parquet\n" in header
    assert header.count("config_name:") == 4
    assert "default: true" in header
    assert "size_categories:\n  - 100K<n<1M" in header
    assert "923,302" in card and "5,121,642" in card and "5,200,000" in card
    assert "110,030" in card and "891,035" in card
    assert "holds 3 unresolved" in card
    assert "`2026-07`" in card and "`2026-06`" in card
    assert IDENTITY.fingerprint() in card
    assert "2.1.0" in card
    assert "match_type" in card and "candidate_dois" in card and "citing_doi" in card
    assert "CC0 1.0 Universal" in card
    lower = card.lower()
    assert "ambigu" in lower and "landing" in lower and "historical" in lower
    assert "cometadata/crossref-datacite-citations" in card
    assert "crossref-arxiv-citations" not in card
    # `mined` is every match without a recognized DOI assertion, from any
    # searchable field, not only unstructured text.
    assert "unstructured reference text" not in card
    mined_row = next(line for line in card.splitlines() if line.startswith("| `mined` |"))
    assert "unstructured" not in mined_row
    mined = next(line for line in card.splitlines() if line.startswith("- `mined`"))
    for field in ("`DOI`", "`URL`", "`article-title`", "`journal-title`", "`unstructured`"):
        assert field in mined
    assert "doi-asserted-by" in mined
    policy = card.split("### Provenance and Ambiguity Policy")[1].split("###")[0]
    assert "structured `DOI` field" in " ".join(policy.split())


def test_datacite_card_size_category_follows_counts() -> None:
    small = {name: ConfigStats(works=12, citations=20, references=25) for name in STATS}
    card = render_datacite_card(
        small,
        0,
        snapshot_id="2026-07",
        datacite_release="2026-07",
        identity_fingerprint="0" * 64,
        extractor_version="2.1.0",
    )
    assert "size_categories:\n  - n<1K" in card
    assert "holds 0 unresolved" in card


# --- publish_prepared against the fake Hub -----------------------------------


def test_publish_prepared_creates_repo_and_commits_once_with_parent_guard(
    tmp_path: Path,
) -> None:
    api = FakeHubApi()
    events, lines = collect_events()
    prepared = prepare(tmp_path, "datacite")
    receipt = publish_prepared(
        api=api, token=TOKEN, prepared=prepared, events=events, download=api.download
    )
    assert api.create_repo_calls == [
        {"repo_id": DATACITE_REPO, "repo_type": "dataset", "private": False, "exist_ok": True}
    ]
    initial = api.history[DATACITE_REPO][0]
    [commit] = api.commit_calls
    assert commit["parent_commit"] == initial
    assert commit["repo_id"] == DATACITE_REPO
    assert sorted(op.path_in_repo for op in commit["operations"]) == sorted(prepared.files)
    assert all(isinstance(op, CommitOperationAdd) for op in commit["operations"])
    assert "2026-07" in commit["commit_message"]
    assert prepared.artifact_digest in commit["commit_message"]
    head = api.heads[DATACITE_REPO]
    assert receipt == PublicationReceipt(
        DATACITE_REPO,
        head,
        f"https://huggingface.co/datasets/{DATACITE_REPO}/commit/{head}",
        prepared.artifact_digest,
    )
    published = api.files_at(DATACITE_REPO)
    assert managed(published) == set(prepared.files)
    assert published[".gitattributes"] == b"*.parquet filter=lfs\n"
    for name, path in prepared.files.items():
        assert published[name] == path.read_bytes()
    # Verification trusts LFS metadata for parquet shards and hashes the Git
    # blobs (card, manifest) after downloading them at immutable revisions.
    downloaded = {name for _repo, name, _rev in api.downloads}
    assert downloaded == {CARD_PATH, MANIFEST_PATH}
    assert all(rev == head for _repo, _name, rev in api.downloads)
    assert api.tokens == {TOKEN}
    assert TOKEN not in "\n".join(lines)
    assert any('"event": "dataset_committed"' in line for line in lines)
    assert any('"event": "publication_verified"' in line for line in lines)


def test_publish_prepared_deletes_stale_managed_shards_in_the_same_commit(
    tmp_path: Path,
) -> None:
    api = FakeHubApi()
    api.seed(
        DATACITE_REPO,
        {
            "data/all/part-00007.parquet": b"old-shard",
            "data/all/part-00000.parquet": b"replaced",
            "data/legacy.parquet": b"old-layout",
            "docs/notes.md": b"unrelated",
            "README.md": b"old card",
        },
    )
    prepared = prepare(tmp_path, "datacite")
    publish_prepared(
        api=api, token=TOKEN, prepared=prepared, events=collect_events()[0], download=api.download
    )
    [commit] = api.commit_calls
    deletes = sorted(
        op.path_in_repo for op in commit["operations"] if isinstance(op, CommitOperationDelete)
    )
    assert deletes == ["data/all/part-00007.parquet", "data/legacy.parquet"]
    adds = sorted(
        op.path_in_repo for op in commit["operations"] if isinstance(op, CommitOperationAdd)
    )
    assert adds == sorted(prepared.files)
    published = api.files_at(DATACITE_REPO)
    assert managed(published) == set(prepared.files)
    assert published["docs/notes.md"] == b"unrelated"
    assert published["README.md"] == prepared.files[CARD_PATH].read_bytes()


def test_arxiv_publication_keeps_legacy_paths(tmp_path: Path) -> None:
    api = FakeHubApi()
    api.seed(
        ARXIV_REPO,
        {
            "README.md": b"legacy card",
            "data/all.parquet": b"legacy all",
            "data/asserted.parquet": b"legacy asserted",
            "data/mined.parquet": b"legacy mined",
        },
    )
    prepared = prepare(tmp_path, "arxiv")
    receipt = publish_prepared(
        api=api, token=TOKEN, prepared=prepared, events=collect_events()[0], download=api.download
    )
    [commit] = api.commit_calls
    assert not any(isinstance(op, CommitOperationDelete) for op in commit["operations"])
    published = api.files_at(ARXIV_REPO)
    assert managed(published) == {
        "README.md",
        "data/all.parquet",
        "data/asserted.parquet",
        "data/mined.parquet",
        MANIFEST_PATH,
    }
    assert published["data/all.parquet"] == b"arxiv-all"
    assert receipt.repo_id == ARXIV_REPO
    verify_current(
        api=api, token=TOKEN, prepared=prepared, receipt=receipt, download=api.download
    )


def test_parent_conflict_fails_without_overwriting(tmp_path: Path) -> None:
    api = FakeHubApi()
    api.seed(DATACITE_REPO)
    prepared = prepare(tmp_path, "datacite")

    def race(repo_id: str) -> None:
        api.before_commit = None
        api.external_commit(repo_id, {"data/all/part-00000.parquet": b"someone else"})

    api.before_commit = race
    with pytest.raises(PublishError, match=DATACITE_REPO) as info:
        publish_prepared(
            api=api, token=TOKEN, prepared=prepared, events=collect_events()[0], download=api.download
        )
    assert "parent" in str(info.value)
    assert api.commit_calls == []
    assert api.files_at(DATACITE_REPO)["data/all/part-00000.parquet"] == b"someone else"
    assert MANIFEST_PATH not in api.files_at(DATACITE_REPO)


def test_commit_failure_names_repo_and_leaves_repo_unchanged(tmp_path: Path) -> None:
    api = FakeHubApi()
    api.seed(DATACITE_REPO, {"README.md": b"before"})
    before = api.heads[DATACITE_REPO]
    api.fail_commit_for.add(DATACITE_REPO)
    prepared = prepare(tmp_path, "datacite")
    with pytest.raises(PublishError, match="injected commit failure") as info:
        publish_prepared(
            api=api, token=TOKEN, prepared=prepared, events=collect_events()[0], download=api.download
        )
    assert DATACITE_REPO in str(info.value)
    assert api.heads[DATACITE_REPO] == before
    assert api.commit_calls == []


def test_lost_receipt_is_recovered_by_manifest_digest_without_a_new_commit(
    tmp_path: Path,
) -> None:
    api = FakeHubApi()
    prepared = prepare(tmp_path, "datacite")
    first = publish_prepared(
        api=api, token=TOKEN, prepared=prepared, events=collect_events()[0], download=api.download
    )
    events, lines = collect_events()
    again = publish_prepared(
        api=api, token=TOKEN, prepared=prepared, events=events, download=api.download
    )
    assert again == first
    assert len(api.commit_calls) == 1
    assert any('"event": "publication_recovered"' in line for line in lines)
    # A superseding publication (different artifact) still commits normally.
    other = prepare(tmp_path, "datacite", extractor_version="2.2.0", label="other")
    third = publish_prepared(
        api=api, token=TOKEN, prepared=other, events=collect_events()[0], download=api.download
    )
    assert len(api.commit_calls) == 2
    assert api.commit_calls[1]["parent_commit"] == first.commit_sha
    assert third.commit_sha == api.heads[DATACITE_REPO] != first.commit_sha
    assert third.artifact_digest == other.artifact_digest


def test_recovery_verifies_the_remote_files_not_just_the_manifest(tmp_path: Path) -> None:
    api = FakeHubApi()
    prepared = prepare(tmp_path, "datacite")
    publish_prepared(
        api=api, token=TOKEN, prepared=prepared, events=collect_events()[0], download=api.download
    )
    api.external_commit(DATACITE_REPO, deletes=("data/mined/part-00000.parquet",))
    with pytest.raises(PublishError, match="data/mined/part-00000.parquet"):
        publish_prepared(
            api=api, token=TOKEN, prepared=prepared, events=collect_events()[0], download=api.download
        )
    assert len(api.commit_calls) == 1


def test_receipt_journal_failure_is_recoverable_through_the_remote_manifest(
    tmp_path: Path,
) -> None:
    api = FakeHubApi()
    prepared = {kind: prepare(tmp_path, kind) for kind in ("datacite", "arxiv")}
    journal: dict[str, PublicationReceipt] = {}

    def failing_save(kind, receipt):
        raise OSError("journal disk full")

    marker = tmp_path / "_PIPELINE_COMPLETE.json"
    with pytest.raises(OSError, match="journal"):
        run_and_activate(
            marker, prepared, dict(journal), hub_commit_one(api), failing_save, hub_check_current(api)
        )
    assert journal == {}
    assert not marker.exists()
    assert len(api.commits_to(DATACITE_REPO)) == 1
    assert api.commits_to(ARXIV_REPO) == []

    result = run_and_activate(
        marker, prepared, dict(journal), hub_commit_one(api), journal.__setitem__, hub_check_current(api)
    )
    assert marker.exists()
    assert set(result) == set(journal) == {"datacite", "arxiv"}
    # DataCite was recovered from its committed manifest; only arXiv was committed.
    assert len(api.commits_to(DATACITE_REPO)) == 1
    assert len(api.commits_to(ARXIV_REPO)) == 1
    assert result["datacite"].commit_sha == api.history[DATACITE_REPO][-1]
    assert result["arxiv"].commit_sha == api.heads[ARXIV_REPO]


def test_second_commit_failure_then_retry_publishes_only_arxiv(tmp_path: Path) -> None:
    api = FakeHubApi()
    prepared = {kind: prepare(tmp_path, kind) for kind in ("datacite", "arxiv")}
    journal: dict[str, PublicationReceipt] = {}
    api.fail_commit_for.add(ARXIV_REPO)
    marker = tmp_path / "_PIPELINE_COMPLETE.json"
    with pytest.raises(PublishError, match=ARXIV_REPO):
        run_and_activate(
            marker, prepared, dict(journal), hub_commit_one(api), journal.__setitem__, hub_check_current(api)
        )
    assert set(journal) == {"datacite"}
    assert not marker.exists()
    assert managed(api.files_at(DATACITE_REPO)) == set(prepared["datacite"].files)
    # The arXiv repository was created idempotently but nothing was published.
    assert [call["repo_id"] for call in api.create_repo_calls] == [DATACITE_REPO, ARXIV_REPO]
    assert managed(api.files_at(ARXIV_REPO)) == set()

    result = run_and_activate(
        marker, prepared, dict(journal), hub_commit_one(api), journal.__setitem__, hub_check_current(api)
    )
    assert marker.exists()
    assert len(api.commits_to(DATACITE_REPO)) == 1
    assert len(api.commits_to(ARXIV_REPO)) == 1
    assert result["datacite"] == journal["datacite"]
    assert managed(api.files_at(ARXIV_REPO)) == set(prepared["arxiv"].files)


def test_no_marker_when_current_files_change_between_runs(tmp_path: Path) -> None:
    api = FakeHubApi()
    prepared = {kind: prepare(tmp_path, kind) for kind in ("datacite", "arxiv")}
    journal: dict[str, PublicationReceipt] = {}
    marker = tmp_path / "_PIPELINE_COMPLETE.json"
    run_and_activate(
        marker, prepared, dict(journal), hub_commit_one(api), journal.__setitem__, hub_check_current(api)
    )
    marker.unlink()
    api.external_commit(ARXIV_REPO, {"data/all.parquet": b"tampered!"})
    with pytest.raises(PublishError, match="data/all.parquet"):
        run_and_activate(
            marker, prepared, dict(journal), hub_commit_one(api), journal.__setitem__, hub_check_current(api)
        )
    assert not marker.exists()
    assert len(api.commits_to(ARXIV_REPO)) == 1


# --- verify_current ----------------------------------------------------------


def publish_once(api: FakeHubApi, tmp_path: Path, kind: str = "datacite"):
    prepared = prepare(tmp_path, kind)
    receipt = publish_prepared(
        api=api, token=TOKEN, prepared=prepared, events=collect_events()[0], download=api.download
    )
    return prepared, receipt


def test_verify_current_accepts_the_published_state(tmp_path: Path) -> None:
    api = FakeHubApi()
    prepared, receipt = publish_once(api, tmp_path)
    api.downloads.clear()
    verify_current(
        api=api, token=TOKEN, prepared=prepared, receipt=receipt, download=api.download
    )
    assert {name for _repo, name, _rev in api.downloads} == {CARD_PATH, MANIFEST_PATH}


def test_changed_current_files_after_successful_commit_are_a_concurrent_change(
    tmp_path: Path,
) -> None:
    api = FakeHubApi()
    prepared, receipt = publish_once(api, tmp_path)
    # A later publication of a different artifact replaces the manifest.
    other = prepare(tmp_path, "datacite", extractor_version="9.9.9", label="other")
    publish_prepared(
        api=api, token=TOKEN, prepared=other, events=collect_events()[0], download=api.download
    )
    with pytest.raises(PublishError, match="concurrent change") as info:
        verify_current(
            api=api, token=TOKEN, prepared=prepared, receipt=receipt, download=api.download
        )
    assert DATACITE_REPO in str(info.value)
    # The historical commit still exists but must not be treated as current.
    manifest = local_manifest(prepared)
    verify_manifest_files(
        api=api,
        token=TOKEN,
        repo_id=DATACITE_REPO,
        revision=receipt.commit_sha,
        manifest=manifest,
        download=api.download,
    )


def test_extra_and_missing_managed_shards_at_head_are_detected(tmp_path: Path) -> None:
    api = FakeHubApi()
    prepared, receipt = publish_once(api, tmp_path)
    api.external_commit(DATACITE_REPO, {"data/all/part-00002.parquet": b"stray"})
    with pytest.raises(PublishError, match="data/all/part-00002.parquet"):
        verify_current(
            api=api, token=TOKEN, prepared=prepared, receipt=receipt, download=api.download
        )
    api.external_commit(
        DATACITE_REPO,
        deletes=("data/all/part-00002.parquet", "data/asserted/part-00000.parquet"),
    )
    with pytest.raises(PublishError, match="data/asserted/part-00000.parquet"):
        verify_current(
            api=api, token=TOKEN, prepared=prepared, receipt=receipt, download=api.download
        )
    api.external_commit(DATACITE_REPO, deletes=(MANIFEST_PATH,))
    with pytest.raises(PublishError, match=MANIFEST_PATH):
        verify_current(
            api=api, token=TOKEN, prepared=prepared, receipt=receipt, download=api.download
        )


def test_same_size_remote_mutation_is_detected(tmp_path: Path) -> None:
    api = FakeHubApi()
    prepared, receipt = publish_once(api, tmp_path)
    original = api.files_at(DATACITE_REPO)["data/all/part-00000.parquet"]
    mutated = bytes(reversed(original))
    assert len(mutated) == len(original) and mutated != original
    api.external_commit(DATACITE_REPO, {"data/all/part-00000.parquet": mutated})
    api.downloads.clear()
    with pytest.raises(PublishError, match="data/all/part-00000.parquet"):
        verify_current(
            api=api, token=TOKEN, prepared=prepared, receipt=receipt, download=api.download
        )
    # The LFS digest exposed the change; the shard itself was never downloaded.
    assert not any(name.endswith(".parquet") for _repo, name, _rev in api.downloads)

    api.external_commit(DATACITE_REPO, {"data/all/part-00000.parquet": original})
    card = api.files_at(DATACITE_REPO)[CARD_PATH]
    same_size_card = card[:-1] + (b"X" if card[-1:] != b"X" else b"Y")
    api.external_commit(DATACITE_REPO, {CARD_PATH: same_size_card})
    with pytest.raises(PublishError, match="README.md"):
        verify_current(
            api=api, token=TOKEN, prepared=prepared, receipt=receipt, download=api.download
        )


def test_git_blobs_are_downloaded_and_hashed_never_compared_by_blob_id(
    tmp_path: Path,
) -> None:
    api = FakeHubApi()
    api.lfs = lambda path: False  # nothing reports LFS metadata
    prepared, receipt = publish_once(api, tmp_path)
    downloaded = {name for _repo, name, _rev in api.downloads}
    assert downloaded == set(prepared.files)
    api.downloads.clear()
    verify_current(
        api=api, token=TOKEN, prepared=prepared, receipt=receipt, download=api.download
    )
    assert {name for _repo, name, _rev in api.downloads} == set(prepared.files)
    original = api.files_at(DATACITE_REPO)["data/mined/part-00000.parquet"]
    api.external_commit(
        DATACITE_REPO, {"data/mined/part-00000.parquet": bytes(reversed(original))}
    )
    with pytest.raises(PublishError, match="data/mined/part-00000.parquet"):
        verify_current(
            api=api, token=TOKEN, prepared=prepared, receipt=receipt, download=api.download
        )


def test_lfs_metadata_size_mismatch_is_detected(tmp_path: Path) -> None:
    api = FakeHubApi()
    prepared, receipt = publish_once(api, tmp_path)
    api.external_commit(DATACITE_REPO, {"data/all/part-00001.parquet": b"longer than before"})
    with pytest.raises(PublishError, match="data/all/part-00001.parquet"):
        verify_current(
            api=api, token=TOKEN, prepared=prepared, receipt=receipt, download=api.download
        )


def test_verify_current_rejects_receipts_with_the_wrong_identity(tmp_path: Path) -> None:
    api = FakeHubApi()
    prepared, receipt = publish_once(api, tmp_path)
    hub_calls = (len(api.tree_calls), len(api.downloads))
    with pytest.raises(PublishError, match=ARXIV_REPO):
        verify_current(
            api=api,
            token=TOKEN,
            prepared=prepared,
            receipt=replace(receipt, repo_id=ARXIV_REPO),
            download=api.download,
        )
    with pytest.raises(PublishError, match="artifact"):
        verify_current(
            api=api,
            token=TOKEN,
            prepared=prepared,
            receipt=replace(receipt, artifact_digest="f" * 64),
            download=api.download,
        )
    # Identity mismatches are rejected before any Hub call.
    assert (len(api.tree_calls), len(api.downloads)) == hub_calls


def test_verify_current_detects_head_moving_during_verification(tmp_path: Path) -> None:
    api = FakeHubApi()
    prepared, receipt = publish_once(api, tmp_path)

    def move_head(repo_id: str, revision: str) -> None:
        api.before_tree = None
        api.external_commit(repo_id, {"docs/notes.md": b"added while verifying"})

    api.before_tree = move_head
    with pytest.raises(PublishError, match="HEAD moved"):
        verify_current(
            api=api, token=TOKEN, prepared=prepared, receipt=receipt, download=api.download
        )


def test_unrelated_commit_after_publication_keeps_the_dataset_current(
    tmp_path: Path,
) -> None:
    api = FakeHubApi()
    prepared, receipt = publish_once(api, tmp_path)
    api.external_commit(DATACITE_REPO, {"docs/notes.md": b"unrelated", ".gitignore": b"x"})
    assert api.heads[DATACITE_REPO] != receipt.commit_sha
    verify_current(
        api=api, token=TOKEN, prepared=prepared, receipt=receipt, download=api.download
    )


# --- verify_manifest_files and manifest validation ---------------------------


def test_verify_manifest_files_ignores_unrelated_files(tmp_path: Path) -> None:
    api = FakeHubApi()
    prepared = prepare(tmp_path, "datacite")
    files = {name: path.read_bytes() for name, path in prepared.files.items()}
    revision = api.seed(DATACITE_REPO, {**files, "docs/x.md": b"x", "LICENSE": b"cc0"})
    manifest = local_manifest(prepared)
    verify_manifest_files(
        api=api, token=TOKEN, repo_id=DATACITE_REPO, revision=revision, manifest=manifest,
        download=api.download,
    )
    with pytest.raises(PublishError, match="describes the datacite dataset"):
        verify_manifest_files(
            api=api, token=TOKEN, repo_id=ARXIV_REPO, revision=revision, manifest=manifest,
            download=api.download,
        )


def test_verify_manifest_files_reports_missing_and_unexpected_paths(tmp_path: Path) -> None:
    api = FakeHubApi()
    prepared = prepare(tmp_path, "datacite")
    files = {name: path.read_bytes() for name, path in prepared.files.items()}
    del files["data/mined/part-00000.parquet"]
    files["data/mined/part-00003.parquet"] = b"unexpected"
    revision = api.seed(DATACITE_REPO, files)
    with pytest.raises(PublishError) as info:
        verify_manifest_files(
            api=api, token=TOKEN, repo_id=DATACITE_REPO, revision=revision,
            manifest=local_manifest(prepared), download=api.download,
        )
    message = str(info.value)
    assert "data/mined/part-00000.parquet" in message
    assert "data/mined/part-00003.parquet" in message
    assert DATACITE_REPO in message and revision in message
    assert api.downloads == []


@pytest.mark.parametrize(
    "mutate",
    [
        lambda m: {**m, "kind": "dataset"},
        lambda m: {**m, "manifest_version": 1},
        lambda m: {**m, "manifest_version": "2"},
        lambda m: {**m, "dataset_kind": "smoke"},
        lambda m: {**m, "publication_schema_version": 1},
        lambda m: {**m, "snapshot_id": "2026-7"},
        lambda m: {**m, "extractor_version": ""},
        lambda m: {**m, "identity": {**m["identity"], "extraction_mode": "arxiv"}},
        lambda m: {**m, "identity": "not-an-object"},
        lambda m: {**m, "ambiguity_rows": -1},
        lambda m: {**m, "ambiguity_rows": "3"},
        lambda m: {**m, "ambiguity_rows": True},
        lambda m: {**m, "counts": {k: v for k, v in m["counts"].items() if k != "mined"}},
        lambda m: {**m, "counts": {**m["counts"], "all": {"works": 1}}},
        lambda m: {**m, "counts": {**m["counts"], "all": {**m["counts"]["all"], "works": -1}}},
        lambda m: {**m, "config_files": {k: v for k, v in m["config_files"].items() if k != "ambiguous_urls"}},
        lambda m: {**m, "config_files": {**m["config_files"], "mined": []}},
        lambda m: {**m, "config_files": {**m["config_files"], "mined": ["data/mined/missing.parquet"]}},
        lambda m: {**m, "config_files": {**m["config_files"], "mined": ["data/all/part-00000.parquet"]}},
        lambda m: {**m, "config_files": {**m["config_files"], "all": m["config_files"]["all"][:1]}},
        lambda m: {**m, "files": {k: v for k, v in m["files"].items() if k != "README.md"}},
        lambda m: {**m, "files": {**m["files"], MANIFEST_PATH: {"size": 1, "sha256": "e" * 64}}},
        lambda m: {**m, "files": {**m["files"], "../data/x.parquet": {"size": 1, "sha256": "e" * 64}}},
        lambda m: {**m, "files": {**m["files"], "/data/all/part-00009.parquet": {"size": 1, "sha256": "e" * 64}}},
        lambda m: {**m, "files": {**m["files"], "data//part-00009.parquet": {"size": 1, "sha256": "e" * 64}}},
        lambda m: {**m, "files": {**m["files"], "data/all/./part-00009.parquet": {"size": 1, "sha256": "e" * 64}}},
        lambda m: {**m, "files": {**m["files"], "docs/notes.md": {"size": 1, "sha256": "e" * 64}}},
        lambda m: {**m, "files": {**m["files"], "data/all/part-00000.parquet": {"size": -1, "sha256": "e" * 64}}},
        lambda m: {**m, "files": {**m["files"], "data/all/part-00000.parquet": {"size": "11", "sha256": "e" * 64}}},
        lambda m: {**m, "files": {**m["files"], "data/all/part-00000.parquet": {"size": 11, "sha256": "E" * 64}}},
        lambda m: {**m, "files": {**m["files"], "data/all/part-00000.parquet": {"size": 11}}},
        lambda m: {**m, "files": []},
        lambda m: {k: v for k, v in m.items() if k != "counts"},
        lambda m: {**m, "extra": 1},
        lambda m: "not-an-object",
    ],
)
def test_malformed_manifests_are_rejected(tmp_path: Path, mutate) -> None:
    manifest = mutate(local_manifest(prepare(tmp_path, "datacite")))
    with pytest.raises(PublishError):
        validate_manifest(manifest)
    api = FakeHubApi()
    revision = api.seed(DATACITE_REPO)
    with pytest.raises(PublishError):
        verify_manifest_files(
            api=api, token=TOKEN, repo_id=DATACITE_REPO, revision=revision, manifest=manifest,
            download=api.download,
        )
    assert api.tree_calls == []


def test_arxiv_manifest_must_declare_no_ambiguity_rows(tmp_path: Path) -> None:
    manifest = local_manifest(prepare(tmp_path, "arxiv"))
    validate_manifest(manifest)
    with pytest.raises(PublishError, match="ambiguity_rows"):
        validate_manifest({**manifest, "ambiguity_rows": 1})
    with pytest.raises(PublishError, match="publication_schema_version"):
        validate_manifest({**manifest, "publication_schema_version": 2})


def test_publish_prepared_rejects_prepared_datasets_that_do_not_match_their_manifest(
    tmp_path: Path,
) -> None:
    api = FakeHubApi()
    prepared = prepare(tmp_path, "datacite")
    with pytest.raises(PublishError, match="artifact"):
        publish_prepared(
            api=api,
            token=TOKEN,
            prepared=replace(prepared, artifact_digest="0" * 64),
            events=collect_events()[0],
            download=api.download,
        )
    prepared.files["data/all/part-00001.parquet"].write_bytes(b"grown after preparation")
    with pytest.raises(PublishError, match="data/all/part-00001.parquet"):
        publish_prepared(
            api=api, token=TOKEN, prepared=prepared, events=collect_events()[0], download=api.download
        )
    assert api.commit_calls == []


def test_unparseable_remote_manifest_is_superseded_by_a_guarded_commit(
    tmp_path: Path,
) -> None:
    api = FakeHubApi()
    before = api.seed(DATACITE_REPO, {MANIFEST_PATH: b"{not json", "data/all/part-00000.parquet": b"?"})
    prepared = prepare(tmp_path, "datacite")
    receipt = publish_prepared(
        api=api, token=TOKEN, prepared=prepared, events=collect_events()[0], download=api.download
    )
    [commit] = api.commit_calls
    assert commit["parent_commit"] == before
    assert managed(api.files_at(DATACITE_REPO)) == set(prepared.files)
    assert receipt.artifact_digest == prepared.artifact_digest


# --- PublicationReceipt and FullPipelineMarker -------------------------------


def receipt_payload(kind: str) -> dict[str, Any]:
    repo = DATACITE_REPO if kind == "datacite" else ARXIV_REPO
    sha = ("1" if kind == "datacite" else "2") * 40
    return {
        "repo_id": repo,
        "commit_sha": sha,
        "commit_url": f"https://huggingface.co/datasets/{repo}/commit/{sha}",
        "artifact_digest": ("3" if kind == "datacite" else "4") * 64,
    }


def marker_payload() -> dict[str, Any]:
    return {
        "kind": "datacite-pipeline",
        "schema_version": 2,
        "identity": IDENTITY.to_dict(),
        "snapshot_id": "2026-07",
        "derived_prefix": "derived-generations/2026-07-" + "0" * 32,
        "publications": {
            "datacite": receipt_payload("datacite"),
            "arxiv": receipt_payload("arxiv"),
        },
        "completed_at": "2026-09-15T00:00:00+00:00",
    }


def test_publication_receipt_round_trip_and_validation() -> None:
    receipt = PublicationReceipt.from_dict(receipt_payload("datacite"))
    assert receipt == PublicationReceipt(**receipt_payload("datacite"))
    assert receipt.to_dict() == receipt_payload("datacite")
    for mutation in (
        {"commit_sha": "1" * 39},
        {"commit_sha": "G" * 40},
        {"artifact_digest": "3" * 63},
        {"repo_id": ""},
        {"commit_url": ""},
        {"extra": 1},
    ):
        with pytest.raises(StateError):
            PublicationReceipt.from_dict({**receipt_payload("datacite"), **mutation})
    with pytest.raises(StateError):
        PublicationReceipt.from_dict({k: v for k, v in receipt_payload("datacite").items() if k != "repo_id"})


def test_pipeline_marker_round_trip() -> None:
    marker = FullPipelineMarker.from_dict(marker_payload())
    assert marker.kind == FULL_PIPELINE_KIND == "datacite-pipeline"
    assert marker.schema_version == paths.DATACITE_MARKER_VERSION == 2
    assert marker.identity == IDENTITY
    assert marker.publications == {
        "datacite": PublicationReceipt(**receipt_payload("datacite")),
        "arxiv": PublicationReceipt(**receipt_payload("arxiv")),
    }
    assert marker.to_dict() == marker_payload()
    assert FullPipelineMarker.from_dict(marker.to_dict()) == marker
    # The marker round-trips through the on-disk JSON form.
    assert FullPipelineMarker.from_dict(json.loads(json.dumps(marker.to_dict()))) == marker


@pytest.mark.parametrize(
    "mutation",
    [
        {"kind": "datacite-extraction"},
        {"kind": "pipeline"},
        {"schema_version": 1},
        {"schema_version": 3},
        {"snapshot_id": "2026-7"},
        {"derived_prefix": "raw-generations/2026-07-" + "0" * 32},
        {"identity": {**IDENTITY.to_dict(), "extraction_mode": "arxiv"}},
        {"publications": {"datacite": receipt_payload("datacite")}},
        {
            "publications": {
                "datacite": receipt_payload("datacite"),
                "arxiv": receipt_payload("arxiv"),
                "smoke": receipt_payload("arxiv"),
            }
        },
        {"publications": {"datacite": receipt_payload("arxiv"), "arxiv": receipt_payload("datacite")}},
        {"publications": {"datacite": receipt_payload("datacite"), "arxiv": {**receipt_payload("arxiv"), "commit_sha": "x"}}},
        {"publications": []},
        {"completed_at": ""},
    ],
)
def test_pipeline_marker_rejects_malformed_payloads(mutation: dict) -> None:
    with pytest.raises(StateError):
        FullPipelineMarker.from_dict({**marker_payload(), **mutation})


def test_pipeline_marker_requires_exact_keys_and_rejects_legacy_markers() -> None:
    payload = marker_payload()
    payload["dataset_commit"] = "0" * 40
    with pytest.raises(StateError, match="extra"):
        FullPipelineMarker.from_dict(payload)
    del payload["dataset_commit"]
    del payload["publications"]
    with pytest.raises(StateError, match="missing"):
        FullPipelineMarker.from_dict(payload)
    legacy = {
        "schema_version": 1,
        "snapshot_id": "2026-07",
        "snapshot_provenance": "mirror",
        "archive_sha256": "a" * 64,
        "datacite_release": "2026-07",
        "fst_sha256": "b" * 64,
        "source_tree_sha256": "c" * 64,
        "derived_prefix": "derived-generations/2026-07-" + "0" * 32,
        "raw_prefix": None,
        "dataset_repo": ARXIV_REPO,
        "dataset_commit": "0" * 40,
        "dataset_commit_url": "https://huggingface.co/x",
        "completed_at": "2026-08-31T00:00:00+00:00",
    }
    PipelineMarker.from_dict(legacy)  # the legacy class still parses it
    with pytest.raises(StateError):
        FullPipelineMarker.from_dict(legacy)
    with pytest.raises(StateError):
        FullPipelineMarker.from_dict({**legacy, "kind": "datacite-pipeline", "schema_version": 2})
