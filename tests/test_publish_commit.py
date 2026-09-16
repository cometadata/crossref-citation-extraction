import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from citations_pipeline.publish import (
    ConfigStats,
    PublishError,
    ensure_not_paired_publication,
    publish_dataset,
    render_dataset_card,
)
from citations_pipeline.telemetry import EventLogger

STATS = {
    "all": ConfigStats(works=923302, citations=5121642, references=5200000),
    "asserted": ConfigStats(works=110030, citations=308474, references=310000),
    "mined": ConfigStats(works=891035, citations=4813168, references=4890000),
}


def test_card_contains_yaml_configs_and_counts() -> None:
    card = render_dataset_card(
        STATS,
        snapshot_id="2026-07",
        datacite_release="2026-07",
        extractor_version="2.1.0",
    )
    assert card.startswith("---\n")
    assert "config_name: all" in card
    assert "config_name: asserted" in card
    assert "config_name: mined" in card
    assert "path: data/all.parquet" in card
    assert "923,302" in card and "5,121,642" in card
    assert "2026-07" in card
    assert "raw_match" in card
    assert "January 2025" not in card


@pytest.mark.parametrize(
    ("works", "category"),
    [
        (999, "n<1K"),
        (1_000, "1K<n<10K"),
        (923_302, "100K<n<1M"),
        (1_040_000, "1M<n<10M"),
        (12_000_000, "10M<n<100M"),
    ],
)
def test_arxiv_card_size_category_follows_the_work_count(works: int, category: str) -> None:
    stats = {**STATS, "all": ConfigStats(works=works, citations=works, references=works)}
    card = render_dataset_card(
        stats, snapshot_id="2026-07", datacite_release="2026-07", extractor_version="2.1.0"
    )
    header = card.split("---\n")[1]
    assert f"size_categories:\n  - {category}\n" in header
    assert header.count("size_categories:") == 1


def test_arxiv_card_describes_mined_provenance_and_raw_match_evidence() -> None:
    card = render_dataset_card(
        STATS, snapshot_id="2026-07", datacite_release="2026-07", extractor_version="2.1.0"
    )
    # `mined` covers every unasserted match, not just unstructured text, and
    # the evidence can be a DOI or a landing-page URL as well as an arXiv id.
    assert "extracted from unstructured reference text" not in card
    assert "exact substring that matched an arXiv identifier pattern" not in card
    raw_match = next(line for line in card.splitlines() if line.startswith("| `raw_match`"))
    assert "DOI" in raw_match and "URL" in raw_match and "arXiv identifier" in raw_match
    mined = next(line for line in card.splitlines() if line.startswith("- `mined`"))
    for field in ("`DOI`", "`URL`", "`article-title`", "`journal-title`", "`unstructured`"):
        assert field in mined
    assert "doi-asserted-by" in mined


class FakeApi:
    def __init__(self) -> None:
        self.tip = "a" * 40
        self.commits: list[dict] = []
        self.fail_create = False
        # Repository files at `tip` beyond the legacy data (path -> bytes).
        self.files: dict[str, bytes] = {}
        self.file_checks: list[tuple[str, str | None]] = []

    def file_exists(self, repo_id, filename, *, repo_type=None, revision=None, token=None):
        assert repo_id == "cometadata/crossref-arxiv-citations" and repo_type == "dataset"
        self.file_checks.append((filename, revision))
        return filename in self.files

    def hf_hub_download(self, *, repo_id, filename, repo_type, revision, token, cache_dir):
        target = Path(cache_dir) / filename
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(self.files[filename])
        return str(target)

    def dataset_info(self, repo_id, revision=None, token=None):
        if revision is None:
            return SimpleNamespace(sha=self.tip)
        if revision != self.commits[-1]["oid"]:
            raise RuntimeError("unknown revision")
        return SimpleNamespace(sha=revision)

    def create_commit(self, **kwargs):
        if self.fail_create:
            raise RuntimeError("parent commit mismatch")
        oid = "b" * 40
        self.commits.append({**kwargs, "oid": oid})
        return SimpleNamespace(
            oid=oid, commit_url=f"https://huggingface.co/datasets/x/commit/{oid}"
        )


def _parquet_files(tmp_path: Path) -> dict[str, Path]:
    files = {}
    for config in ("all", "asserted", "mined"):
        path = tmp_path / f"{config}.parquet"
        path.write_bytes(b"parquet")
        files[config] = path
    return files


def test_publish_guarded_by_parent_commit(tmp_path: Path) -> None:
    api = FakeApi()
    result = publish_dataset(
        api=api,
        token="secret",
        parquet=_parquet_files(tmp_path),
        card="---\ncard\n",
        snapshot_id="2026-07",
        events=EventLogger(),
    )
    commit = api.commits[0]
    assert commit["parent_commit"] == "a" * 40
    assert commit["repo_id"] == "cometadata/crossref-arxiv-citations"
    assert commit["repo_type"] == "dataset"
    paths = sorted(op.path_in_repo for op in commit["operations"])
    assert paths == [
        "README.md",
        "data/all.parquet",
        "data/asserted.parquet",
        "data/mined.parquet",
    ]
    assert "2026-07" in commit["commit_message"]
    assert result.commit_sha == "b" * 40


def paired_manifest(version: object = 2) -> bytes:
    return json.dumps(
        {"kind": "citation-dataset", "manifest_version": version, "dataset_kind": "arxiv"}
    ).encode()


def test_legacy_publication_checks_the_parent_revision_for_a_paired_manifest(
    tmp_path: Path,
) -> None:
    api = FakeApi()
    publish_dataset(
        api=api, token="secret", parquet=_parquet_files(tmp_path), card="c",
        snapshot_id="2026-07", events=EventLogger(),
    )
    assert api.file_checks == [("metadata/pipeline-manifest.json", "a" * 40)]
    assert len(api.commits) == 1


@pytest.mark.parametrize("payload", [paired_manifest(2), paired_manifest(3), b"{not json"])
def test_legacy_publication_refuses_to_commit_over_the_paired_dataset(
    tmp_path: Path, payload: bytes
) -> None:
    api = FakeApi()
    api.files["metadata/pipeline-manifest.json"] = payload
    with pytest.raises(PublishError, match="full DataCite pipeline") as excinfo:
        publish_dataset(
            api=api, token="secret", parquet=_parquet_files(tmp_path), card="c",
            snapshot_id="2026-07", events=EventLogger(),
        )
    assert api.commits == []
    assert "secret" not in str(excinfo.value)


def test_legacy_publication_guard_allows_a_repository_without_a_paired_manifest() -> None:
    api = FakeApi()
    ensure_not_paired_publication(api, "secret")
    assert api.file_checks == [("metadata/pipeline-manifest.json", None)]
    api.files["metadata/pipeline-manifest.json"] = paired_manifest(1)
    ensure_not_paired_publication(api, "secret", revision="b" * 40)


def test_publish_wraps_commit_failure(tmp_path: Path) -> None:
    api = FakeApi()
    api.fail_create = True
    with pytest.raises(PublishError, match="dataset commit failed"):
        publish_dataset(
            api=api,
            token=None,
            parquet=_parquet_files(tmp_path),
            card="c",
            snapshot_id="2026-07",
            events=EventLogger(),
        )


def test_publish_fails_if_missing_parquet(tmp_path: Path) -> None:
    files = _parquet_files(tmp_path)
    files["mined"].unlink()
    with pytest.raises(PublishError, match="mined"):
        publish_dataset(
            api=FakeApi(),
            token=None,
            parquet=files,
            card="c",
            snapshot_id="2026-07",
            events=EventLogger(),
        )
