from pathlib import Path
from types import SimpleNamespace

import pytest

from citations_pipeline.publish import (
    ConfigStats,
    PublishError,
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


class FakeApi:
    def __init__(self) -> None:
        self.tip = "a" * 40
        self.commits: list[dict] = []
        self.fail_create = False

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
