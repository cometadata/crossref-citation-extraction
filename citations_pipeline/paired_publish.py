"""Paired publication of the DataCite and arXiv citation datasets.

A prepared dataset is a set of repository-relative files (data shards, the
dataset card, and `metadata/pipeline-manifest.json`) plus the SHA-256 of the
canonical manifest bytes, the *artifact digest*. Each repository is updated
with one parent-SHA-guarded commit that adds every prepared file and deletes
any stale managed data shard. Because the manifest travels in the commit, a
retry can recognise a completed publication by digest even when the local
receipt was lost, verify the remote files, and reconstruct the receipt
without committing again.

Nothing here declares success on the strength of a returned commit id: every
publication is verified at its immutable revision and again against the
current repository files before the caller may activate a pipeline marker.
Credentials are only ever forwarded to the injected Hub client and download
callable; they never appear in events, manifests, or receipts.
"""

from __future__ import annotations

import hashlib
import json
import re
import tempfile
from collections.abc import Callable
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from .datacite_publish import (
    ARXIV_CONFIGS,
    CITATION_CONFIGS,
    DATACITE_CONFIGS,
    MATCH_TYPES,
    DatasetTransform,
)
from .full_state import (
    PUBLICATION_KINDS,
    PUBLICATION_REPOS,
    FileMeta,
    PublicationReceipt,
    RunIdentity,
    _is_int,
)
from .paths import ARXIV_PUBLISH_SCHEMA_VERSION, DATACITE_PUBLISH_SCHEMA_VERSION
from .publish import (
    MINED_FIELDS,
    ConfigStats,
    PublishError,
    render_dataset_card,
    size_category,
)
from .state import StateError, sha256_file

MANIFEST_KIND = "citation-dataset"
MANIFEST_VERSION = 2
MANIFEST_PATH = "metadata/pipeline-manifest.json"
CARD_PATH = "README.md"
DATA_PREFIX = "data/"
PUBLICATION_SCHEMA_VERSIONS = {
    "datacite": DATACITE_PUBLISH_SCHEMA_VERSION,
    "arxiv": ARXIV_PUBLISH_SCHEMA_VERSION,
}
PUBLICATION_CONFIGS = {"datacite": DATACITE_CONFIGS, "arxiv": ARXIV_CONFIGS}
MANIFEST_KEYS = {
    "kind",
    "manifest_version",
    "dataset_kind",
    "publication_schema_version",
    "identity",
    "snapshot_id",
    "extractor_version",
    "config_files",
    "counts",
    "ambiguity_rows",
    "files",
}
COUNT_KEYS = {"works", "citations", "references"}

# `hf_hub_download`-compatible: (repo_id, filename, *, repo_type, revision,
# token, cache_dir) -> local path.
Download = Callable[..., str]

_COMMIT_RE = re.compile(r"^[0-9a-f]{40}$")
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_SNAPSHOT_RE = re.compile(r"^\d{4}-\d{2}$")


class PublicationTransportError(PublishError):
    """Reading the Hub failed (resolving HEAD, listing files, downloading a
    file): the repository's state is unknown, which is not evidence of a
    concurrent change."""


@dataclass(frozen=True)
class PreparedDataset:
    repo_id: str
    files: dict[str, Path]
    artifact_digest: str


def is_managed_path(path: str) -> bool:
    """The files a publication owns: the card, the manifest, and `data/`."""
    return path in (CARD_PATH, MANIFEST_PATH) or path.startswith(DATA_PREFIX)


def commit_url(repo_id: str, commit_sha: str) -> str:
    return f"https://huggingface.co/datasets/{repo_id}/commit/{commit_sha}"


def canonical_manifest_bytes(manifest: dict[str, Any]) -> bytes:
    """The exact bytes committed as the manifest; their SHA-256 is the artifact
    digest. Sorted keys, compact separators, UTF-8, no trailing newline."""
    return json.dumps(
        manifest, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    ).encode("utf-8")


# --- dataset cards ------------------------------------------------------------


def _count(value: int) -> str:
    return f"{value:,}"


def render_datacite_card(
    stats: dict[str, ConfigStats],
    ambiguity_rows: int,
    *,
    snapshot_id: str,
    datacite_release: str,
    identity_fingerprint: str,
    extractor_version: str,
) -> str:
    match_types = ", ".join(f"`{name}`" for name in MATCH_TYPES)
    return f"""---
license: cc0-1.0
task_categories:
  - feature-extraction
language:
  - en
tags:
  - datacite
  - crossref
  - citations
  - scholarly-communications
  - bibliometrics
  - doi
  - landing-pages
pretty_name: Crossref DataCite Citations
size_categories:
  - {size_category(stats["all"].works)}
configs:
  - config_name: all
    data_files:
      - split: train
        path: data/all/*.parquet
    default: true
  - config_name: asserted
    data_files:
      - split: train
        path: data/asserted/*.parquet
  - config_name: mined
    data_files:
      - split: train
        path: data/mined/*.parquet
  - config_name: ambiguous_urls
    data_files:
      - split: train
        path: data/ambiguous_urls/*.parquet
---

# Crossref DataCite Citations

A dataset of DataCite-registered works and the Crossref-registered works that
cite them, extracted from Crossref reference metadata and confirmed against
the DataCite monthly data file.

## Dataset Description

Each record in the citation configurations is one DataCite DOI together with
every confirmed citing work found in Crossref reference metadata. A reference
is confirmed when it carries a DOI registered in DataCite, an arXiv
identifier whose arXiv DOI is registered in DataCite, or a landing-page URL
that is the registered URL of exactly one DataCite record.

Built from the Crossref Metadata Plus monthly snapshot `{snapshot_id}` and the
DataCite monthly data file `{datacite_release}` with
[crossref-citation-extraction](https://github.com/cometadata/crossref-arxiv-citation-extraction)
version {extractor_version}.

### Provenance and Ambiguity Policy

- `publisher` and `crossref` matches come from references whose structured
  `DOI` field holds the cited DOI with a `doi-asserted-by` value of
  `publisher` or `crossref`. Every other match is `mined`: a DOI, arXiv
  identifier, or registered landing-page URL found in the reference's `DOI`
  (without that assertion), `URL`, `article-title`, `journal-title`, or
  `unstructured` field.
- A landing-page URL registered by more than one DataCite record never
  produces a citation. Such references are published separately in the
  `ambiguous_urls` configuration with every candidate DOI and are excluded
  from every citation count.
- Counts are recomputed per configuration: a citing work appears in a
  configuration only with its matches of that configuration's provenances.

### Dataset Configurations

| Config | Description | Cited Works | Citing Works | Reference Matches |
|--------|-------------|-------------|--------------|-------------------|
| `all` | All confirmed citations (default) | {_count(stats["all"].works)} | {_count(stats["all"].citations)} | {_count(stats["all"].references)} |
| `asserted` | Citations whose DOI was asserted by the publisher or matched by Crossref | {_count(stats["asserted"].works)} | {_count(stats["asserted"].citations)} | {_count(stats["asserted"].references)} |
| `mined` | Citations mined from reference metadata without a publisher or Crossref DOI assertion | {_count(stats["mined"].works)} | {_count(stats["mined"].citations)} | {_count(stats["mined"].references)} |

The `ambiguous_urls` configuration holds {_count(ambiguity_rows)} unresolved
URL references. It is evidence for downstream resolution, not citations, and
it is excluded from the counts above.

### Build Identity

| Field | Value |
|-------|-------|
| Crossref snapshot | `{snapshot_id}` |
| DataCite release | `{datacite_release}` |
| Run identity fingerprint | `{identity_fingerprint}` |
| Extractor version | `{extractor_version}` |
| Artifact manifest | `metadata/pipeline-manifest.json` |

The manifest lists every published file with its size and SHA-256 digest; the
SHA-256 of the manifest bytes identifies this publication.

### Loading the Dataset

```python
from datasets import load_dataset

dataset = load_dataset("cometadata/crossref-datacite-citations")
asserted = load_dataset("cometadata/crossref-datacite-citations", "asserted")
mined = load_dataset("cometadata/crossref-datacite-citations", "mined")
ambiguous = load_dataset("cometadata/crossref-datacite-citations", "ambiguous_urls")
```

## Data Schema

### Citation Configurations (`all`, `asserted`, `mined`)

Each record contains:

| Field | Type | Description |
|-------|------|-------------|
| `doi` | string | Canonical lowercase DataCite DOI of the cited work |
| `citation_count` | integer | Number of distinct citing works in this configuration |
| `cited_by` | array | List of citing work objects |
| `reference_count` | integer | Number of confirmed reference tuples |

Each citing work object in `cited_by` contains:

| Field | Type | Description |
|-------|------|-------------|
| `doi` | string | DOI of the citing work |
| `provenance` | string | Strongest provenance among this work's matches (`publisher` > `crossref` > `mined`) |
| `matches` | array | Reference matches from this citing work, in reference order |

Each match contains:

| Field | Type | Description |
|-------|------|-------------|
| `provenance` | string | How this match was obtained (see below) |
| `match_type` | string | What was matched: {match_types} |
| `raw_match` | string | The exact reference text that matched (a substring, or a whole structured value) |
| `reference` | struct | The Crossref reference metadata (string-valued fields; absent keys are null) |

### `ambiguous_urls`

| Field | Type | Description |
|-------|------|-------------|
| `citing_doi` | string | DOI of the citing work |
| `ref_index` | integer | Position of the reference in the citing work's reference list |
| `url` | string | Normalized landing-page URL that matched several DataCite records |
| `raw_match` | string | The exact substring that matched |
| `candidate_dois` | array | Every DataCite DOI registered with this landing-page URL |
| `reference` | struct | The Crossref reference metadata (string-valued fields; absent keys are null) |

### Provenance Values

- `publisher` - the reference's structured `DOI` field holds the DataCite DOI with `doi-asserted-by: publisher`
- `crossref` - the reference's structured `DOI` field holds the DataCite DOI with `doi-asserted-by: crossref`
- `mined` - any other match: a DOI, arXiv identifier, or registered landing-page URL found in the reference's {MINED_FIELDS} field (a structured `DOI` counts as mined unless `doi-asserted-by` is `publisher` or `crossref`)

The `asserted` config contains only `publisher` and `crossref` matches; the
`mined` config contains only `mined` matches.

## Extraction Process

References in the Crossref snapshot are scanned for DOIs, arXiv identifiers,
and landing-page URLs. Each candidate is normalized and looked up in an index
built from the DataCite monthly data file `{datacite_release}`: DOIs must be
registered in DataCite, arXiv identifiers must map to a registered arXiv DOI,
and URLs must equal the registered landing page of exactly one record. URLs
registered by several records are recorded as ambiguous evidence instead.

## Data Sources

- Crossref Metadata Plus monthly snapshot `{snapshot_id}`
- DataCite monthly data file `{datacite_release}`
- Extraction tools: [cometadata/crossref-arxiv-citation-extraction](https://github.com/cometadata/crossref-arxiv-citation-extraction)

## Limitations

- Only citations registered in Crossref reference metadata are included.
- Only DOIs and landing-page URLs registered in DataCite are matched; works
  registered with other agencies are not represented.
- Landing-page matching uses the URLs registered in the DataCite release
  `{datacite_release}`; earlier or historical URLs of a record are not
  matched.
- The dataset reflects one Crossref snapshot and one DataCite release; each
  publication replaces the previous contents rather than accumulating them.
- References whose URL is registered by several DataCite records appear only
  in `ambiguous_urls` and are never counted as citations.
- Reference parsing depends on successful identifier extraction from
  reference metadata; the `mined` subset may contain lower-quality matches
  from noisy reference text.

## License

This dataset is released under
[CC0 1.0 Universal](https://creativecommons.org/publicdomain/zero/1.0/).

## Citation

```bibtex
@dataset{{crossref_datacite_citations,
  title = {{Crossref DataCite Citations}},
  author = {{Cometadata}},
  publisher = {{Hugging Face}},
  url = {{https://huggingface.co/datasets/cometadata/crossref-datacite-citations}}
}}
```
"""


def _render_card(
    kind: str,
    transform: DatasetTransform,
    identity: RunIdentity,
    snapshot_id: str,
    extractor_version: str,
) -> str:
    stats = {config: transform.stats[config] for config in CITATION_CONFIGS}
    if kind == "datacite":
        return render_datacite_card(
            stats,
            transform.ambiguity_rows,
            snapshot_id=snapshot_id,
            datacite_release=identity.datacite_release,
            identity_fingerprint=identity.fingerprint(),
            extractor_version=extractor_version,
        )
    # The existing arXiv dataset keeps its card layout and schema description.
    return render_dataset_card(
        stats,
        snapshot_id=snapshot_id,
        datacite_release=identity.datacite_release,
        extractor_version=extractor_version,
    )


# --- manifest -------------------------------------------------------------------


def _relative_path(value: Any, where: str) -> str:
    if not isinstance(value, str) or not value:
        raise PublishError(f"{where}: path must be a nonempty string")
    if value.startswith("/") or "\\" in value:
        raise PublishError(f"{where}: path {value!r} must be relative")
    if any(segment in ("", ".", "..") for segment in value.split("/")):
        raise PublishError(f"{where}: path {value!r} must be normalized")
    return value


def _config_path_ok(kind: str, config: str, name: str) -> bool:
    """`data/<config>/<shard>.parquet` for the general dataset, the legacy
    `data/<config>.parquet` for arXiv: exactly what the card globs cover."""
    if kind == "datacite":
        prefix = f"{DATA_PREFIX}{config}/"
        shard = name[len(prefix) :]
        return (
            name.startswith(prefix)
            and "/" not in shard
            and shard.endswith(".parquet")
            and shard != ".parquet"
        )
    return name == f"{DATA_PREFIX}{config}.parquet"


def _non_negative_int(value: Any, where: str) -> int:
    if not _is_int(value) or value < 0:
        raise PublishError(f"{where} must be a non-negative integer")
    return value


def _check_config_files(
    kind: str, config_files: Any, data_files: set[str], where: str
) -> dict[str, tuple[str, ...]]:
    configs = PUBLICATION_CONFIGS[kind]
    if not isinstance(config_files, dict) or set(config_files) != set(configs):
        raise PublishError(
            f"{where}: config_files must cover exactly {list(configs)}"
        )
    covered: dict[str, str] = {}
    result: dict[str, tuple[str, ...]] = {}
    for config in configs:
        names = config_files[config]
        if not isinstance(names, (list, tuple)) or not names:
            raise PublishError(f"{where}: configuration {config} lists no files")
        for name in names:
            _relative_path(name, f"{where} config_files[{config}]")
            if not _config_path_ok(kind, config, name):
                raise PublishError(
                    f"{where}: {name} is not a {config} data file of the {kind} layout"
                )
            if name not in data_files:
                raise PublishError(f"{where}: configuration {config} lists unknown file {name}")
            if name in covered:
                raise PublishError(
                    f"{where}: {name} is listed by both {covered[name]} and {config}"
                )
            covered[name] = config
        result[config] = tuple(names)
    uncovered = sorted(data_files - covered.keys())
    if uncovered:
        raise PublishError(f"{where}: data files belong to no configuration: {uncovered}")
    return result


def _check_counts(counts: Any, where: str) -> dict[str, dict[str, int]]:
    if not isinstance(counts, dict) or set(counts) != set(CITATION_CONFIGS):
        raise PublishError(f"{where}: counts must cover exactly {list(CITATION_CONFIGS)}")
    result: dict[str, dict[str, int]] = {}
    for config, entry in counts.items():
        if not isinstance(entry, dict) or set(entry) != COUNT_KEYS:
            raise PublishError(f"{where}: counts[{config}] must have {sorted(COUNT_KEYS)}")
        result[config] = {
            key: _non_negative_int(entry[key], f"{where}: counts[{config}].{key}")
            for key in sorted(COUNT_KEYS)
        }
    return result


def _check_ambiguity_rows(kind: str, value: Any, where: str) -> int:
    rows = _non_negative_int(value, f"{where}: ambiguity_rows")
    if kind == "arxiv" and rows != 0:
        raise PublishError(f"{where}: ambiguity_rows must be 0 for the arxiv dataset")
    return rows


def validate_manifest(manifest: Any) -> dict[str, FileMeta]:
    """Check every manifest field and return the files it declares.

    The returned map covers the data files and the card; the manifest never
    lists itself, so its digest is not circular.
    """
    where = "manifest"
    if not isinstance(manifest, dict):
        raise PublishError(f"{where} must be an object")
    missing = MANIFEST_KEYS - manifest.keys()
    extra = manifest.keys() - MANIFEST_KEYS
    if missing or extra:
        raise PublishError(
            f"invalid {where} keys: missing={sorted(missing)}, extra={sorted(extra)}"
        )
    if manifest["kind"] != MANIFEST_KIND:
        raise PublishError(f"{where}: kind must be {MANIFEST_KIND!r}")
    if not _is_int(manifest["manifest_version"]) or (
        manifest["manifest_version"] != MANIFEST_VERSION
    ):
        raise PublishError(f"{where}: manifest_version must equal {MANIFEST_VERSION}")
    kind = manifest["dataset_kind"]
    if kind not in PUBLICATION_KINDS:
        raise PublishError(f"{where}: dataset_kind must be one of {list(PUBLICATION_KINDS)}")
    schema_version = manifest["publication_schema_version"]
    if not _is_int(schema_version) or schema_version != PUBLICATION_SCHEMA_VERSIONS[kind]:
        raise PublishError(
            f"{where}: publication_schema_version must equal "
            f"{PUBLICATION_SCHEMA_VERSIONS[kind]} for the {kind} dataset"
        )
    try:
        RunIdentity.from_dict(manifest["identity"])
    except StateError as error:
        raise PublishError(f"{where}: identity is invalid: {error}") from error
    snapshot_id = manifest["snapshot_id"]
    if not isinstance(snapshot_id, str) or not _SNAPSHOT_RE.fullmatch(snapshot_id):
        raise PublishError(f"{where}: snapshot_id must have YYYY-MM form")
    if not isinstance(manifest["extractor_version"], str) or not manifest["extractor_version"]:
        raise PublishError(f"{where}: extractor_version must be a nonempty string")

    entries = manifest["files"]
    if not isinstance(entries, dict):
        raise PublishError(f"{where}: files must be an object")
    files: dict[str, FileMeta] = {}
    for name, entry in entries.items():
        _relative_path(name, f"{where} files")
        if name == MANIFEST_PATH:
            raise PublishError(f"{where}: files must not list the manifest itself")
        if not is_managed_path(name):
            raise PublishError(f"{where}: {name} is outside the managed dataset paths")
        if not isinstance(entry, dict) or set(entry) != {"size", "sha256"}:
            raise PublishError(f"{where}: files[{name}] must have size and sha256")
        digest = entry["sha256"]
        if not isinstance(digest, str) or not _SHA256_RE.fullmatch(digest):
            raise PublishError(f"{where}: files[{name}].sha256 must be a lowercase SHA-256")
        files[name] = FileMeta(
            size=_non_negative_int(entry["size"], f"{where}: files[{name}].size"),
            sha256=digest,
        )
    if CARD_PATH not in files:
        raise PublishError(f"{where}: files must list {CARD_PATH}")
    data_files = {name for name in files if name.startswith(DATA_PREFIX)}
    _check_config_files(kind, manifest["config_files"], data_files, where)
    _check_counts(manifest["counts"], where)
    _check_ambiguity_rows(kind, manifest["ambiguity_rows"], where)
    return files


def _check_transform(kind: str, transform: DatasetTransform) -> None:
    where = f"{kind} transform"
    for name, path in transform.files.items():
        _relative_path(name, where)
        if not name.startswith(DATA_PREFIX):
            raise PublishError(f"{where}: {name} is not under {DATA_PREFIX}")
        if not path.is_file():
            raise PublishError(f"{where}: {name} is missing at {path}")
    _check_config_files(kind, transform.config_files, set(transform.files), where)
    missing = sorted(set(CITATION_CONFIGS) - transform.stats.keys())
    if missing:
        raise PublishError(f"{where}: stats are missing configurations {missing}")
    _check_counts(
        {config: asdict(transform.stats[config]) for config in CITATION_CONFIGS}, where
    )
    _check_ambiguity_rows(kind, transform.ambiguity_rows, where)


def prepare_dataset(
    kind: str,
    transform: DatasetTransform,
    identity: RunIdentity,
    snapshot_id: str,
    extractor_version: str,
    out_dir: Path,
) -> PreparedDataset:
    """Write the card and the manifest for one destination and return the
    complete file set with its artifact digest."""
    if kind not in PUBLICATION_KINDS:
        raise PublishError(f"unknown dataset kind {kind!r}")
    if not _SNAPSHOT_RE.fullmatch(snapshot_id):
        raise PublishError("snapshot_id must have YYYY-MM form")
    if not extractor_version:
        raise PublishError("extractor_version must be a nonempty string")
    _check_transform(kind, transform)

    out_dir.mkdir(parents=True, exist_ok=True)
    card_path = out_dir / CARD_PATH
    card_path.write_bytes(
        _render_card(kind, transform, identity, snapshot_id, extractor_version).encode(
            "utf-8"
        )
    )
    files = {name: transform.files[name] for name in sorted(transform.files)}
    files[CARD_PATH] = card_path

    metas: dict[str, dict[str, Any]] = {}
    for name, path in files.items():
        size = path.stat().st_size
        recorded = transform.sizes.get(name)
        if recorded is not None and recorded != size:
            raise PublishError(
                f"{name} is {size} bytes on disk but the transform recorded {recorded}"
            )
        metas[name] = {"size": size, "sha256": sha256_file(path)}

    manifest = {
        "kind": MANIFEST_KIND,
        "manifest_version": MANIFEST_VERSION,
        "dataset_kind": kind,
        "publication_schema_version": PUBLICATION_SCHEMA_VERSIONS[kind],
        "identity": identity.to_dict(),
        "snapshot_id": snapshot_id,
        "extractor_version": extractor_version,
        "config_files": {
            config: list(transform.config_files[config])
            for config in PUBLICATION_CONFIGS[kind]
        },
        "counts": {
            config: asdict(transform.stats[config]) for config in CITATION_CONFIGS
        },
        "ambiguity_rows": transform.ambiguity_rows,
        "files": metas,
    }
    validate_manifest(manifest)
    payload = canonical_manifest_bytes(manifest)
    manifest_path = out_dir / MANIFEST_PATH
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_bytes(payload)
    files[MANIFEST_PATH] = manifest_path
    return PreparedDataset(
        repo_id=PUBLICATION_REPOS[kind],
        files=files,
        artifact_digest=hashlib.sha256(payload).hexdigest(),
    )


# --- remote verification --------------------------------------------------------


def _resolve_download(download: Download | None) -> Download:
    if download is not None:
        return download
    from huggingface_hub import hf_hub_download

    return hf_hub_download


def _head_sha(api: Any, token: str | None, repo_id: str) -> str:
    try:
        sha = api.dataset_info(repo_id, token=token).sha
    except Exception as error:
        raise PublicationTransportError(
            f"cannot resolve HEAD of {repo_id}: {error}"
        ) from error
    if not isinstance(sha, str) or not _COMMIT_RE.fullmatch(sha):
        raise PublishError(f"{repo_id}: HEAD is not a commit hash: {sha!r}")
    return sha


def _list_files(api: Any, token: str | None, repo_id: str, revision: str) -> dict[str, Any]:
    """File entries (path -> entry) at an immutable revision; folders skipped."""
    try:
        entries = list(
            api.list_repo_tree(
                repo_id, revision=revision, repo_type="dataset", recursive=True, token=token
            )
        )
    except Exception as error:
        raise PublicationTransportError(
            f"cannot list {repo_id} at {revision}: {error}"
        ) from error
    tree: dict[str, Any] = {}
    for entry in entries:
        if hasattr(entry, "tree_id"):
            continue
        path = getattr(entry, "path", None)
        if not isinstance(path, str) or not _is_int(getattr(entry, "size", None)):
            raise PublishError(f"{repo_id}@{revision}: malformed tree entry {path!r}")
        tree[path] = entry
    return tree


def _download(
    download: Download,
    token: str | None,
    repo_id: str,
    revision: str,
    path: str,
    cache_dir: str,
) -> Path:
    try:
        local = download(
            repo_id,
            path,
            repo_type="dataset",
            revision=revision,
            token=token,
            cache_dir=cache_dir,
        )
    except Exception as error:
        raise PublicationTransportError(
            f"cannot download {path} from {repo_id} at {revision}: {error}"
        ) from error
    return Path(local)


def _read_manifest_bytes(
    download: Download, token: str | None, repo_id: str, revision: str
) -> bytes:
    with tempfile.TemporaryDirectory(prefix="dataset-manifest-") as cache_dir:
        return _download(download, token, repo_id, revision, MANIFEST_PATH, cache_dir).read_bytes()


def _parse_manifest(payload: bytes, repo_id: str, revision: str) -> dict[str, Any]:
    try:
        manifest = json.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, ValueError) as error:
        raise PublishError(
            f"{repo_id}@{revision}: {MANIFEST_PATH} is not valid JSON"
        ) from error
    if not isinstance(manifest, dict):
        raise PublishError(f"{repo_id}@{revision}: {MANIFEST_PATH} is not an object")
    return manifest


def _verify_tree(
    *,
    download: Download,
    token: str | None,
    repo_id: str,
    revision: str,
    files: dict[str, FileMeta],
    tree: dict[str, Any],
) -> None:
    expected = set(files) | {MANIFEST_PATH}
    managed = {path for path in tree if is_managed_path(path)}
    missing = sorted(expected - managed)
    unexpected = sorted(managed - expected)
    if missing or unexpected:
        raise PublishError(
            f"{repo_id}@{revision}: managed files differ from the manifest: "
            f"missing={missing}, unexpected={unexpected}"
        )
    with tempfile.TemporaryDirectory(prefix="dataset-verify-") as cache_dir:
        for path, meta in sorted(files.items()):
            entry = tree[path]
            if entry.size != meta.size:
                raise PublishError(
                    f"{repo_id}@{revision}: {path} is {entry.size} bytes, "
                    f"manifest expects {meta.size}"
                )
            lfs = getattr(entry, "lfs", None)
            remote_digest = getattr(lfs, "sha256", None) if lfs is not None else None
            if isinstance(remote_digest, str) and remote_digest:
                lfs_size = getattr(lfs, "size", None)
                if _is_int(lfs_size) and lfs_size != meta.size:
                    raise PublishError(
                        f"{repo_id}@{revision}: {path} LFS size {lfs_size} differs "
                        f"from manifest size {meta.size}"
                    )
                digest = remote_digest
            else:
                # A Git blob id is a SHA-1 of the object; it can never be
                # compared with the manifest's SHA-256. Fetch and hash.
                local = _download(download, token, repo_id, revision, path, cache_dir)
                size = local.stat().st_size
                if size != meta.size:
                    raise PublishError(
                        f"{repo_id}@{revision}: downloaded {path} is {size} bytes, "
                        f"manifest expects {meta.size}"
                    )
                digest = sha256_file(local)
            if digest != meta.sha256:
                raise PublishError(
                    f"{repo_id}@{revision}: {path} digest {digest} does not match "
                    f"manifest digest {meta.sha256}"
                )


def _verify_manifest_at(
    *,
    api: Any,
    token: str | None,
    download: Download,
    repo_id: str,
    revision: str,
    manifest: dict[str, Any],
    tree: dict[str, Any] | None = None,
) -> None:
    files = validate_manifest(manifest)
    kind = manifest["dataset_kind"]
    if PUBLICATION_REPOS[kind] != repo_id:
        raise PublishError(f"manifest describes the {kind} dataset, not {repo_id}")
    if tree is None:
        tree = _list_files(api, token, repo_id, revision)
    _verify_tree(
        download=download,
        token=token,
        repo_id=repo_id,
        revision=revision,
        files=files,
        tree=tree,
    )


def verify_manifest_files(
    *,
    api: Any,
    token: str | None,
    repo_id: str,
    revision: str,
    manifest: dict[str, Any],
    download: Download | None = None,
) -> None:
    """Verify that `revision` publishes exactly the manifest's managed files.

    Validates the manifest, requires the managed paths at the revision to be
    the manifest's files plus the manifest itself (unrelated files elsewhere
    in the repository are ignored), and checks every listed size and SHA-256:
    from the Hub's LFS metadata when present, otherwise by downloading the
    file at that immutable revision and hashing it.
    """
    _verify_manifest_at(
        api=api,
        token=token,
        download=_resolve_download(download),
        repo_id=repo_id,
        revision=revision,
        manifest=manifest,
    )


def verify_current(
    *,
    api: Any,
    token: str | None,
    prepared: PreparedDataset,
    receipt: PublicationReceipt,
    download: Download | None = None,
) -> None:
    """Confirm that the repository's current files are still the receipt's
    artifact. A historical commit is never treated as the current dataset."""
    download = _resolve_download(download)
    repo_id = prepared.repo_id
    if receipt.repo_id != repo_id:
        raise PublishError(
            f"receipt for {receipt.repo_id} does not belong to the prepared {repo_id} dataset"
        )
    if receipt.artifact_digest != prepared.artifact_digest:
        raise PublishError(
            f"{repo_id}: receipt artifact {receipt.artifact_digest} does not match "
            f"the prepared artifact {prepared.artifact_digest}"
        )
    head = _head_sha(api, token, repo_id)
    tree = _list_files(api, token, repo_id, head)
    if MANIFEST_PATH not in tree:
        raise PublishError(
            f"concurrent change in {repo_id}: {MANIFEST_PATH} is missing at HEAD {head}"
        )
    payload = _read_manifest_bytes(download, token, repo_id, head)
    digest = hashlib.sha256(payload).hexdigest()
    if digest != receipt.artifact_digest:
        raise PublishError(
            f"concurrent change in {repo_id}: HEAD {head} publishes artifact {digest}, "
            f"the verified commit {receipt.commit_sha} published {receipt.artifact_digest}"
        )
    _verify_manifest_at(
        api=api,
        token=token,
        download=download,
        repo_id=repo_id,
        revision=head,
        manifest=_parse_manifest(payload, repo_id, head),
        tree=tree,
    )
    after = _head_sha(api, token, repo_id)
    if after != head:
        raise PublishError(
            f"concurrent change in {repo_id}: HEAD moved from {head} to {after} "
            "during verification"
        )


# --- publication ----------------------------------------------------------------


def _check_prepared(prepared: PreparedDataset) -> dict[str, Any]:
    """Local consistency before anything touches the Hub: the manifest is
    ours, and every prepared file exists with the manifest's name, size and
    SHA-256, so a file changed after preparation is never committed. The
    remote copies are verified again after the commit."""
    repo_id = prepared.repo_id
    for name, path in prepared.files.items():
        _relative_path(name, f"{repo_id} prepared files")
        if not is_managed_path(name):
            raise PublishError(f"{repo_id}: prepared file {name} is outside the managed paths")
        if not path.is_file():
            raise PublishError(f"{repo_id}: prepared file {name} is missing at {path}")
    for required in (CARD_PATH, MANIFEST_PATH):
        if required not in prepared.files:
            raise PublishError(f"{repo_id}: prepared dataset lacks {required}")
    payload = prepared.files[MANIFEST_PATH].read_bytes()
    digest = hashlib.sha256(payload).hexdigest()
    if digest != prepared.artifact_digest:
        raise PublishError(
            f"{repo_id}: prepared manifest digest {digest} does not match "
            f"artifact digest {prepared.artifact_digest}"
        )
    manifest = _parse_manifest(payload, repo_id, "local")
    files = validate_manifest(manifest)
    if PUBLICATION_REPOS[manifest["dataset_kind"]] != repo_id:
        raise PublishError(
            f"{repo_id}: prepared manifest describes the {manifest['dataset_kind']} dataset"
        )
    if set(files) != set(prepared.files) - {MANIFEST_PATH}:
        raise PublishError(f"{repo_id}: prepared files differ from the manifest's files")
    for name, meta in sorted(files.items()):
        size = prepared.files[name].stat().st_size
        if size != meta.size:
            raise PublishError(
                f"{repo_id}: prepared file {name} is {size} bytes, manifest expects {meta.size}"
            )
        digest = sha256_file(prepared.files[name])
        if digest != meta.sha256:
            raise PublishError(
                f"{repo_id}: prepared file {name} digest {digest} does not match "
                f"manifest digest {meta.sha256}"
            )
    return manifest


def _recover_publication(
    *,
    api: Any,
    token: str | None,
    download: Download,
    prepared: PreparedDataset,
    head: str,
    tree: dict[str, Any],
    events: Any,
) -> PublicationReceipt | None:
    """If HEAD already publishes this artifact (the receipt was lost), verify
    its files and reconstruct the receipt without committing again."""
    repo_id = prepared.repo_id
    if MANIFEST_PATH not in tree:
        return None
    payload = _read_manifest_bytes(download, token, repo_id, head)
    digest = hashlib.sha256(payload).hexdigest()
    if digest != prepared.artifact_digest:
        events.emit(
            "publication_superseding", repo=repo_id, head=head, current_artifact=digest
        )
        return None
    _verify_manifest_at(
        api=api,
        token=token,
        download=download,
        repo_id=repo_id,
        revision=head,
        manifest=_parse_manifest(payload, repo_id, head),
        tree=tree,
    )
    events.emit("publication_recovered", repo=repo_id, commit=head, artifact_digest=digest)
    return PublicationReceipt(repo_id, head, commit_url(repo_id, head), digest)


def _commit_prepared(
    *,
    api: Any,
    token: str | None,
    download: Download,
    prepared: PreparedDataset,
    manifest: dict[str, Any],
    head: str,
    tree: dict[str, Any],
    events: Any,
) -> PublicationReceipt:
    from huggingface_hub import CommitOperationAdd, CommitOperationDelete

    repo_id = prepared.repo_id
    operations: list[Any] = [
        CommitOperationAdd(path_in_repo=name, path_or_fileobj=str(path))
        for name, path in sorted(prepared.files.items())
    ]
    # Stale managed shards would still be matched by the card's config globs.
    stale = sorted(
        path for path in tree if path.startswith(DATA_PREFIX) and path not in prepared.files
    )
    operations.extend(CommitOperationDelete(path_in_repo=path) for path in stale)
    message = (
        f"Update from Crossref snapshot {manifest['snapshot_id']} "
        f"(artifact {prepared.artifact_digest})"
    )
    try:
        commit = api.create_commit(
            repo_id=repo_id,
            repo_type="dataset",
            operations=operations,
            commit_message=message,
            parent_commit=head,
            token=token,
        )
    except Exception as error:
        raise PublishError(
            f"dataset commit to {repo_id} on parent {head} failed: {error}"
        ) from error
    oid = getattr(commit, "oid", None)
    if not isinstance(oid, str) or not _COMMIT_RE.fullmatch(oid):
        raise PublishError(f"{repo_id}: commit returned no commit hash: {oid!r}")
    url = getattr(commit, "commit_url", None)
    if not isinstance(url, str) or not url:
        url = commit_url(repo_id, oid)
    events.emit("dataset_committed", repo=repo_id, commit=oid, parent=head, deleted=stale)
    _verify_manifest_at(
        api=api,
        token=token,
        download=download,
        repo_id=repo_id,
        revision=oid,
        manifest=manifest,
    )
    return PublicationReceipt(repo_id, oid, url, prepared.artifact_digest)


def publish_prepared(
    *,
    api: Any,
    token: str | None,
    prepared: PreparedDataset,
    events: Any,
    download: Download | None = None,
) -> PublicationReceipt:
    """Publish one prepared dataset with a single parent-guarded commit, or
    recover the receipt of an identical publication already at HEAD. The
    returned receipt has been verified at its revision and as current."""
    download = _resolve_download(download)
    repo_id = prepared.repo_id
    manifest = _check_prepared(prepared)
    with events.phase(f"publish_{manifest['dataset_kind']}_dataset"):
        try:
            api.create_repo(
                repo_id, repo_type="dataset", private=False, exist_ok=True, token=token
            )
        except Exception as error:
            raise PublishError(
                f"cannot create dataset repository {repo_id}: {error}"
            ) from error
        head = _head_sha(api, token, repo_id)
        tree = _list_files(api, token, repo_id, head)
        receipt = _recover_publication(
            api=api,
            token=token,
            download=download,
            prepared=prepared,
            head=head,
            tree=tree,
            events=events,
        )
        if receipt is None:
            receipt = _commit_prepared(
                api=api,
                token=token,
                download=download,
                prepared=prepared,
                manifest=manifest,
                head=head,
                tree=tree,
                events=events,
            )
        verify_current(
            api=api, token=token, prepared=prepared, receipt=receipt, download=download
        )
    events.emit(
        "publication_verified",
        repo=repo_id,
        commit=receipt.commit_sha,
        artifact_digest=receipt.artifact_digest,
    )
    return receipt


def publish_pair(
    prepared: dict[str, PreparedDataset],
    receipts: dict[str, PublicationReceipt],
    commit_one: Callable[[PreparedDataset], PublicationReceipt],
    save_receipt: Callable[[str, PublicationReceipt], None],
    check_current: Callable[[PreparedDataset, PublicationReceipt], None],
) -> dict[str, PublicationReceipt]:
    """Publish both destinations in fixed order, reusing journaled receipts.

    A destination with a receipt is re-checked instead of re-committed; a
    missing one is committed and journaled. Every destination is checked
    again before returning, and the caller activates only after that. A
    failure leaves the other repository as it is: nothing is rolled back.
    """
    result = dict(receipts)
    for kind in ("datacite", "arxiv"):
        if kind in result:
            check_current(prepared[kind], result[kind])
        else:
            result[kind] = commit_one(prepared[kind])
            save_receipt(kind, result[kind])
    for kind in ("datacite", "arxiv"):
        check_current(prepared[kind], result[kind])
    return result
