from __future__ import annotations

import base64
import gzip
import hashlib
import json
import time
from collections import deque
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Self

import pytest
import requests


@dataclass(frozen=True)
class RecordedCall:
    method: str
    url: str
    headers: dict[str, str]
    kwargs: dict[str, Any]


class FakeResponse:
    def __init__(
        self,
        *,
        status: int,
        url: str,
        headers: dict[str, str],
        body: bytes = b"",
        delay_before_body: float = 0.0,
    ) -> None:
        self.status_code = status
        self.url = url
        self.headers = headers.copy()
        self._body = body
        self._delay_before_body = delay_before_body

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def iter_content(self, chunk_size: int = 1) -> Any:
        if self._delay_before_body:
            time.sleep(self._delay_before_body)
        for start in range(0, len(self._body), chunk_size):
            yield self._body[start : start + chunk_size]

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise requests.HTTPError(f"HTTP {self.status_code}")


class FakeSession:
    def __init__(self) -> None:
        self._head_responses: deque[FakeResponse | BaseException] = deque()
        self._get_responses: deque[FakeResponse | BaseException] = deque()
        self.calls: list[RecordedCall] = []
        self.last_headers: dict[str, str] = {}

    def head_response(
        self,
        *,
        status: int,
        url: str,
        headers: dict[str, str],
        body: bytes = b"",
    ) -> None:
        self._head_responses.append(
            FakeResponse(status=status, url=url, headers=headers, body=body)
        )

    def get_response(
        self,
        *,
        status: int,
        url: str,
        headers: dict[str, str],
        body: bytes = b"",
        delay_before_body: float = 0.0,
    ) -> None:
        self._get_responses.append(
            FakeResponse(
                status=status,
                url=url,
                headers=headers,
                body=body,
                delay_before_body=delay_before_body,
            )
        )

    def head_error(self, error: BaseException) -> None:
        self._head_responses.append(error)

    def get_error(self, error: BaseException) -> None:
        self._get_responses.append(error)

    def head(self, url: str, **kwargs: Any) -> FakeResponse:
        return self._request("HEAD", url, self._head_responses, kwargs)

    def get(self, url: str, **kwargs: Any) -> FakeResponse:
        return self._request("GET", url, self._get_responses, kwargs)

    def _request(
        self,
        method: str,
        url: str,
        responses: deque[FakeResponse | BaseException],
        kwargs: dict[str, Any],
    ) -> FakeResponse:
        headers = dict(kwargs.get("headers", {}))
        self.last_headers = headers
        self.calls.append(
            RecordedCall(method=method, url=url, headers=headers, kwargs=kwargs.copy())
        )
        if not responses:
            raise AssertionError(f"No queued {method} response for {url}")
        response = responses.popleft()
        if isinstance(response, BaseException):
            raise response
        return response


@pytest.fixture
def fake_session() -> FakeSession:
    return FakeSession()


# --- DataCite monthly release fixtures (full mode) ---------------------------
#
# These mirror the Rust `tests/support/mod.rs` helpers: a single
# `dois/updated_YYYY-MM/` folder holding one metadata part and one state
# report, a `MANIFEST.json` listing exactly those files with base64 SHA-256
# digests, and a `Complete` `STATUS.json`. `write_full_datacite_release` is
# the low-level builder for hand-crafted bytes; `make_full_datacite_release`
# writes the default gzip content.

FULL_RELEASE_MONTH = "2026-07"


def gzip_bytes(data: bytes) -> bytes:
    """Gzip `data` as one member with a zero mtime so fixtures are deterministic."""
    return gzip.compress(data, mtime=0)


def metadata_jsonl(records: Sequence[dict[str, Any]]) -> bytes:
    return "".join(
        json.dumps(record, separators=(",", ":")) + "\n" for record in records
    ).encode("utf-8")


def state_csv(states: Sequence[tuple[str, str]]) -> bytes:
    lines = ["doi,state,client_id,updated"]
    lines.extend(
        f"{doi},{state},example.repo,2026-07-15T00:00:00.000Z" for doi, state in states
    )
    return ("\n".join(lines) + "\n").encode("utf-8")


def part_path(month: str, index: int) -> str:
    return f"dois/updated_{month}/part_{index:04d}.jsonl.gz"


def csv_path(month: str) -> str:
    return f"dois/updated_{month}/{month}.csv.gz"


def write_full_datacite_release(
    root: Path,
    month: str,
    files: Sequence[tuple[str, bytes]],
    *,
    status: str = "Complete",
    transfer_complete: bool = True,
) -> None:
    """Write each `(relative_path, bytes)` file under `root`, a `MANIFEST.json`
    describing exactly those files, `STATUS.json` for `month`, and (by default)
    the mirror's `_TRANSFER_COMPLETE.json`."""
    root.mkdir(parents=True, exist_ok=True)
    entries = []
    for relative, data in files:
        path = root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)
        entries.append(
            {
                "filename": relative,
                "size": len(data),
                "sha256": base64.b64encode(hashlib.sha256(data).digest()).decode(
                    "ascii"
                ),
            }
        )
    (root / "MANIFEST.json").write_text(json.dumps(entries, indent=2) + "\n")
    (root / "STATUS.json").write_text(
        json.dumps(
            {
                "month": month,
                "datetime": "2026-08-01T07:53:35.713625+00:00",
                "status": status,
            },
            indent=2,
        )
        + "\n"
    )
    if transfer_complete:
        (root / "_TRANSFER_COMPLETE.json").write_text("{}\n")


def make_full_datacite_release(
    root: Path,
    month: str = FULL_RELEASE_MONTH,
    *,
    metadata: Sequence[dict[str, Any]] | None = None,
    states: Sequence[tuple[str, str]] | None = None,
    transfer_complete: bool = True,
) -> None:
    """Write a complete single-month release: `part_0000.jsonl.gz` holding
    `metadata` and `<month>.csv.gz` holding `states`, both gzip."""
    if metadata is None:
        metadata = [{"id": "10.1234/a", "attributes": {"url": "https://example.org/a"}}]
    if states is None:
        states = [("10.1234/a", "findable")]
    write_full_datacite_release(
        root,
        month,
        [
            (part_path(month, 0), gzip_bytes(metadata_jsonl(metadata))),
            (csv_path(month), gzip_bytes(state_csv(states))),
        ],
        transfer_complete=transfer_complete,
    )


@pytest.fixture
def full_datacite_release(tmp_path: Path) -> Path:
    """A complete mirrored release at `<tmp_path>/mnt/datacite-monthly-data-file-2026-07`."""
    root = tmp_path / "mnt" / f"datacite-monthly-data-file-{FULL_RELEASE_MONTH}"
    make_full_datacite_release(root)
    return root
