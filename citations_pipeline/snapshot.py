from __future__ import annotations

import hashlib
import json
import re
import time
from collections.abc import Callable
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

import requests

from citations_pipeline.telemetry import ProgressTracker

REST_PLUS_URL = "https://api.crossref.org/works?rows=0"
LATEST_SNAPSHOT_URL = (
    "https://api.crossref.org/snapshots/monthly/latest/all.json.tar.gz"
)
COMPACT_JWT = re.compile(r"^[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+$")
CONTENT_RANGE = re.compile(r"^bytes 0-0/(?P<length>[1-9]\d*)$")
DATED_SNAPSHOT = re.compile(
    r"/monthly/(?P<year>\d{4})/(?P<month>\d{2})/all\.json\.tar\.gz$"
)
DEFAULT_RETRY_DELAYS = (1.0, 2.0, 4.0, 8.0, 16.0)


class SnapshotError(RuntimeError):
    pass


class SnapshotAuthError(SnapshotError):
    pass


class CrossrefCredentialFormatError(SnapshotAuthError):
    pass


@dataclass(frozen=True)
class CredentialPreflight:
    rest_status: int
    api_pool: str
    snapshot_status: int
    content_length: int


@dataclass(frozen=True)
class SnapshotIdentity:
    snapshot_id: str
    alias_url: str
    effective_url: str
    content_length: int
    etag: str | None
    last_modified: str | None
    content_disposition: str | None

    @property
    def resume_validator(self) -> str | None:
        return self.etag or self.last_modified

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class DownloadResult:
    path: Path
    size: int
    sha256: str


def _request_with_retry(
    request: Callable[[], Any],
    *,
    operation: str,
    sleep: Callable[[float], None],
    retry_delays: tuple[float, ...],
) -> Any:
    for attempt in range(len(retry_delays) + 1):
        try:
            response = request()
        except (requests.ConnectionError, requests.Timeout) as error:
            if attempt == len(retry_delays):
                raise SnapshotError(f"{operation} failed after retries") from error
        else:
            transient = response.status_code == 429 or response.status_code >= 500
            if not transient:
                return response
            close = getattr(response, "close", None)
            if close is not None:
                close()
            if attempt == len(retry_delays):
                raise SnapshotError(
                    f"{operation} failed after retries: HTTP {response.status_code}"
                )
        sleep(retry_delays[attempt])
    raise AssertionError("retry loop exhausted unexpectedly")


def crossref_auth_headers(credential: str) -> dict[str, str]:
    lowered = credential.casefold()
    unsafe = (
        not credential
        or credential != credential.strip()
        or "\r" in credential
        or "\n" in credential
        or lowered.startswith(("crossref-plus-api-token:", "bearer "))
    )
    if unsafe:
        raise CrossrefCredentialFormatError(
            "CROSSREF_API_TOKEN must contain credential material only"
        )
    value = credential if COMPACT_JWT.fullmatch(credential) else f"Bearer {credential}"
    return {"Crossref-Plus-API-Token": value}


def preflight_crossref_access(
    session: requests.Session,
    credential: str,
    rest_url: str = REST_PLUS_URL,
    snapshot_url: str = LATEST_SNAPSHOT_URL,
    timeout: tuple[float, float] = (15.0, 60.0),
    *,
    sleep: Callable[[float], None] = time.sleep,
    retry_delays: tuple[float, ...] = DEFAULT_RETRY_DELAYS,
) -> CredentialPreflight:
    headers = crossref_auth_headers(credential)
    rest = _request_with_retry(
        lambda: session.get(
            rest_url,
            headers=headers,
            allow_redirects=True,
            timeout=timeout,
        ),
        operation="Crossref REST preflight",
        sleep=sleep,
        retry_delays=retry_delays,
    )
    if rest.status_code in {401, 403}:
        raise SnapshotAuthError(
            f"Crossref REST authentication failed: HTTP {rest.status_code}"
        )
    api_pool = rest.headers.get("x-api-pool", "")
    if rest.status_code != 200 or api_pool != "plus-array":
        raise SnapshotAuthError(
            "Crossref REST preflight did not enter the Metadata Plus pool"
        )
    range_headers = {**headers, "Range": "bytes=0-0"}
    snapshot_response = _request_with_retry(
        lambda: session.get(
            snapshot_url,
            headers=range_headers,
            stream=True,
            allow_redirects=True,
            timeout=timeout,
        ),
        operation="Crossref snapshot preflight",
        sleep=sleep,
        retry_delays=retry_delays,
    )
    with snapshot_response as snapshot:
        if snapshot.status_code in {401, 403}:
            raise SnapshotAuthError(
                "Crossref snapshot authentication failed: "
                f"HTTP {snapshot.status_code}"
            )
        match = CONTENT_RANGE.fullmatch(snapshot.headers.get("Content-Range", ""))
        if snapshot.status_code != 206 or match is None:
            raise SnapshotError(
                "Crossref snapshot preflight did not return a one-byte range"
            )
        payload = b"".join(snapshot.iter_content(chunk_size=1))
        if len(payload) != 1:
            raise SnapshotError(
                "Crossref snapshot preflight returned the wrong byte count"
            )
        return CredentialPreflight(
            rest_status=rest.status_code,
            api_pool=api_pool,
            snapshot_status=snapshot.status_code,
            content_length=int(match.group("length")),
        )


def resolve_snapshot(
    session: requests.Session,
    credential: str,
    url: str = LATEST_SNAPSHOT_URL,
    timeout: tuple[float, float] = (15.0, 60.0),
    now: datetime | None = None,
    *,
    sleep: Callable[[float], None] = time.sleep,
    retry_delays: tuple[float, ...] = DEFAULT_RETRY_DELAYS,
) -> SnapshotIdentity:
    headers = crossref_auth_headers(credential)
    range_headers = {**headers, "Range": "bytes=0-0"}

    def request_range(snapshot_url: str, operation: str) -> tuple[Any, int]:
        range_response = _request_with_retry(
            lambda: session.get(
                snapshot_url,
                headers=range_headers,
                stream=True,
                allow_redirects=True,
                timeout=timeout,
            ),
            operation=operation,
            sleep=sleep,
            retry_delays=retry_delays,
        )
        with range_response:
            if range_response.status_code in {401, 403}:
                raise SnapshotAuthError(
                    "Crossref snapshot authentication failed: "
                    f"HTTP {range_response.status_code}"
                )
            content_range = CONTENT_RANGE.fullmatch(
                range_response.headers.get("Content-Range", "")
            )
            if range_response.status_code != 206 or content_range is None:
                raise SnapshotError(
                    "Crossref snapshot identity request did not return a one-byte range"
                )
            payload = b"".join(range_response.iter_content(chunk_size=1))
            if len(payload) != 1:
                raise SnapshotError(
                    "Crossref snapshot identity request returned the wrong byte count"
                )
            return range_response, int(content_range.group("length"))

    response, length = request_range(
        url,
        "Crossref snapshot identity request",
    )
    match = DATED_SNAPSHOT.search(urlsplit(response.url).path)
    dated_response = response
    if match is None:
        current = (now or datetime.now(UTC)).astimezone(UTC)
        candidates = [(current.year, current.month)]
        if current.month == 1:
            candidates.append((current.year - 1, 12))
        else:
            candidates.append((current.year, current.month - 1))
        dated_response = None
        for year, month in candidates:
            candidate_url = (
                f"https://api.crossref.org/snapshots/monthly/{year:04d}/{month:02d}/"
                "all.json.tar.gz"
            )
            candidate, candidate_length = request_range(
                candidate_url,
                "Crossref dated snapshot identity request",
            )
            same_etag = response.headers.get("ETag") and (
                response.headers.get("ETag") == candidate.headers.get("ETag")
            )
            same_fallback = (
                response.headers.get("Last-Modified") is not None
                and response.headers.get("Last-Modified")
                == candidate.headers.get("Last-Modified")
                and length == candidate_length
            )
            if same_etag or same_fallback:
                dated_response = candidate
                match = DATED_SNAPSHOT.search(urlsplit(candidate.url).path)
                break
        if dated_response is None or match is None:
            raise SnapshotError(
                "Crossref latest alias could not be matched to a dated snapshot"
            )
    return SnapshotIdentity(
        snapshot_id=f"{match.group('year')}-{match.group('month')}",
        alias_url=url,
        effective_url=dated_response.url,
        content_length=length,
        etag=dated_response.headers.get("ETag"),
        last_modified=dated_response.headers.get("Last-Modified"),
        content_disposition=dated_response.headers.get("Content-Disposition"),
    )


def download_snapshot(
    session: requests.Session,
    identity: SnapshotIdentity,
    credential: str,
    part_path: Path,
    resume_metadata_path: Path,
    chunk_size: int = 8 * 1024 * 1024,
    *,
    progress: Callable[[dict[str, object]], None] | None = None,
    monotonic: Callable[[], float] = time.monotonic,
    progress_interval: float = 60.0,
    sleep: Callable[[float], None] = time.sleep,
    retry_delays: tuple[float, ...] = DEFAULT_RETRY_DELAYS,
) -> DownloadResult:
    part_path.parent.mkdir(parents=True, exist_ok=True)
    saved = None
    if resume_metadata_path.is_file():
        saved = json.loads(resume_metadata_path.read_text(encoding="utf-8"))
    reusable = saved == identity.to_dict() and identity.resume_validator is not None
    if not reusable:
        part_path.unlink(missing_ok=True)
    resume_metadata_path.write_text(
        json.dumps(identity.to_dict(), sort_keys=True) + "\n",
        encoding="utf-8",
    )
    offset = part_path.stat().st_size if part_path.exists() else 0
    headers = crossref_auth_headers(credential)
    if offset:
        headers["Range"] = f"bytes={offset}-"
        headers["If-Range"] = identity.resume_validator or ""
    download_response = _request_with_retry(
        lambda: session.get(
            identity.effective_url,
            headers=headers,
            stream=True,
            allow_redirects=True,
            timeout=(15.0, 300.0),
        ),
        operation="Crossref snapshot download",
        sleep=sleep,
        retry_delays=retry_delays,
    )
    with download_response as response:
        if response.status_code in {401, 403}:
            raise SnapshotAuthError(
                "Crossref snapshot download rejected: "
                f"HTTP {response.status_code}"
            )
        if offset and response.status_code == 200:
            offset = 0
            part_path.unlink(missing_ok=True)
        elif offset and response.status_code != 206:
            raise SnapshotError(
                f"Crossref range request failed: HTTP {response.status_code}"
            )
        elif not offset and response.status_code != 200:
            raise SnapshotError(
                f"Crossref snapshot download failed: HTTP {response.status_code}"
            )
        mode = "ab" if offset else "wb"
        with ProgressTracker(
            phase="download_snapshot",
            total_bytes=identity.content_length,
            progress=progress,
            resumed_from_bytes=offset,
            interval=progress_interval,
            monotonic=monotonic,
        ) as tracker, part_path.open(mode) as output:
            for chunk in response.iter_content(chunk_size=chunk_size):
                if chunk:
                    output.write(chunk)
                    tracker.advance(len(chunk))
    size = part_path.stat().st_size
    if size != identity.content_length:
        raise SnapshotError(
            f"Crossref snapshot size mismatch: {size} != {identity.content_length}"
        )
    digest = hashlib.sha256()
    with ProgressTracker(
        phase="checksum_snapshot",
        total_bytes=size,
        progress=progress,
        interval=progress_interval,
        monotonic=monotonic,
    ) as tracker, part_path.open("rb") as source:
        while chunk := source.read(chunk_size):
            digest.update(chunk)
            tracker.advance(len(chunk))
    return DownloadResult(part_path, size, digest.hexdigest())
