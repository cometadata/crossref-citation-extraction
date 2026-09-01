import json
from datetime import UTC, datetime

import pytest
import requests

from citations_pipeline.snapshot import (
    CrossrefCredentialFormatError,
    SnapshotAuthError,
    SnapshotError,
    SnapshotIdentity,
    crossref_auth_headers,
    download_snapshot,
    preflight_crossref_access,
    resolve_snapshot,
)

LEGACY_JWT = "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJ0ZXN0In0.signature"


def identity() -> SnapshotIdentity:
    return SnapshotIdentity(
        snapshot_id="2026-08",
        alias_url="https://example.test/latest",
        effective_url="https://example.test/2026/08/all.json.tar.gz",
        content_length=6,
        etag='"v1"',
        last_modified=None,
        content_disposition=None,
    )


def test_legacy_jwt_is_sent_verbatim() -> None:
    assert crossref_auth_headers(LEGACY_JWT) == {
        "Crossref-Plus-API-Token": LEGACY_JWT
    }


def test_modern_api_key_receives_documented_bearer_prefix() -> None:
    assert crossref_auth_headers("modern-key") == {
        "Crossref-Plus-API-Token": "Bearer modern-key"
    }


@pytest.mark.parametrize(
    "credential",
    [
        f"Crossref-Plus-API-Token: {LEGACY_JWT}",
        f"Bearer {LEGACY_JWT}",
        f" {LEGACY_JWT}",
        f"{LEGACY_JWT}\nInjected: value",
    ],
)
def test_preformatted_or_unsafe_credential_is_rejected(credential: str) -> None:
    with pytest.raises(CrossrefCredentialFormatError):
        crossref_auth_headers(credential)


def test_resolve_snapshot_requires_dated_effective_url(fake_session) -> None:
    fake_session.get_response(
        status=206,
        url="https://api.crossref.org/snapshots/monthly/2026/08/all.json.tar.gz",
        headers={
            "Content-Range": "bytes 0-0/250000000000",
            "ETag": '"snapshot-etag"',
            "Last-Modified": "Wed, 05 Aug 2026 00:00:00 GMT",
        },
        body=b"x",
    )
    identity = resolve_snapshot(fake_session, "secret-value")
    assert identity.snapshot_id == "2026-08"
    assert identity.content_length == 250_000_000_000
    assert identity.resume_validator == '"snapshot-etag"'
    assert fake_session.last_headers == {
        "Crossref-Plus-API-Token": "Bearer secret-value",
        "Range": "bytes=0-0",
    }


def test_resolve_snapshot_uses_one_byte_get_when_signed_head_is_forbidden(
    fake_session,
) -> None:
    signed_url = (
        "https://s3.amazonaws.com/api-snapshots-production-crossref/"
        "monthly/2026/07/all.json.tar.gz?X-Amz-Signature=redacted"
    )
    fake_session.head_response(
        status=403,
        url=signed_url,
        headers={},
    )
    fake_session.get_response(
        status=206,
        url=signed_url,
        headers={
            "Content-Range": "bytes 0-0/280078964653",
            "ETag": '"snapshot-etag"',
            "Last-Modified": "Wed, 05 Aug 2026 00:00:00 GMT",
        },
        body=b"x",
    )

    snapshot = resolve_snapshot(fake_session, "secret-value")

    assert snapshot.snapshot_id == "2026-07"
    assert snapshot.content_length == 280_078_964_653
    assert len(fake_session.calls) == 1
    assert fake_session.calls[0].method == "GET"
    assert fake_session.calls[0].headers["Range"] == "bytes=0-0"


def test_resolve_snapshot_probes_dated_route_when_alias_does_not_redirect(
    fake_session,
) -> None:
    common = {
        "Content-Range": "bytes 0-0/250000000000",
        "ETag": '"snapshot-etag"',
        "Last-Modified": "Wed, 05 Aug 2026 00:00:00 GMT",
    }
    fake_session.get_response(
        status=206,
        url="https://api.crossref.org/snapshots/monthly/latest/all.json.tar.gz",
        headers=common,
        body=b"x",
    )
    fake_session.get_response(
        status=206,
        url="https://api.crossref.org/snapshots/monthly/2026/08/all.json.tar.gz",
        headers=common,
        body=b"x",
    )
    identity = resolve_snapshot(
        fake_session,
        "secret-value",
        now=datetime(2026, 8, 26, tzinfo=UTC),
    )
    assert identity.snapshot_id == "2026-08"
    assert identity.effective_url.endswith("/2026/08/all.json.tar.gz")


def test_resolve_snapshot_redacts_rejected_token(fake_session) -> None:
    fake_session.get_response(
        status=401,
        url="https://api.crossref.org/error",
        headers={},
    )
    with pytest.raises(SnapshotAuthError) as error:
        resolve_snapshot(fake_session, "secret-value")
    assert "secret-value" not in str(error.value)
    assert "401" in str(error.value)


def test_preflight_proves_plus_pool_and_one_byte_snapshot_access(
    fake_session,
) -> None:
    fake_session.get_response(
        status=200,
        url="https://api.crossref.org/works?rows=0",
        headers={"x-api-pool": "plus-array"},
        body=b'{"status":"ok"}',
    )
    fake_session.get_response(
        status=206,
        url="https://snapshot-download.test/signed-object",
        headers={"Content-Range": "bytes 0-0/250000000000"},
        body=b"x",
    )
    result = preflight_crossref_access(fake_session, LEGACY_JWT)
    assert result.rest_status == 200
    assert result.api_pool == "plus-array"
    assert result.snapshot_status == 206
    assert result.content_length == 250_000_000_000
    assert fake_session.calls[0].headers["Crossref-Plus-API-Token"] == LEGACY_JWT
    assert fake_session.calls[1].headers["Range"] == "bytes=0-0"


def test_download_writes_and_hashes_complete_payload(fake_session, tmp_path) -> None:
    fake_session.get_response(
        status=200,
        url=identity().effective_url,
        headers={},
        body=b"abcdef",
    )
    result = download_snapshot(
        fake_session,
        identity(),
        "secret-value",
        tmp_path / "archive.part",
        tmp_path / "resume.json",
        chunk_size=2,
    )
    assert result.size == 6
    assert (
        result.sha256
        == "bef57ec7f53a6d40beb640a780a639c83bc29ac8a9816f1fc6c5c6dcd93c4721"
    )
    assert (tmp_path / "archive.part").read_bytes() == b"abcdef"


def test_download_reports_throttled_transfer_and_checksum_progress(
    fake_session, tmp_path
) -> None:
    fake_session.get_response(
        status=200,
        url=identity().effective_url,
        headers={},
        body=b"abcdef",
    )
    ticks = iter((0.0, 30.0, 60.0, 90.0, 100.0, 130.0, 160.0, 190.0))
    events: list[dict[str, object]] = []

    download_snapshot(
        fake_session,
        identity(),
        "secret-value",
        tmp_path / "archive.part",
        tmp_path / "resume.json",
        chunk_size=2,
        progress=events.append,
        monotonic=lambda: next(ticks),
        progress_interval=60.0,
    )

    transfer = [event for event in events if event["phase"] == "download_snapshot"]
    assert [event["bytes_completed"] for event in transfer] == [0, 4, 6]
    assert [event["percent"] for event in transfer] == [0.0, 66.67, 100.0]
    assert transfer[1]["bytes_per_second"] == pytest.approx(4 / 60)
    assert transfer[1]["eta_seconds"] == pytest.approx(30.0)
    assert all(event["resumed_from_bytes"] == 0 for event in transfer)

    checksum = [event for event in events if event["phase"] == "checksum_snapshot"]
    assert [event["bytes_completed"] for event in checksum] == [0, 4, 6]
    assert [event["percent"] for event in checksum] == [0.0, 66.67, 100.0]


def test_download_heartbeat_continues_while_stream_waits(fake_session, tmp_path) -> None:
    fake_session.get_response(
        status=200,
        url=identity().effective_url,
        headers={},
        body=b"abcdef",
        delay_before_body=0.05,
    )
    events: list[dict[str, object]] = []

    download_snapshot(
        fake_session,
        identity(),
        "secret-value",
        tmp_path / "archive.part",
        tmp_path / "resume.json",
        progress=events.append,
        progress_interval=0.01,
    )

    waiting_heartbeats = [
        event
        for event in events
        if event["phase"] == "download_snapshot"
        and event["bytes_completed"] == 0
    ]
    assert len(waiting_heartbeats) >= 2


def test_resume_uses_range_and_if_range(fake_session, tmp_path) -> None:
    (tmp_path / "archive.part").write_bytes(b"abc")
    (tmp_path / "resume.json").write_text(json.dumps(identity().to_dict()))
    fake_session.get_response(
        status=206,
        url=identity().effective_url,
        headers={"Content-Range": "bytes 3-5/6"},
        body=b"def",
    )
    download_snapshot(
        fake_session,
        identity(),
        "secret-value",
        tmp_path / "archive.part",
        tmp_path / "resume.json",
    )
    assert fake_session.last_headers["Range"] == "bytes=3-"
    assert fake_session.last_headers["If-Range"] == '"v1"'


def test_ignored_range_restarts_instead_of_appending(fake_session, tmp_path) -> None:
    (tmp_path / "archive.part").write_bytes(b"abc")
    (tmp_path / "resume.json").write_text(json.dumps(identity().to_dict()))
    fake_session.get_response(
        status=200,
        url=identity().effective_url,
        headers={},
        body=b"abcdef",
    )
    result = download_snapshot(
        fake_session,
        identity(),
        "secret-value",
        tmp_path / "archive.part",
        tmp_path / "resume.json",
    )
    assert result.size == 6
    assert (tmp_path / "archive.part").read_bytes() == b"abcdef"


def test_preflight_retries_transient_rest_failures_with_bounded_delays(
    fake_session,
) -> None:
    fake_session.get_response(
        status=503,
        url="https://api.crossref.org/works?rows=0",
        headers={},
    )
    fake_session.get_error(requests.ConnectionError("temporary"))
    fake_session.get_response(
        status=200,
        url="https://api.crossref.org/works?rows=0",
        headers={"x-api-pool": "plus-array"},
    )
    fake_session.get_response(
        status=206,
        url="https://snapshot-download.test/signed-object",
        headers={"Content-Range": "bytes 0-0/6"},
        body=b"x",
    )
    delays: list[float] = []
    result = preflight_crossref_access(
        fake_session,
        LEGACY_JWT,
        sleep=delays.append,
        retry_delays=(0.25, 0.5),
    )
    assert result.content_length == 6
    assert delays == [0.25, 0.5]


def test_preflight_retries_transient_snapshot_range_failures(fake_session) -> None:
    fake_session.get_response(
        status=200,
        url="https://api.crossref.org/works?rows=0",
        headers={"x-api-pool": "plus-array"},
    )
    fake_session.get_response(
        status=500,
        url="https://api.crossref.org/snapshots/monthly/latest/all.json.tar.gz",
        headers={},
    )
    fake_session.get_error(requests.Timeout("temporary"))
    fake_session.get_response(
        status=206,
        url="https://snapshot-download.test/signed-object",
        headers={"Content-Range": "bytes 0-0/6"},
        body=b"x",
    )
    delays: list[float] = []
    result = preflight_crossref_access(
        fake_session,
        LEGACY_JWT,
        sleep=delays.append,
        retry_delays=(0.25, 0.5),
    )
    assert result.snapshot_status == 206
    assert delays == [0.25, 0.5]


def test_resolve_snapshot_retries_transient_range_failures(fake_session) -> None:
    fake_session.get_response(
        status=429,
        url="https://api.crossref.org/snapshots/monthly/latest/all.json.tar.gz",
        headers={},
    )
    fake_session.get_error(requests.Timeout("temporary"))
    fake_session.get_response(
        status=206,
        url="https://api.crossref.org/snapshots/monthly/2026/08/all.json.tar.gz",
        headers={"Content-Range": "bytes 0-0/6", "ETag": '"v1"'},
        body=b"x",
    )
    delays: list[float] = []
    result = resolve_snapshot(
        fake_session,
        LEGACY_JWT,
        sleep=delays.append,
        retry_delays=(0.25, 0.5),
    )
    assert result.snapshot_id == "2026-08"
    assert delays == [0.25, 0.5]


def test_download_retries_transient_get_failures_without_corrupting_output(
    fake_session, tmp_path
) -> None:
    fake_session.get_response(
        status=503,
        url=identity().effective_url,
        headers={},
    )
    fake_session.get_error(requests.ConnectionError("temporary"))
    fake_session.get_response(
        status=200,
        url=identity().effective_url,
        headers={},
        body=b"abcdef",
    )
    delays: list[float] = []
    result = download_snapshot(
        fake_session,
        identity(),
        "secret-value",
        tmp_path / "archive.part",
        tmp_path / "resume.json",
        sleep=delays.append,
        retry_delays=(0.25, 0.5),
    )
    assert result.size == 6
    assert delays == [0.25, 0.5]
    assert (tmp_path / "archive.part").read_bytes() == b"abcdef"


def test_authentication_failure_is_not_retried(fake_session) -> None:
    fake_session.get_response(
        status=401,
        url="https://api.crossref.org/error",
        headers={},
    )
    delays: list[float] = []
    with pytest.raises(SnapshotAuthError):
        resolve_snapshot(
            fake_session,
            "secret-value",
            sleep=delays.append,
            retry_delays=(0.25, 0.5),
        )
    assert delays == []
    assert len(fake_session.calls) == 1


def test_final_size_mismatch_is_not_retried(fake_session, tmp_path) -> None:
    fake_session.get_response(
        status=200,
        url=identity().effective_url,
        headers={},
        body=b"abcde",
    )
    delays: list[float] = []
    with pytest.raises(SnapshotError, match="size mismatch"):
        download_snapshot(
            fake_session,
            identity(),
            "secret-value",
            tmp_path / "archive.part",
            tmp_path / "resume.json",
            sleep=delays.append,
            retry_delays=(0.25, 0.5),
        )
    assert delays == []
    assert len(fake_session.calls) == 1
