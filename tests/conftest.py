from __future__ import annotations

import time
from collections import deque
from dataclasses import dataclass
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
