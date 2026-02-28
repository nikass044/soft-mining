from __future__ import annotations

from dataclasses import dataclass

import httpx


@dataclass
class HttpResponse:
    status_code: int
    headers: dict[str, str]
    body: bytes

    def json(self) -> dict | list:
        import json
        return json.loads(self.body)


class HttpTransport:
    GITHUB_API_BASE = "https://api.github.com"

    def __init__(self, token: str):
        headers = {
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
        }
        if token:
            headers["Authorization"] = f"Bearer {token}"
        self._client = httpx.Client(
            base_url=self.GITHUB_API_BASE,
            headers=headers,
            timeout=30.0,
        )

    def request(
        self,
        method: str,
        url: str,
        params: dict | None = None,
        json_body: dict | None = None,
    ) -> HttpResponse:
        response = self._client.request(method, url, params=params, json=json_body)
        return HttpResponse(
            status_code=response.status_code,
            headers=dict(response.headers),
            body=response.content,
        )

    def close(self) -> None:
        self._client.close()
