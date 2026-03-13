from __future__ import annotations

import json
import ssl
from urllib.parse import urlencode
from urllib.request import Request, urlopen


class HttpClient:
    def __init__(
        self,
        timeout_sec: int = 8,
        default_headers: dict[str, str] | None = None,
        verify_ssl: bool = False,
    ) -> None:
        self.timeout_sec = timeout_sec
        self.default_headers = default_headers or {
            "User-Agent": "Mozilla/5.0",
            "Accept": "application/json,text/plain,*/*",
        }
        self.ssl_context = ssl.create_default_context() if verify_ssl else ssl._create_unverified_context()

    def _request(self, url: str, *, method: str = "GET", headers: dict[str, str] | None = None, body: bytes | None = None) -> str:
        merged_headers = dict(self.default_headers)
        if headers:
            merged_headers.update(headers)
        request = Request(url, data=body, headers=merged_headers, method=method)
        with urlopen(request, timeout=self.timeout_sec, context=self.ssl_context) as response:
            charset = response.headers.get_content_charset() or "utf-8"
            return response.read().decode(charset, errors="ignore")

    def get_text(self, url: str, *, headers: dict[str, str] | None = None, params: dict[str, str] | None = None) -> str:
        target = url
        if params:
            target = f"{url}?{urlencode(params)}"
        return self._request(target, headers=headers)

    def get_json(self, url: str, *, headers: dict[str, str] | None = None, params: dict[str, str] | None = None) -> dict:
        return json.loads(self.get_text(url, headers=headers, params=params))

    def post_json(self, url: str, payload: dict, *, headers: dict[str, str] | None = None) -> str:
        merged_headers = {"Content-Type": "application/json"}
        if headers:
            merged_headers.update(headers)
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        return self._request(url, method="POST", headers=merged_headers, body=body)
