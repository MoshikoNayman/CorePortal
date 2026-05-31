"""Minimal in-process ASGI caller for tests.

Starlette's TestClient needs httpx, which is not a project dependency. This
tiny helper drives the ASGI app directly so route tests have no extra deps and
make no real network calls.
"""

from __future__ import annotations

import asyncio
from urllib.parse import urlsplit


def request(app, path: str, method: str = "GET") -> tuple[int, dict, str]:
    """Call ``app`` for ``path`` and return (status, headers, body_text)."""
    parts = urlsplit(path)

    async def _run():
        scope = {
            "type": "http",
            "method": method,
            "path": parts.path,
            "raw_path": parts.path.encode(),
            "query_string": parts.query.encode(),
            "headers": [],
            "client": ("127.0.0.1", 12345),
            "server": ("127.0.0.1", 8000),
            "scheme": "http",
        }

        async def receive():
            return {"type": "http.request", "body": b"", "more_body": False}

        status = {"code": None}
        headers: dict[str, str] = {}
        chunks: list[bytes] = []

        async def send(message):
            if message["type"] == "http.response.start":
                status["code"] = message["status"]
                headers.update(
                    {k.decode().lower(): v.decode() for k, v in message.get("headers", [])}
                )
            elif message["type"] == "http.response.body":
                chunks.append(message.get("body", b""))

        await app(scope, receive, send)
        return status["code"], headers, b"".join(chunks).decode("utf-8", "replace")

    return asyncio.run(_run())
