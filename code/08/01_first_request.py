# One request and its response, as the bytes that cross the wire.
import hashlib
import json
import os
import socket
from pathlib import Path
from urllib.parse import urlparse

from meridian.sources import Clock, frontline
from groundwork.config import SANDBOX  # noqa: F401  (loads .env)

base = urlparse(frontline.serve(Clock("2025-12-02 09:00")))
token = os.environ["FRONTLINE_TOKEN"]
shown = "sha256:" + hashlib.sha256(token.encode()).hexdigest()[:12]
target = ("/api/v2/tickets?updated_since=2025-12-01T00:00:00Z"
          "&page=1&per_page=2")
request = (f"GET {target} HTTP/1.1\r\n"
           f"Host: {base.netloc}\r\n"
           f"Authorization: Bearer {token}\r\n"
           "Accept: application/json\r\n"
           "Connection: close\r\n\r\n")

with socket.create_connection((base.hostname, base.port)) as s:
    s.sendall(request.encode("ascii"))
    reply = b""
    while chunk := s.recv(65536):
        reply += chunk

print("--- sent (the first line wrapped to fit)")
for line in request.split("\r\n")[:4]:
    line = (line.replace(token, f"<token {shown}>")
                .replace(base.netloc, "127.0.0.1:<port>"))
    print(line.replace("&page", "\n    &page"))
head, _, body = reply.partition(b"\r\n\r\n")
print("--- received")
lines = head.decode("ascii").split("\r\n")
print(lines[0])
headers = dict(h.split(": ", 1) for h in lines[1:])
for name in ("Content-Type", "Content-Length"):
    print(f"{name}: {headers[name]}")
print(f"(and {len(headers) - 2} more: Server and Date)")
page = json.loads(body)
print(f"body: {len(body)} bytes of JSON, keys {list(page)}")
for t in page["tickets"]:
    print(f"  {t['id']}  updated {t['updated_at']}  {t['status']}")
print(f"  meta {page['meta']}")

# The exchange, token replaced, for the chapter's annotated figure.
Path(__file__).with_suffix(".json").write_text(json.dumps({
    "request": [ln.replace(token, "<token>")
                .replace(base.netloc, "127.0.0.1:<port>")
                for ln in request.split("\r\n")[:5]],
    "status": lines[0], "content_type": headers["Content-Type"],
    "content_length": headers["Content-Length"], "body": page,
}, indent=1, ensure_ascii=False) + "\n", encoding="utf-8")
