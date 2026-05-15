"""
Locust load test — Asset Manager Upload API
============================================
Target   : POST /v1/files/upload
Base URL : https://test-apigateway.erosuniverse.com
Image    : D:\\asset manager\\imgtest.jpeg  (loaded once into memory at startup)
 
Run (web UI — single machine):
    python -m locust --host https://test-apigateway.erosuniverse.com --web-port 8089
 
Run (headless — quick ramp to target):
    python -m locust -f locustfile_asset_manager.py \
        --host https://test-apigateway.erosuniverse.com \
        --headless --users 500 --spawn-rate 50 --run-time 5m
 
Run (distributed — master + 4 workers for 10 000 users):
    # Terminal 1 — master
    python -m locust -f locustfile_asset_manager.py --master \
        --host https://test-apigateway.erosuniverse.com --web-port 8089
 
    # Terminals 2-5 — each worker
    python -m locust -f locustfile_asset_manager.py --worker \
        --master-host 127.0.0.1
 
Recommended ramp stages (increase one stage at a time; watch Failures tab):
    Stage 1 →   100 users, spawn rate  20  — baseline / warm-up
    Stage 2 →   500 users, spawn rate  50  — light load
    Stage 3 →  1000 users, spawn rate 100  — medium load
    Stage 4 →  2000 users, spawn rate 200  — high load
    Stage 5 →  5000 users, spawn rate 500  — stress
    Stage 6 → 10000 users, spawn rate 1000 — target (distributed mode required)
 
Stop / add pods if error rate exceeds 1 % at any stage before moving to the next.
"""
 
from __future__ import annotations
 
import io
import json
import logging
import uuid
from typing import ClassVar
 
from locust import HttpUser, between, constant_pacing, events, task
from locust.env import Environment
 
# ---------------------------------------------------------------------------
# Logging — WARNING level only; avoids log spam under 1000 concurrent users
# ---------------------------------------------------------------------------
logger = logging.getLogger("asset_upload_test")
logging.basicConfig(level=logging.WARNING)
 
# ---------------------------------------------------------------------------
# Hardcoded configuration — no .env, no os.getenv
# ---------------------------------------------------------------------------
BEARER_TOKEN   = "Z0FBQUFBQnBabENPZnJienRjY3lNWHRmWk1meTlyRmZ2QmM1clBYSVVaQlhSOFRMVllxWDBKaXJla1Nvb2hPVW8yNW1va09uWlZRaEVZUVpXZ2JCM0c0aVdaa3JTLU50TXlUVGVYYVdET1Z4eWd5cXRkMlQxckhsb19Md1Bsd0ktLUhoWGs5WlJ3Skk="
UPLOAD_ENDPOINT = "/asset-manager/v1/files/upload"
BUCKET_NAME     = "erosuniversetest01"
FOLDER_PATH     = "uploads/locust/"
IMAGE_PATH      = r"D:\asset manager\imgtest.jpeg"
 
# ---------------------------------------------------------------------------
# Load the real JPEG once at module import time.
# All 1000 virtual users share the exact same bytes object — zero extra memory.
# ---------------------------------------------------------------------------
with open(IMAGE_PATH, "rb") as _f:
    _JPEG_BYTES: bytes = _f.read()
 
print(
    f"[locust] Image loaded: {IMAGE_PATH}  "
    f"({len(_JPEG_BYTES):,} bytes / {len(_JPEG_BYTES) / 1024:.1f} KB)"
)
 
# ---------------------------------------------------------------------------
# Auth header — class-level constant, built once
# ---------------------------------------------------------------------------
_AUTH_HEADERS: dict[str, str] = {
    "Authorization": f"Bearer {BEARER_TOKEN}",
}
 
 
# ---------------------------------------------------------------------------
# Virtual User
# ---------------------------------------------------------------------------
 
class UploadUser(HttpUser):
    """
    Each virtual user loops continuously:
      pick JPEG bytes from memory → POST multipart → validate → wait → repeat
 
    wait_time = between(0, 0.5):
      Low think time — allows each user to fire ~2-10 req/s depending on latency.
      With 1000 users @ 300 ms avg upload latency → ~3000 RPS.
      With 5000 users @ 300 ms avg upload latency → ~10 000+ RPS.
 
    For single-pod capacity testing use between(0, 0) (zero wait).
    For realistic prod simulation use between(1, 3).
    """
 
    wait_time = between(0, 0.5)  # low think-time → max RPS per virtual user
 
    # Shared across every instance — no per-instance overhead
    _headers: ClassVar[dict[str, str]] = _AUTH_HEADERS
 
    # ------------------------------------------------------------------ #
    # Task                                                                 #
    # ------------------------------------------------------------------ #
 
    @task
    def upload_image(self) -> None:
        """
        Uploads imgtest.jpeg with a fresh UUID filename on every call.
 
        Performance notes:
        - _JPEG_BYTES is a module-level bytes object → no disk I/O here.
        - io.BytesIO wraps the same buffer without copying it.
        - UUID filename keeps each upload unique (prevents server dedup cache).
        - catch_response=True allows fine-grained pass/fail marking.
        """
        file_name = f"{uuid.uuid4().hex}.jpeg"
 
        files = {
            "file": (file_name, io.BytesIO(_JPEG_BYTES), "image/jpeg"),
        }
        data = {
            "file_name":   file_name,
            "bucket_name": BUCKET_NAME,
            "folder_path": FOLDER_PATH,
        }
 
        with self.client.post(
            UPLOAD_ENDPOINT,
            files=files,
            data=data,
            headers=self._headers,
            name="upload_image",       # single label in Locust UI charts
            catch_response=True,
        ) as resp:
            _validate(resp, file_name)
 
 
# ---------------------------------------------------------------------------
# Response validator — kept outside the task to stay lean
# ---------------------------------------------------------------------------
 
def _validate(resp, file_name: str) -> None:
    if resp.status_code not in (200, 201, 202):
        resp.failure(
            f"HTTP {resp.status_code} | {file_name} | {resp.text[:150]}"
        )
        return
 
    try:
        body: dict = resp.json()
    except (json.JSONDecodeError, ValueError):
        resp.failure(f"Non-JSON | {file_name} | {resp.text[:150]}")
        return
 
    has_url = bool(
        body.get("signed_url_inline")
        or body.get("signed_url_download")
        or body.get("s3_url")
        or body.get("gcs_url")
    )
    if has_url:
        resp.success()
    else:
        resp.failure(
            f"No URL in response | {file_name} | {str(body)[:200]}"
        )
 
 
# ---------------------------------------------------------------------------
# End-of-test summary
# ---------------------------------------------------------------------------
 
@events.quitting.add_listener
def _summary(environment: Environment, **_kw) -> None:
    s = environment.stats.total
    sep = "=" * 48
    print(
        f"\n{sep}"
        f"\n  Asset Manager -- Upload Load Test Summary"
        f"\n{sep}"
        f"\n  Total requests  : {s.num_requests:,}"
        f"\n  Failures        : {s.num_failures:,}"
        f"\n  Failure rate    : {s.fail_ratio * 100:.1f}%"
        f"\n  Current RPS     : {s.current_rps:.1f}"
        f"\n  Median (ms)     : {s.median_response_time}"
        f"\n  95th pct (ms)   : {s.get_response_time_percentile(0.95)}"
        f"\n  99th pct (ms)   : {s.get_response_time_percentile(0.99)}"
        f"\n{sep}\n"
    )
 