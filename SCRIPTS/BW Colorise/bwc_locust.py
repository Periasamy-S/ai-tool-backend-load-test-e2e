import os
import csv
import json
import time
import random
import logging
import requests
import gevent
from collections import Counter, defaultdict
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv
from gevent import spawn
from gevent.lock import Semaphore
from locust import HttpUser, task, between, events

# ── Paths ──────────────────────────────────────────────────────────────────────
_HERE     = Path(__file__).parent
_ROOT     = _HERE.parents[1]
_INPUTS   = _ROOT / "INPUTS"
_OUTPUTS  = _ROOT / "OUTPUTS"
TOOL_NAME = "BW Colorise"

load_dotenv(_HERE / ".env")
load_dotenv(_ROOT / ".env")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ── Config ─────────────────────────────────────────────────────────────────────
SAVE_OUTPUT      = os.getenv("SAVE_OUTPUT", "false").strip().lower() == "true"
TASK_TIMEOUT     = int(os.getenv("TASK_TIMEOUT", "300"))
SLA_THRESHOLD_MS = int(os.getenv("SLA_THRESHOLD_MS", "120000"))
_FLUSH_EVERY     = int(os.getenv("CSV_FLUSH_EVERY", "10"))
_RUN_DIR         = None

# ── Observability globals ──────────────────────────────────────────────────────
TEST_START   = None
_active_jobs = 0
_jobs_lock   = Semaphore()
 
# ── Module-level inputs (loaded once at test_start, zero I/O during test) ──────
_IMAGES  = []
_PROMPTS = []

PRESETS = ["Natural", "Vibrant", "Warm", "Cool", "Classic",
           "Cinematic", "Retro", "Soft", "Dramatic"]
MODES   = ["Black&White", "Original", "Colorize"]

_MIME = {
    ".jpg": "image/jpeg", ".jpeg": "image/jpeg",
    ".png": "image/png",
}

# ── CSV ────────────────────────────────────────────────────────────────────────
_csv_records = []
_csv_lock    = Semaphore()

_CSV_FIELDS = [
    "timestamp", "user_id", "generation_id",
    "image_file", "prompt", "mode", "preset",
    "status", "issue",
    "post_ms", "queue_ms", "processing_ms", "total_ms",
    "failure_category", "sla_breach",
    "minute", "active_jobs",
    "output_url",
]


# ── Failure taxonomy ───────────────────────────────────────────────────────────
def _categorize(issue: str) -> str:
    if not issue:
        return ""
    l = issue.lower()
    if "timed out" in l or "timeout" in l:
        return "timeout"
    if "http" in l:
        return "http_error"
    if "connection" in l:
        return "connection"
    if "sse" in l:
        return "sse_closed"
    if "no generation_id" in l or "invalid json" in l or "invalid output" in l:
        return "validation"
    return "backend"


# ── Summary / analysis helpers ─────────────────────────────────────────────────
def _generate_summary(records: list[dict]) -> None:
    total     = len(records)
    completed = sum(1 for r in records if r["status"] == "completed")
    failed    = sum(1 for r in records if r["status"] == "failed")
    not_done  = sum(1 for r in records if r["status"] == "not_completed")
    sla_count = sum(1 for r in records if r.get("sla_breach"))

    done      = [r for r in records if r["status"] == "completed"]
    avg_queue = (sum(r.get("queue_ms", 0) for r in done) / len(done)) if done else 0
    avg_proc  = (sum(r.get("processing_ms", 0) for r in done) / len(done)) if done else 0
    bottleneck = "QUEUE BOTTLENECK" if avg_queue > avg_proc else "PROCESSING BOTTLENECK"
    pct_ok     = (completed / total * 100) if total else 0
    pct_sla    = (sla_count / completed * 100) if completed else 0

    logger.info(
        f"\n{'═' * 60}\n"
        f"  PERFORMANCE SUMMARY — {TOOL_NAME}\n"
        f"{'─' * 60}\n"
        f"  Total jobs        : {total}\n"
        f"  Completed         : {completed}  ({pct_ok:.1f}%)\n"
        f"  Failed            : {failed}\n"
        f"  Not completed     : {not_done}\n"
        f"  SLA breaches      : {sla_count}  ({pct_sla:.1f}% of completed)\n"
        f"{'─' * 60}\n"
        f"  Avg queue_ms      : {avg_queue:>8.0f} ms\n"
        f"  Avg processing_ms : {avg_proc:>8.0f} ms\n"
        f"  Bottleneck        : {bottleneck}\n"
        f"{'═' * 60}"
    )
    if total < 100:
        logger.warning(
            "Low sample size (<100 jobs) — "
            "percentile metrics may be unreliable."
        )


def _aggregate_by_minute(records: list[dict]) -> None:
    buckets: dict[int, list] = defaultdict(list)
    for r in records:
        buckets[r.get("minute", 0)].append(r)

    logger.info(f"\n{'─' * 60}\n  TIME-SERIES (per minute)\n{'─' * 60}")
    for minute in sorted(buckets):
        recs  = buckets[minute]
        done  = [r for r in recs if r["status"] == "completed"]
        q_avg = (sum(r.get("queue_ms", 0) for r in done) / len(done)) if done else 0
        p_avg = (sum(r.get("processing_ms", 0) for r in done) / len(done)) if done else 0
        logger.info(
            f"  Minute {minute:>3} → "
            f"jobs={len(recs):>3}  "
            f"queue={q_avg:>7.0f}ms  "
            f"processing={p_avg:>7.0f}ms"
        )
    logger.info(f"{'─' * 60}")


def _failure_breakdown(records: list[dict]) -> None:
    cats = Counter(
        r["failure_category"]
        for r in records
        if r["status"] in ("failed", "not_completed") and r.get("failure_category")
    )
    if not cats:
        logger.info("No failures to break down.")
        return
    logger.info(f"\n{'─' * 60}\n  FAILURE BREAKDOWN\n{'─' * 60}")
    for cat, count in cats.most_common():
        logger.info(f"  {cat:<22}: {count}")
    logger.info(f"{'─' * 60}")


# ── Input loaders ──────────────────────────────────────────────────────────────
def _load_lines(filepath) -> list[str]:
    p = Path(filepath)
    return [ln.strip() for ln in p.read_text(encoding="utf-8").splitlines()
            if ln.strip()] if p.exists() else []

def load_images(folder) -> list[tuple[str, bytes, str]]:
    result = []
    p = Path(folder)
    if not p.exists():
        return result
    for f in sorted(p.iterdir()):
        mime = _MIME.get(f.suffix.lower())
        if not (f.is_file() and mime):
            continue
        try:
            result.append((f.name, f.read_bytes(), mime))
        except OSError as exc:
            logger.warning(f"Could not read image {f.name}: {exc}")
    return result


# ── Output helpers ─────────────────────────────────────────────────────────────
def _valid_url(url: str) -> bool:
    return bool(url) and url.startswith(("http://", "https://"))

def _save_output(gid: str, url: str) -> None:
    if not SAVE_OUTPUT:
        return
    try:
        filename = url.split("?")[0].rsplit("/", 1)[-1] or f"{gid}.jpg"
        resp = requests.get(url, timeout=60)
        resp.raise_for_status()
        (_RUN_DIR / filename).write_bytes(resp.content)
    except Exception as exc:
        logger.warning(f"Output save failed [{gid}]: {exc}")


# ── CSV writers ────────────────────────────────────────────────────────────────
def _write_report(records: list[dict]) -> None:
    if _RUN_DIR is None:
        logger.error("_RUN_DIR not set — report.csv not written.")
        return
    csv_path = _RUN_DIR / "report.csv"
    try:
        with open(csv_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=_CSV_FIELDS)
            writer.writeheader()
            writer.writerows(records)
        logger.info(f"report.csv written — {len(records)} rows: {csv_path}")
    except OSError as exc:
        logger.error(f"Failed to write report.csv: {exc}")

def _write_errors(records: list[dict]) -> None:
    if _RUN_DIR is None:
        return
    failures = [r for r in records if r["status"] in ("failed", "not_completed")]
    if not failures:
        logger.info("errors.csv — no failures, file not written.")
        return
    err_path = _RUN_DIR / "errors.csv"
    try:
        with open(err_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=_CSV_FIELDS)
            writer.writeheader()
            writer.writerows(failures)
        logger.warning(f"errors.csv written — {len(failures)} failures: {err_path}")
    except OSError as exc:
        logger.error(f"Failed to write errors.csv: {exc}")


# ── User ───────────────────────────────────────────────────────────────────────
class ColorizeUser(HttpUser):
    host      = os.getenv("BASE_URL", "https://test-apigateway.erosuniverse.com")
    wait_time = between(1, 3)
    USER_IDS  = [u.strip() for u in os.getenv("USER_IDS", "").split(",") if u.strip()]

    def on_start(self) -> None:
        self._ready = False
        self._sess  = None
        if not self.USER_IDS:
            logger.error("USER_IDS not configured — stopping runner.")
            self.environment.runner.quit()
            return
        self._sess  = requests.Session()
        self._ready = True

    def on_stop(self) -> None:
        if self._sess:
            self._sess.close()

    # ── SSE follower ───────────────────────────────────────────────────
    # timing keys: "first_event", "completed"
    def _follow_sse(self, gid: str, timing: dict) -> tuple[str, str]:
        url = f"{self.host}/aitools/jobs/{gid}/stream"
        logger.info(f"[{gid}] SSE follow start")
        try:
            with self._sess.get(
                url,
                headers={"Accept": "text/event-stream"},
                stream=True,
                timeout=None,
            ) as resp:
                if resp.status_code != 200:
                    return "", f"SSE HTTP {resp.status_code}"
                for raw in resp.iter_lines():
                    line = (raw.decode("utf-8") if isinstance(raw, bytes)
                            else raw).strip()
                    if not line.startswith("data:"):
                        continue
                    try:
                        event = json.loads(line[5:].strip())
                    except json.JSONDecodeError:
                        logger.debug(f"[{gid}] SSE non-JSON ignored: {line!r}")
                        continue

                    if "first_event" not in timing:
                        timing["first_event"] = time.time()

                    status = (event.get("status") or "").lower()
                    logger.debug(f"[{gid}] SSE status={status!r}")

                    if status in ("failed", "error"):
                        return "", (event.get("error_message") or
                                    event.get("error") or f"status={status}")

                    if status == "completed":
                        timing["completed"] = time.time()
                        output_url = next(
                            (event.get(k) for k in
                             ("file_url", "output_url", "image_url", "result_url")
                             if event.get(k)),
                            "",
                        )
                        if not _valid_url(output_url):
                            return "", f"completed event has invalid output URL: {output_url!r}"
                        return output_url, ""

        except gevent.Timeout:
            raise
        except Exception as exc:
            return "", f"SSE error: {exc}"
        return "", "SSE stream closed without a completion event"

    # ── Main task ──────────────────────────────────────────────────────
    @task
    def fire_generate(self) -> None:
        if not self._ready or not _IMAGES:
            return

        user_id = random.choice(self.USER_IDS)
        img_name, img_content, img_mime = random.choice(_IMAGES)
        prompt = random.choice(_PROMPTS)
        mode   = random.choice(MODES)
        preset = random.choice(PRESETS)

        form = [
            ("image",   (img_name, img_content, img_mime)),
            ("user_id", (None, user_id)),
            ("prompt",  (None, prompt)),
            ("mode",    (None, mode)),
            ("preset",  (None, preset)),
        ]

        # ── Concurrency tracking ───────────────────────────────────────
        global _active_jobs
        with _jobs_lock:
            _active_jobs += 1

        t0            = time.time()
        minute        = int((t0 - (TEST_START or t0)) / 60)
        t_post        = t0
        timing        = {}
        failed        = False
        post_failed   = False
        issue         = ""
        gid           = ""
        output_url    = ""
        post_resp_len = 0
        record        = None

        try:
            try:
                with gevent.Timeout(TASK_TIMEOUT):

                    # ── Step 1: POST /generate ─────────────────────────
                    try:
                        post_resp = self._sess.post(
                            f"{self.host}/aitools/colorize/v1/generate",
                            files=form,
                            timeout=None,
                        )
                        t_post        = time.time()
                        post_resp_len = len(post_resp.content)
                        if post_resp.status_code != 202:
                            failed = post_failed = True
                            issue  = f"generate HTTP {post_resp.status_code}"
                            logger.warning(
                                f"[user={user_id}] POST failed — {issue} "
                                f"body={post_resp.text[:200]!r}"
                            )
                        else:
                            try:
                                body = post_resp.json()
                                gid  = body.get("generation_id", "")
                                if not gid:
                                    failed = post_failed = True
                                    issue  = "no generation_id in response"
                                else:
                                    logger.info(
                                        f"[user={user_id}] generate OK — gid={gid}"
                                    )
                            except Exception as exc:
                                failed = post_failed = True
                                issue  = f"invalid JSON from generate: {exc}"
                    except gevent.Timeout:
                        raise
                    except Exception as exc:
                        t_post = time.time()
                        failed = post_failed = True
                        issue  = f"generate connection error: {exc}"
                        logger.warning(f"[user={user_id}] {issue}")

                    # ── Step 2: register CSV record ────────────────────
                    record = {
                        "timestamp":        datetime.now().isoformat(),
                        "user_id":          user_id,
                        "generation_id":    gid,
                        "image_file":       img_name,
                        "prompt":           prompt,
                        "mode":             mode,
                        "preset":           preset,
                        "status":           "submitted",
                        "output_url":       "",
                        "issue":            "",
                        "post_ms":          0,
                        "queue_ms":         0,
                        "processing_ms":    0,
                        "total_ms":         0,
                        "failure_category": "",
                        "sla_breach":       False,
                        "minute":           minute,
                        "active_jobs":      _active_jobs,
                    }
                    with _csv_lock:
                        _csv_records.append(record)

                    # ── Step 3: follow SSE ─────────────────────────────
                    if not failed:
                        output_url, sse_issue = self._follow_sse(gid, timing)
                        if sse_issue:
                            failed = True
                            issue  = sse_issue

            except gevent.Timeout:
                failed = True
                issue  = f"task timed out after {TASK_TIMEOUT}s"
                if t_post == t0:
                    t_post = time.time()
                logger.warning(f"[user={user_id}] [gid={gid or 'NONE'}] {issue}")
                if record is None:
                    record = {
                        "timestamp":        datetime.now().isoformat(),
                        "user_id":          user_id,
                        "generation_id":    gid,
                        "image_file":       img_name,
                        "prompt":           prompt,
                        "mode":             mode,
                        "preset":           preset,
                        "status":           "submitted",
                        "output_url":       "",
                        "issue":            "",
                        "post_ms":          0,
                        "queue_ms":         0,
                        "processing_ms":    0,
                        "total_ms":         0,
                        "failure_category": "",
                        "sla_breach":       False,
                        "minute":           minute,
                        "active_jobs":      _active_jobs,
                    }
                    with _csv_lock:
                        _csv_records.append(record)

            # ── Step 4: compute latency metrics ───────────────────────
            t_end      = time.time()
            post_ms    = int((t_post - t0) * 1000)
            total_ms   = int((t_end - t0) * 1000)
            queue_ms   = int((timing["first_event"] - t_post) * 1000) \
                         if "first_event" in timing else 0
            proc_ms    = int((timing["completed"] - timing["first_event"]) * 1000) \
                         if ("completed" in timing and "first_event" in timing) else 0
            sla_breach       = total_ms > SLA_THRESHOLD_MS
            failure_category = _categorize(issue)

            # ── Step 5: update CSV record ──────────────────────────────
            with _csv_lock:
                record["status"]           = "failed" if failed else "completed"
                record["output_url"]       = output_url
                record["issue"]            = issue
                record["post_ms"]          = post_ms
                record["queue_ms"]         = queue_ms
                record["processing_ms"]    = proc_ms
                record["total_ms"]         = total_ms
                record["failure_category"] = failure_category
                record["sla_breach"]       = sla_breach
                cnt      = len(_csv_records)
                snapshot = list(_csv_records) if cnt % _FLUSH_EVERY == 0 else None

            if snapshot is not None:
                spawn(_write_report, snapshot)

            # ── Step 6: fire Locust events ─────────────────────────────
            self.environment.events.request.fire(
                request_type="POST",
                name="bwc_post",
                response_time=post_ms,
                response_length=post_resp_len,
                exception=Exception(issue) if post_failed else None,
            )
            if not post_failed:
                self.environment.events.request.fire(
                    request_type="SSE",
                    name="bwc_queue",
                    response_time=queue_ms,
                    response_length=0,
                    exception=Exception(issue)
                              if (failed and "first_event" not in timing) else None,
                )
            if "first_event" in timing:
                self.environment.events.request.fire(
                    request_type="SSE",
                    name="bwc_processing&completion",
                    response_time=proc_ms,
                    response_length=0,
                    exception=Exception(issue)
                              if (failed and "completed" not in timing) else None,
                )

            # ── Step 7: outcome logging ────────────────────────────────
            if failed:
                logger.warning(
                    f"[user={user_id}] [gid={gid or 'NONE'}] FAILED "
                    f"in {total_ms}ms (post={post_ms} queue={queue_ms} proc={proc_ms}) "
                    f"active={_active_jobs} category={failure_category} — {issue}"
                )
            else:
                logger.info(
                    f"[user={user_id}] [gid={gid}] COMPLETED "
                    f"in {total_ms}ms (post={post_ms} queue={queue_ms} proc={proc_ms}) "
                    f"active={_active_jobs}"
                    f"{' SLA!' if sla_breach else ''} — {output_url}"
                )
                spawn(_save_output, gid, output_url)

        finally:
            with _jobs_lock:
                _active_jobs -= 1


# ── Events ─────────────────────────────────────────────────────────────────────
@events.test_start.add_listener
def on_test_start(environment, **kwargs):
    global _RUN_DIR, _IMAGES, _PROMPTS, TEST_START
    TEST_START = time.time()
    _input_dir = _INPUTS / TOOL_NAME
    _IMAGES    = load_images(_input_dir / "Images")
    _PROMPTS   = [ln[:1499] for ln in _load_lines(_input_dir / "prompts.txt")]
    random.shuffle(_PROMPTS)

    if not _IMAGES:
        logger.error("No images found — stopping runner.")
        environment.runner.quit()
        return
    if not _PROMPTS:
        logger.error("prompts.txt missing or empty — stopping runner.")
        environment.runner.quit()
        return

    _RUN_DIR = _OUTPUTS / TOOL_NAME / datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    _RUN_DIR.mkdir(parents=True, exist_ok=True)
    logger.info(
        f"\n{'─' * 60}\n"
        f"  Tool       : {TOOL_NAME}\n"
        f"  Target     : {os.getenv('BASE_URL', '(BASE_URL not set)')}\n"
        f"  Images     : {len(_IMAGES)} (preloaded)\n"
        f"  Prompts    : {len(_PROMPTS)}\n"
        f"  Modes      : {MODES}\n"
        f"  Presets    : {len(PRESETS)}\n"
        f"  Task limit : {TASK_TIMEOUT}s\n"
        f"  SLA target : {SLA_THRESHOLD_MS}ms\n"
        f"  CSV flush  : every {_FLUSH_EVERY} records\n"
        f"  Output dir : {_RUN_DIR}\n"
        f"{'─' * 60}"
    )


@events.test_stop.add_listener
def on_test_stop(environment, **kwargs):
    if _RUN_DIR is None:
        logger.error("Test stopped before _RUN_DIR was set — no CSVs written.")
        return
    with _csv_lock:
        for rec in _csv_records:
            if rec["status"] == "submitted":
                rec["status"]           = "not_completed"
                rec["issue"]            = "test stopped before SSE resolved"
                rec["failure_category"] = _categorize(rec["issue"])
        records = list(_csv_records)

    ok       = sum(1 for r in records if r["status"] == "completed")
    failed   = sum(1 for r in records if r["status"] == "failed")
    mid_stop = sum(1 for r in records if r["status"] == "not_completed")
    logger.info(
        f"\n{'─' * 60}\n"
        f"  Total : {len(records)}  Completed : {ok}  "
        f"Failed : {failed}  Not completed : {mid_stop}\n"
        f"{'─' * 60}"
    )
    _write_report(records)
    _write_errors(records)
    _generate_summary(records)
    _aggregate_by_minute(records)
    _failure_breakdown(records)
