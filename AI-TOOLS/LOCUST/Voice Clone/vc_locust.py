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
_ROOT     = _HERE.parents[2]
_INPUTS   = _ROOT / "INPUTS"
_OUTPUTS  = _HERE.parents[1] / "OUTPUTS"
TOOL_NAME = "Voice Clone"

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
_AUDIO_FILES = []
_INPUT_TEXTS = []

_AUDIO_MIME = {
    ".mp3": "audio/mpeg", ".wav": "audio/wav",
}

_FALLBACK_TEXTS = [
    "Her parents were nearby, keeping an eye on her as she ran around with excitement.",
    "The morning sun cast a warm glow over the quiet village streets.",
    "He opened the book and began reading from the very first page.",
]

# ── CSV ────────────────────────────────────────────────────────────────────────
_csv_records = []
_csv_lock    = Semaphore()

_CSV_FIELDS = [
    "timestamp", "user_id", "generation_id",
    "audio_file", "input_text",
    "status", "issue",
    "upload_ms", "validation_queue_ms", "generate_ms",
    "completion_queue_ms", "processing_ms", "total_ms",
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
    if "multiplespeakers" in l or "multiple speakers" in l:
        return "validation"
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

    done = [r for r in records if r["status"] == "completed"]
    # Combined queue = validation wait + completion wait
    avg_queue = (
        sum(r.get("validation_queue_ms", 0) + r.get("completion_queue_ms", 0)
            for r in done) / len(done)
    ) if done else 0
    avg_proc  = (sum(r.get("processing_ms", 0) for r in done) / len(done)) if done else 0
    bottleneck = "QUEUE BOTTLENECK" if avg_queue > avg_proc else "PROCESSING BOTTLENECK"
    pct_ok     = (completed / total * 100) if total else 0
    pct_sla    = (sla_count / completed * 100) if completed else 0

    avg_upload = (sum(r.get("upload_ms", 0) for r in done) / len(done)) if done else 0
    avg_gen    = (sum(r.get("generate_ms", 0) for r in done) / len(done)) if done else 0

    logger.info(
        f"\n{'═' * 60}\n"
        f"  PERFORMANCE SUMMARY — {TOOL_NAME}\n"
        f"{'─' * 60}\n"
        f"  Total jobs           : {total}\n"
        f"  Completed            : {completed}  ({pct_ok:.1f}%)\n"
        f"  Failed               : {failed}\n"
        f"  Not completed        : {not_done}\n"
        f"  SLA breaches         : {sla_count}  ({pct_sla:.1f}% of completed)\n"
        f"{'─' * 60}\n"
        f"  Avg upload_ms        : {avg_upload:>8.0f} ms\n"
        f"  Avg queue_ms (total) : {avg_queue:>8.0f} ms\n"
        f"  Avg generate_ms      : {avg_gen:>8.0f} ms\n"
        f"  Avg processing_ms    : {avg_proc:>8.0f} ms\n"
        f"  Bottleneck           : {bottleneck}\n"
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
        q_avg = (
            sum(r.get("validation_queue_ms", 0) + r.get("completion_queue_ms", 0)
                for r in done) / len(done)
        ) if done else 0
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

def load_audio_files(folder) -> list[tuple[str, bytes, str]]:
    result = []
    p = Path(folder)
    if not p.exists():
        return result
    for f in sorted(p.iterdir()):
        mime = _AUDIO_MIME.get(f.suffix.lower())
        if not (f.is_file() and mime):
            continue
        try:
            result.append((f.name, f.read_bytes(), mime))
        except OSError as exc:
            logger.warning(f"Could not read audio {f.name}: {exc}")
    return result


# ── Output helpers ─────────────────────────────────────────────────────────────
def _valid_url(url: str) -> bool:
    return bool(url) and url.startswith(("http://", "https://"))

def _save_output(gid: str, url: str) -> None:
    if not SAVE_OUTPUT:
        return
    try:
        filename = url.split("?")[0].rsplit("/", 1)[-1] or f"{gid}.mp3"
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
class VoiceCloneUser(HttpUser):
    host      = os.getenv("VOICE_CLONE_BASE_URL", os.getenv("BASE_URL",
                           "https://test-apigateway.erosuniverse.com"))
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

    # ── Stage 1 SSE: wait for UPLOAD_VALIDATED ─────────────────────────
    # timing keys: "validation_first" (first parseable event)
    # Returns (reference_audio_url, issue)
    def _follow_validation_sse(self, gid: str,
                               timing: dict) -> tuple[str, str]:
        url = f"{self.host}/aitools/jobs/{gid}/stream"
        logger.info(f"[{gid}] SSE validation start")
        try:
            with self._sess.get(
                url,
                headers={"Accept": "text/event-stream"},
                stream=True,
                timeout=None,
            ) as resp:
                if resp.status_code != 200:
                    return "", f"validation SSE HTTP {resp.status_code}"
                for raw in resp.iter_lines():
                    line = (raw.decode("utf-8") if isinstance(raw, bytes)
                            else raw).strip()
                    if not line.startswith("data:"):
                        continue
                    try:
                        event = json.loads(line[5:].strip())
                    except json.JSONDecodeError:
                        logger.debug(f"[{gid}] validation SSE non-JSON ignored")
                        continue

                    if "validation_first" not in timing:
                        timing["validation_first"] = time.time()

                    status = (event.get("status") or "").upper()
                    logger.debug(f"[{gid}] validation SSE status={status!r}")

                    if event.get("error") == "MultipleSpeakersDetected":
                        return "", "MultipleSpeakersDetected"

                    if status in ("FAILED", "ERROR"):
                        return "", (event.get("error_message") or
                                    event.get("error") or f"status={status}")

                    if status == "UPLOAD_VALIDATED":
                        timing["validated"] = time.time()
                        ref_url = event.get("reference_audio_url", "")
                        if not ref_url:
                            return "", "UPLOAD_VALIDATED but no reference_audio_url"
                        return ref_url, ""

        except gevent.Timeout:
            raise
        except Exception as exc:
            return "", f"validation SSE error: {exc}"
        return "", "validation SSE ended without UPLOAD_VALIDATED"

    # ── Stage 2 SSE: wait for COMPLETED ───────────────────────────────
    # timing keys: "completion_first", "completed"
    # Returns (output_url, issue)
    def _follow_completion_sse(self, gid: str,
                               timing: dict) -> tuple[str, str]:
        url = f"{self.host}/aitools/jobs/{gid}/stream"
        logger.info(f"[{gid}] SSE completion start")
        try:
            with self._sess.get(
                url,
                headers={"Accept": "text/event-stream"},
                stream=True,
                timeout=None,
            ) as resp:
                if resp.status_code != 200:
                    return "", f"completion SSE HTTP {resp.status_code}"
                for raw in resp.iter_lines():
                    line = (raw.decode("utf-8") if isinstance(raw, bytes)
                            else raw).strip()
                    if not line.startswith("data:"):
                        continue
                    try:
                        event = json.loads(line[5:].strip())
                    except json.JSONDecodeError:
                        logger.debug(f"[{gid}] completion SSE non-JSON ignored")
                        continue

                    if "completion_first" not in timing:
                        timing["completion_first"] = time.time()

                    status = (event.get("status") or "").upper()
                    logger.debug(f"[{gid}] completion SSE status={status!r}")

                    if status in ("FAILED", "ERROR"):
                        return "", (event.get("error_message") or
                                    event.get("error") or f"status={status}")

                    if status == "COMPLETED":
                        timing["completed"] = time.time()
                        output_url = next(
                            (event.get(k) for k in
                             ("file_url", "audio_url", "output_url", "result_url")
                             if event.get(k)),
                            "",
                        )
                        if not _valid_url(output_url):
                            return "", f"completed event has invalid output URL: {output_url!r}"
                        return output_url, ""

        except gevent.Timeout:
            raise
        except Exception as exc:
            return "", f"completion SSE error: {exc}"
        return "", "completion SSE ended without COMPLETED event"

    # ── Main task: upload → validation SSE → generate → completion SSE ─
    # Fires 5 Locust events covering each phase individually.
    @task
    def fire_generate(self) -> None:
        if not self._ready or not _AUDIO_FILES:
            return

        user_id = random.choice(self.USER_IDS)
        audio_name, audio_content, audio_mime = random.choice(_AUDIO_FILES)
        input_text = random.choice(_INPUT_TEXTS)

        # ── Concurrency tracking ───────────────────────────────────────
        global _active_jobs
        with _jobs_lock:
            _active_jobs += 1

        t0         = time.time()
        minute     = int((t0 - (TEST_START or t0)) / 60)
        timing     = {}
        failed     = False
        issue      = ""
        gid        = ""
        output_url = ""
        upload_failed     = False
        val_sse_failed    = False
        generate_failed   = False
        comp_sse_failed   = False
        upload_resp_len   = 0
        record            = None

        try:
            try:
                with gevent.Timeout(TASK_TIMEOUT):

                    # ── Stage 1: POST upload ───────────────────────────
                    try:
                        post_resp = self._sess.post(
                            f"{self.host}/aitools/voice-clone/v1/generate",
                            files=[
                                ("reference_audio", (audio_name, audio_content, audio_mime)),
                                ("user_id",         (None, user_id)),
                                ("action",          (None, "upload")),
                            ],
                            timeout=None,
                        )
                        timing["t_upload_done"] = time.time()
                        upload_resp_len = len(post_resp.content)
                        if post_resp.status_code != 202:
                            failed = upload_failed = True
                            issue  = f"upload HTTP {post_resp.status_code}"
                            logger.warning(
                                f"[user={user_id}] upload failed — {issue} "
                                f"body={post_resp.text[:200]!r}"
                            )
                        else:
                            try:
                                body = post_resp.json()
                                gid  = body.get("generation_id", "")
                                if not gid:
                                    failed = upload_failed = True
                                    issue  = "no generation_id in upload response"
                                else:
                                    logger.info(
                                        f"[user={user_id}] upload OK — gid={gid}"
                                    )
                            except Exception as exc:
                                failed = upload_failed = True
                                issue  = f"invalid JSON from upload: {exc}"
                    except gevent.Timeout:
                        raise
                    except Exception as exc:
                        timing["t_upload_done"] = time.time()
                        failed = upload_failed = True
                        issue  = f"upload connection error: {exc}"
                        logger.warning(f"[user={user_id}] {issue}")

                    # ── Register CSV record ────────────────────────────
                    record = {
                        "timestamp":            datetime.now().isoformat(),
                        "user_id":              user_id,
                        "generation_id":        gid,
                        "audio_file":           audio_name,
                        "input_text":           input_text,
                        "status":               "submitted",
                        "output_url":           "",
                        "issue":                "",
                        "upload_ms":            0,
                        "validation_queue_ms":  0,
                        "generate_ms":          0,
                        "completion_queue_ms":  0,
                        "processing_ms":        0,
                        "total_ms":             0,
                        "failure_category":     "",
                        "sla_breach":           False,
                        "minute":               minute,
                        "active_jobs":          _active_jobs,
                    }
                    with _csv_lock:
                        _csv_records.append(record)

                    # ── Stage 2: Wait for UPLOAD_VALIDATED ────────────
                    ref_url = ""
                    if not failed:
                        ref_url, sse_issue = self._follow_validation_sse(gid, timing)
                        if sse_issue:
                            failed = val_sse_failed = True
                            issue  = sse_issue

                    # ── Stage 3: POST generate ─────────────────────────
                    if not failed:
                        try:
                            gen_resp = self._sess.post(
                                f"{self.host}/aitools/voice-clone/v1/generate",
                                files=[
                                    ("user_id",             (None, user_id)),
                                    ("input_text",          (None, input_text)),
                                    ("reference_audio_url", (None, ref_url)),
                                    ("action",              (None, "generate")),
                                    ("generation_id",       (None, gid)),
                                ],
                                timeout=None,
                            )
                            timing["t_generate_done"] = time.time()
                            if gen_resp.status_code != 202:
                                failed = generate_failed = True
                                issue  = f"generate HTTP {gen_resp.status_code}"
                            else:
                                logger.info(
                                    f"[user={user_id}] [gid={gid}] generate OK"
                                )
                        except gevent.Timeout:
                            raise
                        except Exception as exc:
                            timing["t_generate_done"] = time.time()
                            failed = generate_failed = True
                            issue  = f"generate connection error: {exc}"

                    # ── Stage 4: Wait for COMPLETED ────────────────────
                    if not failed:
                        output_url, sse_issue = self._follow_completion_sse(gid, timing)
                        if sse_issue:
                            failed = comp_sse_failed = True
                            issue  = sse_issue

            except gevent.Timeout:
                failed = True
                issue  = f"task timed out after {TASK_TIMEOUT}s"
                if "t_upload_done" not in timing:
                    timing["t_upload_done"] = time.time()
                logger.warning(f"[user={user_id}] [gid={gid or 'NONE'}] {issue}")
                if record is None:
                    record = {
                        "timestamp":            datetime.now().isoformat(),
                        "user_id":              user_id,
                        "generation_id":        gid,
                        "audio_file":           audio_name,
                        "input_text":           input_text,
                        "status":               "submitted",
                        "output_url":           "",
                        "issue":                "",
                        "upload_ms":            0,
                        "validation_queue_ms":  0,
                        "generate_ms":          0,
                        "completion_queue_ms":  0,
                        "processing_ms":        0,
                        "total_ms":             0,
                        "failure_category":     "",
                        "sla_breach":           False,
                        "minute":               minute,
                        "active_jobs":          _active_jobs,
                    }
                    with _csv_lock:
                        _csv_records.append(record)

            # ── Compute per-phase latencies ────────────────────────────
            t_end           = time.time()
            t_upload_done   = timing.get("t_upload_done",   t_end)
            t_validated     = timing.get("validated",        t_upload_done)
            t_generate_done = timing.get("t_generate_done",  t_validated)

            upload_ms           = int((t_upload_done - t0) * 1000)
            validation_queue_ms = int((timing["validation_first"] - t_upload_done) * 1000) \
                                  if "validation_first" in timing else 0
            generate_ms         = int((t_generate_done - t_validated) * 1000) \
                                  if "validated" in timing else 0
            completion_queue_ms = int((timing["completion_first"] - t_generate_done) * 1000) \
                                  if "completion_first" in timing else 0
            processing_ms       = int((timing["completed"] - timing["completion_first"]) * 1000) \
                                  if ("completed" in timing and
                                      "completion_first" in timing) else 0
            total_ms            = int((t_end - t0) * 1000)
            sla_breach          = total_ms > SLA_THRESHOLD_MS
            failure_category    = _categorize(issue)

            # ── Update CSV record ──────────────────────────────────────
            with _csv_lock:
                record["status"]              = "failed" if failed else "completed"
                record["output_url"]          = output_url
                record["issue"]               = issue
                record["upload_ms"]           = upload_ms
                record["validation_queue_ms"] = validation_queue_ms
                record["generate_ms"]         = generate_ms
                record["completion_queue_ms"] = completion_queue_ms
                record["processing_ms"]       = processing_ms
                record["total_ms"]            = total_ms
                record["failure_category"]    = failure_category
                record["sla_breach"]          = sla_breach
                cnt      = len(_csv_records)
                snapshot = list(_csv_records) if cnt % _FLUSH_EVERY == 0 else None

            if snapshot is not None:
                spawn(_write_report, snapshot)

            # ── Fire Locust events — one per phase ─────────────────────
            # vc_upload: POST upload latency (always fired)
            self.environment.events.request.fire(
                request_type="POST",
                name="vc_upload",
                response_time=upload_ms,
                response_length=upload_resp_len,
                exception=Exception(issue) if upload_failed else None,
            )
            # vc_queue_validation: upload 202 → first validation SSE event
            if not upload_failed:
                self.environment.events.request.fire(
                    request_type="SSE",
                    name="vc_queue_validation",
                    response_time=validation_queue_ms,
                    response_length=0,
                    exception=Exception(issue)
                              if (val_sse_failed and
                                  "validation_first" not in timing) else None,
                )
            # vc_generate: POST generate latency (fired after validation succeeded)
            if "validated" in timing:
                self.environment.events.request.fire(
                    request_type="POST",
                    name="vc_generate",
                    response_time=generate_ms,
                    response_length=0,
                    exception=Exception(issue) if generate_failed else None,
                )
            # vc_queue_completion: generate 202 → first completion SSE event
            if "t_generate_done" in timing and not generate_failed:
                self.environment.events.request.fire(
                    request_type="SSE",
                    name="vc_queue_completion",
                    response_time=completion_queue_ms,
                    response_length=0,
                    exception=Exception(issue)
                              if (comp_sse_failed and
                                  "completion_first" not in timing) else None,
                )
            # vc_processing: first completion SSE event → COMPLETED
            if "completion_first" in timing:
                self.environment.events.request.fire(
                    request_type="SSE",
                    name="vc_processing",
                    response_time=processing_ms,
                    response_length=0,
                    exception=Exception(issue)
                              if (comp_sse_failed and
                                  "completed" not in timing) else None,
                )

            # ── Outcome logging ────────────────────────────────────────
            if failed:
                logger.warning(
                    f"[user={user_id}] [gid={gid or 'NONE'}] FAILED "
                    f"in {total_ms}ms "
                    f"(upload={upload_ms} val_q={validation_queue_ms} "
                    f"gen={generate_ms} comp_q={completion_queue_ms} proc={processing_ms}) "
                    f"active={_active_jobs} category={failure_category} — {issue}"
                )
            else:
                logger.info(
                    f"[user={user_id}] [gid={gid}] COMPLETED "
                    f"in {total_ms}ms "
                    f"(upload={upload_ms} val_q={validation_queue_ms} "
                    f"gen={generate_ms} comp_q={completion_queue_ms} proc={processing_ms}) "
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
    global _RUN_DIR, _AUDIO_FILES, _INPUT_TEXTS, TEST_START
    TEST_START   = time.time()
    _input_dir   = _INPUTS / "AI-TOOLS" / TOOL_NAME
    _AUDIO_FILES = load_audio_files(_input_dir / "input_audio")
    _INPUT_TEXTS = _load_lines(_input_dir / "input_text.txt") or _FALLBACK_TEXTS
    random.shuffle(_INPUT_TEXTS)

    if not _AUDIO_FILES:
        logger.error("No audio files found — stopping runner.")
        environment.runner.quit()
        return

    _RUN_DIR = _OUTPUTS / TOOL_NAME / datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    _RUN_DIR.mkdir(parents=True, exist_ok=True)
    logger.info(
        f"\n{'─' * 60}\n"
        f"  Tool        : {TOOL_NAME}\n"
        f"  Target      : {os.getenv('VOICE_CLONE_BASE_URL', os.getenv('BASE_URL', '(not set)'))}\n"
        f"  Audio files : {len(_AUDIO_FILES)} (preloaded)\n"
        f"  Input texts : {len(_INPUT_TEXTS)}\n"
        f"  Task limit  : {TASK_TIMEOUT}s\n"
        f"  SLA target  : {SLA_THRESHOLD_MS}ms\n"
        f"  CSV flush   : every {_FLUSH_EVERY} records\n"
        f"  Output dir  : {_RUN_DIR}\n"
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
