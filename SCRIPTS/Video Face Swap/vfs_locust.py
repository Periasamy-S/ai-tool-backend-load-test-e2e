import os
import sys
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

# ── TestOps centralized storage ─────────────────────────────────────────────────
sys.path.insert(0, os.getenv("TESTOPS_SHARED", os.path.join(os.path.expanduser("~"), ".testops", "Shared")))
from path_manager import init_run, get_locust_directory

# ── Paths ──────────────────────────────────────────────────────────────────────
_HERE     = Path(__file__).parent
_ROOT     = _HERE.parents[1]
_INPUTS   = _ROOT / "INPUTS"
TOOL_NAME = "Video Face Swap"

init_run(project="Load-Test/AI-Tools", tool=TOOL_NAME)

load_dotenv(_HERE / ".env")
load_dotenv(_ROOT / ".env")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ── Config ─────────────────────────────────────────────────────────────────────
SAVE_OUTPUT      = os.getenv("SAVE_OUTPUT", "false").strip().lower() == "true"
TASK_TIMEOUT     = int(os.getenv("TASK_TIMEOUT", "600"))
SLA_THRESHOLD_MS = int(os.getenv("SLA_THRESHOLD_MS", "120000"))
_FLUSH_EVERY     = int(os.getenv("CSV_FLUSH_EVERY", "10"))
_RUN_DIR         = None

# ── Observability globals ──────────────────────────────────────────────────────
TEST_START   = None
_active_jobs = 0
_jobs_lock   = Semaphore()

# ── Module-level inputs (loaded once at test_start) ────────────────────────────
_VIDEOS  = []
_TARGETS = []

# ── CSV ────────────────────────────────────────────────────────────────────────
_csv_records = []
_csv_lock    = Semaphore()

_CSV_FIELDS = [
    "timestamp", "completed_at", "user_id", "subscription_tier", "generation_id",
    "template_video", "target_image",
    "status", "issue",
    "post_ms", "queue_ms", "processing_ms", "total_ms",
    "failure_category", "sla_breach",
    "minute", "active_jobs",
    # diagnostic — CSV only, not fired as Locust events
    "face_group_detection_ms", "target_detection_ms", "bind_ms", "swap_compute_ms",
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
    if "no generation_id" in l or "invalid json" in l:
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
        logger.warning("Low sample size (<100 jobs) — percentile metrics may be unreliable.")


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
            f"  Minute {minute:>3} → jobs={len(recs):>3}  "
            f"queue={q_avg:>7.0f}ms  processing={p_avg:>7.0f}ms"
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


# ── Input helpers ──────────────────────────────────────────────────────────────
def _find_files(folder, *exts) -> list[str]:
    p = Path(folder)
    return [str(f) for f in p.iterdir()
            if f.is_file() and f.suffix.lower() in exts] if p.exists() else []


# ── Output helpers ─────────────────────────────────────────────────────────────
def _save_output(gid: str, url: str) -> None:
    if not SAVE_OUTPUT or not url:
        return
    try:
        filename = url.split("?")[0].rsplit("/", 1)[-1] or f"{gid}.mp4"
        resp = requests.get(url, timeout=120)
        resp.raise_for_status()
        (_RUN_DIR / filename).write_bytes(resp.content)
    except Exception as exc:
        logger.warning(f"Output save failed [{gid}]: {exc}")


# ── CSV writers ────────────────────────────────────────────────────────────────
def _write_report(records: list[dict]) -> None:
    if _RUN_DIR is None:
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
        logger.info("errors.csv — no failures recorded, file not written.")
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


# ── Queue / priority-hierarchy audit ────────────────────────────────────────────
# Dispatch priority order: pro is served first, basic last.
_TIER_PRIORITY = ["pro", "plus", "lite", "basic"]

def _write_queue_audit(records: list[dict]) -> None:
    """
    Proves the backend's priority queue (pro > plus > lite > basic) is actually
    honoured under load: a per-tier summary (does pro queue faster than basic?)
    and a wall-clock-minute x tier timeline (did a burst of concurrent requests
    resolve in priority order?).
    """
    if _RUN_DIR is None:
        return

    by_tier: dict[str, list] = defaultdict(list)
    for r in records:
        by_tier[r.get("subscription_tier", "")].append(r)

    # ── Per-tier summary ─────────────────────────────────────────────────
    summary_fields = [
        "subscription_tier", "total_requests", "completed", "failed", "not_completed",
        "completion_rate_pct", "avg_queue_ms", "min_queue_ms", "max_queue_ms",
        "avg_processing_ms", "avg_total_ms",
    ]
    summary_rows  = []
    tier_avg_queue = {}
    for tier in _TIER_PRIORITY:
        recs = by_tier.get(tier, [])
        if not recs:
            continue
        completed = [r for r in recs if r["status"] == "completed"]
        failed    = sum(1 for r in recs if r["status"] == "failed")
        not_done  = sum(1 for r in recs if r["status"] == "not_completed")
        q_vals    = [r.get("queue_ms", 0) for r in completed]
        avg_q     = (sum(q_vals) / len(q_vals)) if q_vals else 0
        tier_avg_queue[tier] = avg_q
        summary_rows.append({
            "subscription_tier":   tier,
            "total_requests":      len(recs),
            "completed":           len(completed),
            "failed":              failed,
            "not_completed":       not_done,
            "completion_rate_pct": round(len(completed) / len(recs) * 100, 1),
            "avg_queue_ms":        round(avg_q, 1),
            "min_queue_ms":        min(q_vals) if q_vals else 0,
            "max_queue_ms":        max(q_vals) if q_vals else 0,
            "avg_processing_ms":   round(sum(r.get("processing_ms", 0) for r in completed) / len(completed), 1) if completed else 0,
            "avg_total_ms":        round(sum(r.get("total_ms", 0) for r in completed) / len(completed), 1) if completed else 0,
        })

    summary_path = _RUN_DIR / "queue_audit_summary.csv"
    try:
        with open(summary_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=summary_fields)
            writer.writeheader()
            writer.writerows(summary_rows)
        logger.info(f"queue_audit_summary.csv written — {len(summary_rows)} tiers: {summary_path}")
    except OSError as exc:
        logger.error(f"Failed to write queue_audit_summary.csv: {exc}")

    # ── Priority-hierarchy check (logged, not written) ─────────────────────
    ordered = [t for t in _TIER_PRIORITY if t in tier_avg_queue]
    if len(ordered) > 1:
        hierarchy_str = " > ".join(f"{t}={tier_avg_queue[t]:.0f}ms" for t in ordered)
        violations = [
            (ordered[i], ordered[i + 1])
            for i in range(len(ordered) - 1)
            if tier_avg_queue[ordered[i]] > tier_avg_queue[ordered[i + 1]]
        ]
        if violations:
            logger.warning(
                f"QUEUE PRIORITY CHECK — hierarchy NOT respected (avg queue_ms): "
                f"{hierarchy_str}  violations={violations}"
            )
        else:
            logger.info(f"QUEUE PRIORITY CHECK — hierarchy respected (avg queue_ms): {hierarchy_str}")

    # ── Timeline: wall-clock minute x tier ──────────────────────────────────
    timeline_fields = [
        "window", "subscription_tier", "request_count", "completed",
        "failed", "not_completed", "avg_queue_ms", "avg_total_ms",
    ]
    buckets: dict[tuple, list] = defaultdict(list)
    for r in records:
        ts     = r.get("timestamp", "")
        window = ts[11:16] if len(ts) >= 16 else "unknown"  # HH:MM
        buckets[(window, r.get("subscription_tier", ""))].append(r)

    timeline_rows = []
    for (window, tier), recs in sorted(buckets.items()):
        completed = [r for r in recs if r["status"] == "completed"]
        failed    = sum(1 for r in recs if r["status"] == "failed")
        not_done  = sum(1 for r in recs if r["status"] == "not_completed")
        q_vals    = [r.get("queue_ms", 0) for r in completed]
        t_vals    = [r.get("total_ms", 0) for r in completed]
        timeline_rows.append({
            "window":             window,
            "subscription_tier":  tier,
            "request_count":      len(recs),
            "completed":          len(completed),
            "failed":             failed,
            "not_completed":      not_done,
            "avg_queue_ms":       round(sum(q_vals) / len(q_vals), 1) if q_vals else 0,
            "avg_total_ms":       round(sum(t_vals) / len(t_vals), 1) if t_vals else 0,
        })

    timeline_path = _RUN_DIR / "queue_audit_timeline.csv"
    try:
        with open(timeline_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=timeline_fields)
            writer.writeheader()
            writer.writerows(timeline_rows)
        logger.info(f"queue_audit_timeline.csv written — {len(timeline_rows)} rows: {timeline_path}")
    except OSError as exc:
        logger.error(f"Failed to write queue_audit_timeline.csv: {exc}")


# ── User ───────────────────────────────────────────────────────────────────────
class VideoFaceSwapUser(HttpUser):
    host      = os.getenv("BASE_URL", "")
    wait_time = between(1, 3)
    # ── Subscription-tier user ids (priority queue: pro > plus > lite > basic) ──
    USER_TIERS = [
        (uid.strip(), tier)
        for tier, uid in (
            ("basic", os.getenv("USER_ID_BASIC", "")),
            ("lite",  os.getenv("USER_ID_LITE", "")),
            ("plus",  os.getenv("USER_ID_PLUS", "")),
            ("pro",   os.getenv("USER_ID_PRO", "")),
        )
        if uid.strip()
    ]

    def on_start(self) -> None:
        self._ready = False
        self._sess  = None
        if not self.USER_TIERS:
            logger.error("No USER_ID_* configured (USER_ID_BASIC/USER_ID_LITE/USER_ID_PLUS/USER_ID_PRO) — stopping runner.")
            self.environment.runner.quit()
            return
        self._sess  = requests.Session()
        self._ready = True

    def on_stop(self) -> None:
        if self._sess:
            self._sess.close()

    # ── SSE checkpoint reader ──────────────────────────────────────────────────
    def _next_checkpoint(
        self,
        sse_iter,
        need: set,
        timing: dict,
        done_key: str | None = None,
    ) -> dict | None:
        """
        Advance an existing SSE line iterator until all keys in `need` are satisfied.

        - `sse_iter`: a live iter_lines() iterator — NOT a new connection.
        - `timing["first_event"]` is set on the very first parseable event across all
          checkpoint calls (global to the job, not per call).
        - `timing[done_key]` is set when all need keys are collected.

        VFS need keys:
          "face_groups"    → face_groups_detected dict
          "target_faces"   → signed_target_face_urls list
          "ready_for_swap" → status == "ready_for_swap"
          "completed"      → status == "completed" (also captures output_url)

        Returns collected dict on success, None on error event or stream exhaustion.
        """
        collected = {}
        for raw in sse_iter:
            line = (raw.decode("utf-8") if isinstance(raw, bytes) else raw).strip()
            if not line.startswith("data:"):
                continue
            try:
                event = json.loads(line[5:].strip())
            except json.JSONDecodeError:
                continue

            if "first_event" not in timing:
                timing["first_event"] = time.time()

            status = (event.get("status") or "").lower()
            if status in ("failed", "error"):
                return None

            if "face_groups" in need and not collected.get("face_groups"):
                groups = event.get("face_groups_detected")
                if groups:
                    collected["face_groups"] = groups

            if "target_faces" in need and not collected.get("target_faces"):
                urls = event.get("signed_target_face_urls")
                if urls:
                    collected["target_faces"] = urls

            if "ready_for_swap" in need and not collected.get("ready_for_swap"):
                if status == "ready_for_swap":
                    collected["ready_for_swap"] = True

            if "completed" in need and not collected.get("completed"):
                if status == "completed":
                    collected["completed"] = True
                    out = next(
                        (event.get(k) for k in
                         ("file_url", "video_url", "output_url", "result_url")
                         if event.get(k)),
                        None,
                    )
                    collected["output_url"] = out or ""

            if all(k in collected for k in need):
                if done_key:
                    timing[done_key] = time.time()
                return collected

        return None  # stream exhausted without satisfying need

    # ── Intermediate POST helpers (raw session — invisible to Locust UI) ────────
    def _post_upload_target_face(self, gid: str, image_path: str) -> tuple[bool, str]:
        try:
            with open(image_path, "rb") as fh:
                img_bytes = fh.read()
            resp = self._sess.post(
                f"{self.host}/aitools/video-face-swap/v1/uploadtargetface/{gid}",
                files={"file": (os.path.basename(image_path), img_bytes, "image/jpeg")},
                data={},
                timeout=None,
            )
            if resp.status_code != 202:
                return False, f"upload_target_http_{resp.status_code}"
            return True, ""
        except gevent.Timeout:
            raise
        except Exception as exc:
            return False, f"upload_target connection error: {exc}"

    def _post_bind_face(
        self, gid: str, group_id: str, target_url: str
    ) -> tuple[bool, str]:
        try:
            resp = self._sess.post(
                f"{self.host}/aitools/video-face-swap/v1/uploadnewfaces/{gid}/{group_id}",
                files={"image_url": (None, target_url)},
                timeout=None,
            )
            if resp.status_code != 202:
                return False, f"face_bind_http_{resp.status_code}"
            return True, ""
        except gevent.Timeout:
            raise
        except Exception as exc:
            return False, f"face_bind connection error: {exc}"

    def _post_start_swap(self, gid: str, group_ids: list) -> tuple[bool, str]:
        try:
            resp = self._sess.post(
                f"{self.host}/aitools/video-face-swap/v1/faceswap/{gid}",
                json={"group_ids": [int(g) for g in group_ids]},
                timeout=None,
            )
            if resp.status_code != 202:
                return False, f"faceswap_http_{resp.status_code}"
            return True, ""
        except gevent.Timeout:
            raise
        except Exception as exc:
            return False, f"faceswap connection error: {exc}"

    # ── Main task — sequential inline, single SSE stream ──────────────────────
    @task
    def video_face_swap_flow(self) -> None:
        if not self._ready or not _VIDEOS or not _TARGETS:
            return

        user_id, subscription_tier = random.choice(self.USER_TIERS)
        global _active_jobs
        with _jobs_lock:
            _active_jobs += 1

        t0     = time.time()
        minute = int((t0 - (TEST_START or t0)) / 60)
        tmpl   = random.choice(_VIDEOS)
        target = random.choice(_TARGETS)

        tmpl_label = os.path.basename(tmpl)
        tgt_label  = os.path.basename(target)

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

                    # ── Step 1: POST uploadvideo ───────────────────────────
                    try:
                        with open(tmpl, "rb") as fh:
                            vid_bytes = fh.read()
                        resp = self._sess.post(
                            f"{self.host}/aitools/video-face-swap/v1/uploadvideo/",
                            files={"video": (os.path.basename(tmpl), vid_bytes, "video/mp4")},
                            data={"user_id": user_id},
                            timeout=None,
                        )
                        t_post        = time.time()
                        post_resp_len = len(resp.content)
                        if resp.status_code != 202:
                            failed = post_failed = True
                            issue  = f"upload_video_http_{resp.status_code}"
                        else:
                            try:
                                body = resp.json()
                                gid  = body.get("generation_id", "")
                                if not gid:
                                    failed = post_failed = True
                                    issue  = "upload_video: no generation_id"
                            except Exception as exc:
                                failed = post_failed = True
                                issue  = f"upload_video: invalid JSON: {exc}"
                    except gevent.Timeout:
                        raise
                    except Exception as exc:
                        t_post = time.time()
                        failed = post_failed = True
                        issue  = f"upload_video connection error: {exc}"

                    # ── Step 2: register CSV record ────────────────────────
                    record = {
                        "timestamp":               datetime.now().isoformat(),
                        "completed_at":            "",
                        "user_id":                user_id,
                        "subscription_tier":      subscription_tier,
                        "generation_id":          gid,
                        "template_video":         tmpl_label,
                        "target_image":           tgt_label,
                        "status":                 "submitted",
                        "output_url":             "",
                        "issue":                  "",
                        "post_ms":                0,
                        "queue_ms":               0,
                        "processing_ms":          0,
                        "total_ms":               0,
                        "failure_category":       "",
                        "sla_breach":             False,
                        "minute":                 minute,
                        "active_jobs":            _active_jobs,
                        "face_group_detection_ms": 0,
                        "target_detection_ms":    0,
                        "bind_ms":                0,
                        "swap_compute_ms":        0,
                    }
                    with _csv_lock:
                        _csv_records.append(record)

                    # ── Step 3: SSE pipeline — single persistent stream ────
                    if not failed:
                        try:
                            with self._sess.get(
                                f"{self.host}/aitools/jobs/{gid}/stream",
                                headers={"Accept": "text/event-stream"},
                                stream=True,
                                timeout=None,
                            ) as sse_resp:
                                if sse_resp.status_code != 200:
                                    failed = True
                                    issue  = f"SSE stream http_{sse_resp.status_code}"
                                else:
                                    sse_iter = sse_resp.iter_lines()

                                    # Checkpoint A — video face group detection
                                    step1 = self._next_checkpoint(
                                        sse_iter, {"face_groups"}, timing,
                                        done_key="face_groups_detected",
                                    )
                                    if step1 is None:
                                        failed = True
                                        issue  = "face_group_detection_failed"

                                    if not failed:
                                        group_ids = list(step1["face_groups"].keys())

                                        # Intermediate POST — upload target face image
                                        ok, err = self._post_upload_target_face(gid, target)
                                        if not ok:
                                            failed = True
                                            issue  = err
                                        else:
                                            timing["t_target_uploaded"] = time.time()

                                            # Checkpoint B — target face detection
                                            step2 = self._next_checkpoint(
                                                sse_iter, {"target_faces"}, timing,
                                                done_key="target_detected",
                                            )
                                            if step2 is None:
                                                failed = True
                                                issue  = "target_face_detection_failed"

                                            if not failed:
                                                # Intermediate POST — bind target face to group
                                                ok, err = self._post_bind_face(
                                                    gid, group_ids[0],
                                                    step2["target_faces"][0],
                                                )
                                                if not ok:
                                                    failed = True
                                                    issue  = err
                                                else:
                                                    timing["t_bind_done"] = time.time()

                                                    # Checkpoint C — ready for swap
                                                    step3 = self._next_checkpoint(
                                                        sse_iter, {"ready_for_swap"}, timing,
                                                        done_key="ready_for_swap",
                                                    )
                                                    if step3 is None:
                                                        failed = True
                                                        issue  = "ready_for_swap_timeout"

                                                    if not failed:
                                                        # Intermediate POST — start swap
                                                        ok, err = self._post_start_swap(
                                                            gid, group_ids
                                                        )
                                                        if not ok:
                                                            failed = True
                                                            issue  = err
                                                        else:
                                                            timing["t_faceswap_done"] = time.time()

                                                            # Checkpoint D — completion
                                                            step4 = self._next_checkpoint(
                                                                sse_iter, {"completed"}, timing,
                                                                done_key="completed",
                                                            )
                                                            if step4 is None:
                                                                failed = True
                                                                issue  = "completion_timeout"
                                                            else:
                                                                output_url = step4.get("output_url", "")
                        except gevent.Timeout:
                            raise
                        except Exception as exc:
                            failed = True
                            issue  = f"SSE connection error: {exc}"

            except gevent.Timeout:
                failed = True
                issue  = f"task timed out after {TASK_TIMEOUT}s"
                if t_post == t0:
                    t_post = time.time()
                logger.warning(f"[user={user_id}/{subscription_tier}] [gid={gid or 'NONE'}] {issue}")
                if record is None:
                    record = {
                        "timestamp":               datetime.now().isoformat(),
                        "completed_at":            "",
                        "user_id":                user_id,
                        "subscription_tier":      subscription_tier,
                        "generation_id":          gid,
                        "template_video":         tmpl_label,
                        "target_image":           tgt_label,
                        "status":                 "submitted",
                        "output_url":             "",
                        "issue":                  "",
                        "post_ms":                0,
                        "queue_ms":               0,
                        "processing_ms":          0,
                        "total_ms":               0,
                        "failure_category":       "",
                        "sla_breach":             False,
                        "minute":                 minute,
                        "active_jobs":            _active_jobs,
                        "face_group_detection_ms": 0,
                        "target_detection_ms":    0,
                        "bind_ms":                0,
                        "swap_compute_ms":        0,
                    }
                    with _csv_lock:
                        _csv_records.append(record)

            # ── Step 4: compute metrics ────────────────────────────────────
            t_end    = time.time()
            post_ms  = int((t_post - t0) * 1000)
            total_ms = int((t_end  - t0) * 1000)
            queue_ms = (
                int((timing["first_event"] - t_post) * 1000)
                if "first_event" in timing else 0
            )
            proc_ms = (
                int((timing["completed"] - timing["first_event"]) * 1000)
                if ("completed" in timing and "first_event" in timing) else 0
            )
            # Diagnostic: each sub-stage measured independently
            face_grp_ms = (
                int((timing["face_groups_detected"] - timing["first_event"]) * 1000)
                if ("face_groups_detected" in timing and "first_event" in timing) else 0
            )
            tgt_det_ms = (
                int((timing["target_detected"] - timing["t_target_uploaded"]) * 1000)
                if ("target_detected" in timing and "t_target_uploaded" in timing) else 0
            )
            bind_ms = (
                int((timing["ready_for_swap"] - timing["t_bind_done"]) * 1000)
                if ("ready_for_swap" in timing and "t_bind_done" in timing) else 0
            )
            swap_ms = (
                int((timing["completed"] - timing["t_faceswap_done"]) * 1000)
                if ("completed" in timing and "t_faceswap_done" in timing) else 0
            )
            sla_breach       = total_ms > SLA_THRESHOLD_MS
            failure_category = _categorize(issue)

            # ── Step 5: update CSV record ──────────────────────────────────
            with _csv_lock:
                record["status"]                 = "failed" if failed else "completed"
                record["completed_at"]           = datetime.fromtimestamp(t_end).isoformat()
                record["output_url"]             = output_url
                record["issue"]                  = issue
                record["post_ms"]                = post_ms
                record["queue_ms"]               = queue_ms
                record["processing_ms"]          = proc_ms
                record["total_ms"]               = total_ms
                record["failure_category"]       = failure_category
                record["sla_breach"]             = sla_breach
                record["face_group_detection_ms"] = face_grp_ms
                record["target_detection_ms"]    = tgt_det_ms
                record["bind_ms"]                = bind_ms
                record["swap_compute_ms"]        = swap_ms
                cnt      = len(_csv_records)
                snapshot = list(_csv_records) if cnt % _FLUSH_EVERY == 0 else None

            if snapshot is not None:
                spawn(_write_report, snapshot)

            # ── Step 6: fire Locust events — stage-gated, 3 only ──────────
            self.environment.events.request.fire(
                request_type="POST",
                name="vfs_post",
                response_time=post_ms,
                response_length=post_resp_len,
                exception=Exception(issue) if post_failed else None,
            )
            if not post_failed:
                self.environment.events.request.fire(
                    request_type="SSE",
                    name="vfs_queue",
                    response_time=queue_ms,
                    response_length=0,
                    exception=Exception(issue)
                              if (failed and "first_event" not in timing) else None,
                )
            if "first_event" in timing:
                self.environment.events.request.fire(
                    request_type="SSE",
                    name="vfs_processing",
                    response_time=proc_ms,
                    response_length=0,
                    exception=Exception(issue)
                              if (failed and "completed" not in timing) else None,
                )

            # ── Step 7: outcome logging ────────────────────────────────────
            if failed:
                logger.warning(
                    f"[user={user_id}/{subscription_tier}] [gid={gid or 'NONE'}] FAILED "
                    f"post={post_ms}ms queue={queue_ms}ms proc={proc_ms}ms "
                    f"active={_active_jobs} category={failure_category} — {issue}"
                )
            else:
                logger.info(
                    f"[user={user_id}/{subscription_tier}] [gid={gid}] COMPLETED "
                    f"post={post_ms}ms queue={queue_ms}ms proc={proc_ms}ms "
                    f"active={_active_jobs}"
                    f"{' SLA!' if sla_breach else ''}"
                )
                spawn(_save_output, gid, output_url)

        finally:
            with _jobs_lock:
                _active_jobs -= 1


# ── Events ─────────────────────────────────────────────────────────────────────
@events.test_start.add_listener
def on_test_start(environment, **kwargs):
    global _RUN_DIR, _VIDEOS, _TARGETS, TEST_START
    TEST_START = time.time()

    _VIDEOS  = _find_files(_INPUTS / "Face Swap/TemplateVideos", ".mp4")
    _TARGETS = _find_files(_INPUTS / "Face Swap/TargetImages",
                           ".jpg", ".jpeg", ".png")
    random.shuffle(_VIDEOS)
    random.shuffle(_TARGETS)

    if not _VIDEOS or not _TARGETS:
        logger.error(
            f"Missing inputs — videos={len(_VIDEOS)} targets={len(_TARGETS)} — stopping runner."
        )
        environment.runner.quit()
        return

    _RUN_DIR = get_locust_directory()
    _RUN_DIR.mkdir(parents=True, exist_ok=True)

    logger.info(
        f"\n{'─' * 60}\n"
        f"  Tool         : {TOOL_NAME}\n"
        f"  Target       : {os.getenv('BASE_URL', '(BASE_URL not set)')}\n"
        f"  Videos       : {len(_VIDEOS)} files\n"
        f"  Targets      : {len(_TARGETS)} files\n"
        f"  Task limit   : {TASK_TIMEOUT}s\n"
        f"  SLA target   : {SLA_THRESHOLD_MS}ms\n"
        f"  CSV flush    : every {_FLUSH_EVERY} records\n"
        f"  Save output  : {SAVE_OUTPUT}\n"
        f"  Output dir   : {_RUN_DIR}\n"
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

    total    = len(records)
    ok       = sum(1 for r in records if r["status"] == "completed")
    failed   = sum(1 for r in records if r["status"] == "failed")
    mid_stop = sum(1 for r in records if r["status"] == "not_completed")

    logger.info(
        f"\n{'─' * 60}\n"
        f"  Total requests : {total}\n"
        f"  Completed      : {ok}\n"
        f"  Failed         : {failed}\n"
        f"  Not completed  : {mid_stop}\n"
        f"{'─' * 60}"
    )

    _write_report(records)
    _write_errors(records)
    _write_queue_audit(records)
    _generate_summary(records)
    _aggregate_by_minute(records)
    _failure_breakdown(records)
