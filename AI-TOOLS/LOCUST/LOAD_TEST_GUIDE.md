# AI Tools Load Testing — Complete System Documentation

---

## 1. System Overview

This system load-tests five AI tools that follow an **async POST → SSE → completion** workflow. Requests do not return results immediately. Instead, each request submits a job and then listens to a real-time event stream until the job finishes.

### Tools Covered

| Script | Tool | Input | Endpoint |
|---|---|---|---|
| `t2i_locust.py` | Image Generation (T2I) | Prompt + optional reference image | `/aitools/image-generation/v1/generate` |
| `bgc_locust.py` | Background Change (BGC) | Image file or scene URL + prompt | `/aitools/background-change/v1/generate` |
| `bwc_locust.py` | BW Colorise (BWC) | Image file + mode + preset | `/aitools/colorize/v1/generate` |
| `rvc_locust.py` | Voice Remix (RVC) | Audio file + singer name + parameters | `/aitools/voice-remix/v1/convert` |
| `vc_locust.py` | Voice Clone (VC) | Audio file + input text | `/aitools/voice-clone/v1/generate` |

### Why Async Testing Is Different

In a standard REST API test, each request completes in milliseconds. One request = one measurement.

In this system, a single job takes **30–300 seconds** to complete. The job passes through multiple backend phases: accepted, queued, processed, completed. The client keeps a connection open (SSE) to receive live status updates.

Standard latency tools cannot measure this correctly. This system is purpose-built to:
- Track each phase individually (POST acceptance, queue wait, actual processing)
- Hold connections open across the full job lifetime
- Report meaningful breakdowns, not just "total time"

---

## 2. Architecture Design

### Request Pipeline

```
Virtual User
    │
    ▼
POST /generate          ← Submit job, receive generation_id
    │
    ▼ 202 Accepted
SSE /jobs/{id}/stream   ← Open persistent stream
    │
    ├── [queue phase]   ← Waiting for backend to pick up the job
    │
    ├── first event     ← Backend started processing
    │
    ├── [processing]    ← Model actively running
    │
    └── completed event ← Output URL delivered
```

### Sequential Execution per User

Each virtual user runs **one job at a time**:

1. POST the request
2. Block on SSE until completion or timeout
3. Wait 60–180 seconds
4. Repeat

This mirrors real user behavior. A user does not fire 10 requests simultaneously — they wait for each result before moving on.

### Why a Queue Exists

The backend queues jobs when the inference engine is busy. Jobs submitted in rapid succession do not all process immediately. The queue time measured here reflects real backend saturation — a critical metric under load.

### Why SSE Is Used

Server-Sent Events (SSE) is a standard HTTP streaming protocol. The client opens one long-lived GET connection. The server pushes newline-delimited JSON events as the job progresses. It is more efficient than polling and provides real-time status without WebSocket overhead.

---

## 3. Execution Flow (Step-by-Step)

```
Step 1 — User begins task
         Random inputs selected (prompt, image, audio, etc.)
         _active_jobs incremented

Step 2 — POST /generate sent
         multipart/form-data, no per-request timeout
         gevent.Timeout(TASK_TIMEOUT) is the only ceiling
         t0 recorded

Step 3 — 202 Accepted received
         generation_id extracted from JSON body
         t_post recorded
         CSV record created with status: "submitted"

Step 4 — SSE stream opened
         GET /aitools/jobs/{id}/stream
         stream=True, timeout=None

Step 5 — First parseable data: event arrives
         timing["first_event"] recorded
         queue_ms = first_event − t_post

Step 6 — Completed event arrives
         timing["completed"] recorded
         output URL extracted and validated
         processing_ms = completed − first_event

Step 7 — Metrics computed
         total_ms = t_end − t0
         sla_breach evaluated
         failure_category assigned
         Locust events fired (3 for standard tools, 5 for VC)

Step 8 — CSV record updated
         status set to "completed" or "failed"
         all timing fields written
         rolling flush triggered if record count % FLUSH_EVERY == 0

Step 9 — _active_jobs decremented
         always runs in finally block — guaranteed even on timeout
```

### Voice Clone Extended Flow

VC runs four HTTP operations in sequence:

```
Step 1 — POST upload (action=upload)        → t_upload_done
Step 2 — SSE wait for UPLOAD_VALIDATED      → timing["validated"]
Step 3 — POST generate (action=generate)    → t_generate_done
Step 4 — SSE wait for COMPLETED             → timing["completed"]
```

Five Locust events are fired: `vc_upload`, `vc_queue_validation`, `vc_generate`, `vc_queue_completion`, `vc_processing`.

---

## 4. Concurrency Model

### What a Locust User Represents

Each virtual user is an independent worker that runs tasks in a loop. It holds its own `requests.Session`, its own `user_id`, and executes exactly one job at a time before waiting.

### wait_time

```python
wait_time = between(60, 180)
```

After each task completes, the user waits 60–180 seconds before starting the next one. This simulates realistic user pacing — not a hammer test. It also prevents a single user from flooding the queue.

### Spawn Rate vs Concurrency

- **Spawn rate**: how many new users are created per second at startup
- **Concurrency**: how many users exist and are actively running jobs

With 10 users and jobs taking 60s each, approximately 10 jobs may be in-flight simultaneously. The `active_jobs` metric tracks this exactly.

### Why RPS Is Low

Because each job takes 30–300 seconds and users wait 60–180s between tasks, observed RPS will be low (0.05–0.5 per user per minute). This is expected and correct. Do not compare these numbers to REST API benchmarks.

The metrics that matter here are **job throughput per minute**, **success rate**, and **latency per phase** — not requests per second.

---

## 5. gevent and Greenlets

### What gevent Is

gevent is a Python concurrency library built on cooperative multitasking. It replaces blocking I/O with non-blocking equivalents, allowing thousands of concurrent operations in a single OS thread.

### What a Greenlet Is

A greenlet is a lightweight coroutine — far cheaper than an OS thread. Locust runs each virtual user as one greenlet. A single machine can comfortably run hundreds of greenlets simultaneously.

### Cooperative Multitasking

Unlike threads, greenlets do not preempt each other. A greenlet runs until it **voluntarily yields** — which happens automatically whenever it performs I/O (network read, write, sleep).

```
User A reading SSE stream  → yields (waiting for next event)
                                 ↓
User B sends POST request  → yields (waiting for response)
                                 ↓
User C processes SSE event → yields (waiting for next line)
                                 ↓
User A receives SSE line   → resumes
```

All of this happens in a single OS thread with no locking overhead.

### Why Blocking SSE Does Not Block Other Users

The SSE loop calls `resp.iter_lines()`, which reads from the network socket. When no new data has arrived, gevent detects the wait and switches to another greenlet automatically. User A's open SSE connection does not prevent User B from making a POST.

### gevent.Timeout

```python
with gevent.Timeout(TASK_TIMEOUT):
    # POST + full SSE wait
```

This is the single hard ceiling for the entire job. No per-request read timeouts are set — those would incorrectly disconnect SSE streams during long model runs.

---

## 6. Metrics System

### Timing Breakdown

```
t0 ──────────── t_post ──────────── first_event ──────────── completed ──── t_end
      POST ms          queue_ms              processing_ms
      (API accept)     (backend wait)        (model execution)

      ◄─────────────────────── total_ms ──────────────────────────────►
```

| Metric | Formula | Meaning |
|---|---|---|
| `post_ms` | `t_post − t0` | Time for API to accept the request and return 202 |
| `queue_ms` | `first_event − t_post` | Time the job waited in the backend queue |
| `processing_ms` | `completed − first_event` | Time the model spent executing |
| `total_ms` | `t_end − t0` | Full end-to-end user-facing latency |

> **total_ms ≈ post_ms + queue_ms + processing_ms**

### Supporting Metrics

| Metric | Type | Purpose |
|---|---|---|
| `failure_category` | string | Structured failure class for grouping and trend analysis |
| `sla_breach` | bool | `True` if `total_ms > SLA_THRESHOLD_MS` (default: 120,000ms) |
| `minute` | int | `int((t0 − TEST_START) / 60)` — time-series bucket for trend analysis |
| `active_jobs` | int | Concurrent in-flight jobs at the moment this job was submitted |

### failure_category Values

| Value | Trigger |
|---|---|
| `timeout` | `gevent.Timeout` fired — job exceeded `TASK_TIMEOUT` |
| `http_error` | Non-202 HTTP status from POST or SSE endpoint |
| `connection` | Network failure (refused, reset, DNS) |
| `sse_closed` | SSE stream ended without a completion event |
| `validation` | Missing `generation_id`, bad JSON, invalid output URL |
| `backend` | Server-sent error event — model or backend failure |

### Voice Clone Metrics

VC tracks 5 timing fields to cover its 4-step flow:

| Metric | Measures |
|---|---|
| `upload_ms` | POST upload response time |
| `validation_queue_ms` | Upload 202 → first validation SSE event |
| `generate_ms` | POST generate response time |
| `completion_queue_ms` | Generate 202 → first completion SSE event |
| `processing_ms` | First completion SSE event → COMPLETED |

---

## 7. Event Model (Locust UI)

Each job fires separate Locust events per phase. These appear as individual named rows in the Locust web UI and CSV stats output.

### Standard Tools (T2I, BGC, BWC, RVC)

| Event | Type | Fires When | Marked as Error If |
|---|---|---|---|
| `image_post` / `bgc_post` / etc. | POST | Always | POST returned non-202 or no `generation_id` |
| `image_queue` / `bgc_queue` / etc. | SSE | POST succeeded | SSE failed before first event arrived |
| `image_processing` / `bgc_processing` / etc. | SSE | First SSE event received | Failed before `completed` event |

**Events only fire for stages that were reached.** If the POST fails, queue and processing events are not fired — this keeps histograms accurate and avoids phantom zero-ms entries.

### Voice Clone (5 Events)

| Event | Type | Measures |
|---|---|---|
| `vc_upload` | POST | Upload POST latency |
| `vc_queue_validation` | SSE | Upload 202 → first validation SSE event |
| `vc_generate` | POST | Generate POST latency |
| `vc_queue_completion` | SSE | Generate 202 → first completion SSE event |
| `vc_processing` | SSE | First completion event → COMPLETED |

### Reading the Locust UI

| UI Row | What high P95 means |
|---|---|
| `*_post` | API gateway is slow to accept requests |
| `*_queue` | Backend queue is saturated, jobs waiting too long |
| `*_processing` | Model execution is slow — independent of load |

---

## 8. Data Storage (CSV System)

### Output Directory Structure

Each test run creates a timestamped directory:

```
AI-TOOLS/OUTPUTS/<ToolName>/2025-01-15_14-30-00/
    ├── report.csv    ← All jobs (1 row per job)
    └── errors.csv    ← Failed and not_completed rows only
```

### CSV Schema — Standard Tools

```
timestamp | user_id | generation_id | <tool-specific inputs> |
status | output_url | issue |
post_ms | queue_ms | processing_ms | total_ms |
failure_category | sla_breach | minute | active_jobs
```

### CSV Schema — Voice Clone

```
timestamp | user_id | generation_id | audio_file | input_text |
status | output_url | issue |
upload_ms | validation_queue_ms | generate_ms | completion_queue_ms | processing_ms | total_ms |
failure_category | sla_breach | minute | active_jobs
```

### Sample Row (T2I — successful job)

```
2025-01-15T14:32:11 | usr-pro-001 | gen-abc123 |
a futuristic city skyline | 16:9 | Input3.jpg | 1 |
completed | https://cdn.example.com/output.jpg | |
412 | 8240 | 22180 | 30832 |
 | False | 2 | 4
```

This tells you: the job was submitted at minute 2 of the test, with 4 concurrent jobs active. The API accepted it in 412ms, it waited 8.2s in queue, the model took 22.2s, total 30.8s — within the 120s SLA.

### Status Values

| Status | Meaning |
|---|---|
| `submitted` | Transient — job accepted, SSE in progress |
| `completed` | SSE delivered a valid output URL |
| `failed` | HTTP error, SSE error event, or invalid output |
| `not_completed` | Test stopped before this job's SSE resolved |

### Rolling CSV Flush

```python
_FLUSH_EVERY = int(os.getenv("CSV_FLUSH_EVERY", "10"))

# After each job completes, inside the csv_lock:
if len(_csv_records) % _FLUSH_EVERY == 0:
    spawn(_write_report, snapshot)
```

Every 10 completed jobs, a snapshot of all records is written to disk by a background greenlet. Data is never lost if the test is killed. The final `on_test_stop` always writes the complete authoritative file.

---

## 9. Analysis Functions

Three functions run automatically at test end, printed to the log after CSV files are written.

### `_generate_summary(records)`

Prints a structured performance summary:
- Total / completed / failed / not-completed counts with percentages
- SLA breach count and percentage of completed jobs
- Average `queue_ms` and `processing_ms` for completed jobs only
- **Bottleneck detection**: compares averages to identify whether time is dominated by queue wait or model processing
- Sample size warning if fewer than 100 jobs were run

```
════════════════════════════════════════════════════════════
  PERFORMANCE SUMMARY — Image Generation
────────────────────────────────────────────────────────────
  Total jobs        : 142
  Completed         : 135  (95.1%)
  Failed            : 7
  Not completed     : 0
  SLA breaches      : 12  (8.9% of completed)
────────────────────────────────────────────────────────────
  Avg queue_ms      :     6240 ms
  Avg processing_ms :    18920 ms
  Bottleneck        : PROCESSING BOTTLENECK
════════════════════════════════════════════════════════════
```

### `_aggregate_by_minute(records)`

Groups all records by `minute` bucket. For each minute prints: job count, average queue_ms, average processing_ms (completed jobs only).

```
────────────────────────────────────────────────────────────
  TIME-SERIES (per minute)
────────────────────────────────────────────────────────────
  Minute   0 → jobs=  6  queue=   3100ms  processing=  17200ms
  Minute   1 → jobs=  9  queue=   5400ms  processing=  19800ms
  Minute   2 → jobs= 11  queue=  12300ms  processing=  21100ms
────────────────────────────────────────────────────────────
```

A rising `queue_ms` trend indicates the backend queue is filling faster than it drains. A rising `processing_ms` trend suggests resource contention at the model layer.

### `_failure_breakdown(records)`

Counts `failure_category` occurrences across all failed and not-completed records, sorted by frequency.

```
────────────────────────────────────────────────────────────
  FAILURE BREAKDOWN
────────────────────────────────────────────────────────────
  timeout              : 5
  backend              : 2
────────────────────────────────────────────────────────────
```

If the dominant category is `timeout`, jobs are taking too long — queue or model is the bottleneck. If `connection` dominates, there is a network or infrastructure problem.

---

## 10. Logging System

Every job completion emits a structured log line to stdout with full timing context.

### Success Log

```
INFO [user=usr-pro-001] [gid=gen-abc123] COMPLETED
     in 30832ms (post=412 queue=8240 proc=22180) active=4 — https://cdn.../output.jpg
```

### Failure Log

```
WARNING [user=usr-pro-001] [gid=NONE] FAILED
        in 300001ms (post=0 queue=0 proc=0) active=6 category=timeout
        — task timed out after 300s (POST + SSE)
```

### SLA Breach Marker

When `total_ms > SLA_THRESHOLD_MS`, ` SLA!` is appended inline:

```
INFO ... COMPLETED in 145200ms (...) active=3 SLA! — https://cdn.../output.jpg
```

### active= Field

The `active=N` value in every log line shows the live concurrent job count at completion time. This allows manual correlation between concurrency spikes and latency changes directly in the log — no external tool needed for first-pass analysis.

### Voice Clone Log

VC logs all 5 phases:

```
INFO [user=usr-pro-001] [gid=gen-xyz] COMPLETED in 89400ms
     (upload=820 val_q=4200 gen=310 comp_q=6100 proc=77500)
     active=3 — https://cdn.../output.mp3
```

---

## 11. How to Read Results

### Diagnosing Performance Problems

| Symptom | Likely Cause | Where to Look |
|---|---|---|
| High `post_ms` | API gateway slow to accept | `*_post` in Locust UI |
| High `queue_ms` | Backend queue saturated | `*_queue` + `active_jobs` in CSV |
| High `processing_ms` | Model slow regardless of load | `*_processing` in Locust UI |
| Many `timeout` failures | Jobs never completing | `failure_breakdown` + `queue_ms` trend |
| Many `backend` failures | Model crashes or errors | `errors.csv` → `issue` column |
| Rising latency over time | System degrading under sustained load | `_aggregate_by_minute` output |
| `QUEUE BOTTLENECK` | Waiting time > execution time | Scale backend workers or queue capacity |
| `PROCESSING BOTTLENECK` | Execution time > waiting time | Optimize model or add GPU capacity |

### Using `active_jobs` in the CSV

Sort the CSV by `queue_ms` descending. Check the `active_jobs` values in the high-latency rows. If high queue times consistently appear when `active_jobs >= N`, that N is the system's concurrency limit. Keeping `active_jobs` below that threshold prevents SLA breaches.

### Using `minute` for Trend Analysis

Filter the CSV to `status=completed`. Pivot on `minute`, compute average `queue_ms` per bucket. A flat line indicates a stable system. A rising curve means queue depth is growing — the system cannot clear jobs as fast as they arrive. A spike at a single minute indicates a transient event (deployment, GC pause, hardware issue).

### Correlating SLA Breaches with Concurrency

Filter rows where `sla_breach=True`. Check the `active_jobs` distribution in those rows. If all SLA breaches occur at `active_jobs >= 8`, the system can safely handle 7 concurrent jobs within the SLA but fails at 8 — a concrete capacity boundary.

---

## 12. System Strengths

- **Full latency decomposition**: POST / queue / processing are tracked independently. You know exactly where time is spent, not just the total.
- **Realistic simulation**: Sequential per-user execution with realistic wait times mirrors actual user behavior — not artificial hammering.
- **Structured failure analysis**: Every failure has a named category. Post-test breakdown immediately shows whether the issue is network, API, queue, or model.
- **Time-series built in**: `minute` bucketing enables degradation curves without external tooling. You can see exactly when the system begins struggling.
- **Concurrency visibility**: `active_jobs` is recorded per-job, enabling direct correlation between job count and latency changes in the raw CSV.
- **Crash-safe data collection**: Rolling flush every N records means partial results survive test crashes, kills, and OOM events.
- **Zero disk I/O during test**: All binary inputs (images, audio) are preloaded into memory at startup. No filesystem reads occur inside the task loop.
- **VC 5-stage instrumentation**: Each of the four HTTP operations and both SSE waits in the Voice Clone flow is measured and reported independently.

---

## 13. Limitations and Considerations

### Low Sample Size

The warning `Low sample size (<100 jobs)` is triggered automatically. With fewer than 100 jobs, P95 and P99 values in the Locust UI are statistically unreliable. Run tests long enough to exceed this threshold for meaningful percentile data.

### Low RPS Is Expected

With jobs taking 30–300 seconds and users waiting 60–180s between tasks, observed RPS will be very low (0.05–0.5 per user per minute). This is correct by design. The meaningful metrics are **jobs per minute**, **success rate**, and **latency per phase** — not requests per second.

### Client-Side Timing Only

This system captures client-observed timing. It does not instrument server-side queue depth, GPU utilization, or worker count. For backend root cause analysis, correlate these CSV results with server-side metrics (Grafana, Prometheus, CloudWatch) using the `minute` field as the time axis and `generation_id` for per-job tracing.

### No SSE Reconnection

If an SSE connection drops mid-stream (network hiccup), the result is recorded as `sse_closed`. There is no automatic reconnect. This is intentional — it reflects real client behavior and correctly surfaces network instability as a measurable failure mode.

### CPU-Bound Code in Greenlets

gevent's cooperative model means a CPU-bound operation inside one greenlet blocks all others until it yields. All operations here are I/O-bound (network calls), so this is not a concern in practice. Avoid adding CPU-heavy logic inside `fire_generate`.

---

## 14. Final Summary

**What makes this system production-grade:**

- Measures the complete async job lifecycle, not just HTTP round-trip time
- Decomposes latency into three independently actionable phases: POST, queue, processing
- Every failure is categorized — no unclassified errors in analysis
- Time-series data is built into every record — no external aggregation required
- Active concurrency is tracked per-record — enables direct correlation in the raw CSV
- Data survives crashes via rolling flush
- Voice Clone gets 5-stage instrumentation covering its unique 4-step HTTP flow

**How it differs from basic load tests:**

| Basic Load Test | This System |
|---|---|
| Measures HTTP response time only | POST + queue + processing measured separately |
| All failures logged as "error" | Six structured categories: timeout / http_error / connection / sse_closed / validation / backend |
| No time-series without external tools | `minute` field enables per-minute trend analysis from CSV alone |
| Cannot test async workflows | Designed specifically for POST → SSE → completion patterns |
| One stat per request | 3–5 Locust events per job, each covering a distinct backend phase |
| Data lost on crash | Rolling flush preserves partial results |

**What questions this system can answer:**

| Question | Source |
|---|---|
| Is the API accepting requests fast enough? | `post_ms` — Locust UI + CSV |
| Is the backend queue saturating under load? | `queue_ms` trend — `_aggregate_by_minute` |
| Is the model fast enough at target concurrency? | `processing_ms` — Locust UI |
| What is breaking and why? | `failure_category` — `_failure_breakdown` |
| When does the system start degrading? | `minute` + `queue_ms` trend |
| How does concurrency affect latency? | `active_jobs` vs `queue_ms` in CSV |
| Are we meeting SLAs? | `sla_breach` count — `_generate_summary` |
| What concurrency is safe within SLA? | Filter `sla_breach=True`, check `active_jobs` distribution |
