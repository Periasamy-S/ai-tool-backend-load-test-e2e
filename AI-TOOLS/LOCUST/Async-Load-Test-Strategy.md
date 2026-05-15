# Async Load Testing Strategy — Locust
### BW Colorise API · Production-Grade Strategy for 15,000 Concurrent Users

---

## Executive Summary

This document describes the design, analysis, and corrected load testing strategy for an asynchronous GPU-backed API. The system under test uses a fire-and-forget job model: a client POSTs a request, receives a `generation_id`, then streams job status via Server-Sent Events (SSE).

The initial test configuration appeared correct but contained two parametric flaws that would have caused the load generator itself to collapse before the backend reached meaningful stress: an overly aggressive `wait_time` and an unbounded greenlet spawning model. Left uncorrected, these would produce 90,000+ simultaneous SSE connections from the Locust workers — consuming more resources than the backend under test.

This document explains the root cause, the corrected architecture, scaling math, and a step-by-step execution roadmap for validating a 15K-user production scenario.

---

## Table of Contents

1. [System Overview](#1-system-overview)
2. [Root Cause Analysis](#2-root-cause-analysis)
3. [Corrected Strategy](#3-corrected-strategy)
4. [Architecture Design](#4-architecture-design)
5. [Scaling Strategy for 15,000 Users](#5-scaling-strategy-for-15000-users)
6. [Load Testing Approaches](#6-load-testing-approaches)
7. [Calculations Reference](#7-calculations-reference)
8. [Failure Modes](#8-failure-modes)
9. [Monitoring Strategy](#9-monitoring-strategy)
10. [Execution Roadmap](#10-execution-roadmap)
11. [Best Practices & Key Insights](#11-best-practices--key-insights)

---

## 1. System Overview

### 1.1 The Async Job Workflow

The API under test follows a standard async GPU processing pattern. No response is returned immediately — the client must poll for completion.

```
┌─────────┐   POST /colorize/v1/generate    ┌─────────────┐
│  Client │ ─────────────────────────────► │  API Gateway │
│         │ ◄── 202 + { generation_id } ── │             │
│         │                                 └──────┬──────┘
│         │                                        │ enqueue
│         │                                 ┌──────▼──────┐
│         │   GET /jobs/{id}/stream (SSE)   │  Job Queue  │
│         │ ─────────────────────────────► │             │
│         │                                 └──────┬──────┘
│         │                                        │ process
│         │                                 ┌──────▼──────┐
│         │ ◄── SSE: { status: completed } ─│  GPU Worker │
└─────────┘                                 └─────────────┘
```

### 1.2 Testing Goal

| Goal | Value |
|---|---|
| Target concurrent users | 15,000 |
| Endpoint under test | POST `/aitools/colorize/v1/generate` |
| Completion signal | SSE `status: completed` |
| End-to-end metric | Custom `JOB` event: `colorize_job_complete` |
| Locust UI visibility | POST visible · SSE silent · JOB visible |

### 1.3 Three Distinct Signals in Locust

The test architecture deliberately separates three observable events:

| Signal | Type | Locust UI | Measures |
|---|---|---|---|
| `colorize_generate` | HTTP POST | Visible | Submission latency (202 response time) |
| SSE stream | HTTP GET | **Silent** | Not reported — internal plumbing |
| `colorize_job_complete` | Custom JOB | Visible | **End-to-end job latency** |

This separation is critical: the POST latency and the job completion latency are independent signals. Mixing them produces meaningless aggregates.

---

## 2. Root Cause Analysis

### 2.1 The Original Configuration

```python
# Original — PROBLEMATIC
wait_time = between(5, 15)      # avg 10s
SSE_TIMEOUT = 120               # 120s per job
# Each fire_generate() → spawn(self._watch_job, ...)  [new greenlet per job]
```

This looks reasonable in isolation. At scale it is catastrophic.

### 2.2 The Concurrency Multiplier Problem

The fundamental issue is the ratio between job lifetime and think time.

**Formula:**
```
In-flight jobs per user = avg_job_duration / avg_wait_time
```

**Original values:**
```
avg_job_duration = 60s   (GPU colorization)
avg_wait_time    = 10s   (between 5 and 15)

In-flight ratio  = 60 / 10 = 6 concurrent jobs per user
```

**At 15,000 users:**

| Metric | Calculation | Result |
|---|---|---|
| Concurrent jobs (SSE streams) | 15,000 × 6 | **90,000** |
| POST rate | 15,000 / 10 | **1,500 req/s** |
| Total greenlets | 15,000 + 90,000 | **105,000** |
| Estimated memory | 105,000 × ~50KB | **~5 GB** |

**1,500 POST/s into a GPU backend** is not load testing — it is denial of service. A cluster of 50 GPUs at 5 jobs/GPU/min processes ~4 jobs/s. The queue grows at **1,496 jobs/second**.

### 2.3 Greenlet Explosion

The unbounded `spawn()` pattern creates one new greenlet per submitted job:

```
Time 0s:   User fires job A → spawn(watch A)      [greenlets: 2]
Time 10s:  User fires job B → spawn(watch B)      [greenlets: 3]
Time 20s:  User fires job C → spawn(watch C)      [greenlets: 4]
...
Time 60s:  Job A completes, watch A exits          [greenlets: 3 still live]
```

At steady state (job duration > wait_time), each user accumulates multiple live watcher greenlets simultaneously. The count grows until job completions balance new submissions — which, with a saturated backend, may never happen.

```
Greenlet growth rate = spawn_rate - completion_rate
                     = 1,500/s   -  4/s
                     = 1,496 new greenlets/second
```

The Locust workers run out of memory before the backend reaches a measurable steady state.

### 2.4 Why wait_time = 5–15s is Wrong for This System

`wait_time` in Locust represents a user's think time — the pause between completing one action and starting the next. For a GPU AI tool that takes 30–120 seconds to process:

- A real user submits a job, waits for it to finish, reviews the result, then submits another
- The actual think time is: `job_completion_time + review_time`
- For colorization: minimum realistic think time is **60–180 seconds**
- `wait_time = between(5, 15)` simulates a bot clicking submit every 10 seconds, not a human

---

## 3. Corrected Strategy

### 3.1 Fix 1 — Realistic wait_time

```python
# Corrected
wait_time = between(60, 180)   # avg 120s — realistic human think time
```

**Impact:**
```
In-flight ratio = 60 / 120 = 0.5 concurrent jobs per user

At 15,000 users:
  Concurrent SSE streams = 15,000 × 0.5 = 7,500   (was 90,000)
  POST rate              = 15,000 / 120  = 125/s   (was 1,500/s)
```

A 12× reduction in backend pressure and a 12× reduction in SSE connection load from a single parameter change.

### 3.2 Fix 2 — Per-User Queue + Single Watcher Greenlet

Replace unbounded spawning with a deterministic model: one watcher greenlet per user, processing jobs sequentially through a queue.

```python
# Corrected model
def on_start(self):
    self._job_q   = gevent.queue.Queue()
    self._watcher = spawn(self._process_queue)   # exactly 1 extra greenlet per user

def _process_queue(self):
    while True:
        item = self._job_q.get()   # blocks cooperatively (yields to event loop)
        if item is None:
            break
        self._watch_job(*item)     # sequential — one SSE stream at a time

def fire_generate(self):
    ...
    self._job_q.put((gid, record, t0))   # non-blocking enqueue
```

**Why sequential is correct here:**
- Real users don't have 6 parallel jobs running simultaneously
- With `wait_time = between(60, 180)` and avg job duration 60s, jobs complete before the user submits the next one
- Sequential watching means max 1 open SSE connection per user at any time

**Greenlet count:**

| Model | Formula | At 15K Users |
|---|---|---|
| Unbounded spawn | 15K + (15K × 6) | **105,000** |
| Per-user queue  | 15K + 15K | **30,000** |

### 3.3 Fix 3 — SSE Invisibility via Raw Session

The SSE request must not appear in the Locust UI. Two options exist:

| Approach | Mechanism | Verdict |
|---|---|---|
| `self.client` with no `name=` | Still fires events internally | Unreliable suppression |
| `self.client` with `catch_response` + never call success/failure | Leaks as unnamed requests | Messy |
| **`requests.Session()` directly** | Bypasses Locust's HttpSession entirely | **Correct** |

```python
self._sess = requests.Session()   # initialized in on_start
# All SSE calls use self._sess, not self.client
```

### 3.4 Fix 4 — The JOB Metric

The custom JOB event captures true end-to-end latency:

```
t0 = time.time()    ← set immediately after 202 confirmed
...SSE stream resolves...
elapsed = time.time() - t0

self.environment.events.request.fire(
    request_type = "JOB",
    name         = "colorize_job_complete",
    response_time = int(elapsed * 1000),
    response_length = 0,
    exception    = None               # or Exception(issue) on failure
)
```

This appears in the Locust UI as a separate request type, giving clean p50/p95/p99 distributions for actual job completion time — independent of HTTP submission latency.

---

## 4. Architecture Design

### 4.1 Per-User Flow

```
ColorizeUser
│
├── on_start()
│   ├── self._sess    = requests.Session()   ← raw, invisible to UI
│   ├── self._job_q   = gevent.queue.Queue()
│   └── self._watcher = spawn(_process_queue)  ← 1 greenlet, lives for user lifetime
│
├── @task fire_generate()  [runs every 60–180s]
│   ├── POST /generate via self.client         ← VISIBLE in Locust UI
│   ├── receive 202 + generation_id
│   ├── t0 = time.time()
│   ├── build CSV record { status: submitted }
│   └── self._job_q.put((gid, record, t0))    ← non-blocking
│
├── _process_queue()  [running in watcher greenlet]
│   └── loop:
│       ├── item = self._job_q.get()           ← yields until job arrives
│       └── _watch_job(gid, record, t0)        ← sequential
│
└── _watch_job(gid, record, t0)
    ├── GET /jobs/{gid}/stream via self._sess  ← SILENT (raw session)
    ├── parse SSE events until completed/failed/timeout
    ├── update CSV record
    └── events.request.fire(type="JOB", ...)  ← VISIBLE in Locust UI
```

### 4.2 Data Flow Diagram

```
  fire_generate()                _process_queue()             _watch_job()
  ─────────────                  ────────────────             ────────────
       │                                │                          │
       │  POST /generate                │                          │
       │──────────────────────────────────────────────────────►   │
       │  202 + gid                     │                          │
       │◄──────────────────────────────────────────────────────   │
       │                                │                          │
       │  _job_q.put((gid,rec,t0))      │                          │
       │──────────────────────────────►│                          │
       │                                │                          │
       │  [wait_time: 60–180s]         │  _watch_job(gid,rec,t0) │
       │                                │─────────────────────────►│
       │                                │                          │  GET /stream (SSE)
       │                                │                          │──────────────────►
       │                                │                          │  status: completed
       │                                │                          │◄──────────────────
       │                                │                          │
       │                                │                          │  fire JOB metric
       │                                │                          │──────────────────►
       │                                │◄─────────────────────────│
       │  next task fires               │  ready for next job      │
```

### 4.3 Before vs. After Comparison

```
═══════════════════════════════════════════════════════════════════
  BEFORE (unbounded spawn)          AFTER (per-user queue)
═══════════════════════════════════════════════════════════════════

  User 1 ──► spawn(watch job A)      User 1 ──► queue ──► watcher
         ──► spawn(watch job B)               put(job A) ──► SSE A
         ──► spawn(watch job C)               put(job B)     (wait)
                                              put(job C)     ──► SSE B
  User 2 ──► spawn(watch job D)                             (wait)
         ──► spawn(watch job E)               ...            ──► SSE C
         ──► spawn(watch job F)
                                    User 2 ──► queue ──► watcher
  [1 user = 7 greenlets at t=60s]            put(job D) ──► SSE D
  [15K users = 105K greenlets]               ...
                                    [1 user = 2 greenlets always]
                                    [15K users = 30K greenlets]

  SSE connections: 90,000           SSE connections: ≤15,000
  POST rate:       1,500/s          POST rate:        125/s
  Memory est.:     7.5 GB           Memory est.:      1.5 GB
═══════════════════════════════════════════════════════════════════
```

### 4.4 Distributed Architecture

```
                        ┌─────────────────┐
                        │  Locust Master  │
                        │  (orchestrator) │
                        │  Port 8089 UI   │
                        └────────┬────────┘
                                 │ control + aggregation
           ┌─────────────────────┼─────────────────────┐
           │                     │                     │
    ┌──────▼──────┐       ┌──────▼──────┐      ┌──────▼──────┐
    │  Worker 01  │       │  Worker 02  │  ... │  Worker 20  │
    │  750 users  │       │  750 users  │      │  750 users  │
    │  1.5GB RAM  │       │  1.5GB RAM  │      │  1.5GB RAM  │
    └──────┬──────┘       └──────┬──────┘      └──────┬──────┘
           │                     │                     │
           └─────────────────────┼─────────────────────┘
                                 │ HTTP + SSE
                        ┌────────▼────────┐
                        │   API Gateway   │
                        │  (load balanced)│
                        └────────┬────────┘
                                 │
                    ┌────────────┼────────────┐
                    │            │            │
             ┌──────▼──┐  ┌─────▼───┐  ┌────▼────┐
             │  GPU 01 │  │  GPU 02 │  │  GPU N  │
             └─────────┘  └─────────┘  └─────────┘
```

---

## 5. Scaling Strategy for 15,000 Users

### 5.1 Worker Sizing

| Parameter | Value | Reasoning |
|---|---|---|
| Users per worker | 750 | ~1.5 GB RAM per worker at ~2 KB/greenlet + session overhead |
| Workers required | 20 | 20 × 750 = 15,000 |
| Worker spec | 8 core / 16 GB | Headroom for gevent event loop + OS |
| Spawn rate | 10 users/s | 15,000 / 10 = 25 min ramp — avoids cold-start spike |
| SSE_TIMEOUT | 180s | Covers max job duration + network variance |

### 5.2 Concurrency at Steady State

```
At full load (15,000 users, wait_time avg 120s, job_time avg 60s):

  In-flight ratio    = 60s / 120s         = 0.5
  Concurrent SSE     = 15,000 × 0.5       = 7,500 open streams
  Concurrent SSE/wkr = 7,500 / 20 workers = 375 per worker
  POST rate total    = 15,000 / 120       = 125 req/s
  POST rate/worker   = 125 / 20           = 6.25 req/s per worker
```

### 5.3 Resource Budget Per Worker

| Resource | Per User | Per Worker (750 users) |
|---|---|---|
| Greenlets | 2 (task + watcher) | 1,500 |
| `requests.Session` | 1 raw + 1 HttpSession | 1,500 session objects |
| Active SSE connections | 0.5 avg | ~375 open streams |
| Memory (greenlet stack) | ~16 KB | ~24 MB |
| Memory (session + buffers) | ~100 KB | ~75 MB |
| Memory (SSE response buffers) | ~80 KB per active stream | ~30 MB |
| **Total estimated RAM** | | **~150–200 MB** |

16 GB worker RAM provides **80× headroom** over the minimum — leaving ample room for OS, Python runtime, and measurement overhead.

### 5.4 Network Considerations

```
File descriptors per worker:
  750 task greenlets  × 1 HttpSession connection   =   750 fds
  375 active SSE      × 1 raw session connection   =   375 fds
  OS overhead                                       =   100 fds
  ──────────────────────────────────────────────────────────────
  Total                                             = ~1,225 fds

Linux default ulimit -n = 1,024 → INSUFFICIENT
Required: ulimit -n 10000 (minimum), 100000 (recommended)
```

**Required OS tuning on every worker:**
```bash
ulimit -n 100000
sysctl -w net.ipv4.ip_local_port_range="1024 65535"
sysctl -w net.ipv4.tcp_tw_reuse=1
sysctl -w net.core.somaxconn=65535
```

---

## 6. Load Testing Approaches

Two fundamentally different goals require different configurations.

### 6.1 Realistic User Simulation

**Goal:** Understand the system under conditions that mirror actual usage.

```python
wait_time = between(60, 180)   # human think time
users     = 15,000
workers   = 20
```

| Characteristic | Value |
|---|---|
| POST rate | ~125/s |
| Concurrent SSE streams | ~7,500 |
| Backend queue behavior | Stable (processing rate can keep up) |
| Test duration | 60–90 min (include ramp + steady state + ramp-down) |
| Primary insight | p95/p99 job completion time under realistic concurrency |

**Use when:** Capacity planning, SLA validation, regression testing.

### 6.2 Stress / Breaking Point Test

**Goal:** Find the backend's saturation threshold.

```python
wait_time = between(1, 3)     # fire as fast as possible
users     = 200–500
workers   = 3–5
```

| Characteristic | Value |
|---|---|
| POST rate | ~100–250/s |
| Concurrent SSE streams | ~2,000–5,000 |
| Backend queue behavior | Grows until GPU throughput = arrival rate |
| Test duration | 15–30 min |
| Primary insight | Max sustainable throughput, queue saturation point, error rates under load |

**Use when:** Finding breaking points, validating auto-scaling, testing circuit breakers.

### 6.3 Decision Matrix

| Scenario | Users | wait_time | Duration | Goal |
|---|---|---|---|---|
| Smoke test | 5 | 30s | 5 min | Verify pipeline end-to-end |
| Baseline | 50 | 60–180s | 20 min | Establish p50/p95 latency |
| Load | 1,000 | 60–180s | 30 min | Validate at meaningful scale |
| Stress | 300 | 1–3s | 20 min | Find throughput ceiling |
| Soak | 500 | 60–180s | 4 hr | Detect memory leaks, CSV integrity |
| Scale | 15,000 | 60–180s | 90 min | Full production simulation |

---

## 7. Calculations Reference

### 7.1 In-Flight Concurrency

```
in_flight_per_user = avg_job_duration_s / avg_wait_time_s

Examples:
  wait=10s, job=60s  →  6.0  concurrent jobs/user   [DANGEROUS]
  wait=60s, job=60s  →  1.0  concurrent jobs/user   [borderline]
  wait=120s, job=60s →  0.5  concurrent jobs/user   [SAFE]
  wait=180s, job=60s →  0.33 concurrent jobs/user   [conservative]
```

### 7.2 Total SSE Connections

```
total_SSE = users × in_flight_per_user

  Before: 15,000 × 6.0  = 90,000  connections
  After:  15,000 × 0.5  =  7,500  connections
```

*Note: With per-user sequential watcher, `in_flight_per_user` is capped at 1.0 regardless of the ratio.*

### 7.3 POST Rate

```
post_rate_per_s = users / avg_wait_time_s

  Before: 15,000 / 10  = 1,500  req/s
  After:  15,000 / 120 =   125  req/s
```

### 7.4 Backend Queue Depth (steady state)

```
arrival_rate    = POST rate = 125 jobs/s
processing_rate = GPU_count × jobs_per_GPU_per_second

Example: 20 GPUs, each processing 1 job/10s:
  processing_rate = 20 × 0.1 = 2 jobs/s

  Queue growth = arrival_rate - processing_rate
               = 125 - 2 = 123 jobs/s  → queue grows linearly

Queue is stable only when: processing_rate ≥ arrival_rate
Required GPUs at 125 jobs/s, 10s/job: 125 × 10 = 1,250 GPU-seconds = 1,250 GPUs
```

This calculation reveals the **backend capacity gap** — the test is designed to expose it, not hide it.

### 7.5 Greenlet Count Comparison

```
Model A (unbounded spawn):
  greenlets = users + (users × in_flight_per_user)
            = 15,000 + (15,000 × 6)
            = 105,000

Model B (per-user queue):
  greenlets = users × 2    [1 task greenlet + 1 watcher greenlet]
            = 15,000 × 2
            = 30,000

Reduction: 105,000 → 30,000 = 71% fewer greenlets
```

### 7.6 Memory Estimation

```
Per user (Model B):
  Task greenlet stack:    16 KB
  Watcher greenlet stack: 16 KB
  requests.Session (raw):  5 KB
  HttpSession (Locust):   15 KB
  Active SSE buffer:      80 KB  (only when stream is open)
  Python object overhead: 10 KB
  ─────────────────────────────
  Total per user:        ~142 KB  (no active SSE)
                         ~222 KB  (active SSE)

At 15,000 users (50% with active SSE):
  = 7,500 × 142KB + 7,500 × 222KB
  = 1.065 GB + 1.665 GB
  = ~2.7 GB total across all workers
  = ~135 MB per worker (20 workers)
```

---

## 8. Failure Modes

### 8.1 Worker-Side Failures

| Failure | Root Cause | Symptom | Threshold |
|---|---|---|---|
| File descriptor exhaustion | `ulimit -n` too low | `OSError: [Errno 24] Too many open files` | Default limit: 1,024 fds |
| Worker OOM | Memory grows beyond available RAM | Worker process killed silently; Locust master shows worker disconnect | >80% of RAM |
| gevent loop lag | Too many greenlets competing for single event loop | JOB `response_time` inflated by 5–30s; p99 spikes not caused by backend | >50K greenlets per worker |
| Locust master heartbeat loss | Worker CPU at 100% | User count drops unexpectedly mid-test | Worker CPU >95% sustained |

### 8.2 Network-Side Failures

| Failure | Root Cause | Symptom | Detection |
|---|---|---|---|
| LB idle timeout kills SSE | Load balancer drops connections inactive >60–120s | Mass `SSE timeout after 120s` failures; appears as backend failure | Compare LB timeout config vs SSE_TIMEOUT |
| TCP port exhaustion | Single NIC, single destination IP, >28,000 concurrent connections | `EADDRNOTAVAIL` on new SSE connections | `ss -s` on worker: TIME_WAIT count |
| DNS resolution failures | High connection rate + short TTL | Intermittent `SSE HTTP 0` errors | Add DNS caching on workers |

### 8.3 Backend-Side Failures

| Failure | Root Cause | Symptom | Detection |
|---|---|---|---|
| Job queue saturation | POST rate > GPU processing rate | SSE streams stall; all jobs timeout; `not_completed` fills CSV | Monitor queue depth metric on backend |
| API gateway connection limit | nginx default `worker_connections: 1024` | HTTP 502/503 on SSE requests | `colorize_job_complete` failure rate spikes |
| GPU worker crash | OOM during large image processing | SSE `status: failed` with error | Backend error logs; JOB failure rate |
| Backend SSE heartbeat absent | No `:` heartbeat lines sent by server | LB drops connection before job completes | Check if server sends `:\n\n` keepalives |

### 8.4 Measurement-Side Failures

| Failure | Root Cause | Symptom |
|---|---|---|
| Inflated JOB latency | gevent loop lag on overloaded worker | p95 looks bad; POST p95 is fine |
| `not_completed` in CSV at test stop | Watcher greenlet in-flight when test_stop fires | Expected — mark as acceptable edge case |
| Duplicate JOB failures | Exception path fires metric before CSV update | Only possible if both paths run — guarded by single-greenlet model |

---

## 9. Monitoring Strategy

### 9.1 Locust UI — Key Metrics

| Metric | Watch For | Action if Triggered |
|---|---|---|
| `colorize_generate` RPS | Trending down over time | Backend rate-limiting or rejecting — check 429/503 rate |
| `colorize_generate` failure % | >1% | Investigate HTTP error codes in failures tab |
| `colorize_job_complete` p95 | Exceeds SSE_TIMEOUT × 0.8 | Backend queue saturation; increase SSE_TIMEOUT or reduce user count |
| `colorize_job_complete` failure % | Steady climb | LB timeout, backend failures, or SSE stream drops |
| Gap: generate count vs job_complete count | Widens over time | Queue is growing; backend cannot keep up |

### 9.2 Worker Node Monitoring

```bash
# File descriptor count — run on each worker
watch -n 5 'ls /proc/$(pgrep -f locust)/fd | wc -l'
# Alert if > 80% of ulimit value

# Memory
watch -n 5 'ps -o pid,rss,vsz,comm -p $(pgrep -f locust)'
# Alert if RSS > 70% of available RAM

# Active TCP connections
ss -s | grep -E 'estab|TIME-WAIT'
# TIME-WAIT buildup indicates rapid connection churn

# CPU (gevent is single-core per process — watch for one core at 100%)
top -p $(pgrep -f locust)
```

### 9.3 Backend Monitoring

| Layer | Metric | Tool |
|---|---|---|
| API Gateway | Active connections, request queue depth, 5xx rate | Nginx `stub_status`, Prometheus |
| Job Queue | Queue length (absolute), enqueue rate vs dequeue rate | Application metrics endpoint |
| GPU Workers | GPU utilization %, OOM events, job error rate | DCGM, `nvidia-smi`, app logs |
| SSE Endpoint | Active SSE connections, average stream duration | Custom counter in application |

### 9.4 Key SLIs to Define Before Testing

```
SLI 1: Submission latency   → colorize_generate p95 < 2s
SLI 2: Job completion time  → colorize_job_complete p95 < 90s
SLI 3: Success rate         → colorize_job_complete failure% < 1%
SLI 4: Throughput           → colorize_generate RPS ≥ target under load
```

Define these before the test. Results without predefined acceptance criteria are not a load test — they are an observation.

---

## 10. Execution Roadmap

### Step 1 — Fix wait_time (Immediate)

```python
# Change in bwc_locust.py
wait_time = between(60, 180)   # was between(5, 15)
```

**Validates:** The parameter was the primary risk. This single change makes 15K users achievable.

---

### Step 2 — Implement Per-User Queue Model (Immediate)

Confirm the `_process_queue` + `_job_q` model is in place (already done in current script).

**Validates:** Greenlet count is bounded at `2 × user_count`. Memory is predictable.

---

### Step 3 — OS Tuning on All Worker Nodes

Run on each worker before test:
```bash
ulimit -n 100000
sysctl -w net.ipv4.ip_local_port_range="1024 65535"
sysctl -w net.ipv4.tcp_tw_reuse=1
sysctl -w net.core.somaxconn=65535
# Verify:
cat /proc/sys/net/ipv4/ip_local_port_range
ulimit -n
```

---

### Step 4 — Smoke Test (5 Users, 5 Minutes)

```bash
locust -f bwc_locust.py --users 5 --spawn-rate 1 --run-time 5m --headless
```

**Validates:**
- End-to-end pipeline works (POST → SSE → JOB metric fires)
- CSV report is written correctly
- No Python errors or import failures
- `colorize_job_complete` appears in Locust output

---

### Step 5 — Baseline Test (50 Users, 20 Minutes)

```bash
locust -f bwc_locust.py --users 50 --spawn-rate 1 --run-time 20m --headless
```

**Validates:**
- Establishes p50/p95/p99 latency at low concurrency
- Confirms backend behaves correctly under minimal load
- Baseline for comparison at scale

**Record:** `colorize_job_complete` p50, p95, failure rate.

---

### Step 6 — Distributed Setup (20 Workers)

**On each worker:**
```bash
locust -f bwc_locust.py --worker --master-host <MASTER_IP>
```

**On master:**
```bash
locust -f bwc_locust.py --master --expect-workers 20
```

**Verify:** All 20 workers connected before starting test. Check Locust master UI shows correct worker count.

---

### Step 7 — Gradual Scale Test

Run in sequence, 30 minutes each, recording results:

| Run | Users | Spawn Rate | Expected POST/s | Watch For |
|---|---|---|---|---|
| 1 | 500 | 5/s | ~4 | Baseline at scale |
| 2 | 2,000 | 10/s | ~17 | First concurrency pressure |
| 3 | 5,000 | 10/s | ~42 | SSE stability |
| 4 | 10,000 | 10/s | ~83 | Worker memory, fd count |
| 5 | 15,000 | 10/s | ~125 | Full load — record all metrics |

**Stop criteria:** If `colorize_job_complete` failure rate exceeds 5% at any stage, stop and investigate before proceeding.

---

### Step 8 — Stress Test (Find Breaking Point)

After validating realistic simulation:
```bash
# Separate test — different goal
wait_time = between(1, 3)
users = 300, spawn-rate = 10/s, duration = 20m
```

**Validates:** Maximum sustainable POST rate before backend queue becomes unbounded. This is where you find the GPU capacity gap.

---

### Step 9 — Soak Test (Stability Validation)

```bash
# 500 users, 4 hours, realistic wait_time
locust ... --users 500 --spawn-rate 2 --run-time 4h
```

**Validates:**
- No memory leak in Locust workers (watch RSS over time)
- No SSE connection leak (watch fd count over time)
- CSV report integrity after 4 hours
- No greenlet accumulation (watcher greenlets exiting correctly)

---

## 11. Best Practices & Key Insights

### Critical Rules

**1. Never set wait_time shorter than avg_job_duration for async APIs.**
For synchronous APIs, short wait_time is fine. For async, the job lifetime creates a concurrency multiplier that compounds at scale. The formula `in_flight = job_time / wait_time` must be evaluated before any test run.

**2. Separate submission latency from completion latency.**
`colorize_generate` (HTTP p95) and `colorize_job_complete` (JOB p95) are completely different SLIs. A fast 202 response means nothing if jobs take 10 minutes to complete. Always measure both.

**3. SSE connections must be counted, not assumed to be cheap.**
Each SSE stream is a long-lived TCP connection. At scale, these exhaust file descriptors, port ranges, and API gateway connection pools before CPU or RAM becomes the bottleneck.

**4. One watcher greenlet per user is the correct architecture.**
Unbounded greenlet spawning is an anti-pattern for long-running async jobs. It trades simplicity for explosive resource growth. The per-user queue model adds 7 lines of code and removes a class of failure mode.

**5. `gevent.Timeout` is the only correct timeout for SSE streams.**
`requests` `timeout=(connect, read)` applies per-socket-read, not per-stream. A server that sends a heartbeat every 5s will never trigger a 120s read timeout. `gevent.Timeout` enforces wall-clock total duration regardless of activity.

**6. The gap between generate count and job_complete count is your most important metric.**
If `colorize_generate` fires 10,000 times but `colorize_job_complete` fires only 3,000 times, 7,000 jobs are either in-flight, failed silently, or the queue is growing unboundedly. Monitor this gap in real time.

### Common Mistakes to Avoid

| Mistake | Consequence | Correct Approach |
|---|---|---|
| Short `wait_time` for async APIs | DDoS own workers + backend | `wait_time ≥ avg_job_duration` |
| Using `self.client` for SSE | SSE appears in UI; inflates metrics | Use separate `requests.Session` |
| Single global `poll_manager` | Bottleneck; serial scaling wall | Per-user watcher architecture |
| No `gevent.Timeout` on SSE | Stuck greenlets on hung jobs | Always wrap `iter_lines` in `gevent.Timeout` |
| Starting 15K users immediately | Spike overwhelms backend cold-start | Spawn rate ≤ 10/s; 25 min ramp |
| Not defining SLIs before test | No pass/fail criteria; results are meaningless observations | Define p95 targets before running |
| Ignoring `not_completed` in CSV | Hides queue saturation | Track `not_completed` count as a metric |

---

*Document generated: 2026-04-10*
*Script reference: `AI-TOOLS/LOCUST/BW Colorise/bwc_locust.py`*
*Architecture: Pattern A (fire-and-forget + per-user queue watcher)*
