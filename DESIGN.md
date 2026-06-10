# Design Doc — Document Processing Pipeline with Microservices

## 1. Problem

We have a batch of documents that must each flow through an ordered set of
processing steps ("microservices"): e.g. **text extraction → language
detection → entity tagging**. Some steps are **CPU-intensive** (entity
tagging, language models), others are **I/O-intensive** (fetching a blob,
calling a remote OCR service).

We need an orchestrator that:

1. Accepts a list of `Document`s and passes each through a sequence of
   `DocumentProcessor`s.
2. Runs **concurrently** — multiple documents *and* multiple stages in flight
   at once.
3. Handles **partial failure** — when a processor fails on a document, decide
   per-stage whether to **retry**, **skip** the document, or **abort** the
   batch.
4. Lets us **measure and tune** for throughput (batch efficiency) vs latency
   (fast single-document turnaround).

### Cross-cutting concerns called out by the prompt
- **Concurrency**: documents and stages in parallel.
- **Failure handling**: services fail or become bottlenecks.
- **Load balancing**: distribute work across multiple instances of each stage.
- **Latency vs throughput**: single-document speed vs batch efficiency.

## 2. Goals / Non-goals

**Goals**
- A small, dependency-free (stdlib-only) library that is actually runnable and
  tested, modeling the real trade-offs.
- Two execution strategies that make the latency/throughput trade-off concrete
  rather than hand-wavy.
- Per-stage configuration: parallelism, executor type (thread vs process),
  retry policy, failure policy, timeout, and queue depth.
- First-class metrics so tuning is data-driven.

**Non-goals**
- A real network RPC layer or message broker. The orchestrator is in-process,
  but the abstractions (stages, bounded queues, per-stage worker pools) map
  directly onto a distributed deployment (see §9). We call this out instead of
  building Kafka.
- A full DAG scheduler. We implement a **linear sequence** (the common case)
  and design the types so a DAG is an additive change (see §9).

## 3. Core abstractions

```
Document            an immutable-ish unit of work: id, content, metadata, history
DocumentProcessor   one stage's logic:  process(doc) -> doc   (a "microservice")
Stage               a DocumentProcessor + how to run it (workers, mode, retry, ...)
PipelineOrchestrator runs a list[Stage] over a list[Document]
```

The starter interface was Java:

```java
interface DocumentProcessor { Document process(Document doc); }
class Document { private String content; }
```

The Python translation keeps the same shape — `DocumentProcessor.process(doc)
-> Document` — and enriches `Document` with an id, a metadata dict, and a
`history` of stages applied (useful for debugging and for asserting ordering
in tests). Processors return an **updated copy** (`doc.with_update(...)`)
rather than mutating in place, so a document handed to a retry or to another
worker can never be observed half-mutated, and so it survives being pickled
across a process boundary.

## 4. Concurrency model — two strategies, one set of stages

The key design decision: **the same pipeline definition can be executed two
ways**, because batch throughput and single-document latency want opposite
things.

### 4.1 Throughput-optimized: staged pipeline (`run`)

Each **stage owns its own pool of worker threads** and consumes from a
**bounded input queue**, producing into the next stage's queue:

```
docs ─▶ [Q0] ─▶ Stage0 (w0 workers) ─▶ [Q1] ─▶ Stage1 (w1) ─▶ [Q2] ─▶ Stage2 (w2) ─▶ results
         │                              │                       │
      bounded                        bounded                 bounded   (backpressure)
```

Why this is the throughput design:
- **All stages run simultaneously** on *different* documents — classic
  pipelining. While Stage2 tags entities in doc #1, Stage1 detects language in
  doc #2 and Stage0 extracts text from doc #3. Steady-state throughput is
  governed by the **slowest stage**, not the sum of stages.
- **Load balancing within a stage**: `w_i` worker threads = `w_i` "instances"
  of that microservice. The bounded queue *is* the load balancer — whichever
  worker is free pulls the next document.
- **Backpressure / bottleneck handling**: bounded queues mean a slow stage
  causes its input queue to fill, which blocks upstream producers, which
  propagates back to the feeder. We never build an unbounded backlog in front
  of a bottleneck (the failure mode that turns a slow service into an
  out-of-memory crash).

**CPU vs I/O.** Each stage has an execution `mode`:
- `THREAD` — worker threads call the processor directly. Ideal for
  **I/O-bound** stages: threads block on I/O and release the GIL, so dozens of
  concurrent calls overlap cheaply.
- `PROCESS` — worker threads delegate the call to a per-stage
  `ProcessPoolExecutor`. Ideal for **CPU-bound** stages: the heavy work runs in
  child processes, sidestepping the GIL for true multi-core parallelism. The
  worker thread blocks on `future.result()` (releasing the GIL while it waits).

**Clean shutdown** uses sentinels with ordered teardown. After feeding all
documents, the driver injects one sentinel per worker into stage 0, joins
stage 0's workers, then injects sentinels into stage 1, joins, and so on.
Because the queues are FIFO and sentinels are enqueued only after all real
items for that stage, no worker ever sees a sentinel while real work remains.

### 4.2 Latency-optimized: per-document fan-out (`run_per_document`)

Each document is pushed through **all stages back-to-back on a single worker**,
and we parallelize **across documents** with a thread pool:

```
doc #1 ─▶ Stage0 ─▶ Stage1 ─▶ Stage2 ─▶ result   (worker A)
doc #2 ─▶ Stage0 ─▶ Stage1 ─▶ Stage2 ─▶ result   (worker B)
...
```

Why this is the latency design:
- A single document's end-to-end latency is just the sum of its stage times
  with **zero queue/hand-off overhead** — it never waits behind other documents
  for a queue slot. This is what you want for an interactive "process this one
  document now" request.
- Simpler failure isolation: each document is independent, so retry/skip/abort
  decisions are local to one in-flight item.

The trade-off: with CPU-bound stages this path is GIL-limited (threads don't
give you multiple cores for pure-Python CPU work). For CPU-heavy *batches*,
prefer `run()` with `PROCESS` stages. We make this explicit rather than
papering over it.

### 4.3 Why both, instead of one "smart" mode
Latency and throughput trade off against each other (Little's Law:
`concurrency = throughput × latency`). Batching, large queues, and big
per-stage pools maximize utilization/throughput but add queueing latency to
any single item. Running one item straight through minimizes its latency but
leaves stages idle. Rather than guess, we expose both and let the caller pick
per workload — and we instrument both so the choice is measured (§7).

## 5. Failure handling

Configured **per stage** with two orthogonal knobs:

- `RetryPolicy(max_retries, backoff_base, backoff_max, jitter)` — retries the
  call with **exponential backoff + jitter**. Jitter avoids retry storms where
  many documents hammer a recovering service in lockstep.
- `on_failure: FailureAction` — what to do **after retries are exhausted**:
  - `SKIP` — drop this document, record it in `failed`, keep the batch going.
    Right default for "best-effort enrich a million docs; a few bad ones are
    fine."
  - `ABORT` — stop the whole batch. Right for "all-or-nothing" jobs or for a
    failure that means the stage is fundamentally broken (bad config,
    dependency down) so continuing just wastes work.

| Scenario | max_retries | on_failure | Outcome |
|---|---|---|---|
| Transient I/O blip | ≥1 | SKIP | retried, usually succeeds |
| Persistently bad document (poison) | ≥1 | SKIP | retried, then dropped & recorded |
| Stage dependency down / bad config | 0–N | ABORT | batch stops fast, surfaces the error |

**Timeouts.** `PROCESS` stages enforce a hard `timeout` via
`future.result(timeout=...)` (a wedged child can be detected and the batch can
move on / abort). `THREAD` stages can't be force-killed in Python, so there we
rely on the processor's own I/O timeouts (e.g. a socket/HTTP timeout) — this is
documented in the code rather than faked.

**Bottlenecks.** A slow-but-not-failing stage is handled by backpressure
(§4.1) plus the tuning loop in §7: detect the bottleneck from per-stage metrics
and give it more workers/instances. Extensions like circuit breakers and dead-
letter queues are noted in §9.

## 6. Failure semantics summary

- A document that **succeeds** every stage lands in `result.succeeded`.
- A document that **fails** a `SKIP` stage (after retries) lands in
  `result.failed` with the offending stage, the exception, and the attempt
  count; the rest of the batch continues.
- A document that **fails** an `ABORT` stage sets the abort flag; in-flight work
  drains, no new work starts, `result.aborted == True`, and the cause is in
  `result.failed`.

## 7. Measuring & tuning: throughput vs latency (Task #4)

Every run returns a `RunMetrics` object:

- **End-to-end latency** per document → `p50 / p95 / p99` and mean.
- **Throughput** = succeeded ÷ wall-clock seconds.
- **Per-stage**: received, ok, retried, failed, skipped, and **busy time**
  (sum of processing time across that stage's workers).

How to use it to tune:

1. **Find the bottleneck.** The stage with the highest busy-time / lowest
   spare capacity caps throughput. In a balanced pipeline every stage's
   `busy_time` is similar; a skewed one points at the constraint.
2. **Throughput knobs** (batch jobs):
   - Add **workers/instances** to the bottleneck stage until another stage
     becomes the constraint (balance the line).
   - Use `PROCESS` mode for CPU-bound stages to escape the GIL.
   - Increase queue depth so fast stages don't stall waiting on hand-offs —
     but bounded, to preserve backpressure.
   - Process in larger batches to amortize fixed per-batch costs.
3. **Latency knobs** (interactive):
   - Use `run_per_document` to avoid queueing delay.
   - Shrink queues / pools to reduce time spent waiting in line.
   - Co-locate or cache hot dependencies so per-stage time drops.
4. **The trade-off is visible**: run the same pipeline both ways and compare
   the metrics. Bigger pools/queues raise throughput **and** p95 latency
   (queueing). The demo prints both side by side.

## 8. Testing strategy

- **Ordering / correctness**: a document carries the ordered `history` of
  stages; assert it equals the configured sequence.
- **Concurrency**: a pipeline of sleepy stages finishes in well under the
  serial time; many docs overlap.
- **Retry**: a processor that fails its first *k* attempts then succeeds ends
  up in `succeeded` with the retry counter incremented.
- **Skip**: an always-failing `SKIP` stage drops only the bad docs; the rest
  succeed.
- **Abort**: an always-failing `ABORT` stage yields `aborted == True`.
- **Backpressure**: tiny queues + many docs complete without deadlock.
- **Process mode**: a CPU stage in `PROCESS` mode produces correct output.
- **Metrics**: latencies and per-stage counters are populated.

## 9. Extensions (deliberately out of scope, designed-for)

- **DAG instead of a line.** `Stage` already names its processor; adding
  `depends_on` + per-edge queues turns the linear runner into a DAG scheduler
  with fan-out/fan-in. The worker/queue/backpressure machinery is unchanged.
- **Distributed deployment.** Replace the in-process bounded `queue.Queue`
  with a broker (Kafka/SQS/RabbitMQ) and each stage's worker pool with a
  separately scaled service/replica set. Bounded queue → consumer lag +
  autoscaling; sentinels → end-of-batch markers; retry/skip/abort → broker
  redelivery + a dead-letter queue.
- **Resilience.** Circuit breakers per stage (stop hammering a down
  dependency), bulkheads (isolate pools), and a dead-letter queue for poison
  documents.
- **Persistence / exactly-once.** Checkpoint per stage so a crash resumes
  mid-batch instead of restarting; idempotent processors make retries safe.

## 10. Layout

```
docpipeline/
  document.py        Document
  processor.py       DocumentProcessor ABC, ProcessorError
  policies.py        Stage, RetryPolicy, FailureAction, ExecMode
  metrics.py         StageMetrics, RunMetrics (+ percentiles)
  orchestrator.py    PipelineOrchestrator, PipelineResult, FailedDocument
  processors/
    examples.py      sample I/O- and CPU-bound stages + test doubles
  demo.py            runnable demo: throughput vs latency, retry, skip, abort
tests/
  test_pipeline.py
```
