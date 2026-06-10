---
name: systems-coding-problem
description: Design-doc-first playbook for solving systems coding problems in this repo (concurrent pipelines, orchestrators, RPC/config services, caches, rate limiters, worker pools). Use when asked to solve a practice or interview problem, implement a task in a "Task N" or "Interview Task" folder, or build any component involving concurrency, failure handling (retry/skip/abort), load balancing, or latency-vs-throughput trade-offs. Produces a DESIGN.md, a stdlib-only Python package, a pytest suite, and a runnable demo, following the conventions established in Task 1.
---

# Systems Coding Problem Playbook

Solve every problem to the standard of `Task 1/` (concurrent document-processing
pipeline — the reference implementation for everything below).

## Workflow — always in this order

1. **Pin down the problem.** Extract the full statement (PDF, prompt, or starter
   code). Number the explicit requirements; note cross-cutting concerns
   (concurrency, failure handling, load balancing, latency vs throughput).
2. **Write `DESIGN.md` first** (structure below). Get the trade-offs on paper
   before writing code.
3. **Implement** a small stdlib-only Python package in the task's folder.
4. **Test** with pytest; **demo** with a `python -m <pkg>.demo` script whose
   sections map 1:1 to the numbered requirements.
5. **Verify** (run tests from the task folder *and* repo root, run the demo),
   update READMEs, commit with a descriptive message, `git push -u origin
   <designated branch>`. PR only when explicitly requested.

## Repo conventions

- One top-level folder per problem: `Task 1/`, `Interview Task/`, …
- Inside each folder:
  ```
  DESIGN.md            design doc (written first)
  README.md            maps each numbered requirement -> how the code meets it
  conftest.py          empty; makes `import <pkg>` resolve under pytest
  requirements-dev.txt pytest only — the library itself is stdlib-only
  <package>/           the implementation (+ <package>/demo.py)
  tests/               pytest suite
  ```
- Root `README.md` links every task folder; single `.gitignore` at repo root.
- Python 3.11, no third-party runtime deps unless the problem demands them.

## DESIGN.md structure

1. **Problem** — restated, with the cross-cutting concerns called out.
2. **Goals / Non-goals** — name what you deliberately don't build (broker,
   real RPC, DAG) and map each non-goal to its production analogue.
3. **Core abstractions** — the 3–5 nouns and one-line roles.
4. **Concurrency model** — *implement* the trade-off instead of hand-waving:
   e.g. ship both a throughput path and a latency path over the same config.
5. **Failure handling** — policies plus a scenario→policy table.
6. **Semantics summary** — for every outcome, exactly where the item lands.
7. **Measuring & tuning** — what the metrics expose; bottleneck-finding loop;
   Little's law (`concurrency = throughput × latency`) for the trade-off.
8. **Testing strategy** — bullet per behavior.
9. **Extensions** — out of scope but designed-for (DAG, distributed broker,
   circuit breakers, idempotency/exactly-once).
10. **Layout** — file tree.

## Concurrency toolbox

- **Staged pipeline (throughput):** one bounded `queue.Queue` per stage +
  per-stage worker-thread pool. The bounded queue *is* both the load balancer
  (idle workers pull) and backpressure (full queue blocks upstream).
- **Ordered shutdown:** feed everything, then per stage in order: enqueue one
  sentinel per worker, join that stage's threads, move on. FIFO guarantees
  sentinels trail real work — no flags or races.
- **Abort:** one shared `threading.Event`. Workers drain-and-discard once set;
  any blocking `put`/`get` becomes a timeout loop that re-checks the event
  (otherwise abort deadlocks on a full queue).
- **Per-item fan-out (latency):** each item runs all stages inline on one
  worker; parallelize across items with `ThreadPoolExecutor`. Zero queueing
  delay; ideal for single-request turnaround and I/O-bound work.
- **CPU vs I/O:** per-stage `ExecMode` — `THREAD` for I/O-bound (blocking I/O
  releases the GIL), `PROCESS` for CPU-bound: worker threads submit to a
  per-stage `ProcessPoolExecutor` and block on `future.result(timeout=...)`.
- **Be honest about the GIL:** thread mode cannot hard-kill a stuck call or
  parallelize pure-Python CPU work. Say so in the design; don't fake it.

## Failure-handling toolbox

- **RetryPolicy**: exponential backoff `base * 2^(attempt-2)` capped at a max,
  with ± jitter (prevents retry storms against a recovering dependency).
- **After retries**: `SKIP` (record and continue the batch — default) or
  `ABORT` (set the event, drain, mark the result aborted). Per stage, not
  global.
- **Record failures richly**: (item, stage name, `repr(error)`, attempt count).
- **Timeouts**: enforceable only in PROCESS mode via `future.result(timeout)`;
  a timeout counts as a failed attempt and flows into the same retry/skip/abort
  path.
- **Results over exceptions**: return
  `Result(succeeded, failed, aborted, metrics)`; never raise for per-item
  failures. Reserve exceptions for caller misconfiguration (`ValueError`).

## Metrics (make tuning data-driven)

- One thread-safe `RunMetrics`: per-stage counters (received / ok / retried /
  failed / skipped / busy_time) + per-item end-to-end latencies + wall time.
- Derive throughput (items ÷ wall), nearest-rank p50/p95/p99, mean.
- Provide `summary()` returning a printable block; demos print two strategies
  side by side so the trade-off is visible, not asserted.
- Bottleneck = stage with dominant busy_time → give it workers until another
  stage becomes the constraint.

## Code conventions

- `from __future__ import annotations`; full type hints.
- `@dataclass` for value types; ABC for the pluggable interface (translate
  Java starter interfaces 1:1: `process(doc) -> Document`).
- `str`-based `Enum` for modes and policies; validate config in
  `__post_init__` with `ValueError`.
- Immutable-style updates: `with_update(...)` deep-copies — safe under retries,
  parallel observers, and pickling.
- Module logger (`logging.getLogger("<pkg>")`); `print` only in the demo.
- Ship test doubles beside the examples: `PassThrough(delay)`,
  `FlakyProcessor(fail_first)` (stateful ⇒ thread-only), `AlwaysFail(only_ids)`,
  `SlowProcessor` (picklable, for timeout tests).

## Pitfalls (each one was hit for real in Task 1)

- Anything crossing a process boundary must be picklable: **top-level classes
  only** — no closures, lambdas, or instances holding `threading.Lock`s.
- Classes defined inside a test function are not picklable → process-mode test
  doubles live in the examples module.
- Worker callables passed to `executor.map` must not raise — convert failures
  into recorded results, or one bad item kills the batch.
- Always `pool.shutdown(wait=True, cancel_futures=True)` in a `finally`.
- Sandboxed/odd environments: prefer `python -m pytest` over bare `pytest`.

## Testing checklist (whole suite < ~2 s; delays 0.001–0.02 s)

- [ ] Happy path: every item visits every stage **in order** (assert on a
      recorded `history`), transformations applied.
- [ ] Empty input; invalid config raises `ValueError`.
- [ ] Concurrency speedup: wall time `< 0.7 ×` the serial lower bound —
      generous margin, never exact timings (flaky otherwise).
- [ ] Retry recovers transient failures; retry counts are exact.
- [ ] Retry exhaustion → skipped, with attempts recorded.
- [ ] Skip drops *only* the bad items; the rest succeed.
- [ ] Abort stops early: assert `aborted` and `len(succeeded) < n`, never exact
      counts (shutdown timing is nondeterministic).
- [ ] Backpressure: `queue_size=1` + many items completes without deadlock.
- [ ] Process mode: correct output, and timeout-as-failure.
- [ ] Metrics populated and internally consistent.

## If time-boxed (live interview)

Build in value order — each step is demoable on its own:
1. Abstractions + a working orchestrator with one concurrency strategy.
2. Retry / skip / abort.
3. Metrics + summary.
4. Second strategy (latency vs throughput) if time allows.

Talking points to volunteer: GIL and thread-vs-process choice, bounded queues
as backpressure *and* load balancing, Little's law for the latency/throughput
trade-off, at-least-once delivery ⇒ processors should be idempotent.
