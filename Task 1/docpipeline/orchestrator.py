"""The pipeline orchestrator: runs a list of stages over a batch of documents.

Two execution strategies share the same stage definitions:

* :meth:`PipelineOrchestrator.run` — a **staged pipeline** (per-stage worker
  pools + bounded queues). Optimized for **throughput**: every stage works on a
  different document at the same time.
* :meth:`PipelineOrchestrator.run_per_document` — **per-document fan-out**
  (each document goes through all stages on one worker, parallelized across
  documents). Optimized for **latency**: no queueing between stages.
"""
from __future__ import annotations

import logging
import queue
import threading
import time
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from dataclasses import dataclass, field
from typing import Iterable

from .document import Document
from .metrics import RunMetrics
from .policies import ExecMode, FailureAction, Stage

logger = logging.getLogger("docpipeline")

_SENTINEL = object()  # marks "no more work" on a stage's input queue


@dataclass
class FailedDocument:
    """A document that exhausted retries at some stage."""

    document: Document
    stage: str
    error: str
    attempts: int


@dataclass
class PipelineResult:
    succeeded: list[Document] = field(default_factory=list)
    failed: list[FailedDocument] = field(default_factory=list)
    aborted: bool = False
    metrics: RunMetrics | None = None

    @property
    def ok(self) -> bool:
        return not self.aborted and not self.failed

    def __repr__(self) -> str:  # pragma: no cover - cosmetic
        return (
            f"PipelineResult(succeeded={len(self.succeeded)}, "
            f"failed={len(self.failed)}, aborted={self.aborted})"
        )


class _Failed(Exception):
    """Internal: a stage failed for a document after all retries."""

    def __init__(self, stage: str, cause: BaseException, attempts: int) -> None:
        super().__init__(f"{stage} failed after {attempts} attempt(s): {cause!r}")
        self.stage = stage
        self.cause = cause
        self.attempts = attempts


@dataclass
class _WorkItem:
    doc: Document
    entered_at: float  # perf_counter timestamp when the doc entered the pipeline


def _remote_call(processor, doc: Document) -> Document:
    """Top-level (picklable) entry point for ProcessPoolExecutor."""
    return processor.process(doc)


class PipelineOrchestrator:
    def __init__(self, stages: list[Stage], *, name: str = "pipeline") -> None:
        if not stages:
            raise ValueError("PipelineOrchestrator needs at least one stage")
        self.stages = stages
        self.name = name

    # =====================================================================
    # Shared per-call execution: retry + backoff + (process-mode) timeout
    # =====================================================================
    def _execute(
        self,
        stage: Stage,
        doc: Document,
        metrics: RunMetrics,
        *,
        pool: ProcessPoolExecutor | None,
        abort_event: threading.Event | None,
    ) -> Document:
        """Run one stage on one document with retries. Raises :class:`_Failed`."""
        attempts = 0
        last_exc: BaseException | None = None
        max_attempts = stage.retry.max_retries + 1
        while attempts < max_attempts:
            attempts += 1
            try:
                if pool is not None:
                    fut = pool.submit(_remote_call, stage.processor, doc)
                    return fut.result(timeout=stage.timeout)
                return stage.processor.process(doc)
            except Exception as exc:  # noqa: BLE001 - any failure is retryable
                last_exc = exc
                more = attempts < max_attempts and not (
                    abort_event is not None and abort_event.is_set()
                )
                if more:
                    metrics.record_retry(stage.name)
                    logger.debug(
                        "%s: retry %d/%d for %s (%r)",
                        stage.name, attempts, stage.retry.max_retries, doc.doc_id, exc,
                    )
                    time.sleep(stage.retry.delay(attempts + 1))
                    continue
                break
        assert last_exc is not None
        raise _Failed(stage.name, last_exc, attempts)

    # =====================================================================
    # Throughput-optimized: staged pipeline with bounded queues
    # =====================================================================
    def run(self, documents: Iterable[Document]) -> PipelineResult:
        docs = list(documents)
        n = len(self.stages)
        metrics = RunMetrics([s.name for s in self.stages])
        result = PipelineResult(metrics=metrics)

        # One bounded input queue per stage.
        queues: list[queue.Queue] = [
            queue.Queue(maxsize=s.effective_queue_size) for s in self.stages
        ]
        abort_event = threading.Event()
        lock = threading.Lock()  # guards result.succeeded / result.failed

        # Process pools only for PROCESS-mode stages.
        pools: dict[int, ProcessPoolExecutor] = {
            i: ProcessPoolExecutor(max_workers=s.workers)
            for i, s in enumerate(self.stages)
            if s.mode == ExecMode.PROCESS
        }

        def worker(idx: int) -> None:
            stage = self.stages[idx]
            in_q = queues[idx]
            nxt_q = queues[idx + 1] if idx + 1 < n else None
            pool = pools.get(idx)
            while True:
                item = in_q.get()
                try:
                    if item is _SENTINEL:
                        return
                    if abort_event.is_set():
                        continue  # drain & discard quickly during shutdown
                    metrics.record_received(stage.name)
                    t0 = time.perf_counter()
                    try:
                        new_doc = self._execute(
                            stage, item.doc, metrics, pool=pool, abort_event=abort_event
                        )
                    except _Failed as f:
                        busy = time.perf_counter() - t0
                        skip = stage.on_failure == FailureAction.SKIP
                        metrics.record_failed(stage.name, busy, skipped=skip)
                        with lock:
                            result.failed.append(
                                FailedDocument(item.doc, f.stage, repr(f.cause), f.attempts)
                            )
                        if not skip:  # ABORT
                            logger.warning("%s: aborting batch (%s)", stage.name, f.cause)
                            abort_event.set()
                        continue
                    metrics.record_ok(stage.name, time.perf_counter() - t0)
                    nxt = _WorkItem(new_doc, item.entered_at)
                    if nxt_q is None:  # last stage -> success
                        metrics.record_latency(time.perf_counter() - item.entered_at)
                        with lock:
                            result.succeeded.append(new_doc)
                    else:
                        nxt_q.put(nxt)  # bounded -> backpressure
                finally:
                    in_q.task_done()

        # Spawn worker pools for every stage.
        stage_threads: list[list[threading.Thread]] = []
        for idx, stage in enumerate(self.stages):
            threads = [
                threading.Thread(
                    target=worker, args=(idx,), name=f"{stage.name}-{w}", daemon=True
                )
                for w in range(stage.workers)
            ]
            for t in threads:
                t.start()
            stage_threads.append(threads)

        start = time.perf_counter()
        try:
            # Feed stage 0, honoring backpressure and abort.
            for doc in docs:
                if abort_event.is_set():
                    break
                item = _WorkItem(doc, time.perf_counter())
                while True:
                    try:
                        queues[0].put(item, timeout=0.1)
                        break
                    except queue.Full:
                        if abort_event.is_set():
                            break

            # Ordered shutdown: sentinel + join stage i, then move to i+1.
            # FIFO queues guarantee sentinels trail all real work for the stage.
            for idx, stage in enumerate(self.stages):
                for _ in range(stage.workers):
                    queues[idx].put(_SENTINEL)
                for t in stage_threads[idx]:
                    t.join()
        finally:
            for pool in pools.values():
                pool.shutdown(wait=True, cancel_futures=True)

        metrics.wall_time = time.perf_counter() - start
        result.aborted = abort_event.is_set()
        return result

    # =====================================================================
    # Latency-optimized: each document through all stages, fanned out
    # =====================================================================
    def process_document(self, doc: Document, metrics: RunMetrics | None = None) -> Document:
        """Run a single document straight through every stage (no queueing)."""
        metrics = metrics or RunMetrics([s.name for s in self.stages])
        entered = time.perf_counter()
        cur = doc
        for stage in self.stages:
            metrics.record_received(stage.name)
            t0 = time.perf_counter()
            try:
                cur = self._execute(stage, cur, metrics, pool=None, abort_event=None)
            except _Failed:
                metrics.record_failed(
                    stage.name, time.perf_counter() - t0,
                    skipped=stage.on_failure == FailureAction.SKIP,
                )
                raise
            metrics.record_ok(stage.name, time.perf_counter() - t0)
        metrics.record_latency(time.perf_counter() - entered)
        return cur

    def run_per_document(
        self, documents: Iterable[Document], *, max_parallel: int | None = None
    ) -> PipelineResult:
        docs = list(documents)
        metrics = RunMetrics([s.name for s in self.stages])
        result = PipelineResult(metrics=metrics)
        abort_event = threading.Event()
        lock = threading.Lock()

        # Map each stage to its failure policy for quick lookup on error.
        policy_by_stage = {s.name: s.on_failure for s in self.stages}

        def handle(doc: Document):
            if abort_event.is_set():
                return None  # batch already aborting; don't start new work
            try:
                return self.process_document(doc, metrics)
            except _Failed as f:
                fd = FailedDocument(doc, f.stage, repr(f.cause), f.attempts)
                if policy_by_stage[f.stage] == FailureAction.ABORT:
                    abort_event.set()
                with lock:
                    result.failed.append(fd)
                return None

        workers = max_parallel or min(32, max(1, len(docs)))
        start = time.perf_counter()
        with ThreadPoolExecutor(max_workers=workers) as ex:
            for out in ex.map(handle, docs):
                if out is not None:
                    with lock:
                        result.succeeded.append(out)
        metrics.wall_time = time.perf_counter() - start
        result.aborted = abort_event.is_set()
        return result
