"""Tests for the document processing pipeline.

Run with:  pytest -q
"""
from __future__ import annotations

import time

import pytest

from docpipeline import (
    Document,
    ExecMode,
    FailureAction,
    PipelineOrchestrator,
    RetryPolicy,
    Stage,
)
from docpipeline.processors.examples import (
    AlwaysFail,
    EntityTagger,
    FlakyProcessor,
    LanguageDetector,
    PassThrough,
    SlowProcessor,
    TextExtractor,
)


def make_docs(n: int) -> list[Document]:
    return [Document(content=f"Doc number {i} from London") for i in range(n)]


# --------------------------------------------------------------------------
# Task 1: sequence of processors, correct ordering
# --------------------------------------------------------------------------
def test_documents_pass_through_stages_in_order():
    pipe = PipelineOrchestrator(
        [
            Stage(TextExtractor(io_delay=0), workers=3),
            Stage(LanguageDetector(io_delay=0), workers=3),
            Stage(EntityTagger(work=200), workers=2, mode=ExecMode.THREAD),
        ]
    )
    res = pipe.run(make_docs(6))
    assert res.aborted is False
    assert len(res.succeeded) == 6
    assert not res.failed
    for doc in res.succeeded:
        assert doc.history == ["text-extraction", "language-detection", "entity-tagging"]
        assert doc.content.startswith("extracted::")
        assert doc.metadata["language"] == "en"
        assert "entities" in doc.metadata


def test_single_stage_and_empty_batch():
    pipe = PipelineOrchestrator([Stage(PassThrough("only"), workers=2)])
    assert pipe.run([]).succeeded == []
    res = pipe.run(make_docs(3))
    assert len(res.succeeded) == 3
    assert all(d.history == ["only"] for d in res.succeeded)


def test_requires_at_least_one_stage():
    with pytest.raises(ValueError):
        PipelineOrchestrator([])


# --------------------------------------------------------------------------
# Task 2: concurrency — pipelined run beats the serial lower bound
# --------------------------------------------------------------------------
def test_pipeline_runs_concurrently():
    delay, n_docs, n_stages = 0.02, 8, 3
    pipe = PipelineOrchestrator(
        [Stage(PassThrough(f"s{i}", delay=delay), workers=8) for i in range(n_stages)]
    )
    t0 = time.perf_counter()
    res = pipe.run(make_docs(n_docs))
    elapsed = time.perf_counter() - t0

    serial_lower_bound = n_docs * n_stages * delay
    assert len(res.succeeded) == n_docs
    # Concurrency should beat doing everything one-at-a-time by a wide margin.
    assert elapsed < serial_lower_bound * 0.7


def test_metrics_are_populated():
    pipe = PipelineOrchestrator(
        [Stage(TextExtractor(io_delay=0), workers=2), Stage(LanguageDetector(io_delay=0), workers=2)]
    )
    res = pipe.run(make_docs(5))
    m = res.metrics
    assert m.count == 5
    assert m.wall_time > 0
    assert m.throughput > 0
    assert m.latency_percentile(50) >= 0
    assert m.stages["text-extraction"].ok == 5
    assert m.stages["language-detection"].received == 5


# --------------------------------------------------------------------------
# Task 3: partial-failure handling — retry / skip / abort
# --------------------------------------------------------------------------
def test_retry_recovers_transient_failures():
    flaky = FlakyProcessor(fail_first=2)
    pipe = PipelineOrchestrator(
        [
            Stage(
                flaky,
                workers=3,
                mode=ExecMode.THREAD,
                retry=RetryPolicy(max_retries=3, backoff_base=0.001, jitter=0),
                on_failure=FailureAction.SKIP,
            )
        ]
    )
    res = pipe.run(make_docs(5))
    assert len(res.succeeded) == 5
    assert not res.failed
    assert res.metrics.stages["flaky"].retried == 5 * 2  # two retries per doc
    assert all(d.metadata["flaky_attempts"] == 3 for d in res.succeeded)


def test_retry_exhausted_then_skips():
    flaky = FlakyProcessor(fail_first=5)  # more failures than we allow retries
    pipe = PipelineOrchestrator(
        [
            Stage(
                flaky,
                workers=2,
                mode=ExecMode.THREAD,
                retry=RetryPolicy(max_retries=1, backoff_base=0.001, jitter=0),
                on_failure=FailureAction.SKIP,
            )
        ]
    )
    res = pipe.run(make_docs(3))
    assert res.succeeded == []
    assert len(res.failed) == 3
    assert all(f.attempts == 2 for f in res.failed)  # 1 initial + 1 retry
    assert res.metrics.stages["flaky"].skipped == 3


def test_skip_policy_drops_only_bad_documents():
    docs = make_docs(6)
    bad = {docs[1].doc_id, docs[4].doc_id}
    pipe = PipelineOrchestrator(
        [
            Stage(TextExtractor(io_delay=0), workers=3),
            Stage(AlwaysFail(only_doc_ids=bad), workers=3, on_failure=FailureAction.SKIP),
        ]
    )
    res = pipe.run(docs)
    assert res.aborted is False
    assert len(res.succeeded) == 4
    assert {f.document.doc_id for f in res.failed} == bad
    assert all(f.stage == "always-fail" for f in res.failed)


def test_abort_policy_stops_the_batch():
    pipe = PipelineOrchestrator(
        [
            Stage(TextExtractor(io_delay=0), workers=2),
            Stage(AlwaysFail(), workers=2, on_failure=FailureAction.ABORT),
        ]
    )
    res = pipe.run(make_docs(10))
    assert res.aborted is True
    assert len(res.failed) >= 1
    # On abort we stop early, so not everything completes.
    assert len(res.succeeded) < 10


# --------------------------------------------------------------------------
# Backpressure: tiny queues + many docs must not deadlock
# --------------------------------------------------------------------------
def test_backpressure_with_tiny_queues():
    pipe = PipelineOrchestrator(
        [
            Stage(PassThrough("a", delay=0.002), workers=2, queue_size=1),
            Stage(PassThrough("b", delay=0.005), workers=1, queue_size=1),  # bottleneck
            Stage(PassThrough("c", delay=0.001), workers=2, queue_size=1),
        ]
    )
    res = pipe.run(make_docs(30))
    assert len(res.succeeded) == 30
    assert not res.failed


# --------------------------------------------------------------------------
# Task 4: latency path (per-document fan-out)
# --------------------------------------------------------------------------
def test_per_document_run_preserves_order_and_succeeds():
    pipe = PipelineOrchestrator(
        [Stage(TextExtractor(io_delay=0), workers=4), Stage(LanguageDetector(io_delay=0), workers=4)]
    )
    res = pipe.run_per_document(make_docs(8), max_parallel=4)
    assert len(res.succeeded) == 8
    assert res.metrics.count == 8
    for doc in res.succeeded:
        assert doc.history == ["text-extraction", "language-detection"]


def test_per_document_retry_and_skip():
    pipe = PipelineOrchestrator(
        [
            Stage(
                FlakyProcessor(fail_first=1),
                retry=RetryPolicy(max_retries=2, backoff_base=0.001, jitter=0),
                on_failure=FailureAction.SKIP,
            )
        ]
    )
    res = pipe.run_per_document(make_docs(5))
    assert len(res.succeeded) == 5
    assert not res.failed


def test_per_document_abort():
    pipe = PipelineOrchestrator(
        [Stage(AlwaysFail(), on_failure=FailureAction.ABORT)]
    )
    res = pipe.run_per_document(make_docs(10), max_parallel=2)
    assert res.aborted is True
    assert len(res.succeeded) < 10


# --------------------------------------------------------------------------
# CPU-bound stage in PROCESS mode (true multi-core) produces correct output
# --------------------------------------------------------------------------
def test_process_mode_executes_cpu_stage():
    pipe = PipelineOrchestrator(
        [
            Stage(TextExtractor(io_delay=0), workers=2),
            Stage(EntityTagger(work=500), workers=2, mode=ExecMode.PROCESS),
        ]
    )
    res = pipe.run(make_docs(4))
    assert len(res.succeeded) == 4
    for doc in res.succeeded:
        assert "fingerprint" in doc.metadata
        assert doc.history[-1] == "entity-tagging"


def test_process_mode_timeout_is_recorded_as_failure():
    pipe = PipelineOrchestrator(
        [Stage(SlowProcessor(delay=0.5), workers=1, mode=ExecMode.PROCESS, timeout=0.05,
               on_failure=FailureAction.SKIP)]
    )
    res = pipe.run(make_docs(2))
    assert len(res.succeeded) == 0
    assert len(res.failed) == 2
