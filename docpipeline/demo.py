"""Runnable demo: ``python -m docpipeline.demo``.

Shows the four task requirements end to end:
  1. a pipeline orchestrator over a sequence of processors,
  2. concurrent execution (staged pipeline + per-document fan-out),
  3. partial-failure handling (retry, skip, abort),
  4. throughput vs latency metrics, side by side.
"""
from __future__ import annotations

import logging
import time

from .document import Document
from .orchestrator import PipelineOrchestrator
from .policies import ExecMode, FailureAction, RetryPolicy, Stage
from .processors.examples import (
    AlwaysFail,
    EntityTagger,
    FlakyProcessor,
    LanguageDetector,
    TextExtractor,
)

SAMPLE_TEXTS = [
    "The Eiffel Tower is in Paris and Gustave Eiffel built it.",
    "Ada Lovelace wrote about the Analytical Engine in London.",
    "Marie Curie studied radioactivity in Paris and Warsaw.",
    "The Great Barrier Reef is near Queensland Australia.",
]


def make_documents(n: int) -> list[Document]:
    return [Document(content=SAMPLE_TEXTS[i % len(SAMPLE_TEXTS)]) for i in range(n)]


def _rule(title: str) -> None:
    print(f"\n{'=' * 70}\n{title}\n{'=' * 70}")


def demo_throughput_vs_latency(n: int = 24) -> None:
    _rule(f"1+2+4. Throughput vs latency  (batch of {n} documents)")

    def build() -> PipelineOrchestrator:
        return PipelineOrchestrator(
            [
                Stage(TextExtractor(io_delay=0.01), workers=6),  # I/O-bound -> threads
                Stage(LanguageDetector(io_delay=0.003), workers=4),
                Stage(  # CPU-bound -> processes (true multi-core)
                    EntityTagger(work=12_000),
                    workers=4,
                    mode=ExecMode.PROCESS,
                ),
            ],
            name="doc-enrichment",
        )

    # (a) Throughput: staged pipeline, all stages busy on different docs at once.
    print("\n[throughput] staged pipeline — all stages busy at once:")
    res_tp = build().run(make_documents(n))
    print(res_tp.metrics.summary())

    # (b) Latency: a single document straight through, no batch contention.
    pipe = build()
    t0 = time.perf_counter()
    pipe.process_document(make_documents(1)[0])
    single = (time.perf_counter() - t0) * 1000
    print(f"\n[latency] single-document turnaround (no queueing): {single:.1f}ms")

    # (c) Middle ground: per-document fan-out across a batch (great for I/O-bound).
    print("\n[fan-out] per-document, parallel across the batch:")
    res_lat = build().run_per_document(make_documents(n))
    print(res_lat.metrics.summary())

    print(
        "\nTakeaway: the staged pipeline gives the best batch throughput "
        f"({res_tp.metrics.throughput:.0f} docs/s) because the slowest stage sets the "
        "rate\nand the CPU stage runs in real processes; a single document on its own "
        f"turns around in {single:.0f}ms. Per-document fan-out is GIL-bound on the CPU "
        "stage,\nso for CPU-heavy batches the staged pipeline wins on both axes — use "
        "fan-out for I/O-bound work or low-latency single requests."
    )
    sample = res_tp.succeeded[0]
    print(f"\nExample output: history={sample.history} metadata={sample.metadata}")


def demo_retry() -> None:
    _rule("3a. Partial failure -> RETRY with exponential backoff")
    pipe = PipelineOrchestrator(
        [
            Stage(TextExtractor(io_delay=0.0), workers=2),
            Stage(  # fails twice per doc, then succeeds
                FlakyProcessor(fail_first=2),
                workers=2,
                mode=ExecMode.THREAD,
                retry=RetryPolicy(max_retries=3, backoff_base=0.01),
                on_failure=FailureAction.SKIP,
            ),
        ]
    )
    res = pipe.run(make_documents(5))
    print(res.metrics.summary())
    print(f"\nsucceeded={len(res.succeeded)} failed={len(res.failed)} (all recovered via retry)")


def demo_skip() -> None:
    _rule("3b. Partial failure -> SKIP the bad document, finish the rest")
    docs = make_documents(5)
    bad = {docs[1].doc_id, docs[3].doc_id}
    pipe = PipelineOrchestrator(
        [
            Stage(TextExtractor(io_delay=0.0), workers=2),
            Stage(AlwaysFail(only_doc_ids=bad), workers=2, on_failure=FailureAction.SKIP),
        ]
    )
    res = pipe.run(docs)
    print(res.metrics.summary())
    print(f"\nsucceeded={len(res.succeeded)} skipped/failed={len(res.failed)} aborted={res.aborted}")
    for f in res.failed:
        print(f"  dropped {f.document.doc_id} at {f.stage}: {f.error}")


def demo_abort() -> None:
    _rule("3c. Partial failure -> ABORT the whole batch")
    pipe = PipelineOrchestrator(
        [
            Stage(TextExtractor(io_delay=0.0), workers=2),
            Stage(AlwaysFail(), workers=2, on_failure=FailureAction.ABORT),
        ]
    )
    res = pipe.run(make_documents(8))
    print(res.metrics.summary())
    print(f"\naborted={res.aborted} succeeded={len(res.succeeded)} failed={len(res.failed)}")


def main() -> None:
    logging.basicConfig(level=logging.WARNING, format="%(levelname)s %(name)s: %(message)s")
    demo_throughput_vs_latency()
    demo_retry()
    demo_skip()
    demo_abort()
    print()


if __name__ == "__main__":
    main()
