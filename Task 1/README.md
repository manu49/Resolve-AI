# Resolve-AI

Solution to **Problem 1 — Document Processing Pipeline with Microservices**
from the 2026 Systems Coding Practice Questions.

A small, dependency-free (stdlib-only) Python library that runs a batch of
documents through a sequence of pluggable processors ("microservices"),
concurrently, with per-stage failure handling and built-in
throughput/latency metrics.

See **[`DESIGN.md`](DESIGN.md)** for the full write-up (architecture, the
latency-vs-throughput trade-off, failure semantics, and extension notes).

## What it does (maps to the four task requirements)

1. **Orchestrator over a sequence of processors** — `PipelineOrchestrator`
   takes `list[Stage]` and runs a batch of `Document`s through them.
2. **Concurrency** — two strategies:
   - `run()` — a **staged pipeline**: each stage has its own pool of workers and
     a bounded queue, so every stage works on a different document at once
     (best **throughput**).
   - `run_per_document()` — **per-document fan-out**: each document goes through
     all stages on one worker, parallelized across documents (best **latency**
     for I/O-bound or single-request work).
3. **Partial failure** — per stage: `RetryPolicy` (exponential backoff + jitter)
   then `FailureAction.SKIP` (drop the doc, keep going) or `ABORT` (stop the
   batch). PROCESS-mode stages also enforce a hard `timeout`.
4. **Throughput vs latency** — every run returns `RunMetrics`: per-document
   latency p50/p95/p99, throughput (docs/s), and per-stage busy time so you can
   find and tune the bottleneck.

CPU- vs I/O-bound is handled per stage via `ExecMode`: `THREAD` (I/O-bound,
overlaps on the GIL) or `PROCESS` (CPU-bound, true multi-core). Multiple
workers per stage model multiple **instances** of a microservice, and the
bounded queue is the load balancer + backpressure.

## Quick start

```python
from docpipeline import Document, Stage, PipelineOrchestrator, ExecMode
from docpipeline.processors.examples import TextExtractor, LanguageDetector, EntityTagger

pipe = PipelineOrchestrator([
    Stage(TextExtractor(),  workers=6),                       # I/O-bound -> threads
    Stage(LanguageDetector(), workers=4),
    Stage(EntityTagger(), workers=4, mode=ExecMode.PROCESS),  # CPU-bound -> processes
])

docs = [Document(content="The Eiffel Tower is in Paris.")]
result = pipe.run(docs)                 # throughput-optimized (batch)
# result = pipe.run_per_document(docs)  # latency-optimized (per request)

print(result.succeeded[0].metadata)
print(result.metrics.summary())
```

Write your own stage by subclassing `DocumentProcessor`:

```python
from docpipeline import DocumentProcessor, Document

class UpperCaser(DocumentProcessor):
    name = "uppercase"
    def process(self, doc: Document) -> Document:
        return doc.with_update(content=doc.content.upper(), stage=self.name)
```

## Run it

From this directory (`Task 1/`):

```bash
# Demo: throughput vs latency, retry, skip, abort
python -m docpipeline.demo

# Tests
pip install -r requirements-dev.txt
pytest -q
```

## Layout

```
docpipeline/
  document.py        Document
  processor.py       DocumentProcessor ABC, ProcessorError
  policies.py        Stage, RetryPolicy, FailureAction, ExecMode
  metrics.py         StageMetrics, RunMetrics (+ percentiles)
  orchestrator.py    PipelineOrchestrator, PipelineResult, FailedDocument
  processors/
    examples.py      sample I/O- and CPU-bound stages + test doubles
  demo.py            runnable demo
tests/
  test_pipeline.py
DESIGN.md            design doc
```
