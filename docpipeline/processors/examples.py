"""Sample processors plus a few test doubles.

The "real-ish" processors model the two cost profiles the prompt calls out:

* I/O-bound  -> ``TextExtractor`` (sleeps to model a network/disk call)
* CPU-bound  -> ``EntityTagger`` (burns CPU; ``cpu_bound=True`` so it defaults
  to PROCESS mode for true multi-core parallelism)

All of these are top-level classes so they are picklable and work in PROCESS
mode. The test doubles that keep in-memory state (``FlakyProcessor``) are
THREAD-mode only and say so.
"""
from __future__ import annotations

import hashlib
import threading
import time

from ..document import Document
from ..processor import DocumentProcessor, ProcessorError


class TextExtractor(DocumentProcessor):
    """I/O-bound stage: model fetching/OCR-ing a blob with a small sleep."""

    name = "text-extraction"
    cpu_bound = False

    def __init__(self, io_delay: float = 0.01) -> None:
        self.io_delay = io_delay

    def process(self, doc: Document) -> Document:
        time.sleep(self.io_delay)  # stand-in for a network/disk round-trip
        text = doc.content or f"<raw {doc.doc_id}>"
        return doc.with_update(content=f"extracted::{text}", stage=self.name)


class LanguageDetector(DocumentProcessor):
    """Light stage: naive language guess from the content."""

    name = "language-detection"
    cpu_bound = False

    def __init__(self, io_delay: float = 0.005) -> None:
        self.io_delay = io_delay

    def process(self, doc: Document) -> Document:
        time.sleep(self.io_delay)
        lowered = doc.content.lower()
        if any(w in lowered for w in (" the ", " and ", "extracted")):
            lang = "en"
        elif any(w in lowered for w in (" le ", " la ", " et ")):
            lang = "fr"
        else:
            lang = "unknown"
        return doc.with_update(metadata={"language": lang}, stage=self.name)


class EntityTagger(DocumentProcessor):
    """CPU-bound stage: burns CPU to model entity extraction / a small model.

    ``cpu_bound = True`` makes a ``Stage`` default to PROCESS mode.
    """

    name = "entity-tagging"
    cpu_bound = True

    def __init__(self, work: int = 20_000) -> None:
        self.work = work  # number of hash rounds; tune to taste

    def process(self, doc: Document) -> Document:
        h = doc.content.encode()
        for _ in range(self.work):  # pure-CPU loop (benefits from PROCESS mode)
            h = hashlib.sha256(h).digest()
        digest = h.hex()[:8]
        entities = sorted({w.strip(".,") for w in doc.content.split() if w[:1].isupper()})
        return doc.with_update(
            metadata={"entities": entities, "fingerprint": digest}, stage=self.name
        )


# --------------------------------------------------------------------------
# Test doubles
# --------------------------------------------------------------------------
class FlakyProcessor(DocumentProcessor):
    """Fails the first ``fail_first`` attempts for each document, then succeeds.

    Keeps per-document attempt counts in memory, so it is THREAD-mode only
    (not picklable / not shareable across processes). Used to test retries.
    """

    name = "flaky"
    cpu_bound = False

    def __init__(self, fail_first: int = 1) -> None:
        self.fail_first = fail_first
        self._seen: dict[str, int] = {}
        self._lock = threading.Lock()

    def process(self, doc: Document) -> Document:
        with self._lock:
            n = self._seen.get(doc.doc_id, 0) + 1
            self._seen[doc.doc_id] = n
        if n <= self.fail_first:
            raise ProcessorError(f"transient failure #{n} for {doc.doc_id}")
        return doc.with_update(stage=self.name, metadata={"flaky_attempts": n})


class AlwaysFail(DocumentProcessor):
    """Always raises. Used to test SKIP / ABORT policies."""

    name = "always-fail"
    cpu_bound = False

    def __init__(self, only_doc_ids: set[str] | None = None) -> None:
        # If set, only fail for these doc ids (others pass through untouched).
        self.only_doc_ids = only_doc_ids

    def process(self, doc: Document) -> Document:
        if self.only_doc_ids is None or doc.doc_id in self.only_doc_ids:
            raise ProcessorError(f"always failing for {doc.doc_id}")
        return doc.with_update(stage=self.name)


class PassThrough(DocumentProcessor):
    """No-op stage with an optional delay; handy for tests/benchmarks."""

    def __init__(self, name: str = "passthrough", delay: float = 0.0) -> None:
        self.name = name
        self.delay = delay
        self.cpu_bound = False

    def process(self, doc: Document) -> Document:
        if self.delay:
            time.sleep(self.delay)
        return doc.with_update(stage=self.name)


class SlowProcessor(DocumentProcessor):
    """Sleeps for ``delay`` seconds. Top-level (picklable) so it works in
    PROCESS mode — used to exercise hard timeouts."""

    name = "slow"
    cpu_bound = False

    def __init__(self, delay: float = 0.5) -> None:
        self.delay = delay

    def process(self, doc: Document) -> Document:
        time.sleep(self.delay)
        return doc.with_update(stage=self.name)
