"""How a stage is executed: parallelism, executor type, retry & failure policy."""
from __future__ import annotations

import random
from dataclasses import dataclass, field
from enum import Enum

from .processor import DocumentProcessor


class ExecMode(str, Enum):
    """Execution mode for a stage's workers."""

    #: Worker threads call the processor directly. Best for I/O-bound stages.
    THREAD = "thread"
    #: Worker threads delegate to a process pool. Best for CPU-bound stages
    #: (true multi-core parallelism, no GIL contention).
    PROCESS = "process"


class FailureAction(str, Enum):
    """What to do with a document after its retries are exhausted."""

    #: Drop this document, record it, keep processing the rest of the batch.
    SKIP = "skip"
    #: Stop the whole batch.
    ABORT = "abort"


@dataclass
class RetryPolicy:
    """Exponential backoff with jitter.

    ``max_retries`` is the number of *additional* attempts after the first, so
    ``max_retries=2`` means up to 3 total tries.
    """

    max_retries: int = 0
    backoff_base: float = 0.05  # seconds
    backoff_max: float = 2.0  # cap
    jitter: float = 0.5  # +/- fraction of the computed delay

    def delay(self, attempt: int) -> float:
        """Delay (seconds) to sleep *before* the given attempt (attempt >= 2)."""
        raw = self.backoff_base * (2 ** (attempt - 2))
        capped = min(self.backoff_max, raw)
        if self.jitter:
            spread = capped * self.jitter
            capped = random.uniform(capped - spread, capped + spread)
        return max(0.0, capped)


@dataclass
class Stage:
    """A processor plus how to run it."""

    processor: DocumentProcessor
    workers: int = 1
    mode: ExecMode | None = None  # default inferred from processor.cpu_bound
    retry: RetryPolicy = field(default_factory=RetryPolicy)
    on_failure: FailureAction = FailureAction.SKIP
    timeout: float | None = None  # enforced in PROCESS mode only
    queue_size: int = 0  # 0 => default of workers * 2

    def __post_init__(self) -> None:
        if self.workers < 1:
            raise ValueError("Stage.workers must be >= 1")
        if self.mode is None:
            self.mode = ExecMode.PROCESS if self.processor.cpu_bound else ExecMode.THREAD

    @property
    def name(self) -> str:
        return self.processor.name

    @property
    def effective_queue_size(self) -> int:
        return self.queue_size if self.queue_size > 0 else max(1, self.workers * 2)
