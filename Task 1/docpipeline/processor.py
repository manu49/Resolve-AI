"""The processor interface — one stage of work, i.e. one "microservice"."""
from __future__ import annotations

from abc import ABC, abstractmethod

from .document import Document


class ProcessorError(Exception):
    """Raised by a processor to signal that processing this document failed.

    Any exception type is treated as a failure by the orchestrator; this one
    exists so processors can be explicit and tests can assert on it.
    """


class DocumentProcessor(ABC):
    """Translation of the starter ``interface DocumentProcessor``.

    A processor takes a :class:`Document` and returns a (usually updated)
    :class:`Document`. Subclasses set :attr:`name` (used in metrics/logs) and
    :attr:`cpu_bound` (a hint the caller can use to pick THREAD vs PROCESS
    execution).
    """

    #: Human-readable stage name, surfaced in metrics and logs.
    name: str = "processor"

    #: Hint: True for compute-heavy stages that benefit from PROCESS mode.
    cpu_bound: bool = False

    @abstractmethod
    def process(self, doc: Document) -> Document:
        """Process a single document and return the result."""
        raise NotImplementedError
