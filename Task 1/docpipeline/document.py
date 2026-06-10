"""The unit of work that flows through the pipeline."""
from __future__ import annotations

import copy
import itertools
from dataclasses import dataclass, field
from typing import Any

_id_counter = itertools.count(1)


@dataclass
class Document:
    """A simplified document.

    Mirrors the starter ``class Document { private String content; }`` and adds
    an id, a metadata bag, and the ordered ``history`` of stages applied (handy
    for debugging and for asserting pipeline ordering in tests).

    Processors should return an *updated copy* via :meth:`with_update` rather
    than mutating in place. That keeps a document immutable from the point of
    view of any other worker/retry, and makes it safe to pickle across a
    process boundary.
    """

    content: str = ""
    doc_id: str = field(default_factory=lambda: f"doc-{next(_id_counter)}")
    metadata: dict[str, Any] = field(default_factory=dict)
    history: list[str] = field(default_factory=list)

    def with_update(
        self,
        *,
        content: str | None = None,
        metadata: dict[str, Any] | None = None,
        stage: str | None = None,
    ) -> "Document":
        """Return a new ``Document`` with the given fields updated.

        ``stage`` (when provided) is appended to ``history`` to record that the
        named stage processed this document.
        """
        new = copy.deepcopy(self)
        if content is not None:
            new.content = content
        if metadata:
            new.metadata = {**new.metadata, **metadata}
        if stage is not None:
            new.history = [*new.history, stage]
        return new

    def __repr__(self) -> str:  # pragma: no cover - cosmetic
        preview = (self.content[:30] + "…") if len(self.content) > 31 else self.content
        return f"Document({self.doc_id!r}, content={preview!r}, history={self.history})"
