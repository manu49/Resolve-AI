"""Thread-safe metrics so throughput-vs-latency tuning is data-driven."""
from __future__ import annotations

import threading
from dataclasses import dataclass, field


@dataclass
class StageMetrics:
    name: str
    received: int = 0
    ok: int = 0
    retried: int = 0  # number of retry attempts (not counting the first try)
    failed: int = 0  # retries exhausted
    skipped: int = 0  # failed under SKIP policy
    busy_time: float = 0.0  # summed processing wall-time across workers


def _percentile(sorted_values: list[float], pct: float) -> float:
    """Nearest-rank percentile. ``pct`` in [0, 100]. Empty -> 0.0."""
    if not sorted_values:
        return 0.0
    if pct <= 0:
        return sorted_values[0]
    if pct >= 100:
        return sorted_values[-1]
    rank = max(1, min(len(sorted_values), round(pct / 100 * len(sorted_values))))
    return sorted_values[rank - 1]


class RunMetrics:
    """Aggregates per-stage counters and per-document end-to-end latencies."""

    def __init__(self, stage_names: list[str]) -> None:
        self._lock = threading.Lock()
        self.stages: dict[str, StageMetrics] = {n: StageMetrics(n) for n in stage_names}
        self.latencies: list[float] = []  # seconds, end-to-end per document
        self.wall_time: float = 0.0

    # -- recording (thread-safe) -------------------------------------------
    def record_received(self, stage: str) -> None:
        with self._lock:
            self.stages[stage].received += 1

    def record_ok(self, stage: str, busy: float) -> None:
        with self._lock:
            s = self.stages[stage]
            s.ok += 1
            s.busy_time += busy

    def record_retry(self, stage: str) -> None:
        with self._lock:
            self.stages[stage].retried += 1

    def record_failed(self, stage: str, busy: float, *, skipped: bool) -> None:
        with self._lock:
            s = self.stages[stage]
            s.failed += 1
            s.busy_time += busy
            if skipped:
                s.skipped += 1

    def record_latency(self, seconds: float) -> None:
        with self._lock:
            self.latencies.append(seconds)

    # -- derived -----------------------------------------------------------
    @property
    def count(self) -> int:
        return len(self.latencies)

    @property
    def throughput(self) -> float:
        return self.count / self.wall_time if self.wall_time > 0 else 0.0

    def latency_percentile(self, pct: float) -> float:
        with self._lock:
            return _percentile(sorted(self.latencies), pct)

    @property
    def mean_latency(self) -> float:
        with self._lock:
            return sum(self.latencies) / len(self.latencies) if self.latencies else 0.0

    def summary(self) -> str:
        lines = [
            f"wall={self.wall_time * 1000:.1f}ms  "
            f"completed={self.count}  "
            f"throughput={self.throughput:.1f} docs/s",
            f"latency  p50={self.latency_percentile(50) * 1000:.1f}ms  "
            f"p95={self.latency_percentile(95) * 1000:.1f}ms  "
            f"p99={self.latency_percentile(99) * 1000:.1f}ms  "
            f"mean={self.mean_latency * 1000:.1f}ms",
            "per-stage:",
        ]
        for s in self.stages.values():
            lines.append(
                f"  {s.name:<22} recv={s.received:<4} ok={s.ok:<4} "
                f"retried={s.retried:<3} failed={s.failed:<3} "
                f"skipped={s.skipped:<3} busy={s.busy_time * 1000:.1f}ms"
            )
        return "\n".join(lines)
