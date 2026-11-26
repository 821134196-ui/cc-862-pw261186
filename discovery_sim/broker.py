"""Pub/sub broker with bounded per-subscriber buffers.

Slow-subscriber policy (the heart of the simulator):

* Each subscription owns a *bounded* asyncio queue. A publisher never blocks on
  a slow subscriber - one stalled consumer must not stall the others.
* When a queue overflows the subscriber is **reset to a fresh snapshot**
  instead of growing without limit. A cooldown (hysteresis) prevents snapshot
  storms: while a reset is pending, new events are discarded and the reset is
  deferred until the queue drains to a low-water mark.
* A global memory budget spans every subscription. Breaching it force-resets
  the largest offender even inside its cooldown.
* A connection budget caps the number of concurrent subscriptions.
* Compaction never destroys events already sitting in a queue; it only makes a
  fresh *resume* below the watermark impossible (snapshot required instead).
"""
from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import AsyncIterator, Optional

from .model import Compacted, Event, Snapshot
from .store import HistoryCompacted, RevisionStore


class SubStatus(str, Enum):
    STREAMING = "STREAMING"            # receiving incremental events
    SNAPSHOT = "SNAPSHOT"              # a snapshot is queued / being applied
    PENDING_RESET = "PENDING_RESET"    # overflow seen, snapshot deferred
    DISCONNECTED = "DISCONNECTED"


class TooManySubscribers(Exception):
    """Connection-budget rejection."""


def estimate_size(obj: object) -> int:
    """Rough byte footprint used for memory-budget accounting."""
    if isinstance(obj, Event):
        n = obj.node
        return 64 + len(n.name) + len(n.address) + sum(
            len(k) + len(v) for k, v in n.metadata.items())
    if isinstance(obj, Snapshot):
        return 256 + sum(estimate_size(_snapshot_event(n)) for n in obj.nodes.values())
    if isinstance(obj, Compacted):
        return 32
    return 64


def _snapshot_event(node) -> Event:  # pragma: no cover - trivial helper
    from .model import EventType
    return Event(revision=node.version, type=EventType.UPDATE, node=node)


@dataclass
class SubMetrics:
    events_offered: int = 0
    events_dropped: int = 0
    snapshots: int = 0
    snapshot_resets: int = 0
    budget_resets: int = 0
    compacted_markers: int = 0
    bytes_dropped: int = 0


class Subscription:
    """One client's ordered stream: events, snapshots and compaction markers."""

    def __init__(self, broker: "Broker", name: str, anchor_revision: int,
                 queue_max: int, reset_cooldown: float, low_water_ratio: float):
        self.broker = broker
        self.name = name
        self.anchor_revision = anchor_revision  # client is current through here
        self.queue: asyncio.Queue = asyncio.Queue(maxsize=queue_max)
        self.queue_max = queue_max
        self.reset_cooldown = reset_cooldown
        self.low_water = max(1, int(queue_max * low_water_ratio))
        self.status = SubStatus.STREAMING
        self.metrics = SubMetrics()
        self.created_at = time.monotonic()
        self.last_reset_at = 0.0
        self.pending_reset = False
        self.pending_reason = ""
        self.closed = False

    # ------------------------------------------------------------- server side
    def offer(self, event: Event) -> None:
        if self.closed or event.revision <= self.anchor_revision:
            return
        if self.pending_reset:
            self._discard(event)
            self._maybe_run_pending_reset()
            return
        try:
            self.queue.put_nowait(event)
            self.metrics.events_offered += 1
        except asyncio.QueueFull:
            self._on_overflow(reason="queue overflow")

    def offer_marker(self, marker: Compacted) -> None:
        if self.closed:
            return
        if self.pending_reset:
            return  # the upcoming snapshot already supersedes the marker
        try:
            self.queue.put_nowait(marker)
            self.metrics.compacted_markers += 1
        except asyncio.QueueFull:
            self._on_overflow(reason="compaction marker overflow")

    def force_snapshot(self, reason: str, budget_breach: bool = False) -> Snapshot:
        """Drain everything and replace the stream with a fresh snapshot."""
        snap = self.broker.build_snapshot()
        drained = self._drain()
        self.queue.put_nowait(snap)
        self.anchor_revision = snap.revision
        self.status = SubStatus.SNAPSHOT
        self.pending_reset = False
        self.pending_reason = ""
        self.last_reset_at = time.monotonic()
        self.metrics.snapshots += 1
        if budget_breach:
            self.metrics.budget_resets += 1
        else:
            self.metrics.snapshot_resets += 1
        self.broker.note_reset(self, reason, drained_bytes=drained)
        return snap

    def close(self) -> None:
        # Sentinel unblocks a consumer parked in queue.get(); stream() then
        # exits on its ``closed`` check instead of hanging forever.
        self.closed = True
        self.status = SubStatus.DISCONNECTED
        try:
            self.queue.put_nowait(None)
        except asyncio.QueueFull:
            # Make room by dropping the oldest queued item.
            try:
                self.queue.get_nowait()
            except asyncio.QueueEmpty:
                pass
            self.queue.put_nowait(None)

    # ------------------------------------------------------------- internals
    def _on_overflow(self, reason: str) -> None:
        now = time.monotonic()
        if now - self.last_reset_at >= self.reset_cooldown:
            self.force_snapshot(reason)
        else:
            # Hysteresis: do not emit snapshot back-to-back. Discard incoming
            # events (bounded memory!) until the client drains the backlog.
            self.pending_reset = True
            self.pending_reason = reason
            self.status = SubStatus.PENDING_RESET
            self.broker.note_pending_reset(self, reason)

    def _discard(self, event: Event) -> None:
        self.metrics.events_dropped += 1
        self.metrics.bytes_dropped += estimate_size(event)

    def _maybe_run_pending_reset(self) -> None:
        if self.pending_reset and self.queue.qsize() <= self.low_water:
            self.force_snapshot(f"deferred: {self.pending_reason}")

    def _drain(self) -> int:
        bytes_dropped = 0
        while True:
            try:
                item = self.queue.get_nowait()
            except asyncio.QueueEmpty:
                break
            bytes_dropped += estimate_size(item)
            self.metrics.events_dropped += 1 if isinstance(item, Event) else 0
        return bytes_dropped

    def on_item_consumed(self) -> None:
        """Called by the stream loop after each item is handed to the client."""
        if self.status == SubStatus.SNAPSHOT and self.queue.qsize() == 0:
            self.status = SubStatus.STREAMING
        if self.pending_reset:
            self._maybe_run_pending_reset()

    # -------------------------------------------------------------- client API
    async def stream(self) -> AsyncIterator[object]:
        """Yield Snapshot / Event / Compacted objects until closed."""
        while not self.closed:
            item = await self.queue.get()
            if item is None:
                continue  # close sentinel; loop condition now exits
            if isinstance(item, Event):
                self.anchor_revision = item.revision
            yield item
            self.on_item_consumed()

    @property
    def queued(self) -> int:
        return self.queue.qsize()

    @property
    def queued_bytes(self) -> int:
        total = 0
        for item in list(self.queue._queue):  # type: ignore[attr-defined]
            total += estimate_size(item)
        return total


class Broker:
    def __init__(self, store: RevisionStore, *,
                 global_memory_items: int = 10_000,
                 max_subscriptions: int = 100) -> None:
        self.store = store
        self.global_memory_items = global_memory_items
        self.max_subscriptions = max_subscriptions
        self._subs: dict[str, Subscription] = {}
        # Event log for the dashboard / tests.
        self.audit: list[str] = []

    # ------------------------------------------------------------- membership
    @property
    def subscriptions(self) -> list[Subscription]:
        return list(self._subs.values())

    def get(self, name: str) -> Optional[Subscription]:
        return self._subs.get(name)

    def subscribe(self, name: str, *, start_revision: Optional[int] = None,
                  queue_max: int = 200, reset_cooldown: float = 1.0,
                  low_water_ratio: float = 0.25,
                  replay_budget_items: int = 500) -> Subscription:
        """Register a subscriber and decide snapshot-vs-backfill hand-off.

        * New client (no start revision) or start below the compaction
          watermark -> snapshot first.
        * Reachable history larger than the replay budget -> snapshot first
          (cheaper to ship state once than a huge delta stream).
        * Otherwise -> backfill retained events, then continue live.
        """
        if len(self._subs) >= self.max_subscriptions and name not in self._subs:
            raise TooManySubscribers(
                f"connection budget exhausted ({self.max_subscriptions})")

        # Re-subscribe under an existing name means a reconnect: the previous
        # subscription is closed and must be replaced, never silently reused.
        old = self._subs.pop(name, None)
        if old is not None:
            old.close()

        start = self.store.revision if start_revision is None else start_revision
        sub = Subscription(self, name, anchor_revision=start,
                           queue_max=queue_max,
                           reset_cooldown=reset_cooldown,
                           low_water_ratio=low_water_ratio)
        use_snapshot = False
        reason = ""
        if start_revision is None:
            use_snapshot, reason = True, "new subscriber"
        elif start < self.store.compacted_through:
            use_snapshot, reason = True, "resume below compaction watermark"
        else:
            try:
                backlog = self.store.events_since(start)
            except HistoryCompacted as exc:
                use_snapshot, reason = True, str(exc)
                backlog = []
            if not use_snapshot and len(backlog) > replay_budget_items:
                use_snapshot, reason = True, (
                    f"backlog {len(backlog)} > replay budget {replay_budget_items}")

        if use_snapshot:
            snap = self.build_snapshot()
            sub.anchor_revision = snap.revision
            sub.queue.put_nowait(snap)
            sub.status = SubStatus.SNAPSHOT
            sub.metrics.snapshots += 1
        else:
            for event in backlog:
                sub.queue.put_nowait(event)
                sub.metrics.events_offered += 1
            sub.anchor_revision = start
        self._subs[name] = sub
        if use_snapshot:
            handoff = f"SNAPSHOT@{sub.anchor_revision}"
        else:
            handoff = f"backfill {len(backlog)} events from {start}"
        self._log(f"subscribe {name}: {handoff} ({reason})")
        return sub

    def unsubscribe(self, name: str) -> None:
        sub = self._subs.pop(name, None)
        if sub is not None:
            sub.close()
            self._log(f"unsubscribe {name}")

    # -------------------------------------------------------------- publishing
    def publish(self, event: Event) -> None:
        for sub in self._subs.values():
            sub.offer(event)
        self._enforce_memory_budget()

    def publish_compaction(self, marker: Compacted) -> None:
        for sub in self._subs.values():
            sub.offer_marker(marker)
        self._log(f"compaction watermark -> {marker.compacted_through} "
                  f"(head {marker.current_revision})")

    def build_snapshot(self) -> Snapshot:
        return self.store.snapshot()

    # ---------------------------------------------------------------- budgets
    def total_queued_items(self) -> int:
        return sum(s.queued for s in self._subs.values())

    def total_queued_bytes(self) -> int:
        return sum(s.queued_bytes for s in self._subs.values())

    def _enforce_memory_budget(self) -> None:
        if self.total_queued_items() <= self.global_memory_items:
            return
        # Reset the greediest subscriber; one reset may be enough, loop until
        # the budget is respected (cooldown is waived under budget pressure).
        while self.total_queued_items() > self.global_memory_items:
            offender = max(self._subs.values(), key=lambda s: s.queued)
            if offender.queued == 0:
                break
            offender.force_snapshot("global memory budget", budget_breach=True)

    # ------------------------------------------------------------ observability
    def note_reset(self, sub: Subscription, reason: str,
                   drained_bytes: int = 0) -> None:
        self._log(f"SNAPSHOT RESET on {sub.name} @{sub.anchor_revision} "
                  f"({reason}, drained ~{drained_bytes}B)")

    def note_pending_reset(self, sub: Subscription, reason: str) -> None:
        self._log(f"PENDING RESET on {sub.name}: {reason} "
                  f"(cooldown, dropping events until drain)")

    def _log(self, line: str) -> None:
        self.audit.append(f"{time.strftime('%H:%M:%S')} {line}")
        del self.audit[:-200]
