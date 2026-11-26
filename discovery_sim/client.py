"""Discovery client: local view maintained from snapshots + incremental events.

Correctness fences (both are required):

1. Global revision fence - every applied item must be newer than
   ``last_revision``; this makes the stream idempotent and lets a snapshot
   hand off to deltas at exactly ``snapshot.revision + 1``.
2. Per-node version fence - a node is only touched when the incoming event's
   node version exceeds the version recorded locally. This is the defence
   against *late old events*: after a deregister (or after a reset snapshot),
   an out-of-order event carrying an older version can never resurrect or
   downgrade the node, even if it arrives out of band (e.g. via gossip).
"""
from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Callable, Optional

from .broker import Subscription, TooManySubscribers
from .model import Compacted, Event, EventType, Node, Snapshot
from .server import DiscoveryServer
from .store import HistoryCompacted


class ClientState(str, Enum):
    CONNECTING = "CONNECTING"
    STREAMING = "STREAMING"
    PAUSED = "PAUSED"
    FETCHING_SNAPSHOT = "FETCHING_SNAPSHOT"
    DISCONNECTED = "DISCONNECTED"


@dataclass
class ClientStats:
    snapshots_applied: int = 0
    events_applied: int = 0
    stale_events_rejected: int = 0
    reconnects: int = 0
    resets_observed: int = 0
    last_latency_ms: float = 0.0
    max_latency_ms: float = 0.0
    total_latency_ms: float = 0.0
    latency_samples: int = 0


class DiscoveryClient:
    def __init__(self, name: str, server: DiscoveryServer, *,
                 process_delay: float = 0.0,
                 on_change: Optional[Callable[[str], None]] = None) -> None:
        self.name = name
        self.server = server
        # Simulated per-event processing time: >0 makes the client "slow".
        self.process_delay = process_delay
        self.on_change = on_change

        self.view: dict[str, Node] = {}
        self.tombstones: dict[str, int] = {}
        self.last_revision = 0
        self.state: ClientState = ClientState.CONNECTING
        self.stats = ClientStats()
        self.subscription: Optional[Subscription] = None
        self._task: Optional[asyncio.Task] = None
        self._stop = asyncio.Event()
        # Transport-level reordering pocket: events dropped here are released
        # late, simulating a delayed packet that arrives after newer state.
        self._delay_pocket: list[tuple[float, Event]] = []
        self.delay_pocket_probability: float = 0.0
        self.delay_pocket_seconds: float = 0.0
        # Highest compaction watermark announced on the stream (informational).
        self.last_compacted_through = 0
        # Pause gate: clear() => stop pulling from the queue (backpressure
        # builds server-side); set() => consume normally.
        self._paused = asyncio.Event()
        self._paused.set()
        # Connectivity gate: set while online. disconnect() clears it and the
        # loop suspends on wait(); reconnect() sets it to wake the loop.
        self._online = asyncio.Event()
        self._online.set()

    # ============================================================ lifecycle
    async def start(self) -> None:
        self._stop.clear()
        self._task = asyncio.create_task(self._run(), name=f"client-{self.name}")

    async def stop(self) -> None:
        self._stop.set()
        if self.subscription is not None:
            self.server.unsubscribe(self.name)
            self.subscription = None
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

    def pause(self) -> None:
        """Stop consuming; server keeps queueing (and will reset us eventually)."""
        self._paused.clear()
        self.state = ClientState.PAUSED

    def resume(self) -> None:
        self._paused.set()
        if self.state == ClientState.PAUSED:
            self.state = ClientState.STREAMING

    async def disconnect(self) -> None:
        """Drop the subscription while keeping the local view (network outage).

        The background loop stays alive and suspends on the connectivity gate
        until ``reconnect()`` is called.
        """
        self.stats.reconnects += 1
        self.state = ClientState.DISCONNECTED
        self._online.clear()
        if self.subscription is not None:
            self.server.unsubscribe(self.name)
            self.subscription = None

    async def reconnect(self) -> None:
        """Restore connectivity; the loop resubscribes from last_revision."""
        self._online.set()
        if self.state == ClientState.DISCONNECTED:
            self.state = ClientState.STREAMING

    @property
    def paused(self) -> bool:
        return not self._paused.is_set() and self.state == ClientState.PAUSED

    @property
    def lag(self) -> int:
        head = self.server.store.revision
        return max(0, head - self.last_revision)

    # =============================================================== the loop
    async def _run(self) -> None:
        while not self._stop.is_set():
            # Suspend for the duration of a simulated outage. This is a real
            # suspension (not a poll), so the event loop keeps serving others.
            if not self._online.is_set():
                await self._online.wait()
                continue
            try:
                self.subscription = self.server.subscribe(
                    self.name, start_revision=self._resume_revision())
                if self.state != ClientState.PAUSED:
                    self.state = ClientState.STREAMING
                await self._consume(self.subscription)
                # _consume returns only when the subscription was closed.
            except HistoryCompacted:
                # Resume point was compacted while we were away: full snapshot.
                await self._recover_with_snapshot("history compacted while away")
            except TooManySubscribers:
                # Connection budget full: back off and retry the subscription.
                self.state = ClientState.CONNECTING
                self.broker_log("connection budget full; backing off")
                await asyncio.sleep(0.1)
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # pragma: no cover - defensive
                if self._stop.is_set():
                    break
                self.broker_log(f"stream error: {exc!r}; reconnecting")
                await asyncio.sleep(0.05)
                await self._recover_with_snapshot(f"stream error {exc!r}")

    async def _consume(self, subscription: "Subscription") -> None:
        """Drain one subscription's queue, honoring the pause gate.

        The pause wait sits *before* queue.get(): while paused we never take
        ownership of an item, so it stays in the server-side buffer and the
        bounded-queue / snapshot-reset machinery behaves exactly like a real
        stalled consumer.
        """
        queue = subscription.queue
        while not subscription.closed:
            await self._paused.wait()
            if subscription.closed or self._stop.is_set():
                return
            item = await queue.get()
            if item is None:
                continue  # close sentinel
            if isinstance(item, Event):
                subscription.anchor_revision = item.revision
            await self._handle(item)
            subscription.on_item_consumed()

    def _resume_revision(self) -> Optional[int]:
        return self.last_revision if self.last_revision > 0 else None

    async def _recover_with_snapshot(self, reason: str) -> None:
        self.state = ClientState.FETCHING_SNAPSHOT
        self.broker_log(f"reconnect: fetching fresh snapshot ({reason})")
        # subscribe() replaces any closed subscription of the same name;
        # passing no resume point makes the server hand us a snapshot.
        self.subscription = self.server.subscribe(self.name, start_revision=None)
        self.state = ClientState.STREAMING

    # ================================================================ apply
    async def _handle(self, item: object) -> None:
        if isinstance(item, Snapshot):
            self._apply_snapshot(item)
        elif isinstance(item, Compacted):
            self._apply_compacted(item)
        else:
            assert isinstance(item, Event)
            # Simulated transport reordering: pocket some events for later.
            if (self.delay_pocket_probability
                    and _coin(self.delay_pocket_probability)
                    and item.type != EventType.DEREGISTER):
                self._delay_pocket.append(
                    (time.monotonic() + self.delay_pocket_seconds, item))
                self._release_due_pocket()
                return
            self._release_due_pocket()
            self._apply_event(item)
        if self.process_delay:
            await asyncio.sleep(self.process_delay)

    def _apply_snapshot(self, snap: Snapshot) -> None:
        self.stats.snapshots_applied += 1
        self.stats.resets_observed += 1
        self.last_revision = snap.revision
        # Replace, don't merge: snapshot is authoritative.
        self.view = dict(snap.nodes)
        self.tombstones = dict(snap.tombstones)
        self.broker_log(f"snapshot applied @{snap.revision} "
                        f"({len(self.view)} live nodes)")
        self._notify()

    def _apply_compacted(self, marker: Compacted) -> None:
        self.last_compacted_through = marker.compacted_through
        if self.last_revision >= marker.compacted_through:
            return  # our stream already passed the watermark; nothing to do
        # We are lagging behind the watermark. Events already in our queue stay
        # valid and will be drained; if we later try to *resume* (reconnect)
        # below this watermark the server rejects it with HistoryCompacted and
        # we recover with a full snapshot in _recover_with_snapshot().
        self.broker_log(
            f"COMPACTED marker: watermark {marker.compacted_through} > "
            f"applied {self.last_revision}; snapshot will be required on resume")

    def _apply_event(self, event: Event) -> None:
        now = time.monotonic()
        # Fence 1: global revision ordering.
        if event.revision <= self.last_revision:
            self.stats.stale_events_rejected += 1
            return
        # Fence 2: per-node version. Live node, tombstone, or unknown - the
        # newest writer always wins and older versions can never undo it.
        node = event.node
        known = self.view.get(node.name)
        tomb = self.tombstones.get(node.name)
        local_version = known.version if known is not None else (
            tomb if tomb is not None else -1)
        if node.version <= local_version:
            self.stats.stale_events_rejected += 1
            self.broker_log(
                f"STALE event rejected: {event.type.value} {node.name!r} "
                f"v{node.version} (local v{local_version}, rev {event.revision})")
            # NOTE: do NOT advance last_revision here. Out-of-band stale events
            # (gossip duplicates, retransmits) can carry arbitrary revision
            # numbers; advancing the global fence would make us skip the real
            # stream event carrying the same node's current state.
            self._record_latency(now, event)
            return

        if event.type == EventType.DEREGISTER:
            self.view.pop(node.name, None)
            self.tombstones[node.name] = node.version
        else:
            self.view[node.name] = node
            self.tombstones.pop(node.name, None)
        self.last_revision = event.revision
        self.stats.events_applied += 1
        self._record_latency(now, event)
        self._notify()

    def inject_external_event(self, event: Event) -> bool:
        """Test hook: feed an event that bypassed the ordered stream.

        Models gossip / UDP duplicates. Returns True iff it was applied.
        """
        before = self.stats.stale_events_rejected
        self._apply_event(event)
        return self.stats.stale_events_rejected == before

    def _release_due_pocket(self) -> None:
        if not self._delay_pocket:
            return
        now = time.monotonic()
        due = [e for due_at, e in self._delay_pocket if due_at <= now]
        self._delay_pocket = [(t, e) for t, e in self._delay_pocket if t > now]
        for event in sorted(due, key=lambda e: e.revision):
            self._apply_event(event)

    # --------------------------------------------------------------- helpers
    def _record_latency(self, started: float, event: Event) -> None:
        ms = max(0.0, (time.monotonic() - started) * 1000.0)
        self.stats.last_latency_ms = ms
        self.stats.max_latency_ms = max(self.stats.max_latency_ms, ms)
        self.stats.total_latency_ms += ms
        self.stats.latency_samples += 1

    def broker_log(self, line: str) -> None:
        self.server.broker._log(f"[client {self.name}] {line}")

    def _notify(self) -> None:
        if self.on_change is not None:
            self.on_change(self.name)

    # ------------------------------------------------------------ consistency
    def expected_view(self) -> dict[str, Node]:
        """What the server believes this client *should* eventually hold."""
        return self.server.store.snapshot().nodes

    def is_converged(self) -> bool:
        if self.lag != 0:
            return False
        expected = self.expected_view()
        if set(self.view) != set(expected):
            return False
        return all(self.view[n].version == expected[n].version for n in expected)

    def divergence(self) -> list[str]:
        expected = self.expected_view()
        problems = []
        for name in sorted(set(self.view) | set(expected)):
            have = self.view.get(name)
            want = expected.get(name)
            if have is None:
                problems.append(f"missing node {name!r} (server v{want.version})")
            elif want is None:
                problems.append(f"phantom node {name!r} (client v{have.version})")
            elif have.version != want.version:
                problems.append(
                    f"version mismatch {name!r}: client v{have.version} "
                    f"!= server v{want.version}")
        return problems


def _coin(p: float) -> bool:
    import random
    return random.random() < p
