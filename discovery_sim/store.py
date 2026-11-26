"""Revisioned key-value store: the server's source of truth.

Design points exercised by the rest of the project:

* Every write appends one gapless, globally-ordered event to a revision log
  (etcd-style revisions). Subscribers resume by revision number.
* ``compact()`` truncates the log and raises a watermark (``compacted_through``).
  Any subscriber whose resume point is below the watermark cannot replay
  history and must take a fresh snapshot instead.
* Deregisters leave tombstones (node name -> last version). Tombstones keep
  late old events rejectable and are only garbage-collected after a
  configurable grace period of revisions.
* Writes support an ``expected_version`` fence so a client holding a stale view
  cannot blindly overwrite newer state (optimistic concurrency).
"""
from __future__ import annotations

import threading
from typing import Optional

from .model import Event, EventType, Node, Snapshot, StaleWriteError, with_version


class RevisionStore:
    def __init__(self, tombstone_grace: int = 50) -> None:
        self._lock = threading.RLock()
        self._revision = 0
        self._log: list[Event] = []          # only retained (uncompacted) events
        self._nodes: dict[str, Node] = {}
        self._tombstones: dict[str, int] = {}
        self._compacted_through = 0
        self._tombstone_grace = tombstone_grace
        # Observability counters.
        self.rejected_writes = 0
        self.compactions = 0
        self.gc_runs = 0

    # ------------------------------------------------------------------ reads
    @property
    def revision(self) -> int:
        return self._revision

    @property
    def compacted_through(self) -> int:
        return self._compacted_through

    @property
    def log_len(self) -> int:
        return len(self._log)

    def snapshot(self) -> Snapshot:
        """Point-in-time full state; safe to hand to a slow subscriber."""
        with self._lock:
            return Snapshot(
                revision=self._revision,
                nodes=dict(self._nodes),
                tombstones=dict(self._tombstones),
                compacted_through=self._compacted_through,
            )

    def events_since(self, since_revision: int) -> list[Event]:
        """Return retained events with revision strictly greater than ``since``.

        Raises ``HistoryCompacted`` when the requested starting point has been
        compacted away - the caller must fetch a snapshot instead.
        """
        with self._lock:
            if since_revision < self._compacted_through:
                raise HistoryCompacted(since_revision, self._compacted_through,
                                       self._revision)
            # The retained log starts at compacted_through + 1.
            start = since_revision - self._compacted_through
            if start < 0:
                start = 0
            return list(self._log[start:])

    def node(self, name: str) -> Optional[Node]:
        with self._lock:
            return self._nodes.get(name)

    def tombstone_version(self, name: str) -> Optional[int]:
        with self._lock:
            return self._tombstones.get(name)

    # ----------------------------------------------------------------- writes
    def register(self, name: str, address: str,
                 metadata: Optional[dict[str, str]] = None,
                 expected_version: Optional[int] = None) -> Event:
        """Create or update a node, returning the appended event."""
        with self._lock:
            existing = self._nodes.get(name)
            self._check_fence(name, expected_version, current_version(existing))
            if existing is None and name not in self._tombstones:
                etype = EventType.REGISTER
            else:
                # Re-registering after a deregister is a fresh REGISTER; an
                # existing live node gets an UPDATE.
                etype = EventType.UPDATE if existing is not None else EventType.REGISTER
            self._revision += 1
            node = Node(name=name, address=address, version=self._revision,
                        metadata=dict(metadata or {}))
            if name in self._tombstones:
                # New life for the name: its stale tombstone is superseded.
                del self._tombstones[name]
            event = Event(revision=self._revision, type=etype, node=node)
            self._append(event)
            self._nodes[name] = node
            return event

    def update_metadata(self, name: str, metadata: dict[str, str],
                        expected_version: Optional[int] = None) -> Event:
        with self._lock:
            existing = self._nodes.get(name)
            if existing is None:
                raise KeyError(f"unknown node {name!r}")
            self._check_fence(name, expected_version, existing.version)
            self._revision += 1
            node = Node(name=name, address=existing.address,
                        version=self._revision, metadata=dict(metadata))
            event = Event(revision=self._revision, type=EventType.UPDATE, node=node)
            self._append(event)
            self._nodes[name] = node
            return event

    def deregister(self, name: str,
                   expected_version: Optional[int] = None) -> Event:
        with self._lock:
            existing = self._nodes.get(name)
            if existing is None:
                raise KeyError(f"unknown node {name!r}")
            self._check_fence(name, expected_version, existing.version)
            self._revision += 1
            dead = with_version(existing, self._revision)
            event = Event(revision=self._revision,
                          type=EventType.DEREGISTER, node=dead)
            self._append(event)
            del self._nodes[name]
            # Tombstone records the version at which the node died. Any event
            # carrying an older version for this name is stale forever.
            self._tombstones[name] = self._revision
            return event

    # -------------------------------------------------------------- compaction
    def compact(self, retain_events: int = 10,
                gc_tombstones: bool = True) -> int:
        """Drop history, keeping at most ``retain_events`` log entries.

        Returns the new compacted_through watermark. Garbage-collects tombstones
        older than the grace window so their memory is eventually reclaimed;
        any event older than a GC'd tombstone is still fenced by the client's
        per-node version check on the live side.
        """
        with self._lock:
            if len(self._log) <= retain_events:
                return self._compacted_through
            cut = len(self._log) - retain_events
            watermark = self._log[cut - 1].revision
            del self._log[:cut]
            self._compacted_through = watermark
            self.compactions += 1
            if gc_tombstones:
                self._gc_tombstones()
            return watermark

    def _gc_tombstones(self) -> None:
        # A tombstone may be dropped once its death revision is far enough in
        # the past: any in-flight event still older than it will also be older
        # than the snapshot a lagging subscriber is forced to take.
        horizon = self._compacted_through - self._tombstone_grace
        stale = [name for name, rev in self._tombstones.items() if rev <= horizon]
        for name in stale:
            del self._tombstones[name]
        if stale:
            self.gc_runs += 1

    # ---------------------------------------------------------------- helpers
    def _append(self, event: Event) -> None:
        self._log.append(event)

    def _check_fence(self, name: str, expected: Optional[int],
                     actual: Optional[int]) -> None:
        if expected is None:
            return
        if expected != actual:
            self.rejected_writes += 1
            raise StaleWriteError(name, expected, actual)


def current_version(node: Optional[Node]) -> Optional[int]:
    return node.version if node is not None else None


class HistoryCompacted(Exception):
    """Raised when a resume revision lies below the compaction watermark."""

    def __init__(self, requested: int, watermark: int, current: int):
        self.requested = requested
        self.watermark = watermark
        self.current = current
        super().__init__(
            f"history through revision {watermark} has been compacted "
            f"(requested resume at {requested}, head is {current}); "
            f"a snapshot is required"
        )
