"""Core data model for the service-discovery simulator.

Everything that crosses a wire is an immutable, plain-data object so the same
types work for in-process asyncio streams and for JSON snapshots.
"""
from __future__ import annotations

import time
from dataclasses import dataclass, field, replace
from enum import Enum
from typing import Any, Optional


class EventType(str, Enum):
    REGISTER = "REGISTER"
    UPDATE = "UPDATE"
    DEREGISTER = "DEREGISTER"


@dataclass(frozen=True)
class Node:
    """A service node.

    ``version`` is the global revision of the last write that touched the node.
    Clients fence on it: an event whose node version is not newer than the
    version already applied for that node must be ignored. That fence is what
    keeps a deregistered node from being resurrected by a late old event.
    """

    name: str
    address: str
    version: int
    metadata: dict[str, str] = field(default_factory=dict)
    updated_at: float = field(default_factory=time.time)

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "address": self.address,
            "version": self.version,
            "metadata": dict(self.metadata),
            "updated_at": self.updated_at,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "Node":
        return cls(
            name=data["name"],
            address=data["address"],
            version=int(data["version"]),
            metadata=dict(data.get("metadata", {})),
            updated_at=float(data.get("updated_at", 0.0)),
        )


@dataclass(frozen=True)
class Event:
    """One entry of the revision log. Revision numbers are gapless and global."""

    revision: int
    type: EventType
    node: Node

    def to_dict(self) -> dict[str, Any]:
        return {
            "revision": self.revision,
            "type": self.type.value if isinstance(self.type, EventType) else self.type,
            "node": self.node.to_dict(),
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "Event":
        return cls(
            revision=int(data["revision"]),
            type=EventType(data["type"]),
            node=Node.from_dict(data["node"]),
        )


@dataclass(frozen=True)
class Snapshot:
    """Full state at ``revision``.

    A snapshot is a *hand-off point*: after applying it the client resumes the
    incremental stream at revision + 1, so snapshot and deltas join with no gap
    and no overlap (the per-node version fence makes overlap harmless anyway).
    ``tombstones`` carries the version of every deregistered node that has not
    yet been garbage-collected, so late events stay rejectable.
    """

    revision: int
    nodes: dict[str, Node]
    tombstones: dict[str, int]
    compacted_through: int = 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "revision": self.revision,
            "nodes": {name: node.to_dict() for name, node in sorted(self.nodes.items())},
            "tombstones": dict(sorted(self.tombstones.items())),
            "compacted_through": self.compacted_through,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "Snapshot":
        return cls(
            revision=int(data["revision"]),
            nodes={name: Node.from_dict(nd) for name, nd in data["nodes"].items()},
            tombstones={name: int(v) for name, v in data.get("tombstones", {}).items()},
            compacted_through=int(data.get("compacted_through", 0)),
        )


@dataclass(frozen=True)
class Compacted:
    """In-band marker telling a subscriber that history <= ``revision`` is gone."""

    compacted_through: int
    current_revision: int


class StaleWriteError(Exception):
    """Raised when an optimistic-concurrency (expected_version) check fails."""

    def __init__(self, name: str, expected: Optional[int], actual: Optional[int]):
        self.name = name
        self.expected = expected
        self.actual = actual
        super().__init__(
            f"stale write on {name!r}: expected version {expected}, actual {actual}"
        )


def with_version(node: Node, version: int) -> Node:
    return replace(node, version=version, updated_at=time.time())
