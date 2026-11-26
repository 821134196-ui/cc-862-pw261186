"""DiscoveryServer: facade combining the revision store and pub/sub broker."""
from __future__ import annotations

from typing import Optional

from .broker import Broker, Subscription, TooManySubscribers
from .model import Compacted, Event, Snapshot
from .store import HistoryCompacted, RevisionStore, StaleWriteError


class DiscoveryServer:
    def __init__(self, *, queue_max: int = 200,
                 global_memory_items: int = 10_000,
                 max_subscriptions: int = 100,
                 tombstone_grace: int = 50,
                 reset_cooldown: float = 1.0,
                 replay_budget_items: int = 500) -> None:
        self.store = RevisionStore(tombstone_grace=tombstone_grace)
        self.broker = Broker(
            self.store,
            global_memory_items=global_memory_items,
            max_subscriptions=max_subscriptions,
        )
        self.queue_max = queue_max
        self.reset_cooldown = reset_cooldown
        self.replay_budget_items = replay_budget_items

    # ------------------------------------------------------------------ writes
    def register(self, name: str, address: str,
                 metadata: Optional[dict[str, str]] = None,
                 expected_version: Optional[int] = None) -> Event:
        event = self.store.register(name, address, metadata, expected_version)
        self.broker.publish(event)
        return event

    def update_metadata(self, name: str, metadata: dict[str, str],
                        expected_version: Optional[int] = None) -> Event:
        event = self.store.update_metadata(name, metadata, expected_version)
        self.broker.publish(event)
        return event

    def deregister(self, name: str,
                   expected_version: Optional[int] = None) -> Event:
        event = self.store.deregister(name, expected_version)
        self.broker.publish(event)
        return event

    # -------------------------------------------------------------- compaction
    def compact(self, retain_events: int = 10) -> int:
        watermark = self.store.compact(retain_events=retain_events)
        self.broker.publish_compaction(
            Compacted(compacted_through=watermark,
                      current_revision=self.store.revision))
        return watermark

    # ------------------------------------------------------------------ reads
    def snapshot(self) -> Snapshot:
        return self.store.snapshot()

    def events_since(self, revision: int):
        return self.store.events_since(revision)

    # ----------------------------------------------------------- subscriptions
    def subscribe(self, client_name: str,
                  start_revision: Optional[int] = None) -> Subscription:
        return self.broker.subscribe(
            client_name,
            start_revision=start_revision,
            queue_max=self.queue_max,
            reset_cooldown=self.reset_cooldown,
            replay_budget_items=self.replay_budget_items,
        )

    def unsubscribe(self, client_name: str) -> None:
        self.broker.unsubscribe(client_name)


__all__ = [
    "DiscoveryServer",
    "HistoryCompacted",
    "StaleWriteError",
    "TooManySubscribers",
]
