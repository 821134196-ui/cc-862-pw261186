"""Tests for the revisioned store: ordering, fences, compaction, tombstones."""
import unittest

from discovery_sim import (
    EventType,
    HistoryCompacted,
    RevisionStore,
    StaleWriteError,
)


class TestRevisionStore(unittest.TestCase):
    def setUp(self) -> None:
        self.store = RevisionStore()

    def test_revisions_are_gapless_and_global(self) -> None:
        revs = []
        revs.append(self.store.register("a", "10.0.0.1").revision)
        revs.append(self.store.register("b", "10.0.0.2").revision)
        revs.append(self.store.update_metadata("a", {"v": "2"}).revision)
        revs.append(self.store.deregister("b").revision)
        self.assertEqual(revs, [1, 2, 3, 4])
        self.assertEqual(self.store.revision, 4)

    def test_register_update_deregister_lifecycle(self) -> None:
        self.store.register("a", "10.0.0.1")
        self.assertEqual(self.store.node("a").address, "10.0.0.1")
        self.store.deregister("a")
        self.assertIsNone(self.store.node("a"))
        self.assertEqual(self.store.tombstone_version("a"), 2)
        # Re-register after death is a fresh REGISTER at a newer version.
        ev = self.store.register("a", "10.0.0.9")
        self.assertEqual(ev.type, EventType.REGISTER)
        self.assertNotIn("a", self.store.snapshot().tombstones)

    def test_optimistic_concurrency_fence(self) -> None:
        self.store.register("a", "10.0.0.1")
        v1 = self.store.node("a").version
        # Holder of the current version may update.
        self.store.update_metadata("a", {"k": "v"}, expected_version=v1)
        # A stale holder is rejected.
        with self.assertRaises(StaleWriteError):
            self.store.update_metadata("a", {"k": "stale"},
                                       expected_version=v1)
        self.assertEqual(self.store.rejected_writes, 1)
        with self.assertRaises(StaleWriteError):
            self.store.deregister("a", expected_version=v1)

    def test_compaction_raises_watermark_and_keeps_tail(self) -> None:
        for i in range(20):
            self.store.register(f"n{i}", f"10.0.0.{i}")
        watermark = self.store.compact(retain_events=5)
        self.assertEqual(watermark, 15)
        self.assertEqual(self.store.compacted_through, 15)
        self.assertEqual(self.store.log_len, 5)
        with self.assertRaises(HistoryCompacted) as ctx:
            self.store.events_since(10)
        self.assertEqual(ctx.exception.watermark, 15)
        # Tail replay still works for clients past the watermark.
        tail = self.store.events_since(15)
        self.assertEqual([e.revision for e in tail], [16, 17, 18, 19, 20])

    def test_snapshot_survives_compaction(self) -> None:
        for i in range(12):
            self.store.register(f"n{i}", "addr")
        self.store.deregister("n3")
        self.store.compact(retain_events=2)
        snap = self.store.snapshot()
        self.assertEqual(snap.revision, self.store.revision)
        self.assertEqual(len(snap.nodes), 11)
        self.assertEqual(snap.tombstones.get("n3"), 13)

    def test_tombstone_garbage_collection(self) -> None:
        store = RevisionStore(tombstone_grace=5)
        store.register("a", "1")
        store.deregister("a")  # tombstone at rev 2
        for i in range(20):
            store.register(f"n{i}", "x")
        store.compact(retain_events=2)
        # Death at rev 2 is far behind the watermark minus grace -> GC'd.
        self.assertNotIn("a", store.snapshot().tombstones)
        self.assertGreaterEqual(store.gc_runs, 1)


if __name__ == "__main__":
    unittest.main()
