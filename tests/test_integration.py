"""End-to-end async tests: clients, subscriptions, compaction, backpressure."""
import asyncio
import unittest

from discovery_sim import (
    ClientState,
    DiscoveryClient,
    DiscoveryServer,
    EventType,
    HistoryCompacted,
    Node,
    Event,
)


def async_test(coro):
    def wrapper(self):
        asyncio.run(coro(self))
    return wrapper


async def pump(cycles: int = 1, delay: float = 0.01) -> None:
    """Let producer/consumer tasks run for a few event-loop turns."""
    for _ in range(cycles):
        await asyncio.sleep(delay)


class TestStreamingBasics(unittest.TestCase):
    @async_test
    async def test_client_converges_on_live_stream(self):
        server = DiscoveryServer()
        client = DiscoveryClient("c1", server)
        await client.start()
        await pump()
        for i in range(5):
            server.register(f"svc-{i}", f"10.0.0.{i}")
        await pump(3)
        self.assertTrue(client.is_converged(), client.divergence())
        self.assertEqual(client.stats.events_applied, 5)
        await client.stop()

    @async_test
    async def test_new_subscriber_gets_snapshot_then_deltas(self):
        server = DiscoveryServer()
        for i in range(3):
            server.register(f"svc-{i}", "addr")
        client = DiscoveryClient("late", server)
        await client.start()
        await pump(2)
        self.assertEqual(client.stats.snapshots_applied, 1)
        self.assertEqual(client.last_revision, 3)
        server.register("svc-3", "addr3")
        await pump(2)
        self.assertTrue(client.is_converged())
        self.assertEqual(client.stats.events_applied, 1)
        await client.stop()

    @async_test
    async def test_snapshot_deltas_join_without_gap_or_overlap(self):
        server = DiscoveryServer()
        client = DiscoveryClient("c", server)
        await client.start()
        await pump()
        for i in range(6):
            server.register(f"n{i}", "a")
        await pump(2)
        # Force a server-side reset (simulates queue overflow path).
        sub = server.broker.get("c")
        sub.force_snapshot("test reset")
        for i in range(6, 9):
            server.register(f"n{i}", "a")
        await pump(3)
        self.assertTrue(client.is_converged(), client.divergence())
        self.assertEqual(client.last_revision, server.store.revision)
        await client.stop()


class TestCompactionAndResume(unittest.TestCase):
    @async_test
    async def test_resume_below_watermark_takes_snapshot_and_catches_up(self):
        server = DiscoveryServer(replay_budget_items=1000)
        client = DiscoveryClient("c", server)
        await client.start()
        await pump()
        for i in range(10):
            server.register(f"n{i}", "a")
        await pump(3)
        applied = client.last_revision
        self.assertEqual(applied, 10)

        # Simulate an outage: unsubscribe while history keeps growing.
        await client.disconnect()
        for i in range(10, 25):
            server.register(f"n{i}", "a")
        server.compact(retain_events=2)  # watermark 23; resume point 10 is gone
        self.assertLess(applied, server.store.compacted_through)

        # Reconnect: HistoryCompacted path -> snapshot -> converge.
        client.resume()
        await client.reconnect()
        await pump(6)
        self.assertTrue(client.is_converged(), client.divergence())
        self.assertGreaterEqual(client.stats.snapshots_applied, 1)
        self.assertEqual(client.last_revision, 25)
        await client.stop()

    @async_test
    async def test_resume_within_retained_history_backfills_deltas(self):
        server = DiscoveryServer(replay_budget_items=1000)
        client = DiscoveryClient("c", server)
        await client.start()
        await pump()
        for i in range(5):
            server.register(f"n{i}", "a")
        await pump(2)
        applied = client.last_revision
        await client.disconnect()
        for i in range(5, 8):
            server.register(f"n{i}", "a")
        server.compact(retain_events=3)
        snaps_before = client.stats.snapshots_applied
        await client.reconnect()
        await pump(5)
        self.assertTrue(client.is_converged(), client.divergence())
        self.assertEqual(client.last_revision, 8)
        # No NEW snapshot needed: pure delta backfill from rev 5.
        self.assertEqual(client.stats.snapshots_applied, snaps_before)
        await client.stop()

    @async_test
    async def test_oversized_backlog_is_served_by_snapshot(self):
        server = DiscoveryServer(replay_budget_items=3)
        client = DiscoveryClient("c", server)
        await client.start()
        await pump()
        server.register("n0", "a")
        await pump()
        await client.disconnect()
        for i in range(1, 8):
            server.register(f"n{i}", "a")
        await client.reconnect()
        await pump(4)
        self.assertTrue(client.is_converged())
        self.assertGreaterEqual(client.stats.snapshots_applied, 1)
        await client.stop()


class TestStaleEvents(unittest.TestCase):
    @async_test
    async def test_late_old_event_cannot_resurrect_deregistered_node(self):
        server = DiscoveryServer()
        client = DiscoveryClient("c", server)
        await client.start()
        await pump()
        reg = server.register("ghost", "10.0.0.1")
        await pump(2)
        death = server.deregister("ghost")
        await pump(2)
        self.assertNotIn("ghost", client.view)

        # The old REGISTER arrives very late (retransmit / gossip duplicate).
        accepted = client.inject_external_event(reg)
        self.assertFalse(accepted)
        self.assertNotIn("ghost", client.view)
        self.assertEqual(client.stats.stale_events_rejected, 1)

        # Even an UPDATE fabricated with the dead node's old version is fenced.
        stale = Event(
            revision=999,  # global fence alone would accept this!
            type=EventType.UPDATE,
            node=Node(name="ghost", address="evil", version=reg.node.version),
        )
        accepted = client.inject_external_event(stale)
        self.assertFalse(accepted)
        self.assertNotIn("ghost", client.view)

        # A genuinely new registration after the tombstone does succeed.
        rebirth = server.register("ghost", "10.0.0.2")
        await pump(2)
        self.assertIn("ghost", client.view)
        self.assertEqual(client.view["ghost"].version, rebirth.node.version)
        await client.stop()

    @async_test
    async def test_out_of_order_delivery_converges(self):
        server = DiscoveryServer()
        client = DiscoveryClient("c", server)
        await client.start()
        await pump()
        e1 = server.register("n", "a1")
        e2 = server.update_metadata("n", {"k": "v2"})
        e3 = server.update_metadata("n", {"k": "v3"})
        await pump(2)
        # Replay older events out of order after the newest is already applied.
        self.assertFalse(client.inject_external_event(e1))
        self.assertFalse(client.inject_external_event(e2))
        self.assertTrue(client.view["n"].version == e3.node.version)
        await client.stop()


class TestSlowSubscriberBackpressure(unittest.TestCase):
    @async_test
    async def test_paused_client_overflows_then_resets_with_snapshot(self):
        server = DiscoveryServer(queue_max=8, reset_cooldown=0.05,
                                 global_memory_items=10_000)
        slow = DiscoveryClient("slow", server, process_delay=0.0)
        await slow.start()
        await pump()
        slow.pause()
        for i in range(30):
            server.register(f"n{i}", "a")
        await pump(3)
        sub = server.broker.get("slow")
        # Buffer stayed bounded; a snapshot reset happened.
        self.assertLessEqual(sub.queued, sub.queue_max)
        self.assertGreaterEqual(slow.stats.snapshots_applied
                                + sub.metrics.snapshot_resets
                                + sub.metrics.budget_resets, 1)
        # Resume -> drains the snapshot + tail and converges.
        slow.resume()
        await pump(6)
        self.assertTrue(slow.is_converged(), slow.divergence())
        await slow.stop()

    @async_test
    async def test_slow_subscriber_does_not_block_fast_subscriber(self):
        server = DiscoveryServer(queue_max=6, reset_cooldown=0.05,
                                 global_memory_items=10_000)
        fast = DiscoveryClient("fast", server)
        slow = DiscoveryClient("slow", server)
        await fast.start()
        await slow.start()
        await pump()
        slow.pause()

        fast_lags = []
        for i in range(40):
            server.register(f"n{i}", "a")
            await pump(1, delay=0.005)
            fast_lags.append(fast.lag)
        # The fast client keeps up: its lag never accumulates meaningfully.
        self.assertLessEqual(max(fast_lags), 2, f"fast lagged: {fast_lags}")
        self.assertTrue(fast.is_converged())

        slow_sub = server.broker.get("slow")
        self.assertGreaterEqual(slow_sub.metrics.snapshot_resets
                                + slow_sub.metrics.budget_resets, 1)
        slow.resume()
        await pump(6)
        self.assertTrue(slow.is_converged(), slow.divergence())
        await fast.stop()
        await slow.stop()

    @async_test
    async def test_multiple_slow_subscribers_catch_up_together(self):
        server = DiscoveryServer(queue_max=6, reset_cooldown=0.05)
        clients = [DiscoveryClient(f"slow-{i}", server) for i in range(3)]
        fast = DiscoveryClient("fast", server)
        for c in clients + [fast]:
            await c.start()
        await pump()
        for c in clients:
            c.pause()
        for i in range(36):
            server.register(f"n{i}", "a")
        await pump(4)
        # Fast client unaffected while three slow ones churn through resets.
        self.assertTrue(fast.is_converged())
        for c in clients:
            c.resume()
        await pump(8)
        for c in clients:
            self.assertTrue(c.is_converged(), f"{c.name}: {c.divergence()}")
        await fast.stop()
        for c in clients:
            await c.stop()

    @async_test
    async def test_global_memory_budget_force_resets_greediest(self):
        # Two small per-queue limits but a tiny global budget.
        server = DiscoveryServer(queue_max=50, reset_cooldown=10.0,
                                 global_memory_items=12)
        a = DiscoveryClient("a", server)
        b = DiscoveryClient("b", server)
        await a.start()
        await b.start()
        await pump()
        a.pause()
        b.pause()
        for i in range(20):
            server.register(f"n{i}", "a")
        await pump(3)
        self.assertLessEqual(server.broker.total_queued_items(), 12)
        total_resets = sum(
            s.metrics.budget_resets for s in server.broker.subscriptions)
        self.assertGreaterEqual(total_resets, 1)
        a.resume()
        b.resume()
        await pump(6)
        self.assertTrue(a.is_converged(), a.divergence())
        self.assertTrue(b.is_converged(), b.divergence())
        await a.stop()
        await b.stop()

    @async_test
    async def test_connection_budget_rejects_excess_subscribers(self):
        from discovery_sim.server import TooManySubscribers
        server = DiscoveryServer(max_subscriptions=2)
        c1 = DiscoveryClient("c1", server)
        c2 = DiscoveryClient("c2", server)
        c3 = DiscoveryClient("c3", server)
        await c1.start()
        await c2.start()
        await c3.start()
        await pump(3)
        # The third client's loop keeps retrying; direct subscribe call shows
        # the budget error explicitly.
        with self.assertRaises(TooManySubscribers):
            server.subscribe("one-too-many")
        await c1.stop()
        await c2.stop()
        await c3.stop()


if __name__ == "__main__":
    unittest.main()
