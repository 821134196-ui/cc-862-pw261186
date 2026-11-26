"""Regression tests for lifecycle semantics found via the live dashboard."""
import asyncio
import unittest

from discovery_sim import DiscoveryClient, DiscoveryServer


def async_test(coro):
    def wrapper(self):
        asyncio.run(coro(self))
    return wrapper


async def pump(cycles: int = 1, delay: float = 0.02) -> None:
    for _ in range(cycles):
        await asyncio.sleep(delay)


class TestClientLifecycle(unittest.TestCase):
    @async_test
    async def test_pause_before_start_does_not_get_overridden(self):
        # Regression: _run() used to unconditionally open the pause gate.
        server = DiscoveryServer(queue_max=6)
        client = DiscoveryClient("c", server)
        client.pause()          # paused before the loop boots
        await client.start()
        await pump()
        for i in range(12):
            server.register(f"n{i}", "a")
        await pump(3)
        sub = server.broker.get("c")
        self.assertEqual(client.state.value, "PAUSED")
        self.assertGreater(sub.queued, 0, "paused client must retain a backlog")
        self.assertLessEqual(client.stats.events_applied, 1)  # initial snapshot only
        client.resume()
        await pump(8)
        self.assertTrue(client.is_converged(), client.divergence())
        await client.stop()

    @async_test
    async def test_repeated_pause_resume_cycles_converge_each_time(self):
        server = DiscoveryServer(queue_max=8, reset_cooldown=0.05)
        client = DiscoveryClient("c", server)
        await client.start()
        await pump()
        for cycle in range(3):
            client.pause()
            for i in range(10):
                server.register(f"c{cycle}-n{i}", "a")
            await pump(2)
            self.assertFalse(client.is_converged())
            client.resume()
            await pump(8)
            self.assertTrue(client.is_converged(),
                            f"cycle {cycle}: {client.divergence()}")
        await client.stop()

    @async_test
    async def test_disconnect_does_not_busy_loop_and_reconnect_resubscribes(self):
        # Regression: an inverted offline event made _run spin, starving the
        # event loop so the reconnect coroutine never ran.
        server = DiscoveryServer()
        client = DiscoveryClient("c", server)
        await client.start()
        await pump()
        server.register("n0", "a")
        await pump(2)
        await client.disconnect()
        # While offline, other tasks must still make progress promptly.
        for i in range(1, 6):
            server.register(f"n{i}", "a")
        await pump(2)  # would time out / starve under the old busy loop
        self.assertEqual(client.state.value, "DISCONNECTED")
        self.assertIsNone(server.broker.get("c"))
        await client.reconnect()
        await pump(6)
        self.assertEqual(client.state.value, "STREAMING")
        self.assertIsNotNone(server.broker.get("c"))
        self.assertTrue(client.is_converged(), client.divergence())
        await client.stop()

    @async_test
    async def test_disconnect_while_paused_then_reconnect_and_resume(self):
        server = DiscoveryServer(queue_max=6, reset_cooldown=0.05)
        client = DiscoveryClient("c", server)
        await client.start()
        await pump()
        client.pause()
        await client.disconnect()
        for i in range(10):
            server.register(f"n{i}", "a")
        await pump(2)
        await client.reconnect()
        await pump(2)
        client.resume()
        await pump(8)
        self.assertTrue(client.is_converged(), client.divergence())
        await client.stop()

    @async_test
    async def test_stop_while_disconnected_terminates_cleanly(self):
        server = DiscoveryServer()
        client = DiscoveryClient("c", server)
        await client.start()
        await pump()
        await client.disconnect()
        await pump()
        # Must not hang even though the loop is parked on the offline gate.
        await asyncio.wait_for(client.stop(), timeout=2.0)


if __name__ == "__main__":
    unittest.main()
