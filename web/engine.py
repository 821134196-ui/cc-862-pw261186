"""Background engine for the web dashboard.

Runs the asyncio simulator in a dedicated thread; the HTTP layer drives it
through ``run_on_loop`` (thread-safe coroutine submission) and reads state
through plain synchronous getters.
"""
from __future__ import annotations

import asyncio
import itertools
import threading
import time
from typing import Optional

from discovery_sim import (
    ClientState,
    DiscoveryClient,
    DiscoveryServer,
    EventType,
)


class DashboardEngine:
    def __init__(self) -> None:
        self.loop = asyncio.new_event_loop()
        self.thread = threading.Thread(target=self._run_loop, daemon=True,
                                       name="sim-engine")
        self._ready = threading.Event()
        self.server: Optional[DiscoveryServer] = None
        self.clients: dict[str, DiscoveryClient] = {}
        self.churn_enabled = False
        self._churn_task: Optional[asyncio.Task] = None
        self._churn_counter = itertools.count()
        self._lock = threading.Lock()

    # ------------------------------------------------------------ lifecycle
    def start(self) -> None:
        self.thread.start()
        self._ready.wait()
        fut = asyncio.run_coroutine_threadsafe(self._boot(), self.loop)
        fut.result(timeout=5)

    def _run_loop(self) -> None:
        asyncio.set_event_loop(self.loop)
        self._ready.set()
        try:
            self.loop.run_forever()
        finally:
            # Drain cancels, then close so sockets/selectors are released.
            pending = asyncio.all_tasks(self.loop)
            for task in pending:
                task.cancel()
            if pending:
                self.loop.run_until_complete(
                    asyncio.gather(*pending, return_exceptions=True))
            self.loop.close()

    def run_on_loop(self, coro, timeout: float = 5.0):
        return asyncio.run_coroutine_threadsafe(coro, self.loop).result(timeout)

    async def _boot(self) -> None:
        self.server = DiscoveryServer(
            queue_max=12, reset_cooldown=1.0,
            global_memory_items=60, max_subscriptions=8,
        )
        # Seed a few nodes.
        for i in range(4):
            self.server.register(f"svc-{i}", f"10.0.0.{i}",
                                 {"zone": f"az-{i % 2 + 1}"})
        # Three clients: one fast, one slow-but-running, one initially paused.
        specs = [
            ("fast", 0.0, False),
            ("slow", 0.15, False),
            ("paused", 0.0, True),
        ]
        for name, delay, paused in specs:
            client = DiscoveryClient(name, self.server, process_delay=delay)
            await client.start()
            self.clients[name] = client
            if paused:
                client.pause()
        await asyncio.sleep(0.05)

    def shutdown(self) -> None:
        if self.server is None:
            return
        async def _stop():
            self.churn_enabled = False
            if self._churn_task is not None:
                self._churn_task.cancel()
                try:
                    await self._churn_task
                except (asyncio.CancelledError, Exception):
                    pass
            for client in self.clients.values():
                await client.stop()
            self.loop.call_soon_threadsafe(self.loop.stop)
        try:
            self.run_on_loop(_stop(), timeout=2)
        except Exception:
            pass

    # ------------------------------------------------------------- mutations
    def register(self, name: str, address: str, zone: str = "az-1") -> None:
        with self._lock:
            self.server.register(name, address, {"zone": zone})

    def deregister(self, name: str) -> None:
        with self._lock:
            self.server.deregister(name)

    def compact(self, retain: int = 6) -> int:
        with self._lock:
            return self.server.compact(retain_events=retain)

    def pause_client(self, name: str) -> None:
        self.clients[name].pause()

    def resume_client(self, name: str) -> None:
        async def _resume():
            self.clients[name].resume()
        self.run_on_loop(_resume())

    async def _disconnect(self, name: str) -> None:
        await self.clients[name].disconnect()

    def disconnect_client(self, name: str) -> None:
        self.run_on_loop(self._disconnect(name))

    async def _reconnect(self, name: str) -> None:
        await self.clients[name].reconnect()

    def reconnect_client(self, name: str) -> None:
        self.run_on_loop(self._reconnect(name))

    def set_delay(self, name: str, delay: float) -> None:
        self.clients[name].process_delay = max(0.0, delay)

    # --------------------------------------------------------------- churner
    async def _churn(self) -> None:
        """Background workload: register / update / deregister on a timer."""
        names = [f"dyn-{i}" for i in range(6)]
        idx = 0
        while self.churn_enabled:
            i = idx % len(names)
            idx += 1
            n = next(self._churn_counter)
            name = names[i]
            with self._lock:
                existing = self.server.store.node(name)
                if existing is None:
                    self.server.register(name, f"172.16.{i}.{n % 250}",
                                         {"gen": str(n)})
                elif n % 3 == 0:
                    self.server.deregister(name)
                else:
                    self.server.update_metadata(name, {"gen": str(n)})
            await asyncio.sleep(0.35)

    def set_churn(self, enabled: bool) -> bool:
        self.churn_enabled = enabled
        if enabled:
            # create_task is not thread-safe across the loop boundary; the HTTP
            # handler runs on another thread, so hop onto the engine loop.
            asyncio.run_coroutine_threadsafe(self._start_churn(), self.loop)
        elif self._churn_task is not None:
            task = self._churn_task
            self._churn_task = None
            self.loop.call_soon_threadsafe(task.cancel)
        return enabled

    async def _start_churn(self) -> None:
        if self._churn_task is None or self._churn_task.done():
            self._churn_task = asyncio.create_task(self._churn_guarded())

    async def _churn_guarded(self) -> None:
        try:
            await self._churn()
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # never let the workload die silently
            self.server.broker._log(f"churn task crashed: {exc!r}")
            self.churn_enabled = False

    # --------------------------------------------------------------- snapshot
    def state(self) -> dict:
        srv = self.server
        store = srv.store
        node_rows = []
        snap = store.snapshot()
        for name in sorted(snap.nodes):
            node = snap.nodes[name]
            node_rows.append({
                "name": name,
                "address": node.address,
                "version": node.version,
                "zone": node.metadata.get("zone", ""),
                "age": round(time.time() - node.updated_at, 1),
            })
        clients = []
        for name, client in self.clients.items():
            sub = srv.broker.get(name)
            clients.append({
                "name": name,
                "state": client.state.value,
                "last_revision": client.last_revision,
                "lag": client.lag,
                "converged": client.is_converged(),
                "divergence": client.divergence(),
                "view_size": len(client.view),
                "process_delay": client.process_delay,
                "queued": sub.queued if sub else 0,
                "queue_max": sub.queue_max if sub else 0,
                "queued_bytes": sub.queued_bytes if sub else 0,
                "sub_status": sub.status.value if sub else "NONE",
                "pending_reset": sub.pending_reset if sub else False,
                "snapshots": client.stats.snapshots_applied,
                "events_applied": client.stats.events_applied,
                "stale_rejected": client.stats.stale_events_rejected,
                "resets": (sub.metrics.snapshot_resets
                           + sub.metrics.budget_resets) if sub else 0,
                "drops": sub.metrics.events_dropped if sub else 0,
                "reconnects": client.stats.reconnects,
                "max_latency_ms": round(client.stats.max_latency_ms, 1),
            })
        return {
            "server": {
                "head_revision": store.revision,
                "compacted_through": store.compacted_through,
                "retained_events": store.log_len,
                "live_nodes": len(store._nodes),
                "tombstones": len(store._tombstones),
                "rejected_writes": store.rejected_writes,
                "compactions": store.compactions,
                "total_queued_items": srv.broker.total_queued_items(),
                "total_queued_bytes": srv.broker.total_queued_bytes(),
                "memory_budget_items": srv.broker.global_memory_items,
                "subscription_cap": srv.broker.max_subscriptions,
                "subscriptions": len(srv.broker.subscriptions),
            },
            "nodes": node_rows,
            "tombstones": [
                {"name": n, "version": v}
                for n, v in sorted(snap.tombstones.items())
            ],
            "clients": clients,
            "audit": list(srv.broker.audit[-40:]),
            "churn_enabled": self.churn_enabled,
            "event_types": [t.value for t in EventType],
        }
