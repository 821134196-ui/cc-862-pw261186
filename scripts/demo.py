"""End-to-end scenario walkthrough for the service-discovery simulator.

Run:  python3 -m scripts.demo

Each section maps to one requirement in the task brief and prints what to
look for. The same mechanics are covered assertively in tests/.
"""
from __future__ import annotations

import asyncio
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from discovery_sim import (  # noqa: E402
    ClientState,
    DiscoveryClient,
    DiscoveryServer,
    Event,
    EventType,
    Node,
)

BOLD = "\033[1m"
DIM = "\033[2m"
GREEN = "\033[32m"
RED = "\033[31m"
YELLOW = "\033[33m"
CYAN = "\033[36m"
RESET = "\033[0m"


def banner(title: str) -> None:
    print(f"\n{BOLD}{CYAN}{'=' * 72}\n {title}\n{'=' * 72}{RESET}")


def ok(line: str) -> None:
    print(f"  {GREEN}✓{RESET} {line}")


def warn(line: str) -> None:
    print(f"  {YELLOW}!{RESET} {line}")


def info(line: str) -> None:
    print(f"  {DIM}·{RESET} {line}")


async def settle(cycles: int = 2, delay: float = 0.02) -> None:
    for _ in range(cycles):
        await asyncio.sleep(delay)


async def section_1_basic_stream() -> None:
    banner("1. 版本号续订：客户端从 revision 增量维护本地视图")
    server = DiscoveryServer()
    client = DiscoveryClient("alpha", server)
    await client.start()
    await settle()
    for i in range(4):
        server.register(f"svc-{i}", f"10.0.0.{i}", {"zone": "az-1"})
    await settle(3)
    ok(f"客户端按 revision 顺序应用 {client.stats.events_applied} 个事件，"
       f"本地视图 {len(client.view)} 个节点，head={client.last_revision}")
    server.update_metadata("svc-1", {"zone": "az-2"})
    await settle(2)
    ok(f"更新后 svc-1 版本 v{client.view['svc-1'].version}，"
       f"元数据 {client.view['svc-1'].metadata}")
    return server, client


async def section_2_compaction(server: DiscoveryServer,
                               client: DiscoveryClient) -> None:
    banner("2. 历史压缩：水位线以下的续订被拒绝，快照与增量无缝衔接")
    for i in range(4, 14):
        server.register(f"svc-{i}", f"10.0.0.{i}")
    await settle(3)
    resume_point = client.last_revision
    await client.disconnect()
    info(f"客户端在 rev {resume_point} 断线，服务端继续写入…")
    for i in range(14, 22):
        server.register(f"svc-{i}", f"10.0.0.{i}")
    watermark = server.compact(retain_events=4)
    info(f"压缩完成：水位线 compacted_through={watermark}，"
         f"保留日志 {server.store.log_len} 条")
    assert resume_point < watermark, "演示前提：续订点已落入被压缩区间"
    await client.reconnect()
    await settle(8)
    ok(f"重连后自动改取快照（共 {client.stats.snapshots_applied} 次），"
       f"当前 revision={client.last_revision}，与服务端一致")
    ok(f"无缝衔接校验：{ '收敛，无缺失无幻影' if client.is_converged() else client.divergence()}")


async def section_3_stale_events(server: DiscoveryServer,
                                 client: DiscoveryClient) -> None:
    banner("3. 注销后延迟抵达的旧事件不能复活节点（双版本栅栏）")
    reg = server.register("ghost", "10.0.0.66")
    await settle(2)
    death = server.deregister("ghost")
    await settle(2)
    ok(f"ghost 已注销（tombstone v{death.node.version}），"
       f"客户端视图包含 ghost? {'ghost' in client.view}")

    # Late old REGISTER, as if retransmitted / gossiped after the deregister.
    accepted = client.inject_external_event(reg)
    ok(f"迟到的旧 REGISTER（v{reg.node.version}）被接受? {accepted} "
       f"-> 节点复活? {'ghost' in client.view}")

    # Hostile case: fresh-looking global revision but stale per-node version.
    forged = Event(revision=9999, type=EventType.UPDATE,
                   node=Node(name="ghost", address="10.0.0.99",
                             version=reg.node.version, metadata={}))
    accepted = client.inject_external_event(forged)
    ok(f"伪造高 revision 但节点版本陈旧的 UPDATE 被接受? {accepted} "
       f"-> 节点复活? {'ghost' in client.view}")
    ok(f"累计拦截陈旧事件 {client.stats.stale_events_rejected} 个")

    rebirth = server.register("ghost", "10.0.0.77")
    await settle(2)
    ok(f"合法的重新注册（v{rebirth.node.version}）正常生效，"
       f"地址={client.view['ghost'].address}")


async def section_4_pause_and_catchup() -> None:
    banner("4. 多客户端视图：暂停一个客户端，观察它如何追上")
    server = DiscoveryServer(queue_max=8, reset_cooldown=0.1)
    fast = DiscoveryClient("fast", server)
    slow = DiscoveryClient("slow", server)
    await fast.start()
    await slow.start()
    await settle()
    slow.pause()
    info("slow 已暂停（停止消费），fast 保持在线；持续制造变更…")
    for i in range(24):
        server.register(f"node-{i}", f"172.16.0.{i}")
        await asyncio.sleep(0.005)
    sub = server.broker.get("slow")
    warn(f"slow 服务端缓冲：{sub.queued}/{sub.queue_max}，"
         f"溢出快照重置 {sub.metrics.snapshot_resets} 次，"
         f"丢弃事件 {sub.metrics.events_dropped} 个（内存始终有界）")
    ok(f"fast 全程未被拖慢：lag={fast.lag}，已收敛={fast.is_converged()}")
    slow.resume()
    await settle(10)
    ok(f"slow 恢复后通过快照+尾部增量追平：收敛={slow.is_converged()}，"
       f"revision={slow.last_revision}/{server.store.revision}")
    await fast.stop()
    await slow.stop()


async def section_5_backpressure_policy() -> None:
    banner("5. 慢订阅端策略：积压增量 vs 强制快照（连接与内存预算）")
    server = DiscoveryServer(queue_max=6, reset_cooldown=0.1,
                             global_memory_items=30)
    clients = [DiscoveryClient(f"sub-{i}", server) for i in range(3)]
    fast = DiscoveryClient("fast", server)
    for c in clients + [fast]:
        await c.start()
    await settle()
    for c in clients:
        c.pause()
    info("三个慢订阅端同时暂停，制造 30 个变更…")
    for i in range(30):
        server.register(f"x-{i}", "addr")
    await settle(4)
    total = server.broker.total_queued_items()
    resets = sum(s.metrics.snapshot_resets + s.metrics.budget_resets
                 for s in server.broker.subscriptions)
    ok(f"全局缓冲 {total} 项（每队列上限 6，全局预算 30），"
       f"共触发 {resets} 次强制快照，无界增长从未发生")
    ok(f"正常端 fast 的 lag={fast.lag}（慢端追赶完全没有阻塞它）")
    for c in clients:
        c.resume()
    await settle(12)
    converged = all(c.is_converged() for c in clients)
    ok(f"三个慢端同时追赶后全部收敛: {converged}")
    for c in clients + [fast]:
        await c.stop()


async def section_6_connection_budget() -> None:
    banner("6. 连接预算：超过订阅端上限时明确拒绝")
    from discovery_sim.server import TooManySubscribers
    server = DiscoveryServer(max_subscriptions=2)
    c1 = DiscoveryClient("c1", server)
    c2 = DiscoveryClient("c2", server)
    c3 = DiscoveryClient("c3", server)
    await c1.start()
    await c2.start()
    await c3.start()
    await settle(3)
    try:
        server.subscribe("intruder")
        raise AssertionError("应当拒绝")
    except TooManySubscribers as exc:
        ok(f"超额订阅被拒绝：{exc}")
    await c1.stop()
    await c2.stop()
    await c3.stop()


async def main() -> None:
    print(f"{BOLD}服务发现模拟器 — 场景演示{RESET}")
    server, client = await section_1_basic_stream()
    await section_2_compaction(server, client)
    await section_3_stale_events(server, client)
    await client.stop()
    await section_4_pause_and_catchup()
    await section_5_backpressure_policy()
    await section_6_connection_budget()
    banner(f"{GREEN}全部场景完成{RESET}")
    print(f"{DIM}自动化验证见: python3 -m unittest discover -s tests{RESET}\n")


if __name__ == "__main__":
    asyncio.run(main())
