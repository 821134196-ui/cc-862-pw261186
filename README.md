# 服务发现模拟器（Service Discovery Simulator）

从零实现的、仅依赖 Python 3.11 标准库的服务发现模拟：revision 日志、快照/增量
续订、历史压缩、墓碑防复活、有界订阅缓冲与慢客户端背压，外加一个多客户端实时
监控面板。

## 它解决的问题与对应机制

| 需求 | 实现机制 | 验证 |
| --- | --- | --- |
| 客户端从版本号续订节点变更 | 全局 gapless revision 日志，客户端记录 `last_revision`，重连按 revision 续订 | `test_streaming_basics`、`test_compaction_and_resume` |
| 历史压缩后重新拿快照并与增量无缝衔接 | 压缩水位线 `compacted_through`；低于水位线的续订抛 `HistoryCompacted` → 改取快照；快照在 `revision+1` 处与增量流交接，双版本栅栏保证重叠也无害 | `test_resume_below_watermark_*`、`test_snapshot_deltas_join_*` |
| 注销后延迟旧事件不能复活节点 | 双栅栏：全局 revision 栅栏 + **每节点 version 栅栏**；注销留墓碑（tombstone），任何更旧版本的事件（哪怕伪造了更高 revision）一律拒绝 | `test_late_old_event_cannot_resurrect_*`、`test_out_of_order_*` |
| 多客户端视图，暂停后观察追赶 | 客户端 `pause()` 停止从队列取数据，服务端缓冲真实增长；面板展示 lag/缓冲/快照重置；`resume()` 后快照+尾部增量追平 | `test_lifecycle.py`、`test_paused_client_*` |
| 慢订阅端：积压还是强制快照 | 每订阅端**有界队列**；溢出触发快照重置，冷却期（hysteresis）防抖、事件丢弃但内存有界；回填长度超预算也直接给快照 | `broker.py` 中 `Subscription._on_overflow` |
| 连接与内存预算 | 每队列上限 + 全局 `global_memory_items` 预算（超限重置最贪心者，冷却期也可破例）+ `max_subscriptions` 连接上限 | `test_global_memory_budget_*`、`test_connection_budget_*` |
| 多个慢端同时追赶不拖慢正常端 | 发布对每个订阅端 `put_nowait`，永不因慢消费者阻塞；正常端 lag 始终为 0 | `test_slow_subscriber_does_not_block_*`、`test_multiple_slow_subscribers_*` |

## 架构

```
discovery_sim/
  model.py    Node / Event / Snapshot / Compacted —— 不可变线数据
  store.py    RevisionStore：revision 日志、墓碑、压缩与 GC、乐观写栅栏
  broker.py   Broker + Subscription：有界队列、溢出快照重置、全局预算、审计日志
  server.py   DiscoveryServer：门面，串联 store 与 broker
  client.py   DiscoveryClient：本地视图、双版本栅栏、暂停/断线/重连状态机
web/
  engine.py   后台 asyncio 引擎（独立线程），HTTP 线程通过 run_coroutine_threadsafe 驱动
  app.py      stdlib http.server：/api/state 与控制端点
  index.html / app.js / styles.css   实时多客户端面板（明/暗主题，调色板经 CVD 校验）
scripts/demo.py    六个场景的命令行故事线
tests/             24 个 unittest 用例，零第三方依赖
```

### 关键设计：为什么需要两道版本栅栏

1. **全局 revision 栅栏**（`event.revision > last_revision`）保证有序流的幂等，
   并让快照与增量在 `snapshot.revision + 1` 精确交接。
2. **每节点 version 栅栏**（`event.node.version > 本地该节点 version`）防的是
   *带外*乱序：gossip 重复、UDP 重传、伪造事件可能携带任意 revision 号。只靠
   全局栅栏会被一个「高 revision、旧节点版本」的事件骗过去；节点版本栅栏让
   「最新写入者获胜」，旧版本永远无法撤销更新的注销结果。注意带外陈旧事件被拒时
   **不能**推进全局 revision，否则会吞掉合法流事件。

### 慢订阅端策略的取舍

- **优先积压增量**：队列未满时只做入队，客户端追上即可，成本最低。
- **溢出才强制快照**：满了说明增量积压已不划算；清空队列、用一份当前快照替换，
  客户端从快照 revision 继续。冷却期内再溢出只丢弃事件并挂起重置（pending reset），
  等队列降到低水位再补发快照，避免快照风暴。
- **全局预算可破例**：所有订阅端总积压超过内存预算时，直接重置最贪心的订阅端，
  即使它在冷却期内——保护服务端内存优先于防抖。

## 运行

```bash
# 1) 自动化测试（24 个）
python3 -m unittest discover -s tests -v

# 2) 命令行场景演示（六个故事线）
python3 -I scripts/demo.py

# 3) 多客户端实时面板，浏览器打开 http://127.0.0.1:8000
python3 -I run_dashboard.py          # 可加端口参数，如 8001
```

面板上可做的操作：

- 注册 / 注销节点、一键压缩历史（带压缩水位线条）、开关自动变更负载；
- 每个订阅端卡片：**暂停 / 恢复 / 断线 / 重连**、调节每事件处理延迟（模拟慢客户端）；
- 实时显示：服务端 head/水位线、每客户端 lag、服务端缓冲占用条、快照/重置/丢弃计数、
  陈旧事件拦截数、收敛状态与分歧明细、墓碑表、服务器审计流。

## 模拟操作建议（对应验收路径）

1. 打开面板，启动「自动变更」，观察三个客户端中 `fast` 始终 lag=0。
2. 对 `paused` 点暂停：缓冲条上涨到上限后出现 **SNAPSHOT RESET**，lag 持续增大但
   服务端内存始终有界；点恢复，lag 归零、状态回到 STREAMING。
3. 对 `slow` 点断线 → 制造若干变更 → 点「立即压缩历史」→ 重连：审计流显示其改取
   快照并收敛（续订点已落入压缩区间）。
4. 调高某客户端处理延迟并保持运行，对比它与 `fast` 的追赶差距，同时确认 `fast`
   不受影响。
