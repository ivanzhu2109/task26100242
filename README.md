# failback — 灾备回切协调库

由上层直接调用的进程内 Python 库，状态保存在 SQLite。提供三类能力：

- **恢复动作登记** `register_action`
- **回切请求** `request_failback`
- **状态查询** `query_action` / `query_request`

实际设备操作（恢复 / 回切）由调用方以进程内回调形式提供，库负责状态机、
并发协调与回切顺序。支持最多 8 个库实例同时操作同一 SQLite 文件。

## 快速开始

```python
from failback import FailbackCoordinator, ActionStatus, RequestStatus

coord = FailbackCoordinator("/data/failback.db")   # 多实例传同一路径即可

# 1. 登记恢复动作（前提须已恢复成功且未被撤销，否则整体拒绝）
coord.register_action(
    "restore-db", "exec-1",
    prerequisites=[],                  # 最多 8 个，可跨执行共享
    recover=lambda: my_device.restore("db"),    # 调用方设备动作
    failback=lambda: my_device.rollback("db"),
)

# 2. 查询动作状态
view = coord.query_action("restore-db")
assert view.status == ActionStatus.RECOVERED

# 3. 受理回切请求：冻结撤销范围（目标 + 全部直接/间接依赖动作），
#    范围内立即停止接纳新依赖；受理不等于完成。
rid = coord.request_failback(["restore-db"])
assert coord.query_request(rid).status == RequestStatus.ACCEPTED
# ... 等待范围内动作按依赖顺序完成回切 ...
assert coord.query_request(rid).status == RequestStatus.COMPLETED

coord.close()
```

## 语义约定

- **登记**：所有前提均已恢复成功且未被撤销时整体成功，否则整体拒绝
  （`RegistrationRejected`），不会启动任何设备动作。登记成功后恢复动作
  由后台调度器启动；恢复失败保证无设备残留，动作进入终态 `RECOVER_FAILED`。
- **撤销范围**：受理时按完整依赖记录计算闭包并冻结；与登记并发时，
  先串行提交的登记一定被纳入范围，后到的登记一定被拒绝。
- **回切顺序**：动作的回切只在所有依赖它的已接纳动作均恢复失败或完成
  回切后开始；范围内恢复中的动作若最终成功则必须回切，失败则无需回切。
- **至多一次**：每个动作的恢复与回切各至多启动一次；重叠/重复请求可以
  覆盖同一动作。请求仅在其范围内所有动作均恢复失败或完成回切后才算完成。
- **一致性**：登记、受理、查询均可解释为遵守调用先后关系的串行顺序；
  单次查询读取一致性快照，不会看到部分撤销的混合视图。

## 动作状态机

```
RECOVERING → RECOVERED → (被纳入撤销范围) → FAILBACK_QUEUED → FAILBACK_RUNNING → FAILBACK_DONE
           ↘ RECOVER_FAILED（终态，无需回切）
```

请求状态：`ACCEPTED`（已受理，回切进行中）→ `COMPLETED`（范围内全部终态）。

## 运行测试

```bash
python3 -m pytest tests/                # 单元 + 并发验收场景
python3 -m pytest tests/ --runslow -s   # 含性能用例（20 万历史 + 8 实例混合负载）
```

性能用例：20 万条终态历史下，8 实例按 1:1:1 混合登记/回切/查询 10 秒，
断言总吞吐 ≥ 300 调用/秒，且结束后无遗留应执行而未执行的回切。
本机实测约 7600 调用/秒。

## 设计要点

- SQLite WAL + `BEGIN IMMEDIATE`：多实例写在数据库层面串行化，读不阻塞写。
- 设备动作在各自实例的后台线程池、事务外执行，长动作不占用数据库锁，
  无关联调用可继续推进（实测 p95 延迟 < 1ms）。
- 动作归属登记它的实例（`owner`），设备回调只存在于本进程，认领通过
  `UPDATE ... SET started=1 WHERE started=0` 原子完成，保证至多一次。
- 不考虑进程崩溃与实例失联；`close()` 会等待已启动的设备动作结束。
