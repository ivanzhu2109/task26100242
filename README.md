# failback — 灾备回切协调库

一个纯 Python、零服务依赖的灾备回切协调库。上层系统直接调用，所有协调状态
保存在 SQLite 文件中；真实的设备恢复 / 回切操作由调用方以进程内回调的形式
提供，库负责并发安全的登记、撤销范围计算、回切排序与状态查询。

## 能力

- **恢复动作登记** `register_action`：登记一个属于某次恢复执行的动作，
  可声明最多 8 个前提动作（可跨执行共享）。仅当所有前提均已恢复成功且
  未被撤销时，登记才整体成功并在后台启动恢复；否则整体拒绝，不启动任何
  设备动作。
- **回切请求** `request_failback`：受理即冻结完整撤销范围（目标 + 所有
  直接、间接依赖它的动作），范围内立即停止接纳新依赖，范围外不受影响。
  受理不等于完成；正在恢复的动作等恢复结束后按结果处理（成功则回切，
  失败则无需回切）；一个动作的回切只在所有依赖它的已接纳动作恢复失败
  或完成回切之后才开始。每个动作的恢复与回切各至多启动一次，重叠请求
  安全合并。
- **状态查询** `query_action` / `query_request`：读取一致快照，可区分
  “已受理”与“已完成”，不会读到部分撤销的混合视图。

## 快速开始

```python
from failback import Coordinator

class MyDevice:
    def recover(self, action_id: str) -> None:
        ...  # 执行设备恢复；抛异常表示失败（须保证无设备状态残留）
    def failback(self, action_id: str) -> None:
        ...  # 执行设备回切；保证成功（抛异常会被重试）

coord = Coordinator("failback.db", MyDevice())

# 登记（前提可选，最多 8 个，可跨执行）
res = coord.register_action("act-1", "exec-1", prerequisites=["act-0"])
if res.accepted:
    ...  # 恢复已在后台线程启动
else:
    print("rejected:", res.reason)

# 回切请求：受理后立即返回请求 ID
rid = coord.request_failback("act-0")

# 查询
snap = coord.query_action("act-0")     # ActionSnapshot（phase/recover_state/...）
req = coord.query_request(rid)         # RequestSnapshot（ACCEPTED / COMPLETED、scope、pending）

coord.wait_idle(timeout=60)            # 等待所有在途恢复/回切结束（测试与演练用）
coord.close()
```

设备动作在库的后台线程中执行，持续 1 秒以上的恢复 / 回切不会阻塞无关的
登记、回切受理与查询。

## 多实例

最多 8 个 `Coordinator` 实例（同进程或跨进程）可同时操作同一 SQLite 文件。
实例间通过数据库事务协调：登记与撤销受理是单条 `BEGIN IMMEDIATE` 事务，
回切领取是条件更新（`UPDATE ... WHERE failback_state='NONE'`），因此每个
动作的恢复与回切全局各至多启动一次。库使用 WAL 模式，读写互不阻塞。

规模假设：最多 10 个恢复执行、每执行 200 个动作；不考虑进程崩溃与实例失联。

## 状态模型

动作由三个持久化字段推导（`ActionSnapshot.phase`）：

| phase | 含义 |
|---|---|
| `RECOVERING` | 恢复进行中 |
| `RECOVERED` | 恢复成功，可作为前提 |
| `RECOVER_FAILED` | 恢复失败（终态，无设备残留） |
| `REVOKING` | 已纳入撤销范围，等待恢复结果或依赖者完成 |
| `FAILING_BACK` | 回切执行中 |
| `REVOKED` | 回切完成（终态） |

请求状态：`ACCEPTED`（已受理，范围内尚有未终态动作）→ `COMPLETED`
（范围内所有动作均已恢复失败或完成回切）。

## 运行测试

```bash
python3 -m pytest tests/            # 全部（含约 10 秒的性能用例）
python3 -m pytest tests/ -m "not perf"   # 仅功能与并发场景
```

性能用例（`tests/test_perf.py`）预置 20 万条终态历史，8 实例按 1:1:1
混合登记 / 回切请求 / 单动作查询连续 10 秒，断言总吞吐 ≥ 300 次/秒，
且结束后不存在应执行而未执行的回切；另有独立用例验证设备动作卡住 1 秒
时无关调用的 p95 延迟 < 100 毫秒。

## 代码结构

- `failback/models.py` — 状态枚举、快照数据类、设备动作协议
- `failback/store.py` — SQLite 模式与全部事务逻辑（唯一的并发控制点）
- `failback/coordinator.py` — 公开 API、设备动作执行器与后台调度器
- `tests/` — 业务规则、三个验收并发场景、性能用例
