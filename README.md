# 事件会话发布服务（Event Session Publisher）

FastAPI + SQLite 实现的事件流会话化与 watermark 发布服务，包含一个可交互的网页演示。

## 功能

- **分批事件导入**：事件含 `user_id` / `event_id` / UTC `event_time`；按 `(event_time, event_id)` 排序后，相邻事件间隔 **严格大于** 每用户空闲间隔（idle gap）时分会话，同刻事件按 `event_id` 排序。
- **Watermark 发布**：watermark 只能前进（后退/相等分别返回 409 / 空操作）；仅发布 `end_time < watermark − idle_gap` 的会话，其余保持 `pending`。
- **迟到事件修订**：迟到事件若改变已发布会话（落入其中或桥接两个会话），生成新修订，返回 `retract`（撤回）与 `add`（新增）差异；旧修订快照永久保留、可按修订号查询。
- **幂等导入**：同一 `(user_id, event_id)` 重复导入直接忽略，不产生修订；同 event_id 不同时间返回 409；批内重复返回 422。
- **失败保留旧修订**：每个写请求在单事务内完成，任何异常整体回滚，上一修订仍为最新可见版本。
- **并发安全**：同用户写操作由进程内锁串行化。
- **分组缓存（水位推进快路径）**：每用户按当前 idle gap 会话化后的分组持久化到 `session_grouping` / `grouped_session` 表。纯水位推进直接读取已存分组，不再读取原始事件、也不重新分组；服务重启后同样复用。分组随导入在同一事务内重建，因此“补录只改变待定会话、未生成修订”时，下一次推进仍使用补录后的分组。旧库首次需要时自动初始化；idle gap 改动后在下一次使用时按新值重建。

## 启动（Docker Compose）

```bash
docker compose up --build
# 打开 http://localhost:8000
```

SQLite 数据保存在命名卷 `session-data`（容器内 `/app/data/sessions.db`）。
API 文档：http://localhost:8000/docs 。

## 本地开发（无 Docker 时）

```bash
python3 -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt
SESSION_DB_URL=sqlite:///./data/dev.db uvicorn app.main:app --reload
pytest                      # 单元/接口测试（30 个）
python tests/smoke_e2e.py   # 对运行中的服务器做端到端冒烟（先启动 uvicorn）
python tests/grouping_e2e.py setup        # 分组缓存：导入 + 补录（端到端）
python tests/grouping_e2e.py post-restart # 重启服务后推进，复用已存分组
python tests/grouping_e2e.py gap-change   # 改间隔后按新值重建分组
```

## 网页演示

首页（`/`）提供：

1. 设置用户空闲间隔；
2. 粘贴 JSON 分批导入事件（内置“首批 / 迟到桥接 / 迟到落入”按钮）；
3. 推进 watermark 并发布，含“尝试后退（应失败）”按钮；
4. 查看当前会话状态、已导入事件、修订历史（点击修订行查看该修订的完整快照与差异）；
5. **一键完整演示**：场景 A 迟到事件落入已发布会话（撤回+新增）；场景 B 迟到事件桥接两会话（撤回回退 pending → watermark 再前进重新发布），并验证重复导入幂等。
6. **补录后推进演示**（“演示：补录后推进（复用分组）”按钮）：补录只合并待定会话、不生成修订，随后推进 watermark；服务端直接使用补录后的已存分组，不重读原始事件。重启服务后对同一用户再推进结果一致。

## 主要 API

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| PUT | `/api/users/{user_id}/config` | 设置 idle_gap（秒，>0） |
| POST | `/api/events/import` | 批量导入事件（同批须同一 user_id） |
| POST | `/api/watermark/advance` | 推进 watermark 并发布合格会话 |
| GET | `/api/users/{user_id}/sessions` | 最新修订下的会话（发布前无修订时按已存分组推导 pending） |
| GET | `/api/users/{user_id}/revisions` | 修订列表（含每修订的差异摘要） |
| GET | `/api/users/{user_id}/revisions/{n}` | 某修订的完整会话快照 + 撤回/新增差异 |
| GET | `/api/users/{user_id}/events` | 已导入事件 |

### 示例

```bash
curl -X PUT localhost:8000/api/users/demo/config \
  -H 'Content-Type: application/json' -d '{"idle_gap_seconds":600}'

curl -X POST localhost:8000/api/events/import \
  -H 'Content-Type: application/json' \
  -d '{"events":[{"user_id":"demo","event_id":"e1","event_time":"2026-11-22T10:00:00Z"}]}'

curl -X POST localhost:8000/api/watermark/advance \
  -H 'Content-Type: application/json' \
  -d '{"user_id":"demo","watermark":"2026-11-22T10:20:00Z"}'
```

## 数据模型（SQLite）

- `user_config`：每用户 idle gap 与当前 watermark；
- `event`：原始事件，`(user_id, event_id)` 唯一约束保证幂等；
- `revision`：修订头（序号、原因 `publish`/`late_import`、当时 watermark 与 gap）；
- `session_snapshot`：每个修订下的完整不可变会话快照（published/pending）；
- `revision_change`：修订的撤回/新增差异；
- `session_grouping`：每用户当前 idle gap 下的分组缓存头（间隔、事件数/最大行号、会话数、构建时间）；
- `grouped_session`：缓存分组内的单个会话（时间范围与有序 event_id，不含状态，状态由 watermark 推导）。

时间以固定宽度 UTC 文本存储（自定义 `TZDateTime` 类型），读回始终为带时区 UTC，规避 SQLite 时区丢失问题。

## 设计说明与边界

- 会话 key 为会话内 event_id 有序列表的 SHA-256 前 16 位；内容相同则 key 相同，差异比较基于 key 集合。
- 首次导入（尚无任何发布）不创建修订，会话以 pending 实时推导；修订只在发布会话集合发生变化时产生。
- 迟到事件不会把 watermark 拉回，只重算会话状态：不再合格的已发布会话被撤回并随合并结果回到 pending，待 watermark 再次前进后发布。
- **分组缓存一致性**：分组是“事件集合 + idle gap”的纯函数。所有会改变事件的写操作（导入）都在同一事务里重建分组；导入回滚则分组也回滚。推进只读缓存并按当前 watermark 分类，因此“新增/撤回/待定结果”与按提交顺序全量重算一致。导入失败保留原事件与分组；推进失败保留原水位与修订。
- 进程内锁覆盖单进程部署；多进程/多实例部署需换成数据库级锁或队列（当前 Compose 为单副本，满足演示要求）。
