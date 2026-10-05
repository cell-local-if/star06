# Event Ledger — 公开契约

一个 append-only（只追加）的**事件账本**服务：事件日志是唯一事实来源，按流（stream）分区，每个流有单调递增的 `version`。
支持命令幂等：客户端可携带 `command_id` 安全重试同一命令。

## 运行

```bash
PYTHONPATH=src python3 -m eventledger.app --port 18891 --db ledger.sqlite
PYTHONPATH=src python3 -m unittest discover -s tests -v
```

- 语言/运行时：Python 3.12，**仅标准库**（无第三方依赖）。
- 监听地址：`127.0.0.1`，端口由 `--port` 指定。
- 持久化：单个 sqlite 文件（WAL 模式）；`:memory:` 表示不落盘（测试用）。

## 数据模型

一个流由 `stream_id`（非空字符串，≤200 字符）标识。事件形状：

```json
{"stream_id": "order-1", "version": 1, "event_id": "<uuid>", "type": "OrderPlaced", "payload": {}}
```

- `version` 在**流内**从 1 开始严格递增、不跳号；`event_id` 每次写入生成。
- 目前允许的 `type`：`OrderPlaced`、`LineItemAdded`、`NoteRecorded`、`OrderCancelled`。

## HTTP 接口

### `GET /health`
`200 {"status":"ok"}`

### `POST /streams/{stream_id}/events`
请求体：`{"events": [{"type": ..., "payload": {...}}], "expected_version": <int>, "command_id"?: <string>}`

- `expected_version` 必须是**当前流版本**；不一致返回 `409`（乐观并发控制）。
- 一次最多 100 个事件；成功返回 `201 {"version": <写入后的流版本>, "events": [...]}`。
- 校验失败**不得产生任何写入**。

#### 命令幂等（可选 `command_id`）

- 省略 `command_id`：沿用上述乐观并发行为，每次提交都生成新的 `event_id` 并推进版本。
- 提供 `command_id` 时，它在**同一 SQLite 账本内**唯一标识一次命令；必须是 1–200 字符的非空字符串，否则 `400 invalid_request`。
- 服务持久化首次成功命令的请求内容、`event_id`、版本与完整成功响应。
- 以相同 `command_id` 重试，且 `stream_id`、`expected_version`、`events` **语义相等**时：不新增事件，返回首次的 `201`（含相同的 `version`、`event_id` 与 `payload`）。语义相等指事件数组顺序相同、各 `type`/`payload` 的值相同（payload 键序不影响）。重启后依然成立。
- 并发的相同重试只落一个批次，各方得到同一响应。
- 相同 `command_id` 已成功落账，但 `stream_id`、`expected_version` 或 `events` 任一语义不同：返回 `409 idempotency_conflict`，不修改事件日志。
- 任一事件校验失败或版本冲突时：不留下部分事件，也不留下可命中的成功幂等记录（该 `command_id` 仍可用于后续合法提交）。

### `GET /streams/{stream_id}/events?since=<version>`
按 `version` 升序返回 `version > since` 的事件：`200 {"events": [...]}`。
流不存在（从未写入且 `since=0`）⇒ `404`。

### `GET /streams/{stream_id}`
返回按事件**确定性重放**得到的状态：`200 {"state": {...}, "version": <int>}`。
当前投影：`{"status": "unknown|placed|cancelled", "lines": [...], "notes": [...], "cancelled": bool}`。

### `GET /streams`
`200 {"streams": ["..."]}`（字典序）。

## 错误语义

```json
{"error": {"code": "invalid_request", "message": "<可读说明>"}}
```

| 状态码 | `code` | 何时 |
| --- | --- | --- |
| 400 | `invalid_request` | 缺/多字段、类型错、`events` 空或超 100、`expected_version` 非非负整数、`command_id` 非 1–200 字符非空字符串、`Content-Length` 缺失/非法/超 1 MiB、体不是合法 JSON 对象 |
| 404 | `not_found` | 未知路由，或从未写入过的流 |
| 409 | `version_conflict` | 首次命令的 `expected_version` 与当前流版本不一致 |
| 409 | `idempotency_conflict` | `command_id` 已成功落账但本次 `stream_id`/`expected_version`/`events` 与首次语义不同 |
| 500 | `internal_error` | 未预期错误 |

**优先级**：`Content-Length` 的校验先于读体；路由不匹配先于体校验；`invalid_request` 先于一切冲突；
对携带已落账 `command_id` 的重试，`idempotency_conflict` 先于 `version_conflict`（即使 `expected_version` 已过期）；
首次命令仍按 `invalid_request` → `version_conflict` 的顺序处理。

## 未实现（后续任务的候选方向，非固定题单）

快照与压缩、订阅与投递、跨流事务、时间旅行查询、审计导出等 ——
每道题应依据**当时**的真实代码与契约选择尚未实现、且有独立工程价值的部分。
