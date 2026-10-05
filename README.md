# Event Ledger — 公开契约（baseline）

一个 append-only（只追加）的**事件账本**服务：事件日志是唯一事实来源，按流（stream）分区，每个流有单调递增的 `version`。
本次基线只实现最小可用子集；**后续任务应在此契约之上继续建设**（见仓库根目录的 AGENTS.md 与项目 goal）。

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
请求体：`{"events": [{"type": ..., "payload": {...}}], "expected_version": <int>, "command_id": "<可选>"}`

- `expected_version` 必须是**当前流版本**；不一致返回 `409`（乐观并发控制）。
- 一次最多 100 个事件；成功返回 `201 {"version": <写入后的流版本>, "events": [...]}`。
- 校验失败**不得产生任何写入**。

#### 命令幂等（可选 `command_id`）

- `command_id` 若提供，必须是 1–200 字符的非空字符串，否则 `400 invalid_request`；省略时行为与基线一致。
- 一个 `command_id` 在同一账本内唯一标识一次命令：首次合法提交写入事件并持久化请求与成功响应（同一事务，重启后仍有效）。
- 以相同 `command_id` + 相同 `stream_id`、`expected_version` 和**语义相等**的 `events` 重试：不新增事件，返回首次的 `201`、`version` 与 `events`（`event_id`、`payload` 与首次一致）。语义相等：数组顺序相同；`payload` 键序不同但值相同视为相等。
- `command_id` 已落账但 `stream_id`、`expected_version` 或 `events` 任一不同：`409 idempotency_conflict`，事件日志不变；该判断优先于 `version_conflict`。
- 并发的相同重试只落一个批次，所有请求方得到同一响应。
- 校验失败或版本冲突的请求**不会**留下幂等记录，同一 `command_id` 之后仍可使用。

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
| 400 | `invalid_request` | 缺/多字段、类型错、`events` 空或超 100、`expected_version` 非非负整数、`command_id` 非 1–200 字符字符串、`Content-Length` 缺失/非法/超 1 MiB、体不是合法 JSON 对象 |
| 404 | `not_found` | 未知路由，或从未写入过的流 |
| 409 | `version_conflict` | `expected_version` 与当前流版本不一致 |
| 409 | `idempotency_conflict` | `command_id` 已落账，但 `stream_id`、`expected_version` 或 `events` 与首次不同 |
| 500 | `internal_error` | 未预期错误 |

**优先级**：`Content-Length` 的校验先于读体；路由不匹配先于体校验；`invalid_request` 先于幂等判断；幂等命中/`idempotency_conflict` 先于 `version_conflict`。

## 未实现（后续任务的候选方向，非固定题单）

快照与压缩、订阅与投递、跨流事务、时间旅行查询、审计导出等 ——
每道题应依据**当时**的真实代码与契约选择尚未实现、且有独立工程价值的部分。
