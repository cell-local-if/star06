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

### `GET /streams/{stream_id}`（可带 `?at=<version>`）
返回按事件**确定性重放**得到的状态：`200 {"state": {...}, "version": <int>}`。
当前投影：`{"status": "unknown|placed|cancelled", "lines": [...], "notes": [...], "cancelled": bool}`。

- 不带 `at`：重放该流**全部**事件，`version` 为当前流版本。
- 带 `at`：只重放流内 `version` 为 1..`at` 的事件（时间旅行），`version` 恒等于 `at`。
- `at` 必须是十进制非负整数（`0`、`1`、`10`…）；空白、`+`/`-` 号、小数（`1.0`）、
  科学计数法（`1e1`）、非 ASCII 数字、空值（`at=`）以及同一参数出现多次，一律
  `400 invalid_request`。
- `at` 校验**先于**流存在性检查：对不存在的流传非法 `at` 仍得到 400。
- `at=0` 且流存在：`200 {"state": {"status":"unknown","lines":[],"notes":[],"cancelled":false}, "version": 0}`，
  以此区分「流存在但尚未观察事件」与「流不存在」。
- `at` 大于该流当前版本，或流从未写入：`404 not_found`，不返回未来状态。
- 并发追加期间的单次历史查询落在一个一致版本边界上：边界提交前后的事件不会混入同一结果，
  响应的 `version` 与实际重放的末版本一致。
- 使用持久化 SQLite 文件时，关闭并重新打开账本后，相同 `stream_id` 与 `at` 的结果不变。

### `POST /streams/{stream_id}/snapshots`
请求体：`{"at_version": <int>}` —— 把流在 `at_version` 的确定性投影固化为**快照**。

- 快照只是事件的派生数据，事件日志仍是唯一事实来源；创建快照不删除、截断或改写任何事件与幂等记录。
- `at_version` 必须是 JSON 非负整数（不是布尔值），范围为目标流的 `0` 到当前版本。
- 首次固化返回 `201 {"stream_id": "...", "version": N, "state": {...}}`；`state` 逐字段等于
  `GET /streams/{stream_id}?at=N` 的重放结果，`version` 恒等于 `N`。
- 对同一 `stream_id` 与 `N` 重复提交：不重复固化，返回 `200` 与相同响应内容；持久化账本重启后依然成立。
- 并发提交同一 `stream_id` 与 `N`：只有一个请求得到 `201`，其余得到 `200`，各方 `version`/`state` 一致。
- 不同 `N` 各自保留快照；之后追加的新事件不改变已固化的旧快照。
- 快照的读取与固化落在一个一致版本边界上：并发追加前后的事件不会混入同一快照。
- 请求体含未知字段、缺 `at_version`，或 `at_version` 为负数/布尔/浮点/字符串：`400 invalid_request`，不创建快照。
- 目标流从未写入，或 `at_version` 大于请求处理时观察到的当前版本：`404 not_found`，不留快照。

### `POST /transactions/{command_id}`
请求体：`{"streams": [{"stream_id": ..., "events": [...], "expected_version": <int>}, ...]}`
—— 把涉及多个流的命令作为**一个跨聚合事务**原子落账。

- `command_id` 在路径中，必须是 1–200 字符的非空字符串，否则 `400 invalid_request`。
- 请求体只接受 `streams` 一个字段；`streams` 各项只接受 `stream_id`、`events`、`expected_version`。
  各项的 `stream_id`、事件数量与形状、`expected_version` 沿用单流追加的全部校验规则。
- `streams` 为空、任一 `stream_id` 重复、或任一字段非法：`400 invalid_request`。
  **所有请求先整体校验，再开始写入**；校验失败不产生任何写入，也不留幂等记录。
- 成功时按 `streams` 数组顺序分配**连续全局游标**，每条流获得连续版本；全部事件、
  事务幂等记录与游标在**同一个 SQLite 事务**中提交。返回
  `201 {"transaction_id": "...", "streams": [{"stream_id": "...", "version": N, "events": [...]}]}`，
  其中 `events` 仍是不含 `cursor` 的单流形状，`version` 为该流写入后的版本。
- 任一 `expected_version` 与当前流版本不一致：以数组中**第一处**冲突为准返回
  `409 version_conflict`，**任何流都不写入**，也不消耗游标；该 `command_id` 仍可用于后续合法提交。
- 事务幂等：相同 `command_id` 且 `streams` 各项语义相同（语义规则同单流：顺序敏感、payload 键序不敏感）
  的重试不新增事件或游标，返回首次的 `201` 响应（含相同 `transaction_id` 与 `event_id`）；
  重启后依然成立；并发重试只落一批，各方得到同一响应。
  相同 `command_id` 但语义不同：`409 idempotency_conflict`（优先于 `version_conflict`），不修改事件日志。
- 事务的幂等命名空间与单流追加的 `command_id` 相互独立。
- 提交后各流的版本读取与确定性重放，与按顺序逐条单流追加的结果一致；
  全局审计中整批事件的 `cursor` 连续可见、严格递增。事务不改写或删除已有事件，
  也不会留下部分流已提交、部分流未提交的可见状态；持久化账本重开后结果不变。

### `GET /streams`
`200 {"streams": ["..."]}`（字典序）。

### `GET /events`（全局只读审计，跨流）
可选查询参数 `after`、`limit`：

- `after`：只返回**全局游标严格大于** `after` 的事件；默认 `0`。
- `limit`：本页最多返回的事件数；默认 `100`，范围 `1`–`1000`。
- 成功返回
  `200 {"events":[{"cursor":1,"stream_id":"order-1","version":1,"event_id":"...","type":"OrderPlaced","payload":{}}],"next_cursor":1,"has_more":true}`。
- `cursor` 是**稳定、严格递增的全局位置**（跨所有流，从 1 开始的稠密整数）；`events` 按 `cursor` 升序。
- `next_cursor` 为本页最后一个事件的游标；空页时保持为请求里的 `after`。
- `has_more` 表示查询边界之后是否还有更多事件。
- 依次以上一页的 `next_cursor` 作为下一页的 `after` 翻页：**不重不漏**，即使其他流在持续追加。
  单次响应只包含该次查询边界**之前已提交**的事件，边界后提交的事件留给下一次；整批追加要么整批可见、要么整批不可见。
- 快照与幂等记录不是事件，**不参与**全局审计。
- `after=0`，或 `after` 大于当前最大游标：返回空 `events` 且 `has_more=false`（后者 `next_cursor` 保持 `after`）。
- `after`、`limit` 必须是十进制非负整数（ASCII 数字，允许前导零）。`limit` 为 `0`，或二者为布尔、浮点、字符串符号、空白、
  科学计数法（`1e2`）、非 ASCII 数字（`٢`）、空值、重复参数，或 `limit` 超出 `1`–`1000`：一律 `400 invalid_request`。
- 出现任何**未知查询参数**（如 `since`、`foo`）：`400 invalid_request`。未知路由仍为 `404 not_found`。

#### 全局游标与持久化升级

- 新写入的事件在追加事务内取得连续全局游标：游标与事件在**同一事务**提交，回滚则归还，因此不重号、不跳号；
  冲突的追加与命中的幂等重试不消耗游标。重启后游标不变，新事件取得更大的游标。
- 打开**旧版** SQLite（事件尚无 `cursor` 列）时自动一次性升级：按事件**既有写入先后**（物理插入顺序 `rowid`）
  补编号 `1..N`，**不改写** `stream_id`、`version`、`event_id`、`type`、`payload`，不改变按流读取与确定性重放结果，
  并把序列高水位设为 `N`。
- 整个迁移是一个事务：补号、唯一索引与高水位原子完成；并发打开只形成一套一致顺序（不重号、不改既有游标）。
  若无法在不改变事件事实的前提下完成迁移，返回 `500 internal_error` 并回滚，不留半套顺序（原文件事实不变，可重新升级）。

## 错误语义

```json
{"error": {"code": "invalid_request", "message": "<可读说明>"}}
```

| 状态码 | `code` | 何时 |
| --- | --- | --- |
| 400 | `invalid_request` | 缺/多字段、类型错、`events` 空或超 100、`expected_version` 非非负整数、`command_id` 非 1–200 字符非空字符串、`Content-Length` 缺失/非法/超 1 MiB、体不是合法 JSON 对象、状态查询的 `at` 非十进制非负整数或重复出现、快照请求缺/多字段或 `at_version` 非 JSON 非负整数、事务请求缺/多字段、`streams` 为空或 `stream_id` 重复 |
| 404 | `not_found` | 未知路由，或从未写入过的流，或历史查询的 `at` 超过该流当前版本，或快照的 `at_version` 超过该流当前版本 |
| 409 | `version_conflict` | 首次命令的 `expected_version` 与当前流版本不一致；事务中指数组里第一处不匹配的流 |
| 409 | `idempotency_conflict` | `command_id` 已成功落账但本次 `stream_id`/`expected_version`/`events`（事务为 `streams` 各项）与首次语义不同 |
| 500 | `internal_error` | 未预期错误 |

**优先级**：`Content-Length` 的校验先于读体；路由不匹配先于体校验；`invalid_request` 先于一切冲突；
对携带已落账 `command_id` 的重试，`idempotency_conflict` 先于 `version_conflict`（即使 `expected_version` 已过期）；
首次命令仍按 `invalid_request` → `version_conflict` 的顺序处理。

## 未实现（后续任务的候选方向，非固定题单）

压缩与回收、订阅与投递、审计导出等 ——
每道题应依据**当时**的真实代码与契约选择尚未实现、且有独立工程价值的部分。
