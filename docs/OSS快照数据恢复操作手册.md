# ELK OSS 快照生产恢复手册

> 文档版本：2.7  
> 更新日期：2026-09-04  
> 适用版本：Elasticsearch 8.19.12  
> 生产集群 UUID：`FENiikIpSOuzE5VxCa3nbg`  
> Snapshot 仓库：`oss_s3_backup`  
> 目标：将已由 `elk-cold-archive` 快照并删除的 backing index 恢复到生产集群，长期加入原数据流并补回
> 当前 ILM policy；之后允许它按现有归档规则再次成为候选并重新归档。

## 1. 本次真实示例和替换规则

本文所有可执行请求均以这次已验证的恢复对象为例：

| 项目 | 本次真实值 |
| --- | --- |
| Backing index | `.ds-logs-hdg-hdg-2026.08.01-000089` |
| 对应 data stream | `logs-hdg-hdg` |
| 删除该索引的 Snapshot | `cold-archive-20260901t094456z-6b935e25` |
| Snapshot UUID | `j773I3xUSCmC-TH4zY8T-A` |
| 归档时源 index UUID | `_-yDtjITTfmBH732vW9dvQ` |
| 恢复后 ILM policy | `default` |

> **后续恢复其他历史索引时，必须整体替换 backing index、data stream、Snapshot 和 ILM policy 的完整值。**
> 不能只改日期或 generation 序号；必须重新从审计日志确认前三项对应关系，并从目标数据流当前索引确认
> 应使用的 ILM policy。

本文中的 `exact-data-stream-name` 指 data stream 的精确名称。本例就是 `logs-hdg-hdg`，不是
`.ds-logs-hdg-hdg-2026.08.01-000089`，也不是 `logs-hdg-*`。

## 2. 如何确定完整索引、data stream 和 Snapshot

### 2.1 如何得到完整 backing index 名

如果索引尚未归档删除，可在 Kibana Dev Tools 查询当前数据流成员或索引列表：

```http
GET /_data_stream/logs-hdg-hdg?filter_path=data_streams.name,data_streams.indices.index_name,data_streams.indices.index_uuid

GET /_cat/indices/.ds-logs-hdg-hdg-*?v=true&h=health,status,index,uuid,docs.count,store.size&s=index
```

本例应使用完整名称 `.ds-logs-hdg-hdg-2026.08.01-000089`。后续恢复时，将请求中的
`logs-hdg-hdg` 和完整 `.ds-*` 名称替换为当次值。

索引已经归档删除后，生产集群不会再列出它；此时以归档服务器的审计日志为准。下面命令会列出
`logs-hdg-hdg` 的历史候选索引和源 UUID：

```bash
cd /data/monitor/elk-cold-archive

jq -r '
  select(.event == "plan" and .mode == "LIVE")
  | .candidates[]?
  | select(.data_stream == "logs-hdg-hdg")
  | [.at, .index, .index_uuid] | @tsv
' data/audit.jsonl
```

从需要恢复的时间段中复制完整 `.ds-*` 名称。对于本次记录，应选择
`.ds-logs-hdg-hdg-2026.08.01-000089`，源 UUID 为 `_-yDtjITTfmBH732vW9dvQ`。

### 2.2 如何得到对应 data stream 名

对已删除的历史索引，审计日志中的 live `plan.candidates` 同时记录了准确映射。使用本次真实索引执行：

```bash
cd /data/monitor/elk-cold-archive

INDEX='.ds-logs-hdg-hdg-2026.08.01-000089'

jq -r --arg index "$INDEX" '
  select(.event == "plan" and .mode == "LIVE")
  | .candidates[]?
  | select(.index == $index)
  | [.at, .data_stream, .index, .index_uuid] | @tsv
' data/audit.jsonl
```

本例输出中的 data stream 必须是 `logs-hdg-hdg`。若历史索引尚未删除，也可以在 Dev Tools 使用以下请求，
然后在响应中搜索完整 backing index 名：

```http
GET /_data_stream?filter_path=data_streams.name,data_streams.indices.index_name,data_streams.indices.index_uuid
```

恢复前仍须在 Dev Tools 执行一次下面的请求，记录响应中 `indices.index_name` 数组最后一个元素。这是当前
write index，加入旧索引前后都必须保持不变：

```http
GET /_data_stream/logs-hdg-hdg?filter_path=data_streams.name,data_streams.status,data_streams.generation,data_streams.indices.index_name
```

### 2.3 如何得到真正的完整 Snapshot 名

Snapshot 可以包含同一个历史索引的多个副本。**不能**仅因某个 Snapshot 的 `indices` 列表包含该索引，
就认定它是删除来源；真正值必须来自该索引的 `index_deleted.snapshot`。

在归档服务器执行：

```bash
cd /data/monitor/elk-cold-archive

INDEX='.ds-logs-hdg-hdg-2026.08.01-000089'

jq -r --arg index "$INDEX" '
  select(.event == "index_deleted" and .index == $index)
  | .snapshot
' data/audit.jsonl | sort -u
```

本例必须输出：

```text
cold-archive-20260901t094456z-6b935e25
```

输出只有一个 Snapshot 才能继续。本例的完整 Snapshot 名就是
`cold-archive-20260901t094456z-6b935e25`。输出为空或出现多个不同 Snapshot 时停止操作，不能按日期猜测。

Dev Tools 也可以用于找候选 Snapshot；在结果中搜索完整 backing index 名：

```http
GET /_cat/snapshots/oss_s3_backup?v=true&s=start_epoch:desc&h=id,status,start_epoch,end_epoch,duration,indices,successful_shards,failed_shards

GET /_snapshot/oss_s3_backup/cold-archive-*?verbose=true&filter_path=snapshots.snapshot,snapshots.uuid,snapshots.state,snapshots.indices
```

第一条请求的 `id` 列就是完整 Snapshot 名；它们都只能辅助查找，最后仍须以上面的
`index_deleted.snapshot` 为准。较早的 `cold-archive-20260901t014804z-6e748d32` 虽然也包含本例索引，
但不是实际删除来源，不能用于本次恢复。确认候选后，在 Dev Tools 精确验证：

```http
GET /_snapshot/oss_s3_backup/cold-archive-20260901t094456z-6b935e25?filter_path=snapshots.snapshot,snapshots.uuid,snapshots.state,snapshots.indices,snapshots.data_streams,snapshots.shards,snapshots.failures
```

必须确认 Snapshot UUID 为 `j773I3xUSCmC-TH4zY8T-A`、状态为 `SUCCESS`、failed shard 为 `0`、
`indices` 包含本例索引，且 `data_streams` 包含 `logs-hdg-hdg`。

### 2.4 最后核对审计证据链

以下命令只显示本次索引、Snapshot 的创建、验证、删除和完成记录：

```bash
cd /data/monitor/elk-cold-archive

INDEX='.ds-logs-hdg-hdg-2026.08.01-000089'
SNAPSHOT='cold-archive-20260901t094456z-6b935e25'

jq -r --arg index "$INDEX" --arg snapshot "$SNAPSHOT" '
  select(
    .snapshot == $snapshot and
    (
      (.event == "snapshot_started" and
       (((.deletion_candidates // []) | index($index)) != null)) or
      (.event == "snapshot_verified" and
       (((.indices // []) | index($index)) != null)) or
      (.event == "index_deleted" and .index == $index) or
      (.event == "transaction_complete" and
       (((.deleted // []) | index($index)) != null))
    )
  )
  | [.at, .event, .snapshot] | @tsv
' data/audit.jsonl
```

必须依次看到 `snapshot_started`、`snapshot_verified`、`index_deleted`、`transaction_complete` 四类事件，
且 Snapshot 相同。缺少任何一条时不恢复。

## 3. 恢复前检查

先停止归档调度。完成 restore、加入 data stream、补回 ILM 并审核 dry-run 之前，`elk-cold-archive` 必须
保持停止；`snapshot_cleanup.enabled` 必须已关闭。

```bash
cd /data/monitor/elk-cold-archive

docker compose ps
docker compose stop cold-archive

test ! -e data/transaction.json \
  && echo "NO TRANSACTION" \
  || echo "TRANSACTION EXISTS"
```

若输出 `TRANSACTION EXISTS`，不得删除该文件；先完成原归档事务。

在 Dev Tools 执行：

```http
GET /

GET /_cluster/health?filter_path=status,unassigned_primary_shards,relocating_shards,initializing_shards

GET /_data_stream/logs-hdg-hdg?filter_path=data_streams.name,data_streams.status,data_streams.generation,data_streams.indices.index_name

HEAD /.ds-logs-hdg-hdg-2026.08.01-000089
```

继续前必须满足：

- `cluster_uuid` 精确等于 `FENiikIpSOuzE5VxCa3nbg`，集群不是 red，`unassigned_primary_shards=0`。
- 已记录 `logs-hdg-hdg` 的当前 write index。
- `HEAD /.ds-logs-hdg-hdg-2026.08.01-000089` 返回 `404`。返回 `200` 时立即停止，不得覆盖、关闭或删除该索引。
- 第 2 节已确认 data stream、Snapshot、审计证据链和 Snapshot 状态。

## 4. 恢复精确 backing index

在 Dev Tools 执行。只能恢复这个精确 backing index，不能把 `indices` 改为 `logs-hdg-hdg`：

```http
POST /_snapshot/oss_s3_backup/cold-archive-20260901t094456z-6b935e25/_restore?wait_for_completion=false
{
  "indices": ".ds-logs-hdg-hdg-2026.08.01-000089",
  "ignore_unavailable": false,
  "include_global_state": false,
  "include_aliases": false,
  "partial": false,
  "ignore_index_settings": [
    "index.lifecycle.*",
    "index.routing.allocation.*",
    "index.blocks.*"
  ],
  "index_settings": {
    "index.number_of_replicas": 0,
    "index.auto_expand_replicas": "false",
    "index.routing.allocation.include._tier_preference": "data_warm,data_hot",
    "index.blocks.write": true
  }
}
```

`accepted=true` 只表示请求已被接收。单独恢复的 `.ds-*` 索引此时还是普通索引，尚未加入 data stream。

## 5. 验证恢复索引

等待恢复结束后，在 Dev Tools 执行：

```http
GET /_cat/recovery/.ds-logs-hdg-hdg-2026.08.01-000089?v=true&active_only=true

GET /_cluster/health/.ds-logs-hdg-hdg-2026.08.01-000089?wait_for_status=green&wait_for_no_relocating_shards=true&timeout=30m

GET /_cat/shards/.ds-logs-hdg-hdg-2026.08.01-000089?v=true&h=index,shard,prirep,state,store,node&s=index

GET /.ds-logs-hdg-hdg-2026.08.01-000089/_settings?flat_settings=true&filter_path=*.settings.index.uuid,*.settings.index.blocks.write,*.settings.index.number_of_replicas,*.settings.index.auto_expand_replicas,*.settings.index.lifecycle.*,*.settings.index.routing.allocation.*

GET /.ds-logs-hdg-hdg-2026.08.01-000089/_count
```

记录恢复后的 UUID、文档数量和存储大小。恢复后 UUID 可以与源 UUID 不同。以下请求记录时间范围，并确认
`@timestamp` 可用：

```http
POST /.ds-logs-hdg-hdg-2026.08.01-000089/_search
{
  "size": 0,
  "track_total_hits": true,
  "aggs": {
    "min_time": {
      "min": {
        "field": "@timestamp"
      }
    },
    "max_time": {
      "max": {
        "field": "@timestamp"
      }
    }
  }
}
```

加入 data stream 前还要检查字段兼容性：

```http
GET /logs-hdg-hdg,.ds-logs-hdg-hdg-2026.08.01-000089/_field_caps?fields=@timestamp&include_unmapped=true
```

通过门槛：没有 active recovery、所有 primary shard 为 `STARTED`、health 为 green、
`index.blocks.write=true`、零副本、此时尚没有 `index.lifecycle.name`，且字段类型没有冲突。ILM policy 将在
加入 data stream 后显式补回；任一项不满足时不加入 data stream。

## 6. 加入对应 data stream

恢复验证通过后，在 Dev Tools 执行：

```http
POST /_data_stream/_modify
{
  "actions": [
    {
      "add_backing_index": {
        "data_stream": "logs-hdg-hdg",
        "index": ".ds-logs-hdg-hdg-2026.08.01-000089"
      }
    }
  ]
}
```

成功响应必须为 `acknowledged: true`。`add_backing_index` 不会自动应用 index template，因此接着要确认
`logs-hdg-hdg` 当前使用的 policy，并给恢复索引显式补回 `default`：

```http
GET /logs-hdg-hdg/_settings?flat_settings=true&filter_path=*.settings.index.lifecycle.name

GET /_ilm/policy/default

GET /_ilm/status
```

必须确认原数据流现有 backing index 使用 `default`、ILM 状态为 `RUNNING`，并且当前 `default` policy 符合
生产设计。当前架构应只有 hot/warm phase；如果仍存在 cold 或 delete phase，先停止并处理 policy，不要继续。

确认无误后执行：

```http
PUT /.ds-logs-hdg-hdg-2026.08.01-000089/_settings
{
  "index.lifecycle.name": "default"
}
```

随后验证成员关系、ILM、write index 和查询：

```http
GET /_data_stream/logs-hdg-hdg?filter_path=data_streams.name,data_streams.status,data_streams.generation,data_streams.indices.index_name

GET /.ds-logs-hdg-hdg-2026.08.01-000089/_settings?flat_settings=true&filter_path=*.settings.index.hidden,*.settings.index.blocks.write,*.settings.index.lifecycle.name

GET /.ds-logs-hdg-hdg-2026.08.01-000089/_ilm/explain?human=true

POST /logs-hdg-hdg/_search
{
  "size": 10,
  "track_total_hits": true,
  "query": {
    "term": {
      "_index": ".ds-logs-hdg-hdg-2026.08.01-000089"
    }
  },
  "sort": [
    {
      "@timestamp": "asc"
    }
  ]
}
```

必须确认：

- 索引已出现在 `logs-hdg-hdg` 成员列表中。
- 加入前记录的最后一个 backing index 仍是 write index。
- 恢复索引为 `index.hidden=true`、`index.blocks.write=true`，且不是 write index。
- `_ilm/explain` 显示 `managed=true`、`policy=default`，并且没有进入 `ERROR` step。
- data stream 查询能命中该精确索引。

此后不再执行 `remove_backing_index`、reindex 或删除恢复副本。该索引作为普通历史 backing index 长期挂载，
直到达到现有归档窗口并再次被 `elk-cold-archive` 快照、验证和删除。

## 7. 长期挂载并恢复归档调度

恢复索引保持在 `logs-hdg-hdg` 中，不执行 `remove_backing_index`、reindex 或 `DELETE`。完成第 6 节全部
验证后，在归档服务器执行：

```bash
cd /data/monitor/elk-cold-archive

docker compose run --rm \
  -e ARCHIVE_EXECUTE=false \
  cold-archive --once --dry-run

docker compose start cold-archive
docker compose ps
```

dry-run 中再次出现 `.ds-logs-hdg-hdg-2026.08.01-000089` 候选是允许且符合预期的。必须核对它的完整索引名、
当前 index UUID、data stream、rollover 时间、年龄和 `other-45-to-225-days` 规则均正确：

- 尚未达到 45 天时不应成为候选，它会继续长期挂载。
- 达到 45 至 225 天时可以成为候选；下一次 live 会重新创建并验证 Snapshot，然后删除该 backing index。
- 超过 225 天时会进入 overdue，只告警、不自动删除，这是当前归档规则的既有行为。

候选符合上述规则即可启动调度。若出现 missing rollover metadata、UUID/数据流不一致、陈旧 override、
非预期候选或其他 error，则保持 `cold-archive` 停止。

## 8. 必须停止的情况

- `index_deleted.snapshot` 为空、出现多个不同值，或审计证据链不完整。
- Snapshot 不是 `SUCCESS`、failed shard 非零，或不包含精确索引和 data stream。
- 生产集群 UUID 不匹配、集群为 red，或目标索引 `HEAD` 返回 `200`。
- 恢复索引 primary shard 未启动、字段类型冲突、文档数量或时间范围异常。
- `add_backing_index` 后 write index 发生变化，或补回 policy 后 `_ilm/explain` 仍为 `managed=false`/`ERROR`。
- dry-run 中恢复索引的 UUID、data stream、rollover 时间、年龄或归档规则不符合预期。

禁止恢复完整 data stream、禁止使用通配符恢复或删除 `.ds-*`、禁止直接修改 OSS Snapshot 对象。
