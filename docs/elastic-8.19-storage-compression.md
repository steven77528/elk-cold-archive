# Elasticsearch 8.19.12 热、温、冷数据的存储压缩说明

## 结论

Elastic 官方没有给出“热数据迁移到温数据”或“热数据迁移到普通冷数据”的固定压缩率。ILM 的 `migrate` 动作只修改索引的 `_tier_preference`，让分片分配到目标数据层；它本身不改变 Lucene 段的编码，也不会自动重新压缩数据。

在副本数、codec、段结构等条件都不改变时，单纯迁移可按约 `1:1` 估算，即迁移本身的逻辑存储降幅约为 `0%`。这是根据官方对迁移动作的定义作出的工程结论，并不是 Elastic 发布了一个“0% 压缩率”指标。

| 场景 | 官方能否给固定压缩率 | 可采用的口径 | 说明 |
| --- | --- | --- | --- |
| 热 -> 温，仅执行 ILM migrate | 不能 | 约 `1:1`，迁移本身约 `0%` 降幅 | 只改变分片分配目标 |
| 热 -> 普通冷，仅执行 ILM migrate | 不能 | 约 `1:1`，迁移本身约 `0%` 降幅 | 冷节点使用廉价磁盘不等于数据被压缩 |
| 设置 `best_compression` 并完成段合并 | 不是固定值 | 官方日志实验中，存储用量**最高约降低 28%** | 相对于默认 LZ4；最好约为原来的 72%，实际值依数据而变 |
| 冷层 fully mounted searchable snapshot | 不是压缩率 | 满足条件时，本地磁盘需求**约降低 50%** | 来自取消 replica，不是重新压缩；8.19 社区 Basic 不可用 |
| 普通 snapshot 写入 OSS | 不能 | 首次快照与 primary Lucene 段大致同量级 | 快照复制 primary segments，并做跨快照段去重；不是再次压缩整个索引 |
| synthetic `_source` | 不能 | 官方只称可缩小磁盘占用，无统一百分比 | 8.19 社区 Basic 不可用 |

8.19 文档适用于 8.19.x 补丁版本，因此覆盖 8.19.12；Elastic 的版本化页面标题只精确到 8.19。

## 1. 数据层迁移不等于压缩

官方对 ILM `migrate` 的定义是：

> “Moves the index to the data tier that corresponds to the current phase by updating the `index.routing.allocation.include._tier_preference` index setting.”

也就是说，迁移负责改变索引的层级分配偏好。官方没有说明它会重写 Lucene 段或更换压缩编码。因此，不能仅凭“进入 warm/cold phase”承诺某个压缩率。

官方还明确说明：普通冷层索引如果继续保留副本，而不使用 searchable snapshots，可以降低硬件成本，但**不会相对温层降低所需磁盘空间**：

> “Alternatively, you can use the cold tier to store regular indices with replicas instead of using searchable snapshots. This lets you store older data on less expensive hardware but doesn't reduce required disk space compared to the warm tier.”

来源：

- [Migrate（Elasticsearch 8.19）](https://www.elastic.co/guide/en/elasticsearch/reference/8.19/ilm-migrate.html)
- [Data tiers（Elasticsearch 8.19）](https://www.elastic.co/guide/en/elasticsearch/reference/8.19/data-tiers.html)

## 2. `best_compression`：官方唯一给出近似实验数值的机制

Elasticsearch 默认使用 LZ4 压缩 stored data。`index.codec: best_compression` 改用 ZSTD，以牺牲 stored fields 读取性能换取更高压缩率。

官方原文：

> “The default value compresses stored data with LZ4 compression, but this can be set to `best_compression` which uses ZSTD for a higher compression ratio, at the expense of slower stored fields read performance.”

> “Experiments with indexing log datasets have shown that `best_compression` gives up to ~28% lower storage usage ... compared to default while affecting get by id latencies between ~10% and ~33%.”

正确理解：

- “up to ~28% lower” 是特定日志数据实验中的上限，不是所有日志的固定压缩率。
- 若默认索引占用 100 GB，官方实验的最好结果可表述为约 72 GB；不能据此保证生产数据一定为 72 GB。
- 它影响 stored data，而索引还包括倒排索引、doc values、点数据等，最终总占用取决于 mapping、字段内容和数据重复度。
- 修改 codec 后，只有新合并出的段使用新 codec。官方说明可用 force merge 触发段合并。
- 官方给出的代价是 stored fields 读取变慢；日志实验中 GET by ID 延迟约增加 10% 到 33%，但 indexing throughput 相近。

来源：

- [`index.codec`（Elasticsearch 8.19）](https://www.elastic.co/guide/en/elasticsearch/reference/8.19/index-modules.html#index-codec)
- [ILM force merge action（Elasticsearch 8.19）](https://www.elastic.co/guide/en/elasticsearch/reference/8.19/ilm-forcemerge.html)

## 3. Force merge：释放删除空间并应用 codec，但没有固定节省率

Force merge 合并分片中的段，并可释放已删除文档占用的空间。能节省多少取决于索引中更新/删除产生的 tombstone 以及现有段布局，官方没有统一百分比。

官方原文：

> “Merging reduces the number of segments in each shard by merging some of them together, and also frees up the space used by deleted documents.”

使用限制和风险：

- 官方建议仅对已经只读、不再接收写入的索引执行 force merge。
- 这是资源密集型操作。
- 当 `max_num_segments=1` 时，重写期间可能需要最高达到分片大小 3 倍的临时可用空间。
- 对仍在写入的索引反复 force merge 可能生成大于 5 GB、不会再参与常规合并的大段，导致软删除堆积、磁盘占用上升，并使后续快照更昂贵。

来源：

- [Force merge API（Elasticsearch 8.19）](https://www.elastic.co/guide/en/elasticsearch/reference/8.19/indices-forcemerge.html)
- [ILM force merge action（Elasticsearch 8.19）](https://www.elastic.co/guide/en/elasticsearch/reference/8.19/ilm-forcemerge.html)

## 4. Searchable snapshots：约 50% 是取消副本，不是压缩

冷层 fully mounted searchable snapshot 默认不需要 replica，因为底层 snapshot 提供恢复能力。官方给出的“约 50%”或“potentially halving”指 Elasticsearch 数据节点的**本地磁盘需求**，不是索引内容压缩了 50%，也不是 OSS 仓库占用减少了 50%。

官方原文：

> “These fully mounted indices eliminate the need for replicas, reducing required disk space by approximately 50% compared to the regular indices.”

> “Searchable snapshots eliminate the need for replica shards after rolling over from the hot tier, potentially halving the local storage needed to search your data.”

这个约 50% 只有在如下条件下才成立：

- 原普通索引是 1 primary + 1 replica；
- fully mounted searchable snapshot 不再配置 replica；
- 比较对象是 ES 节点的本地磁盘，而不是 OSS repository。

如果原索引本来就是 `number_of_replicas: 0`，就没有这项约 50% 的节省。如果为 searchable snapshot 再加 replica，该比例也会改变。

许可方面，Elastic 当前官方订阅矩阵把 `Searchable snapshots` 列为 Enterprise 功能，Basic 列没有勾选。因此 Elasticsearch 8.19.12 社区/Basic 方案不能将其作为可用的冷层降本手段。普通 Snapshot/restore APIs 则在 Basic 列有勾选。

来源：

- [Data tiers（Elasticsearch 8.19）](https://www.elastic.co/guide/en/elasticsearch/reference/8.19/data-tiers.html)
- [Searchable snapshots（Elasticsearch 8.19）](https://www.elastic.co/guide/en/elasticsearch/reference/8.19/searchable-snapshots.html)
- [Elastic 官方订阅矩阵](https://www.elastic.co/subscriptions)

## 5. 普通快照写入 OSS 的存储影响

普通 snapshot 的机制是复制 primary shard 的不可变 Lucene segments 到 snapshot repository。它不复制 replica 的另一份，也不会把整个索引再次按某个官方固定比例压缩。

官方原文：

> “To back up an index, a snapshot makes a copy of the index's segments and stores them in the snapshot repository.”

> “Since segments are immutable, the snapshot only needs to copy any new segments created since the repository's last snapshot.”

> “Snapshots are automatically deduplicated to save storage space and reduce network transfer costs.”

因此：

- 首次归档到 OSS 的数据量通常与 primary shard 的 Lucene segments 同量级，但官方没有给固定比例。
- 如果热层是 1 primary + 1 replica，OSS 快照相对热层两份本地 shard 的总量可粗略看作约一半；这是“不快照 replica”，不是 2:1 数据压缩，且还会受到 repository metadata、分块和段共享的影响。
- 后续快照只复制新 segments，并在 repository 内去重；具体增量取决于段是否变化。
- 删除 snapshot 时，Elasticsearch 只删除没有被其他 snapshot 引用的 segments，所以 OSS 释放量也不一定等于该 snapshot 页面显示的逻辑大小。
- S3 repository 的 `compress` 设置只压缩 metadata files；官方明确说它不影响默认已经压缩的 index files。

来源：

- [Snapshot and restore / How snapshots work（Elasticsearch 8.19）](https://www.elastic.co/guide/en/elasticsearch/reference/8.19/snapshot-restore.html#how-snapshots-work)
- [S3 repository `compress` setting（Elasticsearch 8.19）](https://www.elastic.co/guide/en/elasticsearch/reference/8.19/repository-s3.html)

## 6. Synthetic `_source`：能省空间，但社区版不可用

Synthetic `_source` 不在磁盘中原样保存 `_source` JSON，而是在读取时重建，可降低磁盘占用，但会使 GET/Search 中访问 `_source` 变慢。不同 mapping 和字段类型的效果不同，官方没有发布统一压缩百分比。

官方原文：

> “Using synthetic `_source`, which reconstructs source content at the time of retrieval instead of storing it on disk. This shrinks disk usage, at the cost of slower access to `_source` in Get and Search queries.”

> “This feature requires a subscription.”

Elastic 官方订阅矩阵把 8.17 及之后版本的 Synthetic `_source` 列为 Enterprise 功能，因此 8.19.12 社区/Basic 不适用。

来源：

- [Synthetic `_source`（Elasticsearch 8.19）](https://www.elastic.co/guide/en/elasticsearch/reference/8.19/mapping-source-field.html#synthetic-source)
- [Elastic 官方订阅矩阵](https://www.elastic.co/subscriptions)

## 对 8.19.12 社区版的可执行口径

1. 不应在容量规划中使用“热转温固定压缩率”或“热转冷固定压缩率”；迁移本身按约 `1:1` 计算。
2. 若温层的只读日志索引启用 `best_compression` 并完成 segment merge，可把官方“最高约少 28%”作为乐观上界，不能当作承诺值。
3. Force merge 自身没有固定节省率，执行前必须预留足够临时磁盘，并避免对仍写入的索引执行。
4. 社区/Basic 可以做普通 snapshot/restore 到 OSS，但不能把 Enterprise searchable snapshots 的约 50% 本地盘节省套到当前普通 OSS 冷归档方案上。
5. 最终压缩率应在实际 gateway、nginx、other 索引上分别抽样：对比 `pri.store.size`，不要用包含 replica 的 `store.size` 与 OSS 对象物理占用直接混算。
