# ELK 冷数据归档到阿里云 OSS

这个工具的 Compose 配置面向 Elasticsearch `8.19.12` 数据流。它通常读取 backing index 的真实
`rollover_info` 时间；对于经过人工审核且缺少该元数据的历史恢复索引，可以使用一次性的
UUID 绑定时间覆盖。工具将候选索引所属的完整数据流增量快照到现有仓库
`oss_s3_backup`，仅在 Elasticsearch 确认目标 backing index 已包含在完整
`SUCCESS` 快照后逐个删除旧索引。

Elasticsearch 不支持把数据流的单个 backing index 作为独立 Snapshot 选择单元，因此
必须快照整个数据流。Snapshot 在仓库中按不可变 segment 增量存储并去重，不等于每天向
OSS 完整复制整个数据流。

## Docker Compose 一键部署（推荐）

业务配置采用宿主机文件挂载，不会打进镜像：

| 宿主机路径 | 容器路径 | 用途 |
| --- | --- | --- |
| `./config.compose.json` | `/config/config.json`（只读） | ES、仓库、规则和邮件配置 |
| `./secrets/es_api_key` | `/run/secrets/es_api_key`（只读） | 可选 ES API Key |
| `./secrets/es_password` | `/run/secrets/es_password`（只读） | 可选 ES Basic 密码 |
| `./secrets/smtp_password` | `/run/secrets/smtp_password`（只读） | 126 SMTP 授权码 |
| `./data` | `/state` | 锁、事务、调度状态、心跳和审计日志 |
| `./certs` | `/certs`（只读） | 可选 Elasticsearch 私有 CA |

本项目已清空邮箱账号、发件人、收件人和示例认证用户名。部署前请在使用的配置文件中填写
`mail.username`、`mail.from` 和 `mail.to`，并通过环境变量或密钥文件提供实际认证凭据。

Linux 主机安装 Docker 和 Docker Compose v2 后，解压并执行：

```bash
chmod +x deploy.sh
./deploy.sh
```

按你的要求，Docker Compose 容器默认以 root UID/GID（`0:0`）运行，`deploy.sh` 也可以直接
由 root 执行。若以后要改成其他 UID/GID，可在执行前设置 `CONTAINER_UID` 和 `CONTAINER_GID`，
脚本会写入 `.env` 的 `HOST_UID`/`HOST_GID`。

`deploy.sh` 会依次：

1. 询问 Elasticsearch 认证方式并把密钥写入 `./secrets/`。
2. 询问 126 SMTP 授权码；输入过程不回显。
3. 构建镜像并执行一次强制 dry-run，同时测试摘要邮件。
4. 从 dry-run 结果自动取得并绑定真实 `cluster_uuid`。
5. 显示候选索引；只有准确输入 `YES` 才把 `.env` 切换为 live。
6. 启动每天 `02:00 Asia/Shanghai` 运行的容器调度器。

首次启动若当天 02:00 已过，不会立即补跑 live，而是等待下一个 02:00。调度器已有持久化
状态后，如果主机重启错过了 02:00，会合并为一次安全补跑。dry-run/live 模式切换也不会
隐式消费已经过去的调度时段。

### 当前两个一次性覆盖的上线顺序

`config.compose.json` 当前包含两个经过人工审核的 `rollover_time_overrides`。这次迁移不要在
第一次 `deploy.sh` 中直接启用周期性 live，按以下顺序执行：

1. 在 Kibana Dev Tools 执行 `GET /_snapshot/oss_s3_backup`，把返回的仓库 `uuid` 填入
   `repository_guard.uuid`。该字段留空时 dry-run 只会告警，所有 live 都会在写快照前被拒绝。
2. 执行 `./deploy.sh`，审核两个 override warning、候选索引、`Expired snapshots` 和集群 UUID；
   确认提示处不要输入 `YES`，先让周期调度器保持 dry-run。
3. 当前 `snapshot_cleanup.enabled` 为 `true`，手工 live 还会清理最多 30 个到期快照。必须审核
   dry-run 中的完整到期列表；本次只想处理两个异常索引时，先暂时把它改为 `false`。
4. 手工执行一次 `docker compose run --rm cold-archive --once --execute`。
5. 确认摘要中的快照为完整 `SUCCESS`，两个精确索引都在 `Deleted` 中，并确认本地
   `data/transaction.json` 不存在、仓库没有进行中的快照。
6. 立即从 `rollover_time_overrides` 删除这两个已使用项，再手工运行一次 dry-run。陈旧覆盖未删时，
   程序会故意失败，不会继续调度归档。
7. 完成隔离集群 restore 和查询演练；需要 OSS 自动清理时再恢复 `snapshot_cleanup.enabled=true`。
8. 重新执行 `./deploy.sh`，clean dry-run 通过后才输入 `YES` 启用周期性 live。

当前生产架构已完成 `es-cold` 下线，在线层只使用 `data_hot` 和 `data_warm`；历史数据以 OSS Snapshot
离线保存。上述步骤完成后只需继续验证归档事务、恢复演练和周期调度，不再执行 cold 节点排空或停机操作。

常用命令：

```bash
# 查看状态和日志
docker compose ps
docker compose logs -f cold-archive

# 随时手工 dry-run
docker compose run --rm -e ARCHIVE_EXECUTE=false cold-archive --once --dry-run

# 明确手工执行一次 live；仍受 cluster UUID、快照和 UUID 安全门槛保护
docker compose run --rm cold-archive --once --execute

# 停止服务；./data 和挂载配置仍会保留
docker compose down
```

修改 `config.compose.json` 中的年龄或数据流规则后，下一次任务会直接读取新内容，无需重新
构建镜像。修改 `.env` 中的调度时间或 live 模式后，需要执行：

```bash
docker compose up -d --force-recreate cold-archive
```

Compose 服务默认以 root UID/GID（`0:0`）运行，同时保留根文件系统只读、删除全部 Linux capabilities，并设置日志
轮转和心跳健康检查。`.env` 只保存非敏感参数；三个密码/API Key 只存在于挂载的 secret
文件中。健康状态用于监控，Docker Compose 不会仅因 `unhealthy` 自动重启进程；调度器自身
异常退出则由 `restart: unless-stopped` 拉起。不要把 `.env`、`secrets/` 或 `data/` 提交到版本库。

若 Elasticsearch HTTPS 使用私有 CA，把证书放到 `./certs/es-ca.crt`，并在 `.env` 设置：

```dotenv
ES_CA_CERT=/certs/es-ca.crt
```

## 已配置规则

年龄按 `Asia/Shanghai` 的日历日计算，区间含首尾：

| 数据流模式 | 归档并删除的 rollover 年龄 |
| --- | --- |
| `logs-gateway-*` | 10 到 20 天 |
| `logs-nginx-*` | 15 到 30 天 |
| `logs-hdg-hdg` | 45 到 225 天 |
| `logs-standalone-*` | 45 到 225 天 |

每天运行时，索引首次达到 10、15 或 45 天通常就会被归档并删除。gateway、nginx 和
hdg/standalone 分别超过 20、30 和 225 天但仍在 ES 中的索引只会告警，不会自动删除，
避免任务中断后越过区间而发生未经确认的清理。

脚本始终排除每个数据流的当前 write index。它不使用 `index.creation_date`，也不从
索引名称猜日期；缺少 `rollover_info` 且没有完全匹配的显式覆盖时，整次归档失败且零删除。

### 缺失 rollover_info 的一次性覆盖

仅当历史索引经过 restore 或重新挂入数据流，且 Elasticsearch 明确返回 `rollover_info: {}` 时，
才可以在 `selection.rollover_time_overrides` 中添加人工审核项。字段缺失不等同于空对象：

```json
"rollover_time_overrides": [
  {
    "index": ".ds-logs-gateway-example-2026.08.04-000001",
    "index_uuid": "exact-current-index-uuid",
    "rollover_time_ms": 1786605291333,
    "reason": "Reviewed ILM lifecycle_date for restored backing index without rollover_info"
  }
]
```

覆盖项是一次性、失败关闭的安全例外：

- `index` 必须是精确索引名，不允许控制字符、通配符、逗号或路径字符；重复索引会被拒绝。
- `index_uuid` 必须与当前 `_data_stream` 响应完全相同且不含空白/控制字符；同名索引被重建后
  不能复用旧覆盖。
- `rollover_time_ms` 必须是人工审核、可表示且不晚于本次执行时间的正整数毫秒时间；布尔值、
  字符串和浮点数无效。
- `reason` 必填，并写入候选事务和本地审计记录。
- 仅当当前 owner 的 `rollover_info` 完全为空时才使用覆盖。已有正常时间、损坏时间或其他
  owner 记录都会继续让整次任务失败，覆盖不会掩盖异常元数据。
- 未匹配当前非 write backing index 的覆盖也会让整次任务失败，防止拼写错误和陈旧例外静默存在。
- 使用覆盖的 dry-run/live 摘要会产生 warning，候选日志显示 `rollover_source=override`。

操作顺序必须是：添加覆盖 -> 运行并审核 dry-run -> 执行一次 live -> 确认目标索引已经包含在
`SUCCESS` 快照且源索引已删除 -> 立即从配置删除已经使用的覆盖 -> 再运行一次 dry-run。若不删除，
下一次任务会因为覆盖已无法匹配现存 backing index 而主动失败。删除前的快照完整性、data stream
归属、index UUID 和 write index 重验与普通候选完全相同。

## 安全行为

- 不带 `--execute` 时是 dry-run，只执行只读 ES 请求、写本地审计日志并发送摘要邮件。
- 正式模式先检查集群不是 red、版本完全匹配、仓库 UUID/type/settings 完全匹配，并调用仓库
  `_verify`；Compose 配置中的 `repository_guard.uuid` 为空时正式模式会被拒绝。
- 快照使用候选所属的具体数据流名，设置 `partial=false`、`include_global_state=false`。
- 仅当状态为 `SUCCESS`、失败分片为 0、成功分片等于总分片、数据流集合完全一致，并且快照索引集合覆盖全部删除候选时才允许删除。
- 删除前重新读取数据流，复核 data stream、index UUID 和 write index 身份。
- 每个索引使用具体名称单独删除，绝不使用删除通配符。
- 事务状态原子落盘；快照后或部分删除后进程中断，下次运行会复核同一快照并续作。
- 每个待恢复事务记录创建它的 cluster UUID，切换集群后不能继续旧事务。
- 本机文件锁阻止定时任务与人工运行重叠。
- 不同 OSS 保留期的数据流分开创建快照，避免生命周期不同的数据互相拖延清理。
- OSS 清理只在所有归档事务完成后执行；失败只告警，不改变已经完成的归档结果。

归档运行期间不要由其他管理员或自动化执行同名索引的手工删除、恢复或重建，也不要删除
脚本正在验证的快照或直接修改 OSS 仓库。Elasticsearch
的 Delete Index API 没有 UUID 条件参数；脚本已把 UUID/write-index 重验放在每个具体
`DELETE` 前，但仍应从运维流程上排除这类并发特权操作。

## OSS 冷数据清理

最新的 Compose 配置 `config.compose.json` 已启用 `snapshot_cleanup`：

```json
"snapshot_cleanup": {
  "enabled": true,
  "max_deletions_per_run": 30
}
```

规则中的 `min_age_days..max_age_days` 定义归档候选窗口：`min_age_days` 是索引可以开始归档
并从 ES 删除的最小 rollover 年龄，`max_age_days` 是候选上限；超过上限的索引只会告警。
窗口宽度按 `max_age_days - min_age_days` 计算，并在快照创建时写入固定的
`retention_days`：gateway 为 10 天、nginx 为 15 天、hdg/standalone 为 180 天。

例如，hdg/standalone 索引在 rollover 年龄 45 天时归档，快照从 `SUCCESS` 完成时间起固定
保留 180 天，正常情况下数据总保留时间约为 225 天。如果任务延迟到候选窗口内更晚才归档，
快照仍会从实际完成时间起保留完整的 180 天，因此最终总保留时间可能超过 225 天。

每个新归档快照从 Elasticsearch 报告 `SUCCESS` 的完成时间开始，按创建时写入快照 metadata 的
`retention_days` 固定保留；以后修改年龄规则不会追溯缩短或延长它。旧版快照没有该字段时才按
当前数据流规则计算 10/15/180 天。记录值存在但损坏时会告警并跳过清理，不会回退到当前规则。
历史快照若混有不同规则的数据流，创建时记录其中最长的固定保留期。

不带 `--execute` 时，摘要中的 `Expired snapshots` 只是预览；正式模式才删除，并在
`Deleted snapshots` 中报告结果。每次最多删除 `max_deletions_per_run` 个，后续定时任务继续
处理积压。当前 OSS Bucket 未开启版本控制，因此删除快照后，不再被其他快照引用的对象会
被物理回收；共享 segment 仍会保留，空间下降不一定等于快照显示大小。

不要直接给 OSS 的 `elasticsearch/snapshots` 目录设置对象级生命周期删除。Snapshot
仓库对象会跨快照共享，按 OSS 对象时间删除会破坏仓库。

## 还需准备

1. Docker Engine 和 Docker Compose v2；仅使用 systemd 方案时需 Python 3.10 以上。
2. Elasticsearch 认证。脚本依次支持 API Key、Basic Auth、无认证网关。
3. 126 邮箱已开启 SMTP，并准备 SMTP 授权码；通常不是邮箱登录密码。
4. 执行账户对相关数据流、`.ds-*` backing indices、快照 API、仓库验证 API 有足够权限。
5. Compose 的 `deploy.sh` 会自动绑定 `cluster_uuid`；systemd 方案需从 dry-run 手工填写。

API Key 文件应只保存 `Authorization: ApiKey ...` 中省略前缀后的 Base64 值。Basic
Auth 使用 `ES_USERNAME` 和密码文件。不要把任何密码、API Key 或阿里云 AK 写进
`config.json`。OSS AK 继续由 Elasticsearch keystore 中的 `s3.client.oss.*` 管理。

如果 HTTPS 使用私有 CA，再设置 `ES_CA_CERT`；当前配置默认验证系统信任链，不能静默
跳过证书验证。

## systemd 部署（备选）

在 Linux 主机上执行：

```bash
sudo useradd --system --home /nonexistent --shell /usr/sbin/nologin elk-archive

sudo install -d -m 0755 /opt/elk-cold-archive
sudo install -m 0755 archive.py /opt/elk-cold-archive/archive.py

sudo install -d -m 0750 -o root -g elk-archive /etc/elk-cold-archive
sudo install -d -m 0750 -o root -g elk-archive /etc/elk-cold-archive/credentials
sudo install -d -m 0750 -o elk-archive -g elk-archive /var/lib/elk-cold-archive
sudo install -d -m 0750 -o elk-archive -g elk-archive /run/elk-cold-archive
sudo install -m 0640 -o root -g elk-archive config.json /etc/elk-cold-archive/config.json
sudo install -m 0640 -o root -g elk-archive runtime.env.example /etc/elk-cold-archive/runtime.env

sudo install -m 0644 elk-cold-archive.service /etc/systemd/system/elk-cold-archive.service
sudo install -m 0644 elk-cold-archive.timer /etc/systemd/system/elk-cold-archive.timer
sudo systemctl daemon-reload
```

把 SMTP 授权码写入以下文件，文件末尾换行会被自动去除：

```text
/etc/elk-cold-archive/credentials/smtp_password
```

然后设置权限：

```bash
sudo chown root:elk-archive /etc/elk-cold-archive/credentials/smtp_password
sudo chmod 0640 /etc/elk-cold-archive/credentials/smtp_password
```

Elasticsearch 认证三选一，在 `/etc/elk-cold-archive/runtime.env` 中配置：

```dotenv
# API Key
ES_API_KEY_FILE=/etc/elk-cold-archive/credentials/es_api_key

# 或 Basic Auth
ES_USERNAME=
ES_PASSWORD_FILE=/etc/elk-cold-archive/credentials/es_password

# 或由前置网关完成认证：以上两组都不设置
```

凭证文件同样设为 `root:elk-archive 0640`。不要同时配置两套认证；`auto` 模式会优先使用
API Key。

## 首次 dry-run

`runtime.env` 中保持 `ARCHIVE_MODE=`，然后手动启动：

```bash
sudo systemctl start elk-cold-archive.service
sudo journalctl -u elk-cold-archive.service -n 200 --no-pager
```

如果 SMTP 授权码尚未准备好，可以在 dry-run 中先绕过邮件直接运行：

```bash
sudo -u elk-archive /usr/bin/python3 /opt/elk-cold-archive/archive.py \
  --config /etc/elk-cold-archive/config.json --no-mail
```

重点核对日志中的 `candidate`：数据流、rollover 时间、年龄和索引名都必须符合预期。
Dry-run 不会调用 `_verify`，因为 `_verify` 会短暂写入仓库测试对象；正式模式每次都会调用。
把日志或邮件中的 `Cluster UUID` 原样写入 `config.json` 的 `expected_cluster_uuid` 后，才可
进入正式模式。这样可以避免 DNS、代理或灾备切换后误操作另一个同版本集群。

如果某个配置模式没有匹配到真实数据流，脚本会失败，不会继续处理其他模式。这是为了让
拼写错误显式暴露。先修正 `config.json` 中的模式，再继续。

## 启用正式归档

确认至少一次 dry-run 结果后，把 `/etc/elk-cold-archive/runtime.env` 改成：

```dotenv
ARCHIVE_MODE=--execute
```

正式模式禁止 `--no-mail`，并会在接触快照或索引前验证 SMTP 登录、发件人和收件人配置。

建议先手动正式运行一次，并用一个可恢复的非 write backing index 做 restore 演练。确认后
启用每天 02:00 的定时器：

```bash
sudo systemctl start elk-cold-archive.service
sudo systemctl enable --now elk-cold-archive.timer
systemctl list-timers elk-cold-archive.timer
```

`Persistent=true` 会在主机错过凌晨任务后补跑。查看运行记录：

```bash
sudo journalctl -u elk-cold-archive.service
sudo tail -n 50 /var/lib/elk-cold-archive/audit.jsonl
```

## 邮件和退出码

邮件发送到配置文件的 `mail.to`，SMTP 为 `smtp.126.com:465` 且强制 SSL。每次运行都会发送
候选、超龄、快照、删除和错误摘要。

- `0`：成功，或 dry-run 没有安全告警。
- `1`：归档流程失败，未满足删除条件的索引不会删除。
- `2`：存在超龄或处理上限等安全告警。
- `3`：归档本身成功，但摘要邮件发送失败。

## 恢复说明

每个快照包含候选所属的完整数据流及其当时全部 backing indices，但不包含全局状态。
不要直接恢复到仍存在同名数据流或索引的生产集群。推荐恢复到隔离的恢复集群，再把需要
的数据 reindex 回生产环境；恢复集群需要具备相应的 composable/component index templates。

可先查看邮件或审计日志中的 snapshot 名，再调用：

```http
POST /_snapshot/oss_s3_backup/<snapshot-name>/_restore
{
  "indices": "<exact-data-stream-name>",
  "include_global_state": false
}
```

正式上线前必须完成一次隔离环境 restore 演练，并确认能够查询目标时间段。
