---
type: guide
status: Accepted
updated: 2026-11-21
---

# Search 装配指南

## 依赖与来源端口

[模块清单](../../module.yaml)声明 platform 为必需依赖，catalog、reviews、resources、favorites 和 community 为可选依赖。backend 用 platform 提取可信请求人账号，并适配来源模块的公开接口。community 当前无内容或活动投影端口。

| 端口 | 提供方与用途 |
| --- | --- |
| `Current`、`DiscoverySnapshot` | catalog、reviews、resources 提供当前对象及用于重建的内容快照 |
| `CurrentActivity`、`ActivitySnapshot` | catalog、reviews、favorites 提供当前活动及指定日期窗口的快照 |
| `MyVotes` | reviews 提供当前请求人的有效投票 |

[Sources](../../model.go) 使用 source 或目标标识选择提供方。各来源公开自己的 DTO，backend 转换类型，不直接读取来源表。批量扩展 `CurrentBatch`、`CurrentActivities` 每次最多 100 条，否则可逐项查询。

快照每页 1 至 100 条，首个游标和末页游标为空。后续页沿用同一个一致性快照和查询窗口。活动日期边界为 UTC 零点，左闭右开。快照同时包含可见记录和墓碑。

当前查询按请求人的权限返回可读记录，缺失、撤回和无权读取均可按不可见处理。明确停用的来源通过 `ErrSourceDisabled` 表达，超时和错误保留故障含义。reviews 投票端口不可用时，本人投票查询返回暂不可用。

## 数据库角色

1. 使用独立迁移身份运行 `postgres.Migrate`。数据库使用 UTF-8，支持 `pg_c_utf8` 排序规则。旧 public schema 若允许 PUBLIC 创建对象，应先撤销相应 CREATE 权限。
2. 授予运行身份对 search schema 的 USAGE 及下表权限。检查连接数和超时。启动前调用 `CheckRuntime` 检查所需权限、对象属主关系、跨 schema 权限和运行身份属性。
3. 迁移保留既有投影、消费回执、热门快照和游标密钥。旧内容类型约束如缺少学校类型，由迁移扩大并校验。一致且已校验的约束跳过重建。

| 表 | 运行身份 |
| --- | --- |
| `documents`、`activities`、`rankings` | SELECT、INSERT、UPDATE |
| `state` | SELECT、UPDATE |
| `inbox` | SELECT、INSERT |

运行身份不持有 search 对象属主角色、DDL、TRUNCATE 或其他模块数据权限。inbox 由运行身份追加回执，由维护身份按期限删除。

使用独立维护身份调用 `postgres.Maintenance`，在 search schema 授予 USAGE，在 inbox 和 activities 授予 SELECT、DELETE。清理严格早于截止时间的条目，每张表每批最多 1,000 条，单次最多 3 秒。

回执截止时间须至少早于当前时间 24 小时。活动截止日须在当前 UTC 日向前 366 天或更早，保留最大热度窗口内的数据。

## 启动与事件投递

1. 调用 `search.New` 注入来源接口、查询超时、重建超时、热度窗口、权重和最低样本量。查询超时须大于零且最多 30 秒，重建默认两分钟、最多 30 分钟。
2. backend 在启动后异步调用 `Rebuild`。使用首次重建成功及数据库连通性评估模块就绪。HTTP 路由注册 search.query，来源事件消费和重建属于 search.projection。
3. 将内容事件接入 `Consume`，活动事件接入 `ConsumeActivity`。relay 使用认证来源，启动、重连、通知或 60 秒补读时获取积压，每批最多 100 条。
4. 在消费提交后确认事件。失败使用 5 秒起步的指数退避，上限一小时，累计 24 小时进入待处理队列。重试沿用原事件 ID，来源快照事务与事件确认相互独立。

backend 当前默认查询超时 5 秒，热度窗口 30 天，最低样本量 3，浏览、评价、收藏和互动权重分别为 1、5、3、1。部署可通过后台装配的配置覆盖热度参数。

`Service.Status` 和 `Repository.Status` 返回索引是否就绪、最近重建时间、活动窗口、各来源记录数和 inbox 数量。`Config.Observer` 记录操作、来源、错误码、耗时、扫描数与修订跳过数，查询词和身份不进入观测记录。
