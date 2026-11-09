---
type: spec
status: Accepted
updated: 2026-11-09
---

# 搜索与热门发现模块

## 当前行为

OpenAPI 版本为 0.2.0，提供 `/api/v1/search`、`/api/v1/search/popular` 和 `/api/v1/search/my-votes` 三个 GET 接口。Go SDK 与 TypeScript 类型由接口定义生成。

search 通过来源公开事件构建可重建投影，索引只保存公开标题、来源 ID、修订号、学校、归属关系、活动计数及可见状态。search 不读取其他模块业务表，不保存评价正文、作者身份或五维评分数据。

HTTP 请求要求登录。Sources.Current 必须根据可信请求人、学校和操作查询来源当前权限及可见状态。修订不一致的候选暂不展示，来源故障返回 503。删除、下架和权限撤销无需等待投影事件即可影响查询。

搜索使用字面子串匹配，按来源及 ID 排序。每页默认 20 条，上限 100。游标记录已检查候选位置，不暴露隐藏记录总量。一次最多检查 10000 条候选，搜索通过游标继续，热门排行超过上限返回暂不可用。

查询超时由装配配置，必须大于零且不超过 30 秒，包含数据库和来源调用。来源适配器必须传递 context。重建总超时为两分钟，每批最多 100 条，每个来源最多 10000 批。

课程与教师的五维评分统计由 reviews 的 `GET /api/v1/reviews/statistics` 提供，时间范围、最低评分样本量及教师按课程等权汇总均由 reviews 执行。客户端对 2–5 门课程分别读取 reviews 统计，组合展示课程对比。

我的投票委托 reviews 的当前请求人公开接口，只返回有效选择并再次检查目标权限，修改及取消使用 reviews 自身接口。

热门排行按对象汇总窗口内各来源的日快照，再用后台权重计算热度。权重顺序为浏览、评价、收藏、互动。最低样本量使用四项有效计数之和，与权重无关。按加权热度降序，同分按来源和对象 ID 排序。

窗口按 UTC 日查询，开始为 now−HeatWindow 所在日的零点（包含），结束为当前日之后的零点（不包含）。当前日使用已提交快照，窗口参数和当前时间在单次查询或重建开始时固定。

## 事件消费与重建

事件 JSON Schema 位于 `contracts/discovery-event.schema.json`。

Service.Consume 接收 CloudEvents 1.0，source 为 catalog、reviews 或 resources，type 为来源名加 .discovery.v1，subject 与 data.target.id 一致。data 为完整 Document 快照，revision 为该来源对象单调递增修订号。

消费在一个事务内更新投影及 inbox，以 source 和事件 ID 去重。重复事件返回成功，相同 ID 不同载荷拒绝。低于或等于现有修订的事件只记 inbox，不改投影。删除必须保留高修订的不可见墓碑。

活动使用独立事件 `discovery.activity.v1`。Schema 为 `contracts/discovery-activity.schema.json`，版本 1.0.0；相邻 `.sha256` 文件记录原始 UTF-8 文件的 SHA-256，完整检查验证摘要及字段归属。内容事件仅承载检索文档。

活动快照保存在 search.activities，主键为 (source, id)，另以 (source, target.kind, target.id, day) 设置唯一约束。target.source 固定为 catalog，目标为一个规范课程或教师；课程活动不会自动复制到关联教师。

活动 ID 使用 `kind + "/" + base64url(UTF-8 对象 ID，无填充) + "/" + YYYY-MM-DD`，由 ActivityID 生成。事件 subject 等于活动 ID，封套 source 必须等于快照 source。不同来源可使用相同活动 ID，修订独立递增。

新修订完整替换同键快照，不把重复投递当作增量相加。零计数也必须显式发送。取消收藏、撤回或删除引起计数变化时，由拥有事实的模块发布更高修订；search 不反推来源事实。

| 来源 | metrics 必需且仅允许的字段 | 来源统计规则 |
| --- | --- | --- |
| catalog | views | 同一账号对同一课程或教师详情每 UTC 日只计一次 |
| reviews | reviews、interactions | 评价数与有效投票、回复互动数，依 reviews 的有效性规则 |
| favorites | favorites | 当日净新增，取消后不得为负；跨日取消归属由 favorites 的公开规则说明 |

metrics 字段值为 0–1000000000 的整数，禁止负值和来源未拥有的字段，即使越权字段值为 0 也拒绝。Schema 的三个分支同时约束封套和载荷的来源。运行时还校验真实日期、规范活动 ID 和封套一致性。

CurrentActivity 按请求人检查来源活动当前可见性与修订，Current 再检查目标当前权限。修订不匹配的快照暂停计入热度。来源明确未启用时返回 ErrSourceDisabled，该来源贡献为零；超时、查询失败和格式错误返回暂不可用。

重建先读取 catalog、reviews、resources 的 DiscoverySnapshot，再读取 catalog、reviews、favorites 的窗口内 ActivitySnapshot。每批最多 100 条，来源游标必须保持一致性快照、前进且无遗漏。来源首次返回 ErrSourceDisabled 时跳过该来源。

重建持有投影写锁，relay 可重试超时消费；来源快照事务应独立于 relay 确认，避免互相等待。内容与活动投影在同一事务发布，任一已启用来源失败时全部回滚。缺失活动保留不可见墓碑，阻止旧修订重新计数。

search.documents 只保存 catalog 的 course、teacher，reviews 的 review，以及 resources 的 resource。服务层与数据库均校验类型和来源，缺失或非法类型拒绝写入。热门活动由独立事件写入 search.activities。

重建与查询使用 PostgreSQL 事务隔离，查询保留旧的已提交投影，只有完整重建才发布新投影。失败整体回滚，低于已消费修订的快照拒绝。缺失对象保留不可见墓碑，延迟到达的旧事件无法复活内容。

初次部署需要成功重建后才能查询。重建后的事件通过 relay 继续消费。inbox 与墓碑当前不自动清理，清理策略必须先确认来源重放期限。正常索引更新目标为 60 秒，尚未进行端到端延迟验证。

## 来源事件与公开接口

下列事件为 search 消费约定。内容事件需要完整公开投影快照；活动事件需要所属来源的完整日计数。来源实现与 backend 适配的实际组合仍需联调验收。

| 来源 | 需要提供的事件 | 必需事实 |
| --- | --- | --- |
| catalog | catalog.discovery.v1 | 每个课程或教师的规范 ID、学校、公开标题、修订和可见状态；导入、合并、修改及删除均逐对象发出 |
| reviews | reviews.discovery.v1 | 有效课程评价的公开检索标题、规范课程 ID、教师归属、事件时间及可见状态；修订、撤回、到期和删除均发出 |
| resources | resources.discovery.v1 | 资料公开标题、学校、课程关联、修订及当前可见状态；更新、下架及删除均发出 |
| catalog、reviews、favorites | discovery.activity.v1 | 各来源拥有的日指标快照，字段和来源必须符合活动 Schema |

来源还需要提供按请求人检查当前对象与活动的批量或单条公开接口，以及包含墓碑的一致性快照分页接口。reviews 需要提供当前请求人的有效投票分页。community 当前只登记可选依赖，尚无事件接入。

活动来源必须各自提供公开快照和当前可见性接口。search 不读取来源业务表，不要求任何来源汇总其他模块的指标。

## 来源接口

Sources 是 backend 的来源分发适配器，source 参数选择提供方。接口形状如下，返回类型也可由来源定义等价 DTO，再由 backend 转换，来源无需依赖 search 包。limit 为 1–100，初次 cursor 为空，空 next 表示结束。fromDay 和 toDay 必须是 UTC 零点，范围为左闭右开。

```go
DiscoverySnapshot(ctx context.Context, source, cursor string, limit int) ([]Document, string, error)
ActivitySnapshot(ctx context.Context, source string, fromDay, toDay time.Time, cursor string, limit int) ([]ActivitySnapshot, string, error)
CurrentActivity(ctx context.Context, principal, source, activityID string) (ActivitySnapshot, bool, error)
```

catalog、reviews、favorites 各自实现以下提供方端口。source 由模块固定，不能接受调用方指定其他来源。分页后续请求沿用原窗口、快照版本和上限；来源查询通过公开 Repository/Service 接口提供。

```go
ActivitySnapshot(ctx context.Context, fromDay, toDay time.Time, cursor string, limit int) ([]search.ActivitySnapshot, string, error)
CurrentActivity(ctx context.Context, principal, activityID string) (search.ActivitySnapshot, bool, error)
```

ActivitySnapshot 返回窗口内完整日快照及墓碑。CurrentActivity 返回当前可读的快照；bool=false 表示目标不存在或不可读。来源未启用由 backend 适配器返回 ErrSourceDisabled，不能把普通来源故障映射为未启用。

## Backend 装配

1. 使用独立迁移身份调用 postgres.Migrate。创建 search 专用运行身份，只授予 schema USAGE，documents、activities 与 state 的 SELECT、INSERT、UPDATE，以及 inbox 的 SELECT、INSERT。禁止访问其他模块表。
2. 创建运行身份连接池，设置连接数与连接超时，调用 CheckRuntime。实现 Sources，以各模块公开接口提供当前权限查询、重建快照及我的投票。
3. 从后台配置读取最低活动样本量、热度窗口、权重和查询超时，调用 New。使用后台重建任务调用 Rebuild，成功后开放 HTTP 能力。
4. 把 httpapi.Handler 挂到 /api/v1/search 路由，Authenticate 从 platform 已验证身份提取稳定请求人标识，不能接受客户端自报的用户或学校。
5. 将内容 discovery.v1 事件接入 Service.Consume，将 catalog、reviews、favorites 的 discovery.activity.v1 接入 Service.ConsumeActivity。relay 绑定已认证来源。业务与 outbox 同事务执行 pg_notify，relay LISTEN 各模块频道，每批最多 100 条。
6. relay 启动及重连后立即补读，每 60 秒读取未发送部分索引兜底。消费成功后确认发送；失败从 5 秒指数退避，上限一小时，累计 24 小时进入待处理队列，重试保留原事件 ID。
7. 来源不可用时保留已完成业务写入，相关查询返回暂不可用。来源修订更新后核对权限撤销、删除、投影重建和重复投递，并测量 60 秒更新目标。

relay、生产模块 outbox 与公开接口适配器由 backend 及来源负责人装配。本仓提供消费者，未修改 backend 依赖、工作区锁文件或任何来源模块。

## 验证范围

scenario_tests 登记 DS-01、DS-04 至 DS-07。集成测试使用隔离 PostgreSQL，覆盖权限过滤、分页、热度权重与窗口、热门教师、当前投票、并发去重、旧修订、重建回滚及运行身份越权拒绝。

| 场景 | 验收结果 | 测试函数 |
| --- | --- | --- |
| DS-05：热度配置 | 多来源汇总、来源缺失、权重、UTC 窗口、最低样本量及权限生效，越权指标拒绝 | TestHeatWeightsWindowAndTeachers、TestActivityOwnership |
| DS-06：投影写入失败 | 投影与 inbox 事务整体回滚，不保留部分写入 | TestProjectionFailureRollback、TestActivityReplayAndRebuild |
| DS-07：重复乱序与重建 | 重复事件去重，旧修订不覆盖新状态，重建失败回滚，运行身份越权被拒绝，内容类型受数据库约束 | TestContentDocumentKinds、TestProjectionReplayRebuildAndIsolation、TestActivityReplayAndRebuild、TestActivitySnapshotPagination |

2026-11-09 在隔离 PostgreSQL 18.6 上执行完整 make check，通过 SDK 一致性、仓库边界、文档、gofmt、vet、race 测试、构建及工作流检查。真实账号、跨模块 relay、来源接口适配器与生产部署均未验收。
