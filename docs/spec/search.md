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

热门排行使用窗口内的每日活动投影。四项计数顺序为浏览、评价、收藏、互动，均为该日增量快照，修订替换旧快照。窗口、非负权重和最低活动样本量由后台配置。按加权热度降序，同分按来源和 ID 排序。

## 事件消费与重建

事件 JSON Schema 位于 `contracts/discovery-event.schema.json`。

Service.Consume 接收 CloudEvents 1.0，source 为 catalog、reviews 或 resources，type 为来源名加 .discovery.v1，subject 与 data.target.id 一致。data 为完整 Document 快照，revision 为该来源对象单调递增修订号。

消费在一个事务内更新投影及 inbox，以 source 和事件 ID 去重。重复事件返回成功，相同 ID 不同载荷拒绝。低于或等于现有修订的事件只记 inbox，不改投影。删除必须保留高修订的不可见墓碑。

活动投影使用独立的 kind=activity 和稳定的来源活动 ID，建议由对象 ID 与 UTC 日期组成。活动修订独立递增，At 为统计桶时间，CourseID、TeacherIDs 指向规范对象。关键词搜索候选不会包含活动文档。

Sources.Current 对活动 ID 必须检查其来源对象及关联内容当前可见性，返回当前仍有效的计数。活动因撤回或删除发生变化时提高修订号。当前修订不匹配时暂停计入热度，防止旧计数泄漏隐藏活动。

重建依次读取 catalog、reviews、resources 的一致性快照，包含墓碑。来源分页必须保证完整、无遗漏且游标前进。重建持有投影写锁，relay 可重试超时消费；来源快照事务应独立于 relay 确认，避免互相等待。

重建与查询使用 PostgreSQL 事务隔离，查询保留旧的已提交投影，只有完整重建才发布新投影。失败整体回滚，低于已消费修订的快照拒绝。缺失对象保留不可见墓碑，延迟到达的旧事件无法复活内容。

初次部署需要成功重建后才能查询。重建后的事件通过 relay 继续消费。inbox 与墓碑当前不自动清理，清理策略必须先确认来源重放期限。正常索引更新目标为 60 秒，尚未进行端到端延迟验证。

## 来源事件与公开接口缺口

下列事件为本模块所需的新公开约定，当前来源模块尚未提供完整载荷。现有 catalog.corrected、state_changed、merged、imported 及 reviews、resources 业务事件不能直接当作完整投影快照使用。

| 来源 | 需要提供的事件 | 必需事实 |
| --- | --- | --- |
| catalog | catalog.discovery.v1 | 每个课程或教师的规范 ID、学校、公开标题、修订和可见状态；导入、合并、修改及删除均逐对象发出 |
| reviews | reviews.discovery.v1 | 有效课程评价的公开检索标题、规范课程 ID、教师归属、事件时间及可见状态；修订、撤回、到期和删除均发出 |
| resources | resources.discovery.v1 | 资料公开标题、学校、课程关联、修订及当前可见状态；更新、下架及删除均发出 |
| 三个来源 | 对应 discovery.v1 活动文档 | 独立每日活动 ID、修订、统计时间及四类计数；未拥有的指标留零，不复制其他模块事实 |

来源还需要提供按请求人检查当前对象与活动的批量或单条公开接口，以及包含墓碑的一致性快照分页接口。reviews 需要提供当前请求人的有效投票分页。community 当前只登记可选依赖，尚无事件接入。

收藏等指标如由 favorites 拥有，须先由负责人确定事件归属和公开聚合接口，再加入活动源。不能由 search 扫描 favorites 或其他来源的业务表生成这些计数。

## Backend 装配

1. 使用独立迁移身份调用 postgres.Migrate。创建 search 专用运行身份，只授予 schema USAGE，documents 与 state 的 SELECT、INSERT、UPDATE，以及 inbox 的 SELECT、INSERT。禁止访问其他模块表。
2. 创建运行身份连接池，设置连接数与连接超时，调用 CheckRuntime。实现 Sources，以各模块公开接口提供当前权限查询、重建快照及我的投票。
3. 从后台配置读取最低活动样本量、热度窗口、权重和查询超时，调用 New。使用后台重建任务调用 Rebuild，成功后开放 HTTP 能力。
4. 把 httpapi.Handler 挂到 /api/v1/search 路由，Authenticate 从 platform 已验证身份提取稳定请求人标识，不能接受客户端自报的用户或学校。
5. 将三个来源的 discovery.v1 事件订阅到 Service.Consume。生产模块须在业务及 outbox 同一事务执行 pg_notify。backend relay LISTEN 各模块 outbox 频道，每批最多 100 条。
6. relay 启动及重连后立即补读，每 60 秒读取未发送部分索引兜底。消费成功后确认发送；失败从 5 秒指数退避，上限一小时，累计 24 小时进入待处理队列，重试保留原事件 ID。
7. 来源不可用时保留已完成业务写入，相关查询返回暂不可用。来源修订更新后核对权限撤销、删除、投影重建和重复投递，并测量 60 秒更新目标。

relay、生产模块 outbox 与公开接口适配器由 backend 及来源负责人装配。本仓提供消费者，未修改 backend 依赖、工作区锁文件或任何来源模块。

## 验证范围

scenario_tests 登记 DS-01、DS-04，以及热度配置和投影事务场景。集成测试使用隔离 PostgreSQL，覆盖权限过滤、分页、热度权重与窗口、热门教师、当前投票、并发去重、旧修订、重建回滚及运行身份越权拒绝。

2026-11-09 在隔离 PostgreSQL 18.6 上执行完整 make check，通过 SDK 一致性、仓库边界、文档、gofmt、vet、race 测试、构建及工作流检查。真实账号、跨模块 relay、来源接口适配器与生产部署均未验收。
