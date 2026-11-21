---
type: development
status: Accepted
updated: 2026-11-21
---

# Search 实施状态

## 当前实现

search 提供独立 Go 服务、PostgreSQL 仓储、HTTP Handler、OpenAPI 和 SDK。backend 已装配 catalog、reviews、resources 的内容端口及 catalog、reviews、favorites 的活动端口，在重建成功后开放查询。

检索支持多词匹配、学校代码、课程历史字段、组合筛选及按时间浏览。热门榜保留同次分页的排名，每页复核当前权限。事件消费、并发重放和重建使用各自的投影更新事务。

## 验证范围

[module.yaml](../../module.yaml) 的 `scenario_tests` 是场景与 Go 测试函数的登记入口。检查核对引用及内容、活动契约的 SHA-256。

| 场景 | 本仓测试覆盖 |
| --- | --- |
| DS-01 | 当前权限过滤、热门课程和教师及资料、固定排行续页、多维筛选、SDK 契约 |
| DS-04 | 本人投票、可见性过滤、自动补页、超时和返回摘要 |
| DS-05 | 活动所有权、北京时间窗口、权重、最低样本量、当前修订及榜单快照续期 |
| DS-06 | inbox 与投影回滚、并发写入、重复消费和无效输入 |
| DS-07 | 历史回执、乱序修订、重建隔离、运行身份权限与有界清理 |
| DS-SCHOOL-01 | 五位和完整代码、名称、别名、字面查询、分页、当前撤权及迁移 |
| DS-COURSE-01 | 当前课名、课号、英文名、曾用课号及曾用名，多词检索和事件输入校验 |

CI 在 develop、main 的推送和目标分支为两者的 PR 上运行隔离 PostgreSQL 检查。检查含 SDK 一致性、仓库与模块规则、文档、Go 格式、vet、竞态测试、构建和工作流校验。

## 生产与待验证

StuHelper v0.1.31 生产基线记录已启用搜索，发行组合锁定 search v0.1.2。[StuHelper v0.1.31](https://github.com/StuHelper/stuhelper/releases/tag/v0.1.31)

真实账号的跨模块可见性验证和从来源提交到索引可见的 60 秒目标仍待验证。
