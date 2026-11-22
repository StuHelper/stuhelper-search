---
type: guide
status: Accepted
updated: 2026-11-21
---

# 工程约定

遵循 StuHelper 工程约定。search 管理搜索投影、活动日快照、消费回执、排行快照和索引状态。来源拥有业务事实。读取当前权限、记录状态和修订时使用来源公开端口，SQL 留在 `postgres`。

先维护 OpenAPI，再运行 `make generate` 更新 SDK。投影与 inbox 在同一事务写入，消费按来源、类型和事件 ID 去重。查询日志仅记录脱敏诊断。

按 [module.yaml](module.yaml) 的 `scenario_tests` 维护场景与测试函数的对应关系。完整检查运行 `bash scripts/check-with-postgres.sh`。
