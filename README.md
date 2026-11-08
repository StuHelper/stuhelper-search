# StuHelper Search

发现与统计模块提供搜索、热门课程与教师、五维统计、课程对比及我的投票查询。当前实现为独立 Go 库，由 backend 注入身份认证、来源公开接口和事件 relay。跨模块装配尚未完成。

接口定义位于 `contracts/search.openapi.json`，Go 和 TypeScript SDK 位于 `sdk/`。详细数据约定、事件缺口及装配步骤见 [模块说明](docs/spec/search.md)。

```bash
pnpm install --frozen-lockfile
pnpm run hooks
make generate
bash scripts/check-with-postgres.sh
```

数据库检查创建独立、仅 Unix socket 可访问的 PostgreSQL 集群，退出时清理。可用 `STUHELPER_TEST_PG_BIN` 指定 PostgreSQL 二进制目录。每项测试创建独立数据库，不连接业务数据库。

本地 Git 工作分支为 `feat/backend-decisions`。负责人创建公开远程仓库 `StuHelper/stuhelper-search` 后配置默认分支 `develop`，再接入 PR 与必需检查。当前没有远程仓库或远程 CI 结果。
