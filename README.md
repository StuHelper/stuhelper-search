# StuHelper Search

search 提供关键词搜索、热门课程与教师、我的投票查询。当前实现为独立 Go 库，由 backend 注入身份认证、来源公开接口和事件 relay。跨模块装配尚未完成。

课程与教师五维评分统计由 `GET /api/v1/reviews/statistics` 提供。客户端对 2–5 门课程分别读取 reviews 统计，组合展示课程对比。

OpenAPI 版本为 0.2.0，HTTP 路径与响应未变化，接口定义位于 `contracts/search.openapi.json`，Go 和 TypeScript SDK 位于 `sdk/`。详细数据约定、事件缺口及装配步骤见 [模块说明](docs/spec/search.md)。

热门活动由 catalog（浏览）、reviews（评价与互动）、favorites（收藏）分别提供日快照。活动事件使用 `discovery.activity.v1`，契约见 `contracts/discovery-activity.schema.json`（1.0.0），摘要见相邻 `.sha256` 文件。来源接口签名及字段规则见模块说明。

```bash
pnpm install --frozen-lockfile
pnpm run hooks
make generate
bash scripts/check-with-postgres.sh
```

数据库检查创建独立、仅 Unix socket 可访问的 PostgreSQL 集群，退出时清理。可用 `STUHELPER_TEST_PG_BIN` 指定 PostgreSQL 二进制目录。每项测试创建独立数据库，不连接业务数据库。

公开仓库为 [StuHelper/stuhelper-search](https://github.com/StuHelper/stuhelper-search)，默认分支为 `develop`，候选分支为 `feat/backend-decisions`，通过 [草稿 PR #1](https://github.com/StuHelper/stuhelper-search/pull/1) 交付。
