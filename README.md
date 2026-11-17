# StuHelper Search

search 提供学校代码、名称与别名检索，课程（含课程号、曾用课号、英文名与曾用名）、教师、评价与学习资料关键词检索，热门课程与教师排行，以及我的投票查询。本仓库为独立 Go 模块库，由 backend 注入身份认证、来源公开接口与事件 relay。

课程与教师五维评分统计由 `GET /api/v1/reviews/statistics` 提供。客户端对 2–5 门课程分别读取 reviews 统计，组合展示课程对比。

OpenAPI 版本为 0.4.0，接口定义位于 [contracts/search.openapi.json](contracts/search.openapi.json)，Go 和 TypeScript SDK 位于 `sdk/`。详细数据约定、事件协议及装配方式见 [模块说明](docs/spec/search.md)。

内容投影事件契约见 [contracts/discovery-event.schema.json](contracts/discovery-event.schema.json)；热门活动由 catalog（浏览）、reviews（评价与互动）、favorites（收藏）分别提供日快照，契约见 [contracts/discovery-activity.schema.json](contracts/discovery-activity.schema.json)（1.0.0），两份 Schema 摘要均记录于相邻 `.sha256` 文件并由模块检查校验。

```bash
pnpm install --frozen-lockfile
pnpm run hooks
make generate
bash scripts/check-with-postgres.sh
```

数据库检查创建独立、仅 Unix socket 可访问的 PostgreSQL 集群，退出时清理。可用 `STUHELPER_TEST_PG_BIN` 指定 PostgreSQL 二进制目录。每项测试创建独立数据库，不连接业务数据库。

公开仓库为 [StuHelper/stuhelper-search](https://github.com/StuHelper/stuhelper-search)，默认分支为 `develop`。
