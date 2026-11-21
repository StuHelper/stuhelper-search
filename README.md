---
type: index
status: Accepted
updated: 2026-11-21
---

# StuHelper Search

search 提供学校、课程、教师、评价和学习资料检索，热门课程、教师和资料排行，以及本人投票查询。backend 通过可信身份和来源模块的公开接口装配查询，并接收来源事件维护投影。

HTTP 接口采用 OpenAPI 0.4.0，Go 和 TypeScript SDK 位于 [sdk](sdk)。HTTP、内容事件和活动事件的定义位于 [contracts](contracts)。

- [搜索与热门发现规格](docs/spec/search.md)说明排序、分页、当前权限和投影规则。
- [装配指南](docs/guide/integration.md)说明来源端口、数据库权限和事件投递。
- [实施状态](docs/development/implementation.md)记录场景覆盖、生产启用情况和待验证事项。

```bash
make setup
bash scripts/check-with-postgres.sh
```

检查脚本创建仅监听独立 Unix socket 的 PostgreSQL 集群，采用内建 `C.UTF-8` locale，退出时清理。默认从 Debian 系的 `postgresql` 软件包定位程序，也可用 `STUHELPER_TEST_PG_BIN` 指定二进制目录。

修改 OpenAPI 后运行 `make generate`。使用 `pnpm run hooks` 安装 Lefthook。
