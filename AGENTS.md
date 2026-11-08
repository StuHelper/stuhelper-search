# 工程约定

遵循 StuHelper Next 工程约定。search 只拥有投影和 inbox，不得读取其他模块业务表。先维护 OpenAPI，再生成 SDK。SQL 留在 postgres，投影写入与 inbox 同事务，查询必须通过 Sources 检查当前权限和内容状态。
