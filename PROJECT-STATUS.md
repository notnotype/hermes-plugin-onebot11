# PROJECT-STATUS

仓库级现状报告。TODO / 跟进事项一律开 GitHub Issue,不写进本文件。

## 当前状态

- **阶段**：首个功能开发中（issue #1：OneBot 11 接入最小闭环）
- **最近验证**：无（尚未与真实 QQ 框架联调）
- **重点**：消息解析 → 反向 WS → HTTP 发送 → 工具与权限 → adapter 组装 → 联调

## 模块状态

| 模块 | 状态 | 说明 |
|---|---|---|
| onebot11/message.py | 未开始 | array 消息段解析 |
| onebot11/events.py | 未开始 | OneBot 事件 → MessageEvent |
| onebot11/ws_server.py | 未开始 | 反向 WS 服务端 |
| onebot11/http_api.py | 未开始 | HTTP 发送与动作调用 |
| onebot11/tools.py | 未开始 | 平台工具（消息查询） |
| onebot11/permissions.py | 未开始 | 权限模型 |
| adapter.py | 未开始 | 适配器主类 + register |
| plugin.yaml | 未开始 | 插件元数据 |

## 风险

- 群聊权限是安全底线：会话范围校验必须过测试。
- 反向 WS 依赖 Hermes 侧常驻服务,LLBot 断线自动重拨。

## 近期任务

- 见 [docs/tasks/](docs/tasks/) 的 active task walkthrough
