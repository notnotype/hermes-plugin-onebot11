---
name: repository-research
description: 用户要求高级仓库调研、架构调查、运行验证或浏览器证据时使用。
metadata:
  displayName: 仓库高级调研
  version: "1.0.0"
---

# 仓库高级调研与运行证据

## 阶段一：接收 Goal

记录：

- **outcome**：用户要看到什么可感知结果。
- **verification surface**：由哪条命令、页面、测试、日志或 artifact 证明。
- **constraints**：权限、数据、网络、时间和输出边界。
- **boundaries**：允许读取、启动和修改的仓库、目录与 adapter。
- **iteration policy**：每次尝试后按最小可验证下一步推进。
- **blocked stop**：没有真实证据时停止，报告已尝试路径、证据、阻塞和解锁输入。

先返回既有中文进度回执，再进入后台调研；运行时不向用户索要能从仓库或 profile 推导的信息。

## 阶段二：环境准备

读取仓库级 Agent 约定、贡献流程、状态报告、相关 reference 和 task walkthrough。建立本次任务独立的 temp/evidence 根；所有运行态 State/Cache 由项目 adapter 创建并拥有，最终 evidence 不随运行态清理。

真实 Project、Provider 或外部服务缺失时标记 `未验证`，不把 mock、focused test 或静态读取写成真实业务证据。

## 阶段三：结构调研

并行读取互不依赖的入口、状态模型、权限/租约、进程/资源生命周期和已有测试。每个结论附 `file:line` 或 symbol；没有读到的事实写 `unverified — confirm first`，不凭命名推断。

重点核对：谁拥有服务、谁拥有浏览器、谁能删除临时目录、哪些路径属于共享缓存、失败是否能区分环境与产品。

## 阶段四：adapter 执行

查找匹配 `nbook.repository-research-adapter/v1` 的项目 profile。只按 profile 中的 `command`、参数、入口路径、视口和媒体根执行；通用层不拼接项目命令或目录。

服务生命周期只认识五个能力：`start`、`ready`、`browserEntry`、`evidenceDir`、`stop`。服务必须由项目 adapter 持有；浏览器和临时根必须进入 finally。浏览器使用真实 executable；不存在时不下载、不修改系统安装。

运行前加载本 Skill 和项目 profile。child 使用前台 terminal 调用 profile command；不要把服务或浏览器交给裸后台 shell 常驻。

## 阶段五：证据

保存以下证据：命令摘要、版本与 commit、服务 mode/ready、浏览器 executable 与 viewport、console/page error、关键资源失败、截图、媒体路径和版本化 manifest。截图必须来自真实 adapter entry path；focused test 不能冒充真实浏览器或真人证据。
同一 manifest 中，`manifest.evidence.files` 只表示保留在 evidence 目录的证据文件名，不能作为 `MEDIA:` 路径；只有 `manifest.evidence.mediaFiles` 中的安全绝对路径才允许逐行回传。不得从 `repository.root`、截图文件名或 evidence 路径自行重构媒体路径；`mediaFiles` 缺失或为空时报告未验证/阻塞，不输出 `MEDIA:`。
manifest 是回传事实的唯一来源。凭据、token、完整启动 nonce、原始命令参数和真实 Project 内容不进入 manifest、日志或最终正文。

## 阶段六：判定

严格区分：

- **通过**：入口真实可达，约束内证据齐全，服务端口已关闭，owned temp roots 已删除。
- **部分通过**：部分证据齐全，但有明确未覆盖边界。
- **未验证**：页面或真实 provider 可达性不足，不能下产品结论。
- **环境阻塞**：浏览器、端口、依赖、导航或宿主资源无法满足运行条件。
- **发现问题**：稳定 HTTP 500、组件异常、pageerror、关键脚本/样式失败或违反业务合同。

按“场景 → 影响 → 证据 → 原因 → 位置 → 置信度”输出。稳定业务错误标为发现问题，不改用其它页面绕过。

## 阶段七：回传

先给结论，再给代码证据、运行证据、资源回收证据和未验证/阻塞。媒体只使用 manifest 中的安全绝对路径，并逐行使用 `MEDIA:<absolute-path>`。没有真人客户端入站时，只报告插件测试、Hermes 组合和受控 payload 证据；不要写成真人消息链路通过。

## 安全边界

使用项目 adapter 的 owned process、租约和关闭协议。不要调用平台消息、定时任务、再次委派或其它 QQ 能力；不要调用裸后台 shell、按 PID/进程名/端口杀进程、删除共享 cache、读取或输出凭据，也不要在没有用户授权时修改业务代码或真实数据。
