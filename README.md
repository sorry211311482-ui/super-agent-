# SuperAgent

一个本地运行的 SuperAgent。Router 接收用户请求和环境事件，独立的 Worker 并行读取资料、调用工具并交付文件。来往消息和后续事件通过订阅找到原任务。

## 启动

需要 macOS 或 Linux、Python 3.9+ 和支持权限配置的 Codex CLI。本机验收使用 Codex CLI 0.167.2。模型调用使用 CLI 已登录的账号，可以通过环境变量 `SUPERAGENT_MODEL` 指定模型。

```sh
codex login status
python3 app.py
```

在浏览器打开 [SuperAgent 工作台](http://127.0.0.1:8765)。如果 CLI 尚未登录，请先运行 `codex login`。

服务默认使用项目里的 `inbox/` 作为投递目录，使用 `.superagent/` 保存数据库、材料快照和每个任务的工作目录。可以用 `--inbox`、`--state-dir`、`--port` 和 `--slots` 调整，默认并行运行两个 Worker。

任务和所用附件内容会通过已登录的 Codex CLI 发送给模型服务。启动地址仅监听本机。Worker 在专用目录中运行，可以读入本轮材料并在任务目录里生成结果。

## 用法

1. 在聊天框里描述目标和想要拿到的结果，可附加文件。比如提交 Listing 文案与产品资料，请它逐条审核并交付 Markdown。
2. 如果任务需要等待补件，在 `inbox/` 下创建一个子目录，把初始材料放进去，在发送这条任务时选择该目录。比如在 `inbox/P27/` 放三份报价，让 Agent 对比含税含运费总价。每条带目录或附件的消息用于一项工作，便于确定材料归属。
3. 如果缺少一家运费，任务可以保存草稿并进入等待。这期间仍然可以发起和完成其他任务。把补件放进已订阅的目录即可唤醒比价任务。你也可以从任务面板直接回复。
4. 从任务面板下载交付文件。服务重启后会继续处理已接收的任务和目录更新。运行中的任务可能从上一轮记录重新执行。

## 让来源主动触发任务

在工作台的**来源规则**中填写一个来源 ID 和它被授权处理的任务目标。新接入的来源可以使用三种模式。

| 模式 | 处理方式 |
| --- | --- |
| 关闭 | 记录事件，跳过模型评估和执行。未配置规则的来源也按此处理。 |
| 观察 | Router 判断事件是否值得接单，记录判断。 |
| 自动 | Router 路由符合授权目标的事件，Worker 执行并交付。已订阅消息串的后续事件直接送到原任务。 |

可以在工作台提交标准事件，也可以由本机脚本通过同一个 HTTP 接口投递。来源用 `source` 标识，`source_id` 是来源内稳定的事件编号，`thread` 是该来源内可复用的消息串标识。重发同一个编号只记录一次。同一消息串的后续事件使用相同的 `thread` 和新的 `source_id`。

```sh
curl -fsS http://127.0.0.1:8765/api/policies \
  -H 'Content-Type: application/json' \
  -d '{"source":"docs","goal":"审阅收到的文档，输出一份改进建议。","mode":"observe"}'

curl -fsS http://127.0.0.1:8765/api/events \
  -H 'Content-Type: application/json' \
  -d '{"source":"docs","source_id":"manual-001","thread":"setup-guide","text":"运行说明已更新，准备提交评审。"}'
```

事件正文和附件作为工作材料。来源规则提供接单和执行的授权范围。事件支持通过 `attachments` 提交文件，每项包含文件名 `name` 和 Base64 内容 `data`。

在任务卡里可以展开 **执行记录**，查看工具调用和交付文件。任务失败后可用**重试**将原来未完成的输入重新入队。

## 实现

- `app.py` 和 `web/` 提供本地操作界面、附件入口和下载。
- `orchestrator.py` 持久化事件、任务、订阅和投递。可确定归属的事件会直接投到任务；其它请求交给 Router。Worker 之间并行，同一任务的更新按顺序消费。
- `executor.py` 使用 Codex CLI 做结构化路由与实际工具执行。每项任务有独立工作目录及可续接的 Worker 会话。
- `inbox.py` 扫描已订阅的目录，保留不可变的文件快照，并用文件版本给重复投递去重。
- `activity.py` 保存每项任务的工具和交付记录。

架构取自 DeepSeek 的事件驱动调度思路。当前入口包括本地对话、订阅目录和通用事件 API，SQLite 支持单个服务实例。[DeepSeek SuperAgent 领域模型](https://github.com/deepseek-ai/dsh-superagent-prototype/blob/main/docs/001-domain.md)

早期的离线清单示例可通过 `python3 superagent.py demo` 运行。
