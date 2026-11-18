# SuperAgent

一个本地运行的 SuperAgent。你在同一个窗口交代多项工作，Router 识别任务归属，独立的 Worker 并行读取资料、调用工具并交付文件。后续消息和订阅目录的新材料可以唤醒原有任务。

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

## 实现

- `app.py` 和 `web/` 提供本地操作界面、附件入口和下载。
- `orchestrator.py` 持久化事件、任务、订阅和投递。可确定归属的事件会直接投到任务；其它请求交给 Router。Worker 之间并行，同一任务的更新按顺序消费。
- `executor.py` 使用 Codex CLI 做结构化路由与实际工具执行。每项任务有独立工作目录及可续接的 Worker 会话。
- `inbox.py` 扫描已订阅的目录，保留不可变的文件快照，并用文件版本给重复投递去重。

架构取自 DeepSeek 的事件驱动调度思路。当前入口是本地对话和目录，SQLite 支持单个服务实例。[DeepSeek SuperAgent 领域模型](https://github.com/deepseek-ai/dsh-superagent-prototype/blob/main/docs/001-domain.md)

早期的离线清单示例可通过 `python3 superagent.py demo` 运行。
