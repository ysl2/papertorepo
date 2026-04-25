# 后端公共设计

## 1. 文档范围

这组 design 先只覆盖三条后端能力：

- `sync-papers`：把论文同步进系统；当前 provider 是 arXiv
- `find-repos`：为论文寻找 GitHub 仓库链接
- `refresh-metadata`：刷新已发现 GitHub 仓库的元数据

这三条能力组成一条顺序明确的数据链路：

1. 先有论文。
2. 再判断论文是否关联代码仓库。
3. 最后刷新仓库本身的 GitHub 信息。

## 2. 公共业务对象

### 2.1 Paper

`Paper` 是系统中的论文主体。它承载：

- arXiv 标识与链接
- 标题、摘要、作者、分类
- 发布时间、更新时间
- 原始评论字段

`sync-papers` 负责创建和更新它。

### 2.2 PaperRepoState

`PaperRepoState` 是“这篇论文当前是否已经找到稳定仓库答案”的状态对象。它承载：

- 当前稳定状态：`found` / `not_found` / `ambiguous` / `unknown`
- 主仓库 URL
- 候选仓库 URL 列表
- 下次允许刷新时间
- 上一次尝试时间、是否完整、错误信息

`find-repos` 负责更新它。

### 2.3 GitHubRepo

`GitHubRepo` 是已经进入系统视野的 GitHub 仓库实体。它承载：

- 规范化后的 GitHub URL
- owner / repo
- stars、description、topics、license、homepage
- archived、pushed_at
- 首次见到时间与最近检查时间

`refresh-metadata` 负责更新它。

### 2.4 RepoObservation 与 RawFetch

系统保留两层证据：

- `RepoObservation`：一次“在哪个来源、哪个页面上看到了什么”的结构化观察
- `RawFetch`：对应的原始响应快照，便于追查抓取行为和解析问题

这意味着三条能力都不是“只保留最终结果”，而是会尽量保留过程证据。

## 3. 公共输入模型：Scope

这三条能力都围绕 `scope` 工作，但使用方式不同。

### 3.1 Scope 的核心字段

- `categories`
- `day`
- `month`
- `from` / `to`
- `force`

当前三条后端任务都要求显式给出 `categories` 和时间窗口。

### 3.2 时间窗口规则

时间窗口只允许三种表达方式之一：

- 单日：`day`
- 整月：`month`
- 日期范围：`from` + `to`

规则如下：

- `day` 不能和 `month` / `from` / `to` 同时出现
- `month` 不能和 `day` / `from` / `to` 同时出现
- `from` 和 `to` 必须同时出现，且 `from` 不能大于 `to`

系统会把等价输入规范化。例如：

- 同一天的 `from/to` 会被收敛成 `day`
- 完整自然月的 `from/to` 会被收敛成 `month`

### 3.3 `force` 的统一语义

`force=true` 表示跳过“是否到期”的判断，强制重新执行当前 scope 对应的业务动作。

它不等于“忽略所有并发保护”：

- 仍然会受作业去重影响
- 仍然会受资源锁影响

## 4. 公共任务模型

### 4.1 串行队列

当前系统是单队列、单 worker、串行执行：

- 同一时刻只会有一个被 worker 认领的任务
- 后续任务按顺序排队

这是一种刻意保守的运行模型，优先保证可理解性和可恢复性。

### 4.2 任务去重

同步类任务会基于“任务类型 + 规范化 scope”生成去重键。

因此，当同一种任务、同一个 scope 已经存在 `pending` 或新鲜 `running` 任务时，不再创建第二个等价任务。

### 4.3 批量根任务与子任务

当一个 scope 太大时，系统不会直接把它当成一个扁平任务执行，而会先创建批量根任务，再拆成子任务：

- `sync-papers`：按 `category x archive_month` 拆分
- `find-repos`：按月份优先拆分
- `refresh-metadata`：按月份优先拆分

这样做的目的不是并行执行，而是：

- 让失败范围更小
- 让重跑可以只补局部
- 让进度和结果更容易解释

### 4.4 停止与重跑

任务支持停止，也支持重跑。

重跑的设计目标不是“再来一遍全量”，而是“进入同一条修复链”：

- 新尝试会沿用同一个 `attempt_series`
- 批量修复时，已经成功的子任务可以被复用
- 真正需要补跑的 scope 才重新入队

## 5. 公共正确性原则

### 5.1 资源级锁

除了串行队列外，系统还会对具体资源加锁，避免不同入口重复处理同一资源：

- arXiv 同步按分类/窗口加锁
- 仓库发现按论文加锁
- 元数据刷新按 repo URL 加锁

锁拿不到时，当前任务会跳过该资源，而不是阻塞整个任务。

### 5.2 尽量保留“最近一个稳定答案”

如果一次运行没有得到完整结论，系统优先保留之前已经确定的稳定结果，而不是轻易把状态打回空白。

这条原则尤其适用于 `find-repos`。

### 5.3 原始抓取可追溯

只要发生网络抓取，系统原则上都会保留对应的原始响应快照，方便：

- 调试解析逻辑
- 复核第三方站点返回内容
- 在不重新请求外部服务的情况下复盘问题

## 6. 公共配置关注点

虽然实现细节分散在代码里，但从 design 视角，当前最重要的公共配置有：

- `DATABASE_URL`
- `DATA_DIR`
- `DEFAULT_CATEGORIES`
- `JOB_QUEUE_WORKER_POLL_SECONDS`
- `JOB_QUEUE_RUNNING_TIMEOUT_SECONDS`

这几个配置分别影响：

- 数据写入位置
- 运行时产物目录
- 默认分类选择
- 后台任务的拉取频率
- 运行中任务被视为过期的时间阈值

后续 feature design 只补充各自专属配置，不再重复这部分。

## 7. 公共常量命名原则

无论常量是否暴露给用户，都按所属模块加前缀命名。

规则如下：

- `sync-papers` 相关常量使用 `SYNC_PAPERS_...` 或 `SYNC_PAPERS_ARXIV_...`
- `find-repos` 相关常量使用 `FIND_REPOS_...`
- `refresh-metadata` 相关常量使用 `REFRESH_METADATA_...`
- 公共 HTTP 常量使用 `HTTP_...`
- 公共任务队列常量使用 `JOB_QUEUE_...`
- 停止任务相关常量使用 `JOB_STOP_...`

这个规则同时适用于：

- `.env` / `.env.example` 中暴露给用户的配置
- 代码内部写死但不暴露给用户的常量
- 以前 inline 的关键硬编码值

token 相关配置例外：它们按外部平台集中命名，继续使用 `GITHUB_TOKEN`、`HUGGINGFACE_TOKEN`、`ALPHAXIV_TOKEN`。

## 8. 当前写死但需要让用户知道的公共常量

以下常量当前写死在代码里，还没有暴露成配置项，但会直接影响三条后端能力的运行行为。

### 8.1 外部请求超时

所有外部 HTTP 请求共用一套基础超时策略：

- `HTTP_TOTAL_TIMEOUT = 20` 秒
- `HTTP_CONNECT_TIMEOUT = 10` 秒

这意味着：

- 第三方平台响应很慢时，请求不会无限等待
- 但当前也不能通过配置把超时调大或调小

### 8.2 外部请求重试

系统默认采用统一的瞬时错误重试策略：

- `HTTP_MAX_RETRIES = 2`
- 只对超时、网络异常，以及 `429 / 500 / 502 / 503 / 504` 这类状态做重试
- `HTTP_RETRY_BASE_DELAY_SECONDS = 0.2`
- `HTTP_RETRY_MAX_DELAY_SECONDS = 3.0`
- `HTTP_RETRY_JITTER_RATIO = 0.1`

这意味着：

- 并不是所有失败都会自动重试
- 对限流和临时故障有基础恢复能力
- 但当前重试强度本身不是用户可配置项

### 8.3 队列内部常量

任务队列还有少量内部常量：

- `JOB_QUEUE_INIT_DATABASE_LOCK_ID`
- `JOB_QUEUE_REUSED_CHILD_LOCKED_BY`

它们不需要暴露给用户，但也需要遵守公共前缀规则。
