# sync-arxiv

## 1. 目标

`sync-arxiv` 的目标是把指定 arXiv 范围内的论文带入系统，并维护“论文在哪个分类、哪个归档月份里出现过”的记录。

它解决的问题是：

- 给系统建立论文主表
- 让后续 `find-repos` 有可处理的论文集合
- 让按分类、按月份、按日期范围的查询具备稳定数据基础

它不负责：

- 猜测代码仓库
- 刷新 GitHub 仓库元数据

## 2. 输入与范围

### 2.1 必填输入

- `categories`

### 2.2 支持的时间模式

- `day`
- `month`
- `from` + `to`

### 2.3 额外输入

- `force`

## 3. 业务模式

### 3.1 单日模式

当输入是 `day` 时，系统按日期同步某一日论文。

当前实现分成两种抓取路径：

- 对“过去且距离今天不超过 90 天”的日期，优先走 arXiv catchup 页面，然后走id_list_feed补齐细节
- 对今天，或更久以前的单日，走 submitted-date feed，然后走 id_list_feed 补齐细节

这样做的目标是兼顾近期页面可读性和较老日期的可获得性。

### 3.2 整月模式

当输入是 `month` 时，系统按该月的 arXiv listing 页面抓取论文 ID，再按 id_list_feed 回填完整 metadata。

这是一种“先拿目录，再补详情”的模式。

### 3.3 日期范围模式

当输入是 `from/to` 时，系统按自然月切成多个执行单元。

当前行为有一个重要约束：

- 调度和 TTL 判断按请求范围工作，但TTL update仍然按照拿到的结果范围更新。
- 但实际抓取载体仍然是“整月 listing”，然后用 id_list_feed 补齐细节。

也就是说，跨月范围更像是“按月补齐相关月份”，而不是“精确裁剪到某几天的 listing 内容”。

### 3.4 id_list_feed 可获取的字段，以及这些字段在项目中的去向

| 字段名                           | 字段所属API  | 字段含义                                                                                   | 是否进数据库                                                   | 是否在前端表格展示                                      | 是否在前端详情页展示 |
| -------------------------------- | ------------ | ------------------------------------------------------------------------------------------ | -------------------------------------------------------------- | ------------------------------------------------------- | -------------------- |
| feed.id                          | id_list_feed | 本次 feed 的唯一标识                                                                       | 否，仅 RawFetch                                                | 否                                                      | 否                   |
| feed.title                       | id_list_feed | 本次查询说明文字                                                                           | 否，仅 RawFetch                                                | 否                                                      | 否                   |
| feed.updated                     | id_list_feed | feed 生成时间                                                                              | 否，仅 RawFetch                                                | 否                                                      | 否                   |
| feed.link.{href,type}            | id_list_feed | 本次查询 feed 链接                                                                         | 否，仅 RawFetch                                                | 否                                                      | 否                   |
| opensearch:totalResults          | id_list_feed | 查询总结果数                                                                               | 否，仅 RawFetch                                                | 否                                                      | 否                   |
| opensearch:startIndex            | id_list_feed | 当前页起始偏移                                                                             | 否，仅 RawFetch                                                | 否                                                      | 否                   |
| opensearch:itemsPerPage          | id_list_feed | 当前页大小                                                                                 | 否，仅 RawFetch                                                | 否                                                      | 否                   |
| entry.id                         | id_list_feed | 条目 ID，如 <http://arxiv.org/abs/2603.00114v1> ；当前代码还会从它派生 arxiv_id 和 abs_url | 是，进 papers.entry_id，并派生 papers.arxiv_id、papers.abs_url | 否                                                      | 否                   |
| entry.title                      | id_list_feed | 论文标题                                                                                   | 是，进 papers.title                                            | 是，Title 列                                            | 是，详情页标题       |
| entry.updated                    | id_list_feed | 条目最近更新时间                                                                           | 是，进 papers.updated_at                                       | 是，Updated 列                                          | 是                   |
| entry.published                  | id_list_feed | 条目首次发布时间                                                                           | 是，进 papers.published_at                                     | 是，Published 列                                        | 是                   |
| entry.summary                    | id_list_feed | 摘要                                                                                       | 是，进 papers.abstract                                         | 否                                                      | 是，Abstract         |
| entry.author.name                | id_list_feed | 作者名                                                                                     | 是，进 papers.authors_json                                     | 是，但默认隐藏 Authors 列                               | 是                   |
| entry.author.arxiv:affiliation   | id_list_feed | 作者单位                                                                                   | 是，进 papers.author_details_json                              | 否                                                      | 否                   |
| entry.link.{href,rel,type,title} | id_list_feed | 条目相关链接，常见是 abs/pdf，也可能包含别的 link                                          | 是，进 papers.links_json                                       | 否                                                      | 否                   |
| entry.category.term              | id_list_feed | 分类代码，如 cs.CV                                                                         | 是，进 papers.categories_json                                  | 是，Category 列                                         | 是                   |
| entry.category.scheme            | id_list_feed | 分类 scheme URI                                                                            | 是，进 papers.category_details_json                            | 否                                                      | 否                   |
| arxiv:primary_category.term      | id_list_feed | 主分类代码                                                                                 | 是，进 papers.primary_category                                 | 是，但只在 categories_json 为空时作为 Category fallback | 是，顶部 meta chip   |
| arxiv:primary_category.scheme    | id_list_feed | 主分类 scheme URI                                                                          | 是，进 papers.primary_category_scheme                          | 否                                                      | 否                   |
| arxiv:comment                    | id_list_feed | comments 字段                                                                              | 是，进 papers.comment                                          | 是，但默认隐藏 Comment 列                               | 是，条件展示         |
| arxiv:journal_ref                | id_list_feed | journal reference                                                                          | 是，进 papers.journal_ref                                      | 否                                                      | 是，条件展示         |
| arxiv:doi                        | id_list_feed | DOI                                                                                        | 是，进 papers.doi                                              | 否                                                      | 是，条件展示         |

## 4. 批量拆分规则

当 scope 跨越多个归档月份时，任务会先变成批量根任务，再拆成子任务。

子任务粒度是：

- 一个分类
- 一个归档月份

例如：

- `categories=cs.CV,cs.LG`
- `from=2026-03-20`
- `to=2026-04-10`

会拆成 4 个子任务：

- `cs.CV + 2026-03`
- `cs.CV + 2026-04`
- `cs.LG + 2026-03`
- `cs.LG + 2026-04`

## 5. 新鲜度与跳过规则

### 5.1 Past day TTL

系统会按 `category + day` 记录“这个自然日上次成功完成同步的时间”。

对已经结束的过去日期：

- 如果还在 TTL 内，则可跳过
- 如果超过 TTL，则重新同步

### 5.2 今天和未完成窗口永远视为可同步

只要请求范围触及今天或未来日期，系统会把它视为“仍可能变化”的窗口，因此不会因为 past-day TTL 而整体跳过。

这意味着：

- 今天的单日同步总是可重新执行
- 当前月通常也会被视为仍有必要同步

### 5.3 `force`

`force=true` 会跳过 TTL 判断，直接执行。并且用执行结果更新TTL。

## 6. 抓取与落库流程

`sync-arxiv` 的核心流程如下：

1. 根据 scope 规划抓取单元。
2. 对每个抓取单元尝试获取资源锁。
3. 抓取 listing / search / catchup / submitted-day 内容。
4. 解析 arXiv ID。
5. 按批次调用 metadata feed，拿到完整论文信息。
6. upsert `Paper`。
7. 记录 `ArxivArchiveAppearance`。
8. 记录原始抓取快照。
9. 成功后更新 day 级完成时间，用于后续 TTL 判断。

## 7. 产出

一个成功的 `sync-arxiv` 会产出三类结果：

- 论文主体数据更新
- 归档出现关系更新
- 可追溯的原始抓取快照

对后续流程的直接影响是：

- `find-repos` 能看到更多论文
- 按分类、月份、日期范围的筛选更稳定

## 8. 关键配置项

- `ARXIV_API_MIN_INTERVAL` 默认值为3秒
- `ARXIV_SYNC_TTL_DAYS`
- `ARXIV_ID_BATCH_SIZE`
- `ARXIV_LIST_PAGE_SIZE`

它们分别影响：

- 请求节流速度
- 过去日期多久视为过期
- 临时错误的重试强度
- metadata 批量回填的大小
- listing / submitted-day 的分页大小

## 9. 当前写死但需要让用户知道的常量

### 9.1 `ARXIV_CATCHUP_MAX_AGE_DAYS = 90`

过去日期距离今天不超过 `90` 天时，单日同步优先走 catchup 页面；超过这个阈值则改走 submitted-date feed。

这意味着：

- “最近历史”与“更老历史”使用的是两套抓取载体
- 这个分界线当前不是配置项

### 9.2 arXiv 抓取并发当前固定为 `1`

单个 `sync-arxiv` 任务内部，对 arXiv 的 listing / metadata / catchup 请求流当前是串行的，没有暴露单独的并发配置项。

这意味着：

- `sync-arxiv` 的吞吐主要靠分页大小、批量大小和请求间隔调节
- 不能通过提高 arXiv 内部并发来直接提速
