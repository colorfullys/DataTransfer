# DataTransfer

生产级 Rust 数据同步平台：`DataTransfer`（编排器二进制）+ `LibDatasource`（数据源 SDK/插件）+ `LibETL`（ETL SDK/插件）。

核心设计约束：**DataTransfer 二进制不依赖任何数据库驱动**（无 mysql/postgres/oracle crate）。所有数据访问都经由 C ABI 动态库插件（`.so`/`.dylib`/`.dll`）完成，因此新增一个数据库只需写一个插件，无需改动编排器。

---

## 特性

- **插件化数据源**：MySQL / PostgreSQL / Oracle 已内置；新增数据库只写一个 cdylib 插件，二进制与 SDK 都不用改。
- **插件化 ETL**：内置 `rename / set / filter / cast` 处理器，自定义插件可做跨表查询（`lookup_table`）与一对多拆分。
- **三种同步模式**：`full`（TRUNCATE 全量）、`upsert`（ON DUPLICATE KEY / ON CONFLICT / MERGE）、`append`（纯 INSERT）。
- **增量状态**：按 `max` 水位游标，只增不减，原子落盘，跨进程安全。
- **并行写入**：每个任务可配置多个 writer，经 router（按主键哈希 / 轮询）在 ETL 之后分发，充分利用连接池并行写目标库。
- **模板变量**：`where` 等位置支持 `${state.*}`、`${env.*}`、`${sys.now(...)}` 展开。
- **安全**：所有 SQL 只经参数绑定或白名单拼接，标识符反引号转义；密码支持 `${ENV}` 注入、YAML 数字标量容错。

---

## 架构

### 分层总览

```
 ┌────────────────────────────────────────────────────────────────┐
 │                     DataTransfer (bin, host)                    │
 │  config.yaml ──► plugins ──► scheduler(cron) ──► runner         │
 │                                       (WorkerGate 并发闸)        │
 │                                                │                │
 │        ┌───────────────────────────────────────┤                │
 │        │  read → map columns → ETL → ROUTER → N × writer        │
 │        ▼                                                        │
 │  DatasourceCapi (C ABI, ABI v1)              EtlCapi (C ABI)    │
 │   libmysql.so / libpostgresql.so             process / lookup   │
 │   / liboracle.so                                                 │
 │    query_page / query_params / batch_insert                     │
 │    get_schema / truncate / execute                              │
 └────────────────────────────────────────────────────────────────┘
```

### 关键设计点

1. **插件隔离（C ABI）**：二进制与插件通过稳定的 `DatasourceCapi` / `EtlCapi` 函数指针表通信，载荷为 NUL 结尾 JSON。二进制不静态链接任何驱动，插件可以各自使用任何底层库（甚至不同版本）。
2. **宿主导入回调**：ETL 插件不直接连库，而是把 `lookup` 请求通过 C ABI 回调回宿主（`TableLookup`），宿主经已连接的数据源执行参数化查询——插件与宿主分离编译、不共享对象类型。
3. **单线程任务 → 多 writer 扇形分发**：读、列映射、ETL 在任务线程串行执行；ETL 输出行经 `Router` 投递到 `writers` 个独立工作线程（有界 `sync_channel`），每个 writer 独立攒批/自刷，通过连接池获得并行的目标连接。`full` 模式的 TRUNCATE 用共享集合保证只执行一次。
4. **连接超时**：`connect_one` 把建连放到工作线程并以硬超时兜底，某些驱动（如 Oracle）自身超时不可靠时宿主接管。
5. **值模型统一**：所有数据类型收敛为 `Value`（见下文），跨边界传输时按列元数据还原真实类型（整数/小数/日期/字符串/二进制），避免文本协议把一切当字符串。

### 目录结构

```
DataTransfer/              工作区根（workspace，resolver=3）
├── Cargo.toml             workspace 根（成员、共享依赖、license）
├── build.sh               构建全部插件并复制到 plugins/
├── LICENSE-MIT / LICENSE-APACHE
├── README.md
├── DataTransfer/          编排器二进制 crate
│   ├── src/{main,config,connections,scheduler,runner,reader,writer,router,etl,state,templates,logging,error}.rs
│   ├── config.yaml        主配置
│   ├── datasource.yaml    连接配置
│   ├── jobs/              任务定义（每文件一个任务）
│   └── plugins/           构建产物：datasource/、etl/
├── LibDatasource/         SDK：值模型、FFI、plugin API、registry
│   ├── Mysql/             MySQL 插件（cdylib）
│   ├── Postgresql/        PostgreSQL 插件（cdylib）
│   └── Oracle/            Oracle 插件（cdylib）
└── LibETL/                ETL SDK：EtlProcessor trait、C ABI、内置处理器
    └── OrderSplitPlugin/  参考插件：订单行一对多拆分（含跨表 lookup + 单测）
```

---

## 构建

```bash
# 需要 Rust 1.85+（edition 2024）
cargo build --workspace          # 编译二进制 + 所有插件（dev profile）
./build.sh                       # 构建并把 .so 复制到 DataTransfer/plugins/
cargo build --workspace --release   # 生产构建（thin LTO）
```

工作区已配置 `rsproxy.cn` 稀疏镜像（`.cargo/config.toml`），国内网络可直接拉依赖。

### 插件产物

| 产物 | 来源 crate | 目标 |
|---|---|---|
| `libmysql.so` | `LibDatasource/Mysql` | `plugins/datasource/` |
| `libpostgresql.so` | `LibDatasource/Postgresql` | `plugins/datasource/` |
| `liboracle.so` | `LibDatasource/Oracle` | `plugins/datasource/` |
| `liborder_split.so` | `LibETL/OrderSplitPlugin` | `plugins/etl/` |

---

## 运行

```bash
./target/debug/DataTransfer DataTransfer/config.yaml
# 或生产
./target/release/DataTransfer DataTransfer/config.yaml
```

主进程为每个启用的任务按 cron 调度一个线程，用令牌门（`WorkerGate`）限制并发任务数为 `runtime.workers`。日志默认输出到终端，可配置写入文件。

---

## 配置

### config.yaml（主配置）

| 字段 | 说明 |
|---|---|
| `datasource` | 插件类型 → cdylib 路径 |
| `etl` | ETL 插件 cdylib 列表（可选） |
| `datasource_file` | 连接文件路径（默认 `datasource.yaml`） |
| `jobs` | 任务文件列表 |
| `state_dir` | 增量状态目录（每任务一个 `<name>.json`） |
| `logging.level/file` | 日志级别与可选文件 |
| `runtime.workers` | 最大并发任务数 |
| `runtime.retry` | 每个 batch 除首次外的额外重试次数（指数退避） |
| `runtime.page_size` | 源端分页大小 |

### datasource.yaml（连接）

```yaml
connections:
  mysql_prod:
    type: mysql
    host: 192.168.1.10
    port: 3306
    database: erp
    username: sync
    password: ${MYSQL_PASSWORD}   # 支持 ${ENV_VAR}；未加引号的数字也会被容错读取
    max_pool_size: 8              # 连接池大小；writer 数量超过它时并行会受限于池
    params:
      charset: utf8mb4            # mysql: utf8mb4/latin1/gbk/big5
    # ssl_mode: require           # mysql 可选
```

`type` 必须是 `datasource` 段里注册过的插件名（`mysql`/`postgresql`/`oracle`）。

### jobs/<name>.yaml（任务）

```yaml
name: user_sync
enabled: true
writers: 4                        # 并行 writer 数（默认 1）；ETL 之后按主键哈希/轮询分发

source:
  connection: mysql_prod
  table: t_user
  primary_key: [id]
  columns: id,name,age            # 空 = SELECT *
  batch_limit: 5000               # 可选：每次查询拉取的行数上限（覆盖 runtime.page_size），拉完再分发给 writer
  # select: |                     # 可选：自定义完整 SELECT（支持 JOIN），替代 columns+where
  #   SELECT u.id, o.name FROM t_user u LEFT JOIN t_order o ON u.id = o.user_id
  #   WHERE u.id > ${state.max_id}
  where: |
    id > ${state.max_id} or
    create_time > ${sys.now('%Y-%m-%d %H:%M:%S') -30S}

target:
  connection: pg_test
  table: sys_user
  primary_key: [id]
  columns: id,userName,age        # 与 source.columns 位置对应

sync:
  mode: upsert                    # full | upsert | append
  state:
    max_id: { type: max, column: id }

etl:                              # 可选：按顺序执行的内置/插件步骤
  - cast:
      column: [{ name: amount, type: decimal }]

schedule:
  cron: "0 */5 * * * *"
```

#### 并行写（writers）

`writers: N` 开启 N 个并行 writer。任务线程完成读→列映射→ETL 后，把每行通过 `Router` 分发：

- **主目标表行**：按主键列哈希 → 同一主键恒落同一 writer，保证 upsert 顺序确定。
- **其它表（拆分细节行）或没有主键的行**：轮询分发。

建议把目标连接的 `max_pool_size` 设为 ≥ `writers`，否则 writer 会在连接池上串行（启动时会打日志警告）。

#### source.select 与 batch_limit 分页

**`source.select`**：自定义完整 SELECT 语句，用于多表 JOIN 等场景，直接替代自动生成的 `SELECT <columns> FROM <table>`。内部仍会展开模板（`${state.*}`、`${sys.now(...)}`），所以增量过滤要写进 select 本身；此时 `where` 会被忽略（日志会提示）。

**`source.batch_limit`**：每次查询拉取的行数上限（默认取全局 `runtime.page_size`）。读取按此值分页，每页拉满后立即经 ETL 分发给 writer，不会一次把全部数据读进内存。

**分页在 lib 实现**：分页 SQL（`LIMIT/OFFSET`、Oracle `OFFSET ... ROWS FETCH NEXT ... ROWS ONLY`）由各数据源插件的 `query_page` 在插件库内追加，编排器只传 `sql, offset, limit`，因此 SELECT 语句本身不写分页也能走统一分页。

#### 模板变量（在 `where` 等处展开）

| token | 说明 |
|---|---|
| `${state.<key>}` | 持久化的水位标记；首跑时按列类型取低界（`0` / `'1970-01-01 00:00:00'` / `''`） |
| `${env.<NAME>}` | 环境变量；未设置时保留原 token 并报错 |
| `${sys.now('FMT') ±N<S\|M\|H\|D>}` | 当前本地时间按格式展开，可选偏移（秒/分/时/天） |

#### sync.mode

| mode | 行为 |
|---|---|
| `full` | 每次运行先 `TRUNCATE` 目标表再全量插入（多 writer 下只 truncate 一次） |
| `upsert` | MySQL `ON DUPLICATE KEY UPDATE`；PG `ON CONFLICT ... DO UPDATE`；Oracle `MERGE` |
| `append` | 纯 `INSERT`（仅 Oracle 支持 `INSERT ALL` 批量；MySQL/PG 走普通多值 INSERT） |

#### ETL 内置步骤

| 步骤 | 配置 |
|---|---|
| `rename` | `column: [{from, to}]` |
| `set` | `constant: {col: value}`（值为 JSON 字面量或 `{t, v}` 标签形式） |
| `filter` | `keep: "<col> <op> <value>"` 或 `drop: ...`；op ∈ `eq/ne/gt/gte/lt/lte`，值为 `$col` 引用或字面量 |
| `cast` | `column: [{name, type}]`；type ∈ `int/float/bool/string/decimal` |

内置步骤输出单行；**多目标拆分**由自定义 ETL 插件返回多个输出行实现（每行的 `table` 字段指向目标表，见 LibETL SDK）。

---

## 增量状态

每个任务维护 `state_dir/<name>.json`，`sync.state` 里的 `max` 类型键取当次读取页中的列最大值，且只增不减。状态先写临时文件再原子 rename，避免进程被杀损坏。

---

## 插件开发

### 数据源插件（LibDatasource）

插件导出 `ds_get_api`，返回静态的 `DatasourceCapi` 函数指针表（ABI version = 1）。所有载荷以 NUL 结尾 JSON 跨边界；`out` 字符串归插件所有，宿主用 `free_string` 释放。

```rust
use libdatasource::ffi::DatasourceCapi;
use libdatasource::plugin_api;
use libdatasource::datasource::Datasource;   // 实现该 trait

export_datasource_plugin!(MyDatasource, "mydb");  // 宏生成 ds_get_api
```

关键 C 函数：`connect`（JSON ConnectionConfig）、`query_page`（offset/limit 分页）、`query_params`（`?`/`$N`/`:N` 位置绑定）、`batch_insert`（表+列+行矩阵+mode+主键）、`truncate`、`get_schema`。

### ETL 插件（LibETL）

插件导出 `etl_get_api`，返回静态 `EtlCapi`。`process` 接收 `CLookupFn` + 不透明指针，宿主注入 `TableLookup` 实现，插件可用 `CbLookup` 转发跨表查询——双方分离编译、不共享对象类型。

```rust
use libetl::{export_etl_plugin, EtlConfigure, EtlContext, EtlProcessor};
use libetl::model::{EtlOutputRow, EtlRow};

#[derive(Default)]
struct MyStep;

impl EtlProcessor for MyStep {
    fn name(&self) -> &str { "my_step" }
    fn process(&self, ctx: &mut EtlContext, input: &EtlRow) -> libetl::Result<Vec<EtlOutputRow>> {
        // ctx.lookup_table(conn, table, cols, where, params, limit) 读其他表
        Ok(vec![EtlOutputRow::new(String::new(), input.row.clone())])
    }
}

impl EtlConfigure for MyStep {
    fn configure(&mut self, config: &serde_json::Value) -> libetl::Result<()> { Ok(()) }
}

export_etl_plugin!(MyStep, "my_step");
```

插件 crate 需是 `cdylib`，`[lib] name` 决定产物名（`.so`）；依赖用包名 `LibETL`、`LibDatasource`（代码里用 `libetl::`/`libdatasource::` 引用）。**完整可用的参考实现见 `LibETL/OrderSplitPlugin`**（含跨表 `lookup_table` 和一对多拆分 + 单元测试）。

```toml
[package]
name = "OrderSplitPlugin"
edition = "2024"
license = "MIT OR Apache-2.0"

[dependencies]
LibETL = { path = ".." }
LibDatasource = { path = "../../LibDatasource", default-features = false }
serde = { workspace = true }
serde_json = { workspace = true }

[lib]
name = "order_split"        # -> liborder_split.so
crate-type = ["cdylib"]
```

`export_etl_plugin!` 只用 LibETL 的非 host 部分（`plugin_api`/`ffi`），插件无需 `plugin-host` feature；`registry` 仅在 DataTransfer 宿主内启用。

在任务文件里按名字引用，并在 `config.yaml` 的 `etl:` 段注册插件：

```yaml
etl:
  - plugin: my_step
    config:
      detail_table: order_item
```

任务还支持 `{builtin: rename, config: {...}}` / `{name: {...}}` / 裸字符串等价写法。

---

## 值模型

`Value` 为所有插件与宿主的固定交换格式：`Null / Bool / Int / UInt / Float / Decimal(String) / String / Bytes / Date(String)`。序列化为标签形式 `{"t":"Int","v":42}`；反序列化同时接受标签形式和 JSON 字面量（便于在配置里写自然值）。`Row` = `BTreeMap<String, Value>`。

读端按列元数据还原类型（MySQL 文本协议下整数/小数/日期/字符串/二进制），写端按类型转回驱动原生值，精度保持。

---

## 测试与校验

```bash
cargo test --workspace     # 单元测试
cargo clippy --workspace   # 0 警告
```

端到端验证需要可达的 MySQL/PG，配置好 `datasource.yaml` 后用环境变量注入密码运行即可；也可用 `config.yaml` 中空连接的任务做“插件加载 → 任务解析 → cron 调度 → runner”的冒烟验证。

---

## 已知运行时依赖

- Oracle 插件运行时需要 OCI client（SID/service name 连接）。
- PG 插件默认 `NoTls`；Oracle 批量写用 `INSERT ALL`、upsert 用 `MERGE`。

---

## 发布到 GitHub

仓库为 Rust workspace，包含数个 crate（`LibDatasource` 及其插件、`LibETL`、`DataTransfer`）。当前采用 **双许可 `MIT OR Apache-2.0`**（见 `LICENSE-MIT` / `LICENSE-APACHE`），各 crate 的 `Cargo.toml` 均已声明 `license`。

> 注意：各 crate 依赖彼此使用 `path` 相对路径；若要在 **crates.io** 发布，需要把 `path` 依赖改为 `version` 并逐一 `cargo publish`（或只把发布目标定为 GitHub 源码仓库，跑 `cargo build` 自助构建）。cargo registry 发布前请先补上各 crate 的 `repository` / `homepage` 字段。

发布到 GitHub 的建议步骤：

```bash
# 1. 本地首次提交（如尚未提交）
git add -A
git commit -m "init: DataTransfer sync platform (MIT OR Apache-2.0)"

# 2. 推送前先在 GitHub 建好空仓库（不要勾选 README/.gitignore/LICENSE，避免冲突）
git remote add origin https://github.com/<you>/DataTransfer.git
git push -u origin master

# 3. 在 GitHub 仓库页面新建 Release，tag 为 v0.1.0
```

发布前请确认：`.gitignore` 含 `/target`（已含），**不要**把 `DataTransfer/plugins/*.so`、`logs/`、`state/`、`datasource.yaml` 中的真实密码提交进仓库——推荐把真实 `datasource.yaml` 改为 `datasource.yaml.example`，密码一律用 `${ENV}`。

---

## License

DataTransfer 采用双许可：

```text
MIT OR Apache-2.0
```

即在 `LICENSE-MIT`（MIT License）与 `LICENSE-APACHE`（Apache License 2.0）中任选其一使用。
