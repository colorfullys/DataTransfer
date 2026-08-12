# DataTransfer

生产级 Rust 数据同步平台：`DataTransfer`（编排器二进制）+ `LibDatasource`（数据源 SDK/插件）+ `LibETL`（ETL SDK/插件）。

核心设计约束：**DataTransfer 二进制不依赖任何数据库驱动**（无 mysql/postgres/oracle crate）。所有数据访问都经由 C ABI 动态库插件（`.so`/`.dylib`/`.dll`）完成，因此新增一个数据库只需写一个插件，无需改动编排器。

```
┌─────────────────────────────────────────────────────────┐
│  DataTransfer (bin)                                     │
│   config → datasource/etl plugins → scheduler → runner  │
│        │                              │                 │
│        ▼                              ▼                 │
│  DatasourceCapi (C ABI)          EtlCapi (C ABI)        │
│   libmysql / libpostgresql        custom ETL plugins    │
│   / liboracle .so                                        │
└─────────────────────────────────────────────────────────┘
```

## 目录结构

```
DataTransfer/              工作区根（workspace）
├── build.sh               构建全部插件并复制到 plugins/
├── DataTransfer/          编排器二进制
│   ├── config.yaml        主配置
│   ├── datasource.yaml    连接配置
│   ├── jobs/              任务定义（每文件一个任务）
│   └── plugins/           构建产物：datasource/、etl/
├── LibDatasource/         SDK：值模型、FFI、plugin API
│   ├── Mysql/             MySQL 插件（cdylib）
│   ├── Postgresql/        PostgreSQL 插件（cdylib）
│   └── Oracle/            Oracle 插件（cdylib）
└── LibETL/                ETL SDK：EtlProcessor trait、C ABI、内置处理器
```

## 构建

```bash
# 需要 Rust 1.85+（edition 2024）
cargo build --workspace          # 编译二进制 + 所有插件
./build.sh                       # 构建并把 .so 复制到 DataTransfer/plugins/
```

工作区已配置 `rsproxy.cn` 稀疏镜像（`.cargo/config.toml`），国内网络可直接拉依赖。

## 运行

```bash
./target/debug/DataTransfer DataTransfer/config.yaml
```

主进程为每个启用的任务按 cron 调度一个线程，用令牌门（`WorkerGate`）限制并发任务数为 `runtime.workers`。日志默认输出到终端，可配置写入文件。

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
| `runtime.retry` | 每个 batch 除首次外的额外重试次数 |
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
    password: ${MYSQL_PASSWORD}   # 支持 ${ENV_VAR}
    params:
      charset: utf8mb4
```

`type` 必须是 `datasource` 段里注册过的插件名（`mysql`/`postgresql`/`oracle`）。

### jobs/<name>.yaml（任务）

```yaml
name: user_sync
enabled: true

source:
  connection: mysql_prod
  table: t_user
  primary_key: [id]
  columns: id,name,age            # 空 = SELECT *
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

#### 模板变量（在 `where` 等处展开）

| token | 说明 |
|---|---|
| `${state.<key>}` | 持久化的水位标记；首跑时按列类型取低界（`0` / `'1970-01-01 00:00:00'` / `''`） |
| `${env.<NAME>}` | 环境变量；未设置时保留原 token 并报错 |
| `${sys.now('FMT') ±N<S\|M\|H\|D>}` | 当前本地时间按格式展开，可选偏移（秒/分/时/天） |

#### sync.mode

| mode | 行为 |
|---|---|
| `full` | 每次运行先 `TRUNCATE` 目标表再全量插入 |
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

## 增量状态

每个任务维护 `state_dir/<name>.json`，`sync.state` 里的 `max` 类型键取当次读取页中的列最大值，且只增不减。状态先写临时文件再原子 rename，避免进程被杀损坏。

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

## 值模型

`Value` 为所有插件与宿主的固定交换格式：`Null / Bool / Int / UInt / Float / Decimal(String) / String / Bytes / Date(String)`。序列化为标签形式 `{"t":"Int","v":42}`；反序列化同时接受标签形式和 JSON 字面量（便于在配置里写自然值）。`Row` = `BTreeMap<String, Value>`。

## 测试与校验

```bash
cargo test --workspace     # 单元测试
cargo clippy --workspace   # 0 警告
```

端到端验证需要可达的 MySQL/PG，配置好 `datasource.yaml` 后用环境变量注入密码运行即可；也可用 `config.yaml` 中空连接的任务做“插件加载 → 任务解析 → cron 调度 → runner”的冒烟验证（见仓库测试记录）。

## 已知运行时依赖

- Oracle 插件运行时需要 OCI client（SID/service name 连接）。
- PG 插件默认 `NoTls`；Oracle 批量写用 `INSERT ALL`、upsert 用 `MERGE`。
