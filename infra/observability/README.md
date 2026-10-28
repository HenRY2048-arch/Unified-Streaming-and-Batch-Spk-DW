# CRT Observability

独立的只读可视化环境，不参与数仓主链路。

## 环境

已安装目录：仓库根目录 `.observability-venv/`（已 gitignore）。

重新安装：

```bash
cd /home/henry/projects/Community_Real_Time
python3.11 -m venv .observability-venv
.observability-venv/bin/pip install -r infra/observability/requirements.txt
```

## 当前依赖

- Streamlit：本地交互界面
- Plotly：图表
- pandas / PyArrow：表格与 Arrow 数据
- DuckDB：轻量只读查询
- deltalake：Python 直接读取 Delta

## 验证

```bash
.observability-venv/bin/python -c \
  "import streamlit, plotly, pandas, pyarrow, duckdb, deltalake; print('OK')"
```

## 启动

```bash
infra/scripts/run_observability.sh
```

浏览器打开：

```text
http://localhost:8501
```

应用只读四处：

- SeaweedFS Filer 上的 observation JSON；
- `s3://crt-lake/dws/content_daily`；
- `s3://crt-lake/dws/channel_hourly`；
- `s3://crt-lake/ops/partition_status`。

## 实现结构

```text
浏览器
  │ HTTP + WebSocket
  ▼
Streamlit :8501
  │
  ├─ Filer HTTP → observation JSON
  ├─ deltalake → Delta on SeaweedFS
  ├─ DuckDB → DataFrame SQL 聚合
  └─ Plotly → 图表
```

- `app.py`：页面、查询和数据转换；
- `requirements.txt`：Python 依赖版本；
- `../scripts/run_observability.sh`：启动 Streamlit；
- `.observability-venv/`：隔离运行环境。

Streamlit 不是静态文件服务器。浏览器与 Python 服务建立会话；切换快照、页签或渠道筛选时，
Python 会重新执行对应查询，并把新的 Plotly 图表和 DataFrame 推送到页面。

## 页面范围

1. 运行总览：observation JSON 的层表行数、事件类型、V-01/V-02 KPI；
2. 核心趋势：`content_daily` 日漏斗和 CTR、`channel_hourly` 渠道差异；
3. Gate / 分区信任：最新 Gate 明细和 `partition_status` 的 `publish_final`。

## 与 Data Agent 的关系

**这个可视化器不是 Data Agent 原型。**

它是：

- 人工查看数仓是否完整运行的只读观测器；
- 人工核验 Agent 回答时的比较基线；
- Delta/DuckDB 只读访问方式的验证。

它不是：

- Agent 的路由、工作流、记忆或事故模型；
- Text2SQL；
- 执行器；
- Agent 产品形态；
- Agent 的权威真值来源。

其中可复用的部分只有底层只读访问能力，例如“读取指定分区状态”“读取某日 DWS 聚合”。
是否将这些能力工具化给 Agent，属于后续独立架构决策，不能从当前页面自然外推。
