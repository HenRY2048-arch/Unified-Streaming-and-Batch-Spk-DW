from __future__ import annotations

import json
import os
import urllib.request
from typing import Any

import duckdb
import pandas as pd
import plotly.express as px
import streamlit as st
from deltalake import DeltaTable


S3_ENDPOINT = os.getenv("CRT_S3_ENDPOINT", "http://localhost:8333")
FILER_ROOT = os.getenv(
    "CRT_FILER_ROOT",
    "http://localhost:8888/buckets/crt-lake",
)
OBSERVATION_PATH = "/ops/reports/observations"

# deltalake uses s3:// rather than Spark's s3a://.
os.environ.setdefault("AWS_ENDPOINT_URL", S3_ENDPOINT)
os.environ.setdefault("AWS_ACCESS_KEY_ID", "crtadmin")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "crtadmin123")
os.environ.setdefault("AWS_REGION", "us-east-1")
os.environ.setdefault("AWS_ALLOW_HTTP", "true")
os.environ.setdefault("AWS_VIRTUAL_HOSTED_STYLE_REQUEST", "false")
os.environ.setdefault("AWS_S3_ALLOW_UNSAFE_RENAME", "true")


st.set_page_config(
    page_title="CRT Warehouse Observer",
    page_icon="📊",
    layout="wide",
)


@st.cache_data(ttl=60, show_spinner=False)
def list_observations() -> pd.DataFrame:
    url = FILER_ROOT + OBSERVATION_PATH + "/?limit=1000"
    request = urllib.request.Request(url, headers={"Accept": "application/json"})
    with urllib.request.urlopen(request, timeout=10) as response:
        payload = json.load(response)
    rows = [
        {
            "name": entry["FullPath"].rsplit("/", 1)[-1],
            "mtime": pd.to_datetime(entry["Mtime"], utc=True),
            "size": entry["FileSize"],
            "url": FILER_ROOT + entry["FullPath"].removeprefix(
                "/buckets/crt-lake"
            ),
        }
        for entry in payload.get("Entries", [])
        if entry.get("FullPath", "").endswith(".json")
    ]
    return pd.DataFrame(rows).sort_values("mtime", ascending=False)


@st.cache_data(ttl=60, show_spinner=False)
def load_observation(url: str) -> dict[str, Any]:
    with urllib.request.urlopen(url, timeout=10) as response:
        return json.load(response)


@st.cache_data(ttl=60, show_spinner=False)
def load_delta(table: str) -> pd.DataFrame:
    uri = "s3://crt-lake/" + table.lstrip("/")
    return DeltaTable(uri).to_pandas()


def table_rows(observation: dict[str, Any]) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for section in (
        "SOURCE_AND_SERVICE",
        "DIM",
        "DWD",
        "DWS",
        "VERIFY_AND_CONTRACT",
    ):
        for item in observation.get(section, []):
            rows.append(
                {
                    "section": section,
                    "table": item["table"],
                    "rows": item["rows"],
                    "exists": item["exists"],
                    "min_dt": item.get("min_dt"),
                    "max_dt": item.get("max_dt"),
                }
            )
    return pd.DataFrame(rows)


def gate_table(observation: dict[str, Any]) -> pd.DataFrame:
    return pd.DataFrame(observation.get("latest_gate", []))


def status_color(status: str) -> str:
    return {
        "pass": "#1f8f5f",
        "pass_with_limitations": "#c88a26",
        "needs_confirmation": "#b26a00",
        "blocked": "#b42318",
        "wait": "#5b6b7a",
    }.get(status, "#5b6b7a")


def daily_content_metrics() -> tuple[pd.DataFrame, pd.DataFrame]:
    content = load_delta("dws/content_daily")
    con = duckdb.connect()
    con.register("content_daily", content)
    daily = con.execute(
        """
        select
            dt,
            sum(exposure_cnt) as exposure_cnt,
            sum(exposure_uv) as exposure_uv,
            sum(click_cnt) as click_cnt,
            sum(consume_cnt) as consume_cnt,
            sum(interact_flow_cnt) as interact_cnt,
            round(
                sum(click_cnt) * 1.0 / nullif(sum(exposure_cnt), 0),
                4
            ) as ctr
        from content_daily
        group by dt
        order by dt
        """
    ).fetchdf()

    channel = load_delta("dws/channel_hourly")
    con.register("channel_hourly", channel)
    channel_daily = con.execute(
        """
        select
            dt,
            channel,
            sum(exposure_cnt) as exposure_cnt,
            sum(click_cnt) as click_cnt,
            sum(follow_cnt) as follow_cnt,
            round(
                sum(click_cnt) * 1.0 / nullif(sum(exposure_cnt), 0),
                4
            ) as ctr
        from channel_hourly
        group by dt, channel
        order by dt, channel
        """
    ).fetchdf()
    return daily, channel_daily


def main() -> None:
    st.title("CRT Warehouse Observer")
    st.caption(
        "只读观测器：observation JSON + Delta DWS + Gate/partition_status。"
        "不写数仓、不触发任务。"
    )

    try:
        observations = list_observations()
    except Exception as exc:
        st.error(f"无法读取 observation 目录：{exc}")
        st.stop()

    if observations.empty:
        st.warning("尚无 observation JSON。先运行完整观测流程。")
        st.stop()

    with st.sidebar:
        st.subheader("观测快照")
        selected_name = st.selectbox(
            "observation",
            observations["name"].tolist(),
            index=0,
        )
        selected_url = observations.iloc[
            observations.index[observations["name"] == selected_name][0]
        ]["url"]
        st.caption(f"数据源：{selected_url}")
        if st.button("刷新数据", width="stretch"):
            st.cache_data.clear()
            st.rerun()
        st.divider()
        st.markdown(
            "**读取边界**\n\n"
            "- observation JSON：运行摘要\n"
            "- `dws/content_daily`：内容和漏斗趋势\n"
            "- `dws/channel_hourly`：渠道趋势\n"
            "- `partition_status`：可发布状态"
        )

    observation = load_observation(selected_url)
    tables = table_rows(observation)
    gates = gate_table(observation)
    partition_status = load_delta("ops/partition_status")
    daily, channel_daily = daily_content_metrics()

    existing = tables[tables["exists"]].copy()
    source_rows = tables.loc[
        tables["table"] == "ods/events", "rows"
    ].squeeze()
    truth_rows = tables.loc[
        tables["table"] == "ods/groundtruth", "rows"
    ].squeeze()
    v01 = observation.get("v01", {})
    v02 = observation.get("v02", {})
    latest_dt = observation.get("partition_status_dt", "-")

    overview_tab, trend_tab, trust_tab = st.tabs(
        ["运行总览", "核心趋势", "Gate / 分区信任"]
    )

    with overview_tab:
        st.subheader(f"快照 {observation['observation_id']}")
        c1, c2, c3, c4, c5 = st.columns(5)
        c1.metric("ODS 事件", f"{int(source_rows):,}")
        c2.metric("归因真值", f"{int(truth_rows):,}")
        c3.metric(
            "V-01 召回",
            f"{float(v01.get('min_recall') or 0):.1%}",
        )
        c4.metric(
            "V-02 p95",
            f"{float(v02.get('rel_diff_p95') or 0):.2%}",
        )
        c5.metric("目标分区", latest_dt)

        left, right = st.columns([1.45, 1])
        with left:
            fig = px.bar(
                existing.sort_values("rows", ascending=True),
                x="rows",
                y="table",
                color="section",
                orientation="h",
                height=620,
                title="各层表当前行数",
            )
            fig.update_layout(
                yaxis_title="",
                xaxis_title="rows",
                legend_title="",
            )
            st.plotly_chart(fig, width="stretch")
        with right:
            events = pd.DataFrame(observation.get("ods_event_types", []))
            if not events.empty:
                fig = px.bar(
                    events.sort_values("count", ascending=False),
                    x="count",
                    y="event_type",
                    orientation="h",
                    height=620,
                    title="ODS 事件类型",
                )
                fig.update_layout(
                    yaxis_title="",
                    xaxis_title="count",
                )
                st.plotly_chart(fig, width="stretch")

        st.dataframe(
            existing[
                ["section", "table", "rows", "min_dt", "max_dt"]
            ],
            width="stretch",
            hide_index=True,
        )

    with trend_tab:
        st.subheader("内容漏斗日趋势")
        fig = px.line(
            daily,
            x="dt",
            y=["exposure_cnt", "click_cnt", "consume_cnt", "interact_cnt"],
            markers=True,
            title="DWS 内容主题日汇总",
        )
        fig.update_layout(
            xaxis_title="",
            yaxis_title="count",
            legend_title="",
        )
        st.plotly_chart(fig, width="stretch")

        fig = px.bar(
            daily,
            x="dt",
            y="ctr",
            text="ctr",
            title="每日 CTR = click / exposure",
        )
        fig.update_traces(texttemplate="%{text:.2%}")
        fig.update_layout(
            xaxis_title="",
            yaxis_title="CTR",
            yaxis_tickformat=".1%",
        )
        st.plotly_chart(fig, width="stretch")

        st.subheader("渠道差异")
        channels = sorted(channel_daily["channel"].dropna().unique())
        selected_channels = st.multiselect(
            "渠道",
            channels,
            default=channels,
        )
        channel_view = channel_daily[
            channel_daily["channel"].isin(selected_channels)
        ]
        if not channel_view.empty:
            fig = px.line(
                channel_view,
                x="dt",
                y="ctr",
                color="channel",
                markers=True,
                title="渠道日 CTR",
            )
            fig.update_layout(
                xaxis_title="",
                yaxis_title="CTR",
                yaxis_tickformat=".1%",
            )
            st.plotly_chart(fig, width="stretch")
        st.dataframe(
            channel_view,
            width="stretch",
            hide_index=True,
        )

    with trust_tab:
        st.subheader("最新 Gate")
        if gates.empty:
            st.info("observation 中没有 Gate 结果。")
        else:
            status_counts = (
                gates.groupby("status", as_index=False)
                .size()
                .rename(columns={"size": "count"})
            )
            fig = px.bar(
                status_counts,
                x="status",
                y="count",
                color="status",
                color_discrete_map={
                    status: status_color(status) for status in status_counts["status"]
                },
                title="Gate 状态分布",
            )
            fig.update_layout(showlegend=False, xaxis_title="", yaxis_title="count")
            st.plotly_chart(fig, width="stretch")
            st.dataframe(
                gates[
                    ["gate_id", "status", "action", "table", "detail"]
                ],
                width="stretch",
                hide_index=True,
            )

        st.subheader(f"分区信任 · dt={latest_dt}")
        selected_partition = partition_status[
            partition_status["dt"].astype(str) == str(latest_dt)
        ].copy()
        if selected_partition.empty:
            st.warning("partition_status 中没有该日期。")
        else:
            status_order = {
                "blocked": 4,
                "needs_confirmation": 3,
                "pass_with_limitations": 2,
                "pass": 1,
            }
            selected_partition["severity"] = selected_partition["status"].map(
                status_order
            ).fillna(0)
            trust_view = selected_partition.sort_values(
                ["severity", "table"],
                ascending=[False, True],
            )
            st.dataframe(
                trust_view[
                    ["table", "status", "publish_final", "limitations"]
                ],
                width="stretch",
                hide_index=True,
            )
            st.caption(
                "publish_final=false 表示可用于带限制分析，但不能作为已定稿数据。"
            )


if __name__ == "__main__":
    main()
