"""CloudPulse dashboard (Streamlit). Reads everything through the authenticated API; auto-refreshes every 30 s."""
from __future__ import annotations

import inspect
import os

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import requests
import streamlit as st

API_URL = os.getenv("CLOUDPULSE_API_URL", "http://localhost:8000").rstrip("/")
API_KEY = os.getenv("CLOUDPULSE_DASHBOARD_API_KEY", "")
REFRESH_S = 30
SEV_ICON = {"critical": "🔴", "warning": "🟠", "high": "🔴", "medium": "🟠", "low": "🟡", "info": "🟢"}

st.set_page_config(page_title="CloudPulse", page_icon="📡", layout="wide")


def _stretch(fn) -> dict:
    """Streamlit is replacing use_container_width=True with width="stretch" (the old argument will be removed).
    Detect which one the installed version supports so the dashboard works on both sides of the change."""
    return {"width": "stretch"} if "width" in inspect.signature(fn).parameters else {"use_container_width": True}


PLOT_W, DF_W = _stretch(st.plotly_chart), _stretch(st.dataframe)


def api(path: str, method: str = "GET", **params):
    r = requests.request(method, f"{API_URL}{path}", params={k: v for k, v in params.items() if v is not None},
                         headers={"X-API-Key": API_KEY}, timeout=10)
    r.raise_for_status()
    return r


@st.cache_data(ttl=10, show_spinner=False)
def fetch(path: str, **params):
    return api(path, **params).json()


@st.cache_data(ttl=60, show_spinner=False)
def report_md(kind: str) -> str:
    return api(f"/api/v1/reports/{kind}", format="markdown").text


def fmt(v, unit="", nd=2):
    return "-" if v is None else f"{v:,.{nd}f}{unit}"


# ----------------------------------------------------------------------------- sidebar
with st.sidebar:
    st.title("📡 CloudPulse")
    st.caption("EC2 ↔ S3 transfer performance & reliability")
    range_ = st.selectbox("Time range", ["1h", "6h", "24h", "7d", "30d"], index=2)
    direction_label = st.radio("Direction", ["All", "Uploads (EC2→S3)", "Downloads (S3→EC2)"])
    direction = {"All": None, "Uploads (EC2→S3)": "upload", "Downloads (S3→EC2)": "download"}[direction_label]
    rows = st.select_slider("History rows", [50, 100, 250, 500], value=100)
    try:
        who = fetch("/api/v1/whoami")
        st.success(f"Connected · role: {who['role']}")
        is_admin = who["role"] == "admin"
        for kind in ("daily", "weekly"):
            md = report_md(kind)
            st.download_button(f"⬇ {kind.capitalize()} report", md, f"cloudpulse-{kind}.md", "text/markdown")
    except requests.RequestException as exc:
        st.error(f"API unavailable: {exc}")
        st.stop()
    st.caption(f"Auto-refresh every {REFRESH_S}s")


def ack_alert(alert_id: int):
    api(f"/api/v1/alerts/{alert_id}/ack", method="POST")
    fetch.clear()


# ----------------------------------------------------------------------------- main (live fragment)
@st.fragment(run_every=REFRESH_S)
def live():
    try:
        summ = fetch("/api/v1/metrics/summary", range=range_, direction=direction)
        series = fetch("/api/v1/metrics/timeseries", range=range_, direction=direction)
        rel = fetch("/api/v1/reliability", range=range_, direction=direction)
        alerts = fetch("/api/v1/alerts", range=range_)["items"]
        hist = fetch("/api/v1/transfers", range=range_, direction=direction, limit=rows)
    except requests.RequestException as exc:
        st.error(f"Could not refresh data: {exc}")
        return
    s = summ["summary"]
    st.header("Transfer health")
    st.caption("All timestamps are UTC.")
    if not s["total_transfers"]:
        st.info("No transfers in this window. Run `cloudpulse seed` for demo data or start the monitoring agent.")
        return

    c = st.columns(6)
    c[0].metric("Success rate", fmt(s["success_rate_pct"], "%"), f"{s['failed']} failed", delta_color="inverse")
    c[1].metric("Avg throughput", fmt(s["avg_throughput_mbps"], " MB/s"), f"p5 {fmt(s['p5_throughput_mbps'], '', 1)}")
    c[2].metric("Avg latency", fmt(s["avg_latency_ms"], " ms", 0), f"p95 {fmt(s['p95_latency_ms'], '', 0)}", delta_color="off")
    c[3].metric("Avg duration", fmt(s["avg_duration_s"], " s"))
    c[4].metric("Retries / transfer", fmt(s["retry_rate"], "", 3), f"{s['total_retries']} total", delta_color="off")
    c[5].metric("CPU / Mem", f"{fmt(s['avg_cpu_percent'], '%', 0)} / {fmt(s['avg_memory_percent'], '%', 0)}")

    left, right = st.columns([1, 2])
    with left:
        score = rel["reliability"]
        fig = go.Figure(go.Indicator(
            mode="gauge+number", value=score["score"] or 0, title={"text": f"Reliability · grade {score['grade']}"},
            gauge={"axis": {"range": [0, 100]}, "bar": {"color": "#2563eb"},
                   "steps": [{"range": [0, 70], "color": "#fecaca"}, {"range": [70, 90], "color": "#fde68a"},
                             {"range": [90, 100], "color": "#bbf7d0"}]}))
        fig.update_layout(height=260, margin=dict(l=20, r=20, t=60, b=10))
        st.plotly_chart(fig, **PLOT_W)
        comp = score["components"]
        st.caption(f"Success {comp['success']} · Retry health {comp['retry']} · Stability {comp['stability']}")
        st.caption(f"Statistical anomalies: **{rel['anomaly_count']}**")
        for d in rel["degradation"]:
            st.warning(f"{d['direction']} {d['size_bucket']} {d['metric']} {d['change_pct']:+.0f}% vs baseline")
    with right:
        df = pd.DataFrame(series["points"])
        df["time"] = pd.to_datetime(df["bucket_start"], unit="s", utc=True)
        t1, t2, t3, t4 = st.tabs(["Throughput", "Latency", "Success & failures", "By file size"])
        with t1:
            st.plotly_chart(px.line(df, x="time", y="avg_throughput_mbps", color="direction", markers=True,
                                    labels={"avg_throughput_mbps": "MB/s"}), **PLOT_W)
        with t2:
            fig = px.line(df, x="time", y="avg_latency_ms", color="direction", labels={"avg_latency_ms": "ms"})
            fig.add_scatter(x=df["time"], y=df["p95_latency_ms"], mode="markers", name="p95", marker=dict(size=4))
            st.plotly_chart(fig, **PLOT_W)
        with t3:
            st.plotly_chart(px.bar(df, x="time", y=["count", "failures"], barmode="overlay", opacity=0.8,
                                   color_discrete_map={"count": "#93c5fd", "failures": "#dc2626"}),
                            **PLOT_W)
        with t4:
            sz = pd.DataFrame(summ["throughput_by_size"]).T.reset_index(names="size")
            if not sz.empty:
                st.plotly_chart(px.bar(sz, x="size", y="avg_throughput_mbps", text="count",
                                       category_orders={"size": ["small", "medium", "large", "xlarge"]}),
                                **PLOT_W)

    st.subheader("Alerts")
    open_alerts = [a for a in alerts if not a["acknowledged"]]
    st.caption(f"{len(open_alerts)} open · {len(alerts) - len(open_alerts)} acknowledged")
    for a in alerts[:15]:
        cols = st.columns([8, 1])
        cols[0].markdown(f"{SEV_ICON.get(a['severity'], '')} `{a['created_iso']}` **{a['rule']}** - {a['message']}"
                         + (" ✅" if a["acknowledged"] else ""))
        if is_admin and not a["acknowledged"]:
            cols[1].button("Ack", key=f"ack-{a['id']}", on_click=ack_alert, args=(a["id"],))

    a, b = st.columns(2)
    with a:
        st.subheader("Recurring failure patterns")
        pats = rel["failure_patterns"]["recurring"]
        st.dataframe(pd.DataFrame(pats)[["pattern", "count", "share_of_failures_pct"]] if pats else
                     pd.DataFrame({"pattern": []}), hide_index=True, **DF_W)
    with b:
        st.subheader("Recommendations")
        for r in rel["recommendations"]:
            with st.expander(f"{SEV_ICON[r['severity']]} {r['title']}"):
                st.write(r["detail"])

    st.subheader(f"Transfer history ({hist['total']} in range, showing {len(hist['items'])})")
    h = pd.DataFrame(hist["items"])
    h["size_MB"] = (h["file_size_bytes"] / 1048576).round(2)
    show = h[["started_iso", "direction", "bucket", "key", "size_MB", "status", "duration_s", "throughput_mbps",
              "latency_ms", "retries", "error_code", "instance_id"]]
    st.dataframe(show.style.map(lambda v: "color:#dc2626;font-weight:600" if v == "failed" else "", subset=["status"]),
                 hide_index=True, **DF_W)


live()
