import streamlit as st
import requests
import pandas as pd
import json
import time

st.set_page_config("SLA Dashboard", layout="wide")
st.title("📊 SLA Freshness Dashboard")

api_url = st.text_input(
    "API Gateway URL",
    "https://7iw8ntpmy0.execute-api.us-east-1.amazonaws.com"
)

auto = st.checkbox("Auto refresh", value=True)
interval = st.number_input("Refresh seconds", 30, 300, 60)

def load_data():
    r = requests.get(api_url, timeout=10)
    data = r.json()
    if isinstance(data, dict) and "body" in data:
        data = json.loads(data["body"])
    return data

if st.button("Load SLA Data") or auto:
    data = load_data()

    pipeline = pd.DataFrame(data["pipeline_sla"])
    kpi = data["business_kpi"]
    trend = pd.DataFrame(data["business_trend_90d"])

    critical = pipeline[pipeline["status"] == "critically_late"]

    if not critical.empty:
        st.error(f"🚨 ALERT: {len(critical)} source(s) critically late")

    st.subheader("Layer 1 — Pipeline SLA")
    st.dataframe(pipeline, use_container_width=True)

    st.subheader("Layer 2 — Business SLA (KPI)")
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Delivered", kpi["total_delivered"])
    c2.metric("Late Orders", kpi["late_orders"])
    c3.metric("Late %", kpi["late_percentage"])
    c4.metric("Avg Days Late", kpi["avg_days_late"])

    st.subheader("Business SLA Trend (90 Days)")
    st.line_chart(trend.set_index("delivered_day")["late_percentage"])

    if auto:
        time.sleep(interval)
        st.rerun()
