import streamlit as st
import duckdb
from pathlib import Path

st.set_page_config(
    page_title="Live Flight Activity Dashboard",
    layout="centered"
)

st.title("✈️ Live Global Flight Activity")
st.caption("Data updates as the pipeline runs. Click refresh to see latest data.")

# Manual refresh button (industry standard)
if st.button("🔄 Refresh data"):
    st.rerun()

# Find latest parquet file
parquet_files = sorted(Path("data/processed").rglob("flights.parquet"))
if not parquet_files:
    st.error("No data found yet. Run the pipeline first.")
    st.stop()

latest = parquet_files[-1]

con = duckdb.connect()

# KPIs
total_flights = con.execute(
    f"SELECT COUNT(*) FROM read_parquet('{latest}')"
).fetchone()[0]

top_countries = con.execute(
    f"""
    SELECT origin_country, COUNT(*) AS flights
    FROM read_parquet('{latest}')
    GROUP BY origin_country
    ORDER BY flights DESC
    LIMIT 5
    """
).fetchdf()

avg_metrics = con.execute(
    f"""
    SELECT 
        ROUND(AVG(altitude), 0) AS avg_altitude_m,
        ROUND(AVG(velocity), 1) AS avg_speed_mps
    FROM read_parquet('{latest}')
    """
).fetchone()

# Display KPIs
st.metric("✈️ Active Flights", total_flights)
st.metric("🛫 Avg Altitude (m)", avg_metrics[0])
st.metric("💨 Avg Speed (m/s)", avg_metrics[1])

st.subheader("🌍 Top Countries by Active Flights")
st.dataframe(top_countries, use_container_width=True)

st.caption("Data source: Live aircraft transponder data (ADS-B)")