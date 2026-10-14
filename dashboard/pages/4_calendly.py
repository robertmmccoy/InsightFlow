import streamlit as st
import plotly.express as px
from utils.athena import run_query

st.title("Calendly Analytics")

# =====================================
# KPI Metrics
# =====================================

kpi_df = run_query("""
SELECT
(
    SELECT SUM(total_bookings)
    FROM gold_calendly_daily_booking_summary
    ) AS total_bookings,

    (
    SELECT SUM(total_spend)
    FROM gold_calendly_spend_summary
    ) AS total_spend
""")

total_bookings = int(
    kpi_df.iloc[0]["total_bookings"]
)

total_spend = float(
    kpi_df.iloc[0]["total_spend"]
)

avg_daily_bookings_df = run_query("""
SELECT
    AVG(total_bookings) AS avg_daily_bookings
FROM gold_calendly_daily_booking_summary
""")

avg_daily_bookings = float(
    avg_daily_bookings_df.iloc[0]["avg_daily_bookings"]
)

cost_per_booking = (
    total_spend / total_bookings
    if total_bookings > 0
    else 0
)

c1, c2, c3, c4 = st.columns(4)

with c1:
    st.metric(
        "Total Bookings",
        f"{total_bookings:,}"
    )

with c2:
    st.metric(
        "Marketing Spend",
        f"${total_spend:,.2f}"
    )

with c3:
    st.metric(
        "Avg Daily Bookings",
    f"{avg_daily_bookings:.1f}"
    )
with c4:
    st.metric(
        "Cost Per Booking",
        f"${cost_per_booking:.2f}"
    )

st.divider()

# =====================================
# Booking Trend
# =====================================

booking_df = run_query("""
SELECT
    booking_date,
    total_bookings
FROM gold_calendly_daily_booking_summary
WHERE booking_date IS NOT NULL
ORDER BY booking_date
""")

st.subheader("Daily Booking Trend")

booking_fig = px.line(
    booking_df,
    x="booking_date",
    y="total_bookings",
    markers=True
)

booking_fig.update_layout(
    xaxis_title="Booking Date",
    yaxis_title="Total Bookings"
)

st.plotly_chart(
    booking_fig,
    use_container_width=True
)

st.divider()

# =====================================
# Spend by Channel
# =====================================

spend_df = run_query("""
SELECT
    channel,
    SUM(total_spend) AS total_spend
FROM gold_calendly_spend_summary
GROUP BY channel
ORDER BY total_spend DESC
""")

st.subheader("Spend by Marketing Channel")

spend_fig = px.bar(
    spend_df,
    x="channel",
    y="total_spend",
    color="total_spend",
    text="total_spend"
)

spend_fig.update_layout(
    xaxis_title="Channel",
    yaxis_title="Total Spend ($)",
    showlegend=False
)

spend_fig.update_traces(
    texttemplate="$%{y:,.0f}",
    textposition="outside"
)

st.plotly_chart(
    spend_fig,
    use_container_width=True
)

st.divider()

# =====================================
# Event Type Performance
# =====================================

event_df = run_query("""
SELECT
    event_name,
    total_bookings
FROM gold_calendly_event_summary
ORDER BY total_bookings DESC
LIMIT 10
""")

st.subheader("Top Event Types")

event_fig = px.bar(
    event_df,
    x="event_name",
    y="total_bookings",
    color="total_bookings",
    text="total_bookings"
)

event_fig.update_layout(
    xaxis_title="Event Type",
    yaxis_title="Bookings",
    showlegend=False
)

event_fig.update_traces(
    textposition="outside"
)

st.plotly_chart(
    event_fig,
    use_container_width=True
)

# =====================================
# Raw Data
# =====================================

with st.expander("View Calendly Summary Data"):

    st.markdown("### Booking Summary")
    st.dataframe(
    booking_df,
    use_container_width=True
    )

    st.markdown("### Spend Summary")
    st.dataframe(
        spend_df,
        use_container_width=True
    )

    st.markdown("### Event Summary")
    st.dataframe(
        event_df,
        use_container_width=True
    )