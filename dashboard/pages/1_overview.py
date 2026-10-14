import streamlit as st
from utils.athena import run_query

st.title("InsightFlow Marketing Analytics Platform")

# -------------------------------
# CRM
# -------------------------------

crm_df = run_query("""
SELECT SUM(total_leads) AS total_leads
FROM gold_crm_lead_summary
""")

total_leads = int(crm_df.iloc[0]["total_leads"])

# -------------------------------
# Wistia
# -------------------------------

wistia_df = run_query("""
SELECT SUM(total_events) AS total_events,
       SUM(unique_visitors) AS unique_visitors
FROM gold_wistia_event_summary
""")
total_events = int(
    wistia_df.iloc[0]["total_events"]
)

unique_visitors = int(
    wistia_df.iloc[0]["unique_visitors"]
)

# -------------------------------
# Calendly Bookings
# -------------------------------

booking_df = run_query("""
SELECT SUM(total_bookings) AS total_bookings
FROM gold_calendly_daily_booking_summary
""")

total_bookings = int(
    booking_df.iloc[0]["total_bookings"]
)

# -------------------------------
# Calendly Spend
# -------------------------------

spend_df = run_query("""
SELECT SUM(total_spend) AS total_spend
FROM gold_calendly_spend_summary
""")

total_spend = float(
    spend_df.iloc[0]["total_spend"]
)

# -------------------------------
# KPI Cards
# -------------------------------

c1, c2, c3, c4 = st.columns(4)

with c1:
    st.metric(
        "Total Leads",
        f"{total_leads:,}"
    )

with c2:
    st.metric(
        "Video Events",
        f"{total_events:,}"
    )

with c3:
    st.metric(
        "Bookings",
        f"{total_bookings:,}"
    )

with c4:
    st.metric(
        "Marketing Spend",
        f"${total_spend:,.2f}"
    )

st.divider()

# -------------------------------
# Secondary Metrics
# -------------------------------

c5, c6 = st.columns(2)

with c5:
    st.metric(
        "Unique Visitors",
    f"{unique_visitors:,}"
    )

with c6:
    conversion_rate = (
        (total_leads / total_bookings) * 100
        if total_bookings > 0
        else 0
    )

    st.metric(
        "Lead / Booking %",
        f"{conversion_rate:.2f}%"
    )