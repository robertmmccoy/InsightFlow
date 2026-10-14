import streamlit as st
from utils.athena import run_query

st.title("Marketing Funnel Analysis")

# =====================================
# CRM Metrics
# =====================================

crm_df = run_query("""
    SELECT
        SUM(total_leads) AS total_leads
    FROM gold_crm_lead_summary
""")

total_leads = int(
    crm_df.iloc[0]["total_leads"]
)

# =====================================
# Wistia Metrics
# =====================================

wistia_df = run_query("""
    SELECT
        SUM(unique_visitors) AS unique_visitors
    FROM gold_wistia_video_summary
""")

unique_visitors = int(
    wistia_df.iloc[0]["unique_visitors"]
)

# =====================================
# Calendly Bookings
# =====================================

booking_df = run_query("""
    SELECT
        SUM(total_bookings) AS total_bookings
    FROM gold_calendly_daily_booking_summary
""")

total_bookings = int(
    booking_df.iloc[0]["total_bookings"]
)

# =====================================
# Marketing Spend
# =====================================

spend_df = run_query("""
    SELECT
        SUM(total_spend) AS total_spend
    FROM gold_calendly_spend_summary
""")

total_spend = float(
    spend_df.iloc[0]["total_spend"]
)

# =====================================
# Cost Metrics
# =====================================

cost_per_booking = (
    total_spend / total_bookings
    if total_bookings > 0
    else 0
)

cost_per_lead = (
    total_spend / total_leads
    if total_leads > 0
    else 0
)

# =====================================
# Executive Overview
# =====================================

st.subheader("Executive Overview")

c1, c2, c3, c4 = st.columns(4)

with c1:
    st.metric(
        "Marketing Spend",
    f"${total_spend:,.2f}"
    )

with c2:
    st.metric(
        "Video Visitors",
        f"{unique_visitors:,}"
    )

with c3:
    st.metric(
        "Bookings",
        f"{total_bookings:,}"
    )

with c4:
    st.metric(
        "Leads",
        f"{total_leads:,}"
    )

st.divider()

# =====================================
# Marketing To Lead Journey
# =====================================

st.subheader("Marketing to Lead Journey")

j1, j2, j3, j4 = st.columns(4)

with j1:
    st.metric(
        "Marketing Spend",
        f"${total_spend:,.2f}"
    )

with j2:
    st.metric(
        "Video Visitors",
        f"{unique_visitors:,}"
    )

with j3:
    st.metric(
        "Bookings",
        f"{total_bookings:,}"
    )

with j4:
    st.metric(
        "Leads",
        f"{total_leads:,}"
    )

st.info(
    """
    Marketing Spend
    ➜ Video Visitors
    ➜ Calendly Bookings
    ➜ CRM Leads
    """
)

st.divider()

# =====================================
# Marketing Performance Metrics
# =====================================

st.subheader("Marketing Performance Metrics")

c5, c6 = st.columns(2)

with c5:
    st.metric(
        "Cost Per Booking",
        f"${cost_per_booking:.2f}"
    )

with c6:
    st.metric(
        "Cost Per Lead",
        f"${cost_per_lead:.2f}"
    )

st.divider()

# =====================================
# Executive Summary Data
# =====================================

channel_df = run_query("""
    SELECT
        channel,
        SUM(total_spend) AS total_spend
    FROM gold_calendly_spend_summary
    GROUP BY channel
    ORDER BY total_spend DESC
    LIMIT 1
""")

top_channel = channel_df.iloc[0]["channel"]

country_df = run_query("""
    SELECT
        country,
        unique_visitors
    FROM gold_wistia_country_summary
    ORDER BY unique_visitors DESC
    LIMIT 1
""")

top_country = country_df.iloc[0]["country"]

funnel_df = run_query("""
    SELECT
        cf_source_funnel,
        SUM(total_leads) AS total_leads
    FROM gold_crm_funnel_summary
    WHERE cf_source_funnel IS NOT NULL
    AND cf_source_funnel <> 'None'
    GROUP BY cf_source_funnel
    ORDER BY total_leads DESC
    LIMIT 1
""")

top_funnel = funnel_df.iloc[0]["cf_source_funnel"]

# =====================================
# Executive Summary
# =====================================

st.subheader("Executive Summary")

st.markdown(
    f"""
    ### Key Findings
    
    **Marketing Performance**

    - Total marketing spend: **${total_spend:,.2f}**
    - Highest spend channel: **{top_channel.replace("_", " ").title()}**
    - Cost per booking: **${cost_per_booking:.2f}**
    - Cost per lead: **${cost_per_lead:.2f}**

    **Audience Engagement**

    - Total video visitors: **{unique_visitors:,}**
    - Strongest visitor country: **{top_country}**

    **Lead Generation**

    - Total bookings generated: **{total_bookings:,}**
    - Total leads generated: **{total_leads:,}**
    - Most successful lead funnel: **{top_funnel}**
    """
)