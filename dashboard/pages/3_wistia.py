import streamlit as st
import plotly.express as px
from utils.athena import run_query

st.title("Wistia Analytics")

# =====================================
# KPI Metrics
# =====================================

kpi_df = run_query("""
SELECT
    SUM(total_events) AS total_events,
    SUM(unique_visitors) AS unique_visitors,
    AVG(avg_percent_viewed) AS avg_percent_viewed,
    AVG(engagement_score) AS engagement_score,
    SUM(hours_watched) AS hours_watched
FROM gold_wistia_video_summary
""")

total_events = int(
    kpi_df.iloc[0]["total_events"]
)

unique_visitors = int(
    kpi_df.iloc[0]["unique_visitors"]
)

hours_watched = float(
    kpi_df.iloc[0]["hours_watched"]
)

avg_percent_viewed = (
    float(
        kpi_df.iloc[0]["avg_percent_viewed"]
    ) * 100
)

engagement_score = float(
    kpi_df.iloc[0]["engagement_score"]
)

c1, c2, c3, c4, c5 = st.columns(5)

with c1:
    st.metric(
        "Video Events",
        f"{total_events:,}"
    )

with c2:
    st.metric(
        "Unique Visitors",
    f"{unique_visitors:,}"
    )

with c3:
    st.metric(
        "Hours Watched",
        f"{hours_watched:.2f}"
    )

with c4:
    st.metric(
        "Avg % Viewed",
    f"{avg_percent_viewed:.1f}%"
    )

with c5:
    st.metric(
        "Engagement Score",
        f"{engagement_score:.2f}"
    )

st.divider()

# =====================================
# Country Data
# =====================================

country_df = run_query("""
SELECT
    country,
    total_events,
    unique_visitors,
    avg_percent_viewed
FROM gold_wistia_country_summary
ORDER BY total_events DESC
LIMIT 10
""")

# =====================================
# Top Countries by Video Events
# =====================================

st.subheader("Top Countries by Video Events")

country_fig = px.bar(
    country_df,
    x="country",
    y="total_events",
    color="total_events",
    text="total_events"
)

country_fig.update_layout(
    xaxis_title="Country",
    yaxis_title="Video Events",
    showlegend=False
)

country_fig.update_traces(
    textposition="outside"
)

st.plotly_chart(
    country_fig,
    use_container_width=True
)

st.divider()

# =====================================
# Unique Visitors by Country
# =====================================

st.subheader("Unique Visitors by Country")

visitor_fig = px.bar(
    country_df,
    x="country",
    y="unique_visitors",
    color="unique_visitors",
    text="unique_visitors"
)

visitor_fig.update_layout(
    xaxis_title="Country",
    yaxis_title="Unique Visitors",
    showlegend=False
)

visitor_fig.update_traces(
    textposition="outside"
)

st.plotly_chart(
    visitor_fig,
    use_container_width=True
)

st.divider()

# =====================================
# Average Engagement by Country
# =====================================

st.subheader("Average Engagement by Country")

engagement_fig = px.bar(
    country_df,
    x="country",
    y="avg_percent_viewed",
    color="avg_percent_viewed",
    text="avg_percent_viewed"
)

engagement_fig.update_layout(
    xaxis_title="Country",
    yaxis_title="Average % Viewed",
    showlegend=False
)

engagement_fig.update_traces(
    texttemplate="%{y:.2f}",
    textposition="outside"
)

st.plotly_chart(
    engagement_fig,
    use_container_width=True
)

# =====================================
# Raw Data
# =====================================

with st.expander("View Wistia Summary Data"):

    st.markdown("### Country Summary")

    st.dataframe(
        country_df,
        use_container_width=True
    )