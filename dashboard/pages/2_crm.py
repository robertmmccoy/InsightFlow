import streamlit as st
import plotly.express as px
from utils.athena import run_query

st.title("CRM Analytics")

# =====================================
# Lead Status Data
# =====================================

status_df = run_query("""
SELECT
    status_label,
    SUM(total_leads) AS total_leads
FROM gold_crm_status_summary
GROUP BY status_label
ORDER BY total_leads DESC
""")

# =====================================
# CRM KPI Cards
# =====================================

total_leads = int(
    status_df["total_leads"].sum()
)

potential_leads = int(
    status_df.loc[
        status_df["status_label"] == "Potential",
        "total_leads"
    ].sum()
)

interested_leads = int(
    status_df.loc[
        status_df["status_label"] == "Interested",
        "total_leads"
    ].sum()
)

c1, c2, c3 = st.columns(3)

with c1:
    st.metric(
        "Total Leads",
        f"{total_leads:,}"
    )

with c2:
    st.metric(
        "Potential Leads",
    f"{potential_leads:,}"
    )

with c3:
    st.metric(
        "Interested Leads",
        f"{interested_leads:,}"
    )

st.divider()

# =====================================
# Lead Status Distribution
# =====================================

st.subheader("Lead Status Distribution")

status_fig = px.pie(
    status_df,
    names="status_label",
    values="total_leads",
    hole=0.4,
    title=""
)

status_fig.update_traces(
    textposition="inside",
    textinfo="percent+label"
)

st.plotly_chart(
    status_fig,
    use_container_width=True
)

st.divider()

# =====================================
# Funnel Performance
# =====================================
funnel_df = run_query("""
SELECT
    cf_source_funnel,
    SUM(total_leads) AS total_leads
FROM gold_crm_funnel_summary
WHERE cf_source_funnel IS NOT NULL
AND cf_source_funnel <> 'None'
GROUP BY cf_source_funnel
ORDER BY total_leads DESC
""")

# Show only Top 10 funnels
funnel_df = funnel_df.head(10)

st.subheader("Top 10 Funnel Performance")

funnel_fig = px.bar(
    funnel_df,
    x="cf_source_funnel",
    y="total_leads",
    color="total_leads",
    text="total_leads",
    title=""
)

funnel_fig.update_layout(
    xaxis_title="Lead Source Funnel",
    yaxis_title="Total Leads",
    showlegend=False
)

funnel_fig.update_traces(
    textposition="outside"
)

st.plotly_chart(
    funnel_fig,
    use_container_width=True
)

# =====================================
# Raw Data (Optional)
# =====================================

with st.expander("View CRM Summary Data"):

    st.markdown("### Lead Status Summary")
    st.dataframe(
        status_df,
        use_container_width=True
    )

    st.markdown("### Funnel Summary")
    st.dataframe(
        funnel_df,
        use_container_width=True
    )