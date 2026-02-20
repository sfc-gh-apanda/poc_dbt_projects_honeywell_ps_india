# ═══════════════════════════════════════════════════════════════════════════════
# Snowflake Native Streamlit App - Hello World
# ═══════════════════════════════════════════════════════════════════════════════
#
# Deploy as Snowflake Native Streamlit App:
#
#   CREATE OR REPLACE STREAMLIT MY_DB.MY_SCHEMA.HELLO_WORLD_APP
#       ROOT_LOCATION = '@MY_DB.MY_SCHEMA.MY_STAGE/streamlit_app'
#       MAIN_FILE = 'streamlit_app.py'
#       QUERY_WAREHOUSE = 'COMPUTE_WH';
#
# ═══════════════════════════════════════════════════════════════════════════════

import streamlit as st

# ─────────────────────────────────────────────
# Page Config
# ─────────────────────────────────────────────
st.set_page_config(
    page_title="Hello and welcome to Snowflake",
    page_icon="❄️",
    layout="centered"
)

# ─────────────────────────────────────────────
# Header
# ─────────────────────────────────────────────
st.title("❄️ Hello, Snowflake!, we have changed the base code.")
st.subheader("Snowflake Native Streamlit App")

st.divider()

# ─────────────────────────────────────────────
# Snowflake Session Info
# ─────────────────────────────────────────────
st.header("Session Information")

from snowflake.snowpark.context import get_active_session

session = get_active_session()

session_info = session.sql("""
    SELECT
        CURRENT_USER()       AS current_user,
        CURRENT_ROLE()       AS current_role,
        CURRENT_WAREHOUSE()  AS current_warehouse,
        CURRENT_DATABASE()   AS current_database,
        CURRENT_SCHEMA()     AS current_schema,
        CURRENT_TIMESTAMP()  AS current_timestamp
""").collect()

if session_info:
    row = session_info[0]
    col1, col2 = st.columns(2)
    with col1:
        st.metric("User", row["CURRENT_USER"])
        st.metric("Role", row["CURRENT_ROLE"])
        st.metric("Warehouse", row["CURRENT_WAREHOUSE"])
    with col2:
        st.metric("Database", row["CURRENT_DATABASE"])
        st.metric("Schema", row["CURRENT_SCHEMA"])
        st.metric("Timestamp", str(row["CURRENT_TIMESTAMP"])[:19])

st.divider()

# ─────────────────────────────────────────────
# Interactive Query
# ─────────────────────────────────────────────
st.header("Run a Query")

default_query = "SELECT 'Hello from Snowflake!' AS greeting, 42 AS answer"
query = st.text_area("Enter SQL:", value=default_query, height=100)

if st.button("Run Query", type="primary"):
    try:
        df = session.sql(query).to_pandas()
        st.success(f"Query returned {len(df)} row(s)")
        st.dataframe(df, use_container_width=True)
    except Exception as e:
        st.error(f"Query failed: {e}")

st.divider()

# ─────────────────────────────────────────────
# Footer
# ─────────────────────────────────────────────
st.caption("Built with Streamlit on Snowflake | Honeywell PoC")
