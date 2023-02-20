import streamlit as st
import duckdb


conn = duckdb.connect("apache_web_logs")

# Header
st.title("Apache Web Logs Analysis")
st.header("Logs - 22nd January 2019")

# Metadata
st.subheader("Table metadata")
st.dataframe(conn.execute("DESCRIBE joined_log_table").fetchdf())

# Analysis
# total bytes in mb
with st.container():
    col1, col2, col3 = st.columns(3)
    with col1:
        records = int(conn.execute("SELECT COUNT(*) FROM joined_log_table").fetchone()[0])  # type: ignore
        st.metric("Total records in table", records)

    with col2:
        records = sum(1 for _ in open("errors/incorrect_data.txt"))
        st.metric("Total errors recrds", records)

    with col3:
        total_mb = (
            conn.execute("SELECT SUM(bytes) FROM joined_log_table").fetchone()[0] / 1000 / 1000  # type: ignore
        )
        st.metric("Total bytes in MB", total_mb)

with st.container():
    # bytes by hour
    st.subheader("Bytes in MB by hour")
    mb_by_hour = (
        conn.execute(
            """
        SELECT 
            strftime(datetime, '%H')log_hour, 
            (SUM(bytes)/1000/1000)MB 
        FROM joined_log_table 
        GROUP BY log_hour 
        ORDER BY log_hour
        """
        )
        .fetchdf()
        .set_index("log_hour")
    )
    st.line_chart(mb_by_hour)

with st.container():
    col1, col2 = st.columns(2)
    with col1:
        # bytes by status
        st.subheader("Bytes in MB by status")
        mb_by_status = conn.execute(
            "SELECT status, (SUM(bytes)/1000/1000)MB FROM joined_log_table GROUP BY status HAVING MB > 0 ORDER BY MB "
        ).fetchdf()
        st.dataframe(mb_by_status)

        # bytes by method
        st.subheader("Bytes in MB by method")
        mb_by_method = conn.execute(
            "SELECT method, (SUM(bytes)/1000/1000)MB FROM joined_log_table GROUP BY method HAVING MB > 0 ORDER BY MB"
        ).fetchdf()
        st.dataframe(mb_by_method)

    with col2:
        # max bytes record by hour
        st.subheader("Maximum bytes processed record by hour")
        max_host_by_hour = conn.execute(
            """
            WITH table_1 AS (
                SELECT 
                    strftime(datetime, '%H')dt,
                    hostname,
                    bytes
                FROM joined_log_table
            ),
            table_2 AS (SELECT 
                dt,
                hostname,
                bytes,
                ROW_NUMBER() OVER (PARTITION BY dt ORDER BY bytes DESC) row_number
            FROM table_1)
            SELECT dt, hostname, (bytes/1000)KB FROM table_2 WHERE row_number = 1
            ORDER BY 1
            """
        ).fetchdf()
        st.dataframe(max_host_by_hour)
