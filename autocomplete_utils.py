import streamlit as st
import pandas as pd

# These functions EXPECT that your main app provides:
# - run_databricks_sql(sql)
# - response_to_df(resp)
# - esc(string)

@st.cache_data(ttl=300)
def _fetch_values(sql: str, run_sql, to_df) -> list[str]:
    """
    Execute SQL and return first column as a list of unique strings.
    Cached for performance.
    """
    resp, err = run_sql(sql)
    if err or not resp:
        return []

    df = to_df(resp)
    if df.empty:
        return []

    col = df.columns[0]
    values = df[col].dropna().astype(str).tolist()

    seen = set()
    unique = []
    for v in values:
        if v not in seen:
            seen.add(v)
            unique.append(v)
    return unique


def suggest_values(
    *,
    table: str,
    column: str,
    typed: str,
    run_sql,
    to_df,
    esc_fn,
    extra_where: str = "",
    limit: int = 20
) -> list[str]:
    """
    Generic autocomplete suggestion function.

    Parameters:
    - table: full table name
    - column: column to suggest from
    - typed: user input text
    - run_sql: function to execute SQL
    - to_df: function to convert response to DataFrame
    - esc_fn: SQL escape function
    - extra_where: optional SQL filter (e.g. country/state)
    - limit: max suggestions
    """
    typed = (typed or "").strip()
    if len(typed) < 1:
        return []

    typed_sql = esc_fn(typed.lower())

    where = f"WHERE lower({column}) LIKE '{typed_sql}%'"
    if extra_where:
        where += f" AND {extra_where}"

    sql = f"""
    SELECT DISTINCT {column} AS value
    FROM {table}
    {where}
    ORDER BY value
    LIMIT {limit}
    """

    return _fetch_values(sql, run_sql, to_df)
