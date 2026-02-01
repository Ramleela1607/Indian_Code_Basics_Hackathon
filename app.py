import os
import base64
import time
import requests
import pandas as pd
import streamlit as st
from io import BytesIO
import base64


# =========================
# PAGE CONFIG
# =========================
st.set_page_config(page_title="Intelligent Farm AI", page_icon="🌾", layout="wide")

# =========================
# BACKGROUND IMAGE (BASE64) + THEME
# =========================
def get_base64_image(path: str) -> str:
    with open(path, "rb") as f:
        return base64.b64encode(f.read()).decode()

BG_IMAGE = "farmer1.jpg"  # put this image in same folder as app.py

if not os.path.exists(BG_IMAGE):
    st.error(f"Background image not found: {BG_IMAGE}. Put it next to app.py")
    st.stop()

bg_base64 = get_base64_image(BG_IMAGE)

st.markdown(
    f"""
<style>
/* Full-page farmer background */
.stApp {{
    background-image: url("data:image/jpg;base64,{bg_base64}");
    background-size: cover;
    background-position: center;
    background-repeat: no-repeat;
}}

/* Light green overlay (tune opacity) */
.stApp::before {{
    content: "";
    position: fixed;
    inset: 0;
    background: rgba(232, 245, 233, 0.65);
    z-index: 0;
}}

/* Keep content above overlay & push down */
.block-container {{
    position: relative;
    z-index: 1;
    padding-top: 6rem;
}}

.main-title {{
    color: #1b5e20;
    font-size: 44px;
    font-weight: 900;
    margin-bottom: 0.2rem;
}}
.sub-title {{
    color: #2e7d32;
    font-size: 18px;
    margin-bottom: 1.2rem;
}}

.glass {{
    background: rgba(255, 255, 255, 0.88);
    border: 1px solid rgba(255,255,255,0.35);
    backdrop-filter: blur(8px);
    -webkit-backdrop-filter: blur(8px);
    border-radius: 16px;
    padding: 18px;
    box-shadow: 0 8px 22px rgba(0,0,0,0.12);
}}

.stButton>button {{
    border-radius: 12px;
    font-weight: 700;
    padding: 0.7rem 1rem;
}}
</style>
""",
    unsafe_allow_html=True
)

# =========================
# HEADER
# =========================
st.markdown('<div class="main-title">🌾 Intelligent Farm AI</div>', unsafe_allow_html=True)
st.markdown('<div class="sub-title">Smart crop & farming insights powered by Databricks</div>', unsafe_allow_html=True)

st.markdown(
    """
<div class="glass">
<b>How it works:</b><br>
Type Country, State/Region and City/District — you will get AI-suggestions.<br>
Click the button to get farmer-friendly AI wording + raw data.
</div>
""",
    unsafe_allow_html=True
)

st.divider()

# =========================
# DATABRICKS CONFIG
# =========================
# ✅ Replace with your NEW token (recommended: use st.secrets later)
DATABRICKS_TOKEN = "dapi2d2657b6fd12953643f6a799ab069394"
DATABRICKS_SQL_ENDPOINT = "https://dbc-ecdd486b-6f8d.cloud.databricks.com/api/2.0/sql/statements"
WAREHOUSE_ID = "b4504872c07b5058"

LANG_MAP = {
    "English": "en",
    "Hindi": "hi",
    "Spanish": "es",
    "French": "fr",
    "German": "de",
    "Italian": "it",
    "Portuguese": "pt",
    "Thai": "th",
}

lang_label = st.selectbox("🌐 Output Language", list(LANG_MAP.keys()), index=0, key="lang_pick")
lang_code = LANG_MAP[lang_label]



def translate_supported(text: str, lang_code: str):
    """
    Uses ai_translate() only (no endpoint).
    If lang_code is None, returns (None, error message).
    """
    text = (text or "").strip()
    if not text:
        return "", None

    if lang_code is None:
        return None, "This language requires a Databricks Model Serving endpoint (not available in Free Edition)."

    q_text = esc(text)
    q_lang = esc(lang_code)

    sql = f"""
    SELECT ai_translate('{q_text}', '{q_lang}') AS translated
    """
    resp, err = run_databricks_sql(sql, max_wait_s=40)
    if err:
        return None, err

    df_t = response_to_df(resp)
    if df_t.empty:
        return None, "ai_translate returned empty result"
    return str(df_t.iloc[0]["translated"]), None


TABLE = "databricks_free_edition.databricks_gold.gold_farm_advisor"

if not all([DATABRICKS_TOKEN, DATABRICKS_SQL_ENDPOINT, WAREHOUSE_ID]):
    st.error("❌ Databricks config is missing")
    st.stop()


# =========================
# HELPERS
# =========================
def esc(s: str) -> str:
    """Escape single quotes for SQL string literals."""
    return (s or "").replace("'", "''")


def run_databricks_sql(statement: str, max_wait_s: int = 40):
    """
    Runs Databricks SQL via /api/2.0/sql/statements and polls until completion.
    Returns (final_json, error_text_or_None).
    """
    headers = {
        "Authorization": f"Bearer {DATABRICKS_TOKEN}",
        "Content-Type": "application/json"
    }

    payload = {
        "statement": statement,
        "warehouse_id": WAREHOUSE_ID,
        "disposition": "INLINE"
    }

    r = requests.post(DATABRICKS_SQL_ENDPOINT, headers=headers, json=payload, timeout=30)
    if r.status_code != 200:
        return None, f"Submit failed: {r.status_code} - {r.text}"

    resp = r.json()

    # Inline results might already be present
    if resp.get("result") and (resp["result"].get("data_array") or resp["result"].get("data_typed_array")):
        return resp, None

    statement_id = resp.get("statement_id")
    if not statement_id:
        return None, f"No statement_id returned. Response: {resp}"

    status_url = f"{DATABRICKS_SQL_ENDPOINT}/{statement_id}"
    start = time.time()

    while time.time() - start < max_wait_s:
        time.sleep(1.0)
        rr = requests.get(status_url, headers=headers, timeout=30)
        if rr.status_code != 200:
            return None, f"Poll failed: {rr.status_code} - {rr.text}"

        final = rr.json()
        state = final.get("status", {}).get("state", "")

        if state == "SUCCEEDED":
            return final, None
        if state in ("FAILED", "CANCELED"):
            return None, f"Query {state}. Details: {final}"

    return None, "Query timed out while waiting for results."


def response_to_df(resp: dict) -> pd.DataFrame:
    """Convert Databricks SQL response (INLINE) into a pandas DataFrame if possible."""
    if not resp:
        return pd.DataFrame()

    cols = resp.get("manifest", {}).get("schema", {}).get("columns", [])
    col_names = [c.get("name") for c in cols] if cols else None
    result = resp.get("result", {}) or {}

    if "data_array" in result and col_names:
        return pd.DataFrame(result["data_array"], columns=col_names)

    if "data_typed_array" in result and col_names:
        rows = []
        for row in result["data_typed_array"]:
            rows.append([
                cell.get("str") if cell.get("str") is not None else
                cell.get("double") if cell.get("double") is not None else
                cell.get("long") if cell.get("long") is not None else
                cell.get("bool") if cell.get("bool") is not None else
                None
                for cell in row
            ])
        return pd.DataFrame(rows, columns=col_names)

    return pd.DataFrame()


@st.cache_data(ttl=300)
def fetch_distinct_values(sql: str) -> list[str]:
    """Run SQL and return first column as a unique list (cached)."""
    resp, err = run_databricks_sql(sql, max_wait_s=30)
    if err or not resp:
        return []
    df = response_to_df(resp)
    if df.empty:
        return []
    col = df.columns[0]
    vals = df[col].dropna().astype(str).tolist()
    seen = set()
    out = []
    for v in vals:
        if v not in seen:
            seen.add(v)
            out.append(v)
    return out


def suggest_values_prefix(column: str, typed: str, extra_where: str = "", limit: int = 20) -> list[str]:
    """Prefix suggestions from GOLD table using LIKE 'typed%'."""
    typed = (typed or "").strip()
    if len(typed) < 1:
        return []

    t = esc(typed.lower())
    where = f"WHERE lower({column}) LIKE '{t}%'"
    if extra_where:
        where += f" AND {extra_where}"

    sql = f"""
    SELECT DISTINCT {column} AS value
    FROM {TABLE}
    {where}
    ORDER BY value
    LIMIT {limit}
    """
    return fetch_distinct_values(sql)


def generate_farm_advisory(row: dict) -> str:
    """GenAI-style farmer-friendly wording (template-based)."""
    def fmt(x, decimals=2):
        if x is None:
            return "N/A"
        try:
            return f"{float(x):.{decimals}f}"
        except:
            return str(x)

    crop = row.get("crop_cropName", "a suitable crop")
    stage = row.get("crop_growthStage", "unknown stage")

    return f"""
### ✅ Farm Advisory Summary

**Recommended focus crop:** **{crop}**  
**Current crop stage:** {stage}

---

### 🌱 Crop Health
- Health score: **{fmt(row.get("crop_cropHealthScore"))}**
- NDVI index: **{fmt(row.get("crop_ndviIndex"))}**
- Leaf moisture: **{fmt(row.get("crop_leafMoisture"))}**

---

### 🧪 Soil Condition
- Soil moisture: **{fmt(row.get("soil_soilMoisture"))}** (**{row.get("soilMoistureCategory", "Unknown")}**)
- Soil temperature: **{fmt(row.get("soil_temperature"))} °C**
- Soil humidity: **{fmt(row.get("soil_humidity"))} %**

**Action:** If soil moisture is low, use drip irrigation or mulching.

---

### 🐛 Pest Risk
- Category: **{row.get("pestRiskCategory", "Unknown")}**
- Risk score: **{fmt(row.get("pest_pestRisk"))}**

**Action:** If risk is High, inspect leaves weekly and use IPM.

---

### 🌧 Weather & Rainfall
- Rainfall: **{fmt(row.get("rainfall_rainfallMm"))} mm**
- Type: **{row.get("rainfall_rainfallType", "Unknown")}**

---

### 📈 Yield & Profitability (AI)
- Yield prediction: **{fmt(row.get("yieldPredictionScore"))}**
- Profitability: **{fmt(row.get("profitabilityIndex"))}**
- Sustainability: **{fmt(row.get("sustainabilityScore"))}**

---

### 💰 Market Signal
- Market crop price: **{fmt(row.get("market_cropPrice"))}**

---

✅ **Farmer-friendly advice:**  
Focus on **{crop}** now. Keep soil moisture healthy, watch pests (**{row.get("pestRiskCategory","Unknown")}**) and plan irrigation based on rainfall.
""".strip()

@st.cache_data(ttl=300)
def distinct_prefix_matches(column: str, typed: str, extra_where: str = "", limit: int = 50) -> list[str]:
    typed = (typed or "").strip()
    if not typed:
        return []

    t = esc(typed.lower())
    where = f"WHERE lower({column}) LIKE '{t}%'"
    if extra_where:
        where += f" AND {extra_where}"

    sql = f"""
    SELECT DISTINCT {column} AS value
    FROM {TABLE}
    {where}
    ORDER BY value
    LIMIT {limit}
    """
    resp, err = run_databricks_sql(sql, max_wait_s=30)
    if err or not resp:
        return []

    df = response_to_df(resp)
    if df.empty:
        return []

    return df[df.columns[0]].dropna().astype(str).tolist()


def auto_pick_first_match(label: str, column: str, key_prefix: str, extra_where: str = "") -> str:
    """
    User types -> we automatically select the FIRST matching DISTINCT value.
    No dropdown shown.
    """
    typed = st.text_input(label, value=st.session_state.get(f"{key_prefix}_typed", ""), key=f"{key_prefix}_typed").strip()

    matches = distinct_prefix_matches(column, typed, extra_where=extra_where, limit=50)

    # If user typed something and we have matches, auto-pick the first
    picked = typed
    if typed and matches:
        picked = matches[0]

    # Store picked so other fields can filter correctly
    st.session_state[f"{key_prefix}_picked"] = picked

    # Optional: show what got auto-picked + how many matches (no dropdown)
    if typed and matches:
        st.caption(f"✅ Auto-selected: **{picked}**  |  {len(matches)} matches")

    return picked



# =========================
# INPUT UI (AUTO-SUGGEST)
# =========================
st.markdown("### 🌍 Enter Location & Soil Details")
col1, col2, col3, col4 = st.columns(4)

with col1:
    # COUNTRY: auto-pick
    country = auto_pick_first_match(
        label="Country",
        column="soil_country",
        key_prefix="country"
    )

with col2:
    state = st.selectbox(
        "State / Region",
        ["Region-0", "Region-1", "Region-2", "Region-3", "Region-4"],
        key="state_fixed"
    )

with col3:
    # CITY: auto-pick, filtered by country + state
    city_filters = []
    if country:
        city_filters.append(f"lower(soil_country) = lower('{esc(country)}')")
    if state:
        city_filters.append(f"lower(soil_stateOrRegion) = lower('{esc(state)}')")
    city_where = " AND ".join(city_filters)

    city = auto_pick_first_match(
        label="City / District",
        column="city",
        key_prefix="city",
        extra_where=city_where
    )

with col4:
    soil_type = st.selectbox(
        "Soil Type (Optional)",
        ["Alluvial", "Black", "Red", "Laterite", "Sandy", "Clay"],
        key="soil_type"
    )

st.divider()

def get_localized_description_from_row(best_row: dict, lang_code: str):
    desc = best_row.get("description")
    if not desc:
        return None, "No description in row."

    q_desc = esc(desc)
    lang = esc(lang_code)

    sql = f"""
    SELECT ai_translate('{q_desc}', '{lang}') AS localized_description
    """
    resp, err = run_databricks_sql(sql, max_wait_s=30)
    if err:
        return None, err

    df_desc = response_to_df(resp)
    if df_desc.empty:
        return None, "Translation returned empty."

    return str(df_desc.iloc[0]["localized_description"]), None

# =========================
# ACTION BUTTON
# =========================
if st.button("🌾 Get Farming Recommendation", use_container_width=True, key="get_reco_btn"):
    st.toast(
        "🙏 Thank you, farmers, for turning soil into sustenance and effort into hope.",
        icon="🌾"
    )
    missing = []
    if not country: missing.append("Country")
    if not state: missing.append("State/Region")
    if not city: missing.append("City/District")

    if missing:
        st.warning("Please enter: " + ", ".join(missing))
        st.stop()

    q_country = esc(country)
    q_state = esc(state)
    q_city = esc(city)

    query = f"""
    SELECT
        date,
        soil_country,
        soil_stateOrRegion,
        city,
        crop_cropName,
        crop_growthStage,
        crop_cropHealthScore,
        crop_ndviIndex,
        crop_leafMoisture,
        soil_soilMoisture,
        soil_temperature,
        soil_humidity,
        soilMoistureCategory,
        pestRiskCategory,
        pest_pestRisk,
        rainfall_rainfallMm,
        rainfall_rainfallType,
        yieldPredictionScore,
        profitabilityIndex,
        sustainabilityScore,
        market_cropPrice,
        description
    FROM {TABLE}
    WHERE lower(soil_country) = lower('{q_country}')
      AND lower(soil_stateOrRegion) = lower('{q_state}')
      AND lower(city) = lower('{q_city}')
    ORDER BY profitabilityIndex DESC, yieldPredictionScore DESC
    LIMIT 5
    """

    with st.spinner("🔍 Analysis in progress..."):
        resp, err = run_databricks_sql(query, max_wait_s=40)

    if err:
        st.error(err)
        st.stop()

    df = response_to_df(resp)

    if df.empty:
        st.info("No data found OR results were not returned inline.")
        with st.expander("🔎 Debug Response"):
            st.json(resp)
    
    else:
        best = df.iloc[0].to_dict()
        st.markdown(generate_farm_advisory(best))
    
        localized_desc, desc_err = translate_supported(best.get("description", ""), lang_code)
        
        if desc_err:
            st.warning(f"AI description not available: {desc_err}")
            df["ai_description"] = ""
        else:
            st.markdown("### 🤖 AI Description (Translated)")
            st.markdown(f"<div class='glass'>{localized_desc}</div>", unsafe_allow_html=True)
            df["ai_description"] = localized_desc










