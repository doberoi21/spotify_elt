import streamlit as st
import pandas as pd
import plotly.express as px
import snowflake.connector
from pathlib import Path

# ── PAGE CONFIG ──────────────────────────────────────────────────
st.set_page_config(
    page_title="Music Chart Analytics",
    page_icon="🎵",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ── CUSTOM CSS ───────────────────────────────────────────────────
st.markdown("""
<style>
.stApp { background-color: #0e0e0e; }
.main .block-container { padding-top: 1.5rem; }
[data-testid="metric-container"] {
    background: linear-gradient(135deg, #1a1a2e 0%, #16213e 100%);
    border: 1px solid #1DB954;
    border-radius: 12px;
    padding: 1rem;
}
[data-testid="metric-container"] label {
    color: #1DB954 !important;
    font-size: 0.8rem !important;
    font-weight: 600 !important;
    text-transform: uppercase;
    letter-spacing: 0.05em;
}
[data-testid="metric-container"] [data-testid="stMetricValue"] {
    color: #ffffff !important;
    font-size: 2rem !important;
    font-weight: 700 !important;
}
h1, h2 { color: #1DB954 !important; }
h3 { color: #ffffff !important; }
[data-testid="stSidebar"] { background: #111111; }
hr { border-color: #1a1a2e; }
</style>
""", unsafe_allow_html=True)

# ── SNOWFLAKE CONNECTION ─────────────────────────────────────────
@st.cache_resource
def get_connection():
    # Always use st.secrets on Streamlit Cloud
    return snowflake.connector.connect(
        account   = st.secrets["snowflake"]["account"],
        user      = st.secrets["snowflake"]["user"],
        password  = st.secrets["snowflake"]["password"],
        warehouse = st.secrets["snowflake"]["warehouse"],
        database  = st.secrets["snowflake"]["database"],
        role      = st.secrets["snowflake"]["role"],
    )


@st.cache_data
def load_data():
    base = Path(__file__).parent
    tracks  = pd.read_csv(base / "data" / "tracks.csv")
    artists = pd.read_csv(base / "data" / "artists.csv")
    return tracks, artists

# ── LOAD DATA ────────────────────────────────────────────────────
try:
    tracks_df, artists_df = load_data()
except Exception as e:
    st.error(f"Could not load data: {e}")
    st.stop()

# ── DETECT COLUMN NAMES ──────────────────────────────────────────
# Handle both 'genre' and 'track_genre' depending on dbt model output
GENRE_COL    = "track_genre"    if "track_genre"    in tracks_df.columns else "genre"
MARKET_COL   = "chart_market"   if "chart_market"   in tracks_df.columns else "market"
RELEASE_COL  = "release_category" if "release_category" in tracks_df.columns else "release_category"
DAYS_COL     = "days_since_release" if "days_since_release" in tracks_df.columns else "days_since_release"
TRACK_COL    = "track_name"     if "track_name"     in tracks_df.columns else "name"
ARTIST_COL   = "artist_name"    if "artist_name"    in tracks_df.columns else "artist"
PRICE_COL    = "price"          if "price"          in tracks_df.columns else "price"
TIER_COL     = "price_tier"     if "price_tier"     in tracks_df.columns else "price_tier"

# ── SIDEBAR ──────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("# 🎵 Music Charts")
    st.markdown("---")

    if MARKET_COL in tracks_df.columns:
        markets = ["All"] + sorted(tracks_df[MARKET_COL].dropna().unique().tolist())
        selected_market = st.selectbox("Chart Market", markets)
    else:
        selected_market = "All"

    if GENRE_COL in tracks_df.columns:
        genres = ["All"] + sorted(tracks_df[GENRE_COL].dropna().unique().tolist())
        selected_genre = st.selectbox("Genre", genres)
    else:
        selected_genre = "All"

    if RELEASE_COL in tracks_df.columns:
        release_cats = ["All"] + sorted(tracks_df[RELEASE_COL].dropna().unique().tolist())
        selected_release = st.selectbox("Release Category", release_cats)
    else:
        selected_release = "All"

    st.markdown("---")
    st.markdown("""
    **Data pipeline:**
    - Source: iTunes Top 50
    - Warehouse: Snowflake
    - Transform: dbt
    - Orchestration: Airflow
    """)
    st.markdown("---")
    if st.button("🔄 Refresh data"):
        st.cache_data.clear()
        st.rerun()

    # Debug: show actual column names
    with st.expander("Debug — column names"):
        st.write("Tracks:", list(tracks_df.columns))
        st.write("Artists:", list(artists_df.columns))

# ── FILTER DATA ──────────────────────────────────────────────────
filtered = tracks_df.copy()
if selected_market != "All" and MARKET_COL in filtered.columns:
    filtered = filtered[filtered[MARKET_COL] == selected_market]
if selected_genre != "All" and GENRE_COL in filtered.columns:
    filtered = filtered[filtered[GENRE_COL] == selected_genre]
if selected_release != "All" and RELEASE_COL in filtered.columns:
    filtered = filtered[filtered[RELEASE_COL] == selected_release]

filtered_artists = artists_df.copy()
if selected_market != "All" and MARKET_COL in filtered_artists.columns:
    filtered_artists = filtered_artists[
        filtered_artists[MARKET_COL] == selected_market
    ]

# ── HEADER ───────────────────────────────────────────────────────
st.markdown(
    "<h1 style='color:#1DB954;font-size:2.5rem;'>🎵 Global Music Chart Analytics</h1>",
    unsafe_allow_html=True
)
st.markdown(
    f"<p style='color:#888;'>iTunes Top 50 · US & UK Markets · "
    f"Showing <b style='color:#fff;'>{len(filtered)}</b> tracks</p>",
    unsafe_allow_html=True
)

# ── METRIC CARDS ─────────────────────────────────────────────────
col1, col2, col3, col4, col5 = st.columns(5)

col1.metric("Total Tracks",
    len(filtered[TRACK_COL].unique()) if TRACK_COL in filtered.columns else len(filtered))
col2.metric("Unique Artists",
    len(filtered[ARTIST_COL].unique()) if ARTIST_COL in filtered.columns else "—")
col3.metric("Genres",
    len(filtered[GENRE_COL].dropna().unique()) if GENRE_COL in filtered.columns else "—")
col4.metric("Avg Days Since Release",
    f"{filtered[DAYS_COL].mean():.0f}" if DAYS_COL in filtered.columns and not filtered.empty else "—")
col5.metric("New Releases",
    len(filtered[filtered[RELEASE_COL] == "new_release"]) if RELEASE_COL in filtered.columns else "—")

st.markdown("---")

# ── ROW 1: TOP ARTISTS + GENRE PIE ───────────────────────────────
col_left, col_right = st.columns(2)

with col_left:
    st.markdown("## Top Artists")
    if ARTIST_COL in filtered.columns and TRACK_COL in filtered.columns:
        top_artists = (
            filtered.groupby(ARTIST_COL)[TRACK_COL]
            .count().reset_index()
            .rename(columns={TRACK_COL: "tracks"})
            .sort_values("tracks", ascending=True)
            .tail(15)
        )
        fig = px.bar(
            top_artists, x="tracks", y=ARTIST_COL,
            orientation="h",
            color="tracks",
            color_continuous_scale=["#0a4a0a", "#1DB954"],
            labels={"tracks": "Tracks Charted", ARTIST_COL: ""},
        )
        fig.update_layout(
            plot_bgcolor="#0e0e0e", paper_bgcolor="#0e0e0e",
            font_color="#ffffff", showlegend=False,
            coloraxis_showscale=False,
            margin=dict(l=0, r=0, t=10, b=0),
            height=420
        )
        st.plotly_chart(fig, use_container_width=True)

with col_right:
    st.markdown("## Genre Breakdown")
    if GENRE_COL in filtered.columns and TRACK_COL in filtered.columns:
        genre_counts = (
            filtered.groupby(GENRE_COL)[TRACK_COL]
            .count().reset_index()
            .rename(columns={TRACK_COL: "tracks"})
            .sort_values("tracks", ascending=False)
            .head(10)
        )
        fig2 = px.pie(
            genre_counts, values="tracks", names=GENRE_COL,
            color_discrete_sequence=px.colors.sequential.Greens_r,
            hole=0.45
        )
        fig2.update_layout(
            plot_bgcolor="#0e0e0e", paper_bgcolor="#0e0e0e",
            font_color="#ffffff",
            margin=dict(l=0, r=0, t=10, b=0),
            height=420
        )
        fig2.update_traces(textfont_color="white")
        st.plotly_chart(fig2, use_container_width=True)

st.markdown("---")

# ── ROW 2: US VS UK + RELEASE CATEGORY ───────────────────────────
col_l2, col_r2 = st.columns(2)

with col_l2:
    st.markdown("## US vs UK — Genre Battle")
    if MARKET_COL in filtered.columns and GENRE_COL in filtered.columns:
        market_genre = (
            filtered.groupby([MARKET_COL, GENRE_COL])[TRACK_COL]
            .count().reset_index()
            .rename(columns={TRACK_COL: "tracks"})
            .sort_values("tracks", ascending=False)
            .head(20)
        )
        fig3 = px.bar(
            market_genre, x=GENRE_COL, y="tracks",
            color=MARKET_COL,
            barmode="group",
            color_discrete_map={"us": "#1DB954", "gb": "#535353"},
            labels={"tracks": "Tracks", GENRE_COL: "", MARKET_COL: "Market"},
        )
        fig3.update_layout(
            plot_bgcolor="#0e0e0e", paper_bgcolor="#0e0e0e",
            font_color="#ffffff",
            margin=dict(l=0, r=0, t=10, b=0),
            xaxis_tickangle=-30,
            height=360
        )
        st.plotly_chart(fig3, use_container_width=True)

with col_r2:
    st.markdown("## Release Category Mix")
    if RELEASE_COL in filtered.columns and MARKET_COL in filtered.columns:
        rel_counts = (
            filtered.groupby([RELEASE_COL, MARKET_COL])[TRACK_COL]
            .count().reset_index()
            .rename(columns={TRACK_COL: "tracks"})
        )
        fig4 = px.bar(
            rel_counts, x=RELEASE_COL, y="tracks",
            color=MARKET_COL,
            barmode="stack",
            color_discrete_map={"us": "#1DB954", "gb": "#535353"},
            labels={"tracks": "Tracks", RELEASE_COL: "", MARKET_COL: "Market"},
        )
        fig4.update_layout(
            plot_bgcolor="#0e0e0e", paper_bgcolor="#0e0e0e",
            font_color="#ffffff",
            margin=dict(l=0, r=0, t=10, b=0),
            height=360
        )
        st.plotly_chart(fig4, use_container_width=True)

st.markdown("---")

# ── ROW 3: PRICE TIER + DAYS HISTOGRAM ───────────────────────────
col_l3, col_r3 = st.columns(2)

with col_l3:
    st.markdown("## Price Tier Distribution")
    if TIER_COL in filtered.columns:
        price_counts = (
            filtered.groupby(TIER_COL)[TRACK_COL]
            .count().reset_index()
            .rename(columns={TRACK_COL: "tracks"})
        )
        fig5 = px.bar(
            price_counts, x=TIER_COL, y="tracks",
            color=TIER_COL,
            color_discrete_map={
                "free": "#1DB954",
                "standard": "#535353",
                "premium": "#b3b3b3"
            },
            labels={"tracks": "Tracks", TIER_COL: ""},
        )
        fig5.update_layout(
            plot_bgcolor="#0e0e0e", paper_bgcolor="#0e0e0e",
            font_color="#ffffff", showlegend=False,
            margin=dict(l=0, r=0, t=10, b=0),
            height=300
        )
        st.plotly_chart(fig5, use_container_width=True)

with col_r3:
    st.markdown("## Days Since Release")
    if DAYS_COL in filtered.columns:
        fig6 = px.histogram(
            filtered.dropna(subset=[DAYS_COL]),
            x=DAYS_COL, nbins=20,
            color_discrete_sequence=["#1DB954"],
            labels={DAYS_COL: "Days Since Release", "count": "Tracks"},
        )
        fig6.update_layout(
            plot_bgcolor="#0e0e0e", paper_bgcolor="#0e0e0e",
            font_color="#ffffff",
            margin=dict(l=0, r=0, t=10, b=0),
            height=300
        )
        st.plotly_chart(fig6, use_container_width=True)

st.markdown("---")

# ── FULL TRACK TABLE ─────────────────────────────────────────────
st.markdown("## Full Track List")

search = st.text_input("Search tracks or artists",
    placeholder="e.g. Taylor Swift, pop...")

display = filtered.copy()
if search:
    mask = pd.Series([False] * len(display), index=display.index)
    for col in [TRACK_COL, ARTIST_COL, GENRE_COL]:
        if col in display.columns:
            mask = mask | display[col].str.contains(
                search, case=False, na=False
            )
    display = display[mask]

# Show available columns cleanly
show_cols = [c for c in [
    TRACK_COL, ARTIST_COL, GENRE_COL, MARKET_COL,
    TIER_COL, RELEASE_COL, DAYS_COL
] if c in display.columns]

st.dataframe(
    display[show_cols],
    use_container_width=True,
    hide_index=True,
    height=400
)

# ── FOOTER ───────────────────────────────────────────────────────
st.markdown("---")
st.markdown(
    "<p style='text-align:center;color:#535353;font-size:0.8rem;'>"
    "Built by Divyanshi Oberoi · "
    "Stack: iTunes API → Python → Snowflake → dbt → Airflow → Streamlit"
    "</p>",
    unsafe_allow_html=True
)