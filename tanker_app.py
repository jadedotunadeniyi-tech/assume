"""
=============================================================
  OIL TANKER DAUGHTER VESSEL OPERATION — STREAMLIT DASHBOARD
  Wraps: tanker_simulation_v5.py
=============================================================
  Run locally:
      streamlit run tanker_app.py

  Deploy (Streamlit Community Cloud — free public link):
      1. Push tanker_app.py + tanker_simulation_v5.py + requirements.txt
         to a GitHub repository
      2. Go to share.streamlit.io → New app → point to tanker_app.py
      3. Deploy → share the permanent URL with your team

  Google Sheets live sync (optional):
      Enable in the sidebar, paste your Sheet ID + upload service-account JSON.
      Expected sheet columns: timestamp | chapel_bbl | jasmines_bbl |
        westmore_bbl | duke_bbl | starturn_bbl | mother_bbl |
        production_bph | sim_days
=============================================================
"""

import sys, os, types, colorsys, time
import unittest.mock as _mock
from datetime import datetime

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

# ── Page config ────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Tanker Operations v5",
    page_icon="🛢️",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown("""
<style>
  .block-container{padding-top:1.2rem;padding-bottom:1rem}
  div[data-testid="stSidebarContent"]{background:#111827}
  .kpi-card{background:#1e2130;border-radius:8px;padding:14px 10px;
            text-align:center;border:1px solid #2d3748;margin-bottom:4px}
  .kpi-label{color:#94a3b8;font-size:11px;font-weight:700;
             text-transform:uppercase;letter-spacing:.06em;margin-bottom:4px}
  .kpi-value{color:#f1f5f9;font-size:21px;font-weight:700}
  .kpi-sub{color:#64748b;font-size:11px;margin-top:3px}
  .sec-hdr{background:linear-gradient(90deg,#1e293b,#0f172a);
           border-left:3px solid #3b82f6;padding:6px 14px;border-radius:4px;
           margin:18px 0 8px;color:#e2e8f0;font-weight:700;font-size:15px}
  .pill{display:inline-block;padding:2px 10px;border-radius:999px;
        font-size:11px;font-weight:700;margin:2px}
  .warn{background:#451a03;border:1px solid #92400e;border-radius:6px;
        padding:8px 14px;color:#fbbf24;font-size:13px;margin:6px 0}
</style>
""", unsafe_allow_html=True)

# ── Colour palettes ────────────────────────────────────────────────────────────
VESSEL_COLORS = {
    "Sherlock":"#e74c3c","Laphroaig":"#2ecc71","Rathbone":"#9b59b6",
    "Bedford":"#f39c12","Balham":"#1abc9c","Woodstock":"#e91e63",
    "Bagshot":"#00bcd4","Watson":"#95a5a6",
}
STORAGE_COLORS = {
    "Chapel":"#f1c40f","JasmineS":"#8e44ad","Westmore":"#27ae60",
    "Duke":"#3498db","Starturn":"#d35400",
}
MOTHER_COLORS = {
    "Bryanston":"#16a085","Alkebulan":"#c0392b","GreenEagle":"#8e44ad",
}
STATUS_LIGHTNESS = {
    "IDLE_A":2.0,"WAITING_STOCK":1.8,"WAITING_BERTH_A":1.7,"WAITING_DEAD_STOCK":1.6,
    "BERTHING_A":1.3,"HOSE_CONNECT_A":1.1,"LOADING":1.0,"PF_LOADING":1.0,"PF_SWAP":0.9,
    "DOCUMENTING":0.9,"WAITING_CAST_OFF":0.85,"CAST_OFF":0.8,
    "SAILING_AB":0.7,"SAILING_AB_LEG2":0.65,"SAILING_D_CHANNEL":0.68,
    "WAITING_FAIRWAY":0.6,"WAITING_BERTH_B":0.6,"WAITING_MOTHER_RETURN":0.55,
    "WAITING_MOTHER_CAPACITY":0.5,"WAITING_RETURN_STOCK":0.52,
    "BERTHING_B":0.5,"HOSE_CONNECT_B":0.45,"DISCHARGING":0.4,
    "CAST_OFF_B":0.38,"SAILING_BA":0.5,"IDLE_B":0.55,"WAITING_DAYLIGHT":1.5,
}
STATUS_LABELS = {
    "IDLE_A":"Idle at storage","WAITING_STOCK":"Waiting — low stock",
    "WAITING_DEAD_STOCK":"Waiting dead-stock","PF_LOADING":"Loading at Point F",
    "PF_SWAP":"Point F swap","LOADING":"Loading","BERTHING_A":"Berthing (storage)",
    "HOSE_CONNECT_A":"Hose connection (storage)","DOCUMENTING":"Documentation",
    "CAST_OFF":"Cast off (storage)","WAITING_CAST_OFF":"Waiting — cast-off window",
    "SAILING_AB":"Sailing → mother","SAILING_AB_LEG2":"Approaching mother",
    "SAILING_D_CHANNEL":"Via Cawthorne Channel","WAITING_FAIRWAY":"Holding at fairway",
    "WAITING_BERTH_B":"Waiting — berth at mother","BERTHING_B":"Berthing (mother)",
    "HOSE_CONNECT_B":"Hose connection (mother)","DISCHARGING":"Discharging",
    "CAST_OFF_B":"Cast off (mother)","SAILING_BA":"Returning to storage",
    "WAITING_MOTHER_RETURN":"Waiting — mother exporting",
    "WAITING_MOTHER_CAPACITY":"Waiting — mother capacity",
    "WAITING_RETURN_STOCK":"Waiting — return stock","WAITING_DAYLIGHT":"Waiting — daylight",
}


def _shade(hex_color, factor):
    h = hex_color.lstrip("#")
    r, g, b = (int(h[i:i+2], 16)/255 for i in (0, 2, 4))
    hh, l, s = colorsys.rgb_to_hls(r, g, b)
    l2 = max(0.0, min(1.0, l * factor))
    r2, g2, b2 = colorsys.hls_to_rgb(hh, l2, s)
    return "#{:02x}{:02x}{:02x}".format(int(r2*255), int(g2*255), int(b2*255))


def vcolor(name, status):
    return _shade(VESSEL_COLORS.get(name, "#95a5a6"), STATUS_LIGHTNESS.get(status, 1.0))


# =============================================================================
# ── SIMULATION ENGINE LOADER ──────────────────────────────────────────────────
# =============================================================================

@st.cache_resource(show_spinner="Loading simulation engine…")
def _load_mod():
    """Load tanker_simulation_v5.py as a Python module (strips charts/output)."""
    sim_path = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                            "tanker_simulation_v5.py")
    if not os.path.exists(sim_path):
        st.error("❌  tanker_simulation_v5.py not found — place it next to tanker_app.py")
        st.stop()

    source = open(sim_path).read()
    # Strip everything from the RUN SIMULATION block onwards
    marker = "# -----------------------------------------------------------------\n# RUN SIMULATION"
    if marker in source:
        source = source.split(marker)[0]

    # Stub matplotlib so exec doesn't fail
    for m in ["matplotlib", "matplotlib.pyplot", "matplotlib.patches"]:
        if m not in sys.modules:
            sys.modules[m] = _mock.MagicMock()

    mod = types.ModuleType("tanker_sim_v5")
    mod.__file__ = sim_path
    exec(compile(source, sim_path, "exec"), mod.__dict__)
    return mod


@st.cache_data(ttl=300, show_spinner="Running simulation…")
def run_sim(sim_days, chapel, jasmines, westmore, duke, starturn, mother, prod_rate):
    """Run simulation with given parameters; cached for 5 minutes."""
    mod = _load_mod()

    # Save originals
    orig = {k: getattr(mod, k) for k in
            ("SIMULATION_DAYS", "STORAGE_INIT_BBL", "MOTHER_INIT_BBL", "PRODUCTION_RATE_BPH")}

    mod.SIMULATION_DAYS     = sim_days
    mod.STORAGE_INIT_BBL    = chapel
    mod.MOTHER_INIT_BBL     = mother
    mod.PRODUCTION_RATE_BPH = prod_rate

    sim = mod.Simulation()

    # Override per-storage starting levels
    sim.storage_bbl["Chapel"]   = min(chapel,   mod.STORAGE_CAPACITY_BY_NAME["Chapel"])
    sim.storage_bbl["JasmineS"] = min(jasmines, mod.STORAGE_CAPACITY_BY_NAME["JasmineS"])
    sim.storage_bbl["Westmore"] = min(westmore, mod.STORAGE_CAPACITY_BY_NAME["Westmore"])
    sim.storage_bbl["Duke"]     = min(duke,     mod.STORAGE_CAPACITY_BY_NAME["Duke"])
    sim.storage_bbl["Starturn"] = min(starturn, mod.STORAGE_CAPACITY_BY_NAME["Starturn"])
    for mn in mod.MOTHER_NAMES:
        sim.mother_bbl[mn] = mother

    # Restore originals
    for k, v in orig.items():
        setattr(mod, k, v)

    log_df, tl_df = sim.run()

    summary = dict(
        loadings    = int(len(log_df[log_df.Event == "LOADING_START"])),
        discharges  = int(len(log_df[log_df.Event == "DISCHARGE_START"])),
        loaded      = int(sim.total_loaded),
        exported    = float(sim.total_exported),
        produced    = float(sim.total_produced),
        spilled     = float(sim.total_spilled),
        exports     = int(len(log_df[log_df.Event == "EXPORT_COMPLETE"])),
        ovf_events  = int(sim.storage_overflow_events),
        vessel_names= [v.name for v in sim.vessels],
        **{f"final_{k}": float(v) for k, v in sim.storage_bbl.items()},
        **{f"final_{k}": float(v) for k, v in sim.mother_bbl.items()},
    )
    return log_df, tl_df, summary


# =============================================================================
# ── GOOGLE SHEETS ─────────────────────────────────────────────────────────────
# =============================================================================

def gsheets_load(sheet_id, creds_json):
    try:
        import gspread
        from google.oauth2.service_account import Credentials
        import json
        scopes = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
        creds  = Credentials.from_service_account_info(json.loads(creds_json), scopes=scopes)
        rows   = gspread.authorize(creds).open_by_key(sheet_id).sheet1.get_all_records()
        if not rows:
            return {}
        latest = rows[-1]
        mapping = {
            "chapel_bbl":"chapel","jasmines_bbl":"jasmines","westmore_bbl":"westmore",
            "duke_bbl":"duke","starturn_bbl":"starturn","mother_bbl":"mother",
            "production_bph":"prod_rate","sim_days":"sim_days",
        }
        return {v: int(latest[k]) for k, v in mapping.items()
                if k in latest and latest[k] not in ("", None)}
    except ImportError:
        st.sidebar.warning("Install gspread: pip install gspread google-auth")
    except Exception as e:
        st.sidebar.error(f"Sheets error: {e}")
    return {}


# =============================================================================
# ── CHARTS ────────────────────────────────────────────────────────────────────
# =============================================================================

_DARK = dict(paper_bgcolor="#0e1117", plot_bgcolor="#0e1117",
             font=dict(color="#e2e8f0"), margin=dict(l=60, r=20, t=50, b=30))
_GRID = dict(gridcolor="#1e2130")


def chart_storage_points(tl_df):
    fig = make_subplots(
        rows=2, cols=1, shared_xaxes=True,
        subplot_titles=("Point A — Chapel & JasmineS",
                        "Points C / D / E — Westmore · Duke · Starturn"),
        vertical_spacing=0.1,
    )
    for name, col, dash in [("Chapel","Chapel_bbl","solid"),("JasmineS","JasmineS_bbl","dot")]:
        fig.add_trace(go.Scatter(x=tl_df.Time, y=tl_df[col], name=name,
            line=dict(color=STORAGE_COLORS[name], width=2, dash=dash)), row=1, col=1)
    for name, col in [("Westmore","Westmore_bbl"),("Duke","Duke_bbl"),("Starturn","Starturn_bbl")]:
        fig.add_trace(go.Scatter(x=tl_df.Time, y=tl_df[col], name=name,
            line=dict(color=STORAGE_COLORS[name], width=2)), row=2, col=1)
    fig.update_layout(height=460, **_DARK,
                      legend=dict(bgcolor="#1e2130", bordercolor="#2d3748"))
    fig.update_yaxes(tickformat=",", **_GRID, title_text="bbl")
    fig.update_xaxes(**_GRID)
    return fig


def chart_overflow(tl_df):
    cols_ovf = [c for c in tl_df.columns if "Overflow_Accum" in c]
    if not cols_ovf:
        return None
    fig = go.Figure()
    name_map = {
        "Chapel_Overflow_Accum_bbl":"Chapel OVF","JasmineS_Overflow_Accum_bbl":"JasmineS OVF",
        "Westmore_Overflow_Accum_bbl":"Westmore OVF","Duke_Overflow_Accum_bbl":"Duke OVF",
        "Starturn_Overflow_Accum_bbl":"Starturn OVF","PointF_Overflow_Accum_bbl":"Point F OVF",
    }
    for col in cols_ovf:
        lbl = name_map.get(col, col)
        fig.add_trace(go.Scatter(x=tl_df.Time, y=tl_df[col], name=lbl,
                                 stackgroup="ovf", line=dict(width=1.5)))
    fig.update_layout(height=240, title="Cumulative Overflow (all storage points)", **_DARK,
                      legend=dict(bgcolor="#1e2130"))
    fig.update_yaxes(tickformat=",", **_GRID, title_text="bbl")
    fig.update_xaxes(**_GRID)
    return fig


def chart_mothers(tl_df, export_trigger, cap):
    fills = {"Bryanston":"rgba(22,160,133,0.10)","Alkebulan":"rgba(192,57,43,0.10)",
             "GreenEagle":"rgba(142,68,173,0.10)"}
    fig = go.Figure()
    for name, col in [("Bryanston","Bryanston_bbl"),("Alkebulan","Alkebulan_bbl"),
                      ("GreenEagle","GreenEagle_bbl")]:
        fig.add_trace(go.Scatter(x=tl_df.Time, y=tl_df[col], name=name,
            fill="tozeroy", fillcolor=fills[name],
            line=dict(color=MOTHER_COLORS[name], width=2)))
    fig.add_hline(y=export_trigger, line=dict(color="#e74c3c", dash="dash", width=1.5),
                  annotation_text=f"Export trigger ({export_trigger:,} bbl)",
                  annotation_font_color="#e74c3c")
    fig.add_hline(y=cap, line=dict(color="#7f1d1d", dash="dot"),
                  annotation_text=f"Per-mother capacity ({cap:,} bbl)",
                  annotation_font_color="#fca5a5")
    fig.update_layout(height=300, title="🛢️ Point B Mother Vessels — Volume", **_DARK,
                      legend=dict(bgcolor="#1e2130"))
    fig.update_yaxes(tickformat=",", **_GRID, title_text="bbl")
    fig.update_xaxes(**_GRID)
    return fig


def chart_gantt(tl_df, vessel_names):
    y_pos = {n: i for i, n in enumerate(vessel_names)}
    fig = go.Figure()
    half_day = 0.5 / 24

    for vn in vessel_names:
        if vn not in tl_df.columns:
            continue
        sub = tl_df[["Time", "Day", vn]].dropna(subset=[vn]).copy()
        sub["color"] = sub[vn].apply(lambda s: vcolor(vn, s))
        sub["lbl"]   = sub[vn].apply(lambda s: STATUS_LABELS.get(s, s))
        sub["hover"] = sub.apply(
            lambda r: f"<b>{vn}</b><br>{r['lbl']}<br>{r['Time'].strftime('%d %b %H:%M')}", axis=1)
        sub["x"]     = (sub.Day - 1 + sub.Time.apply(
            lambda d: (d.hour + d.minute/60)/24)) + half_day/2

        for color, grp in sub.groupby("color"):
            fig.add_trace(go.Bar(
                x=grp["x"], y=[0.7]*len(grp),
                base=[y_pos[vn] - 0.35]*len(grp),
                orientation="h", width=half_day,
                marker_color=color, marker_line_width=0,
                hovertext=grp["hover"], hoverinfo="text",
                showlegend=False,
            ))

    fig.update_layout(
        height=max(280, 46*len(vessel_names)),
        barmode="overlay", bargap=0, **_DARK,
        margin=dict(l=100, r=20, t=10, b=40),
        xaxis=dict(title="Simulation Day", **_GRID,
                   tickvals=list(range(0, int(tl_df.Day.max())+2, 5))),
        yaxis=dict(tickvals=list(y_pos.values()),
                   ticktext=[
                       "<span style='color:{}'><b>{}</b></span>".format(
                           VESSEL_COLORS.get(n, "#fff"), n)
                       for n in vessel_names],
                   **_GRID, range=[-0.5, len(vessel_names)-0.5]),
    )
    return fig


def chart_voyage_bars(log_df, vessel_names):
    loads = log_df[log_df.Event=="LOADING_START"].groupby("Vessel").size().reindex(vessel_names, fill_value=0)
    discs = log_df[log_df.Event=="DISCHARGE_START"].groupby("Vessel").size().reindex(vessel_names, fill_value=0)
    fig = go.Figure([
        go.Bar(name="Loadings",   x=vessel_names, y=loads.values, opacity=0.9,
               marker_color=[VESSEL_COLORS.get(n,"#aaa") for n in vessel_names]),
        go.Bar(name="Discharges", x=vessel_names, y=discs.values, opacity=0.9,
               marker_color=[_shade(VESSEL_COLORS.get(n,"#aaa"), 0.55) for n in vessel_names]),
    ])
    fig.update_layout(barmode="group", title="Voyages per Vessel",
                      height=260, **_DARK, yaxis=_GRID, legend=dict(bgcolor="#1e2130"))
    return fig


def chart_util(tl_df):
    items = [("Chapel","Chapel_bbl",270_000),("JasmineS","JasmineS_bbl",290_000),
             ("Westmore","Westmore_bbl",290_000),("Duke","Duke_bbl",90_000),
             ("Starturn","Starturn_bbl",70_000)]
    fig = go.Figure()
    for name, col, cap in items:
        if col in tl_df.columns:
            fig.add_trace(go.Scatter(x=tl_df.Time, y=(tl_df[col]/cap*100).round(1),
                name=name, line=dict(color=STORAGE_COLORS[name], width=1.8)))
    fig.add_hline(y=90, line=dict(color="#ef4444", dash="dash"),
                  annotation_text="90%", annotation_font_color="#ef4444")
    fig.update_layout(title="Storage Utilisation %", height=250, **_DARK,
                      yaxis=dict(**_GRID, title_text="%", range=[0, 105]),
                      xaxis=_GRID, legend=dict(bgcolor="#1e2130"))
    return fig


# =============================================================================
# ── UI HELPERS ────────────────────────────────────────────────────────────────
# =============================================================================

def sec(title):
    st.markdown(f'<div class="sec-hdr">{title}</div>', unsafe_allow_html=True)


def kpi(label, value, sub=None):
    sub_html = f'<div class="kpi-sub">{sub}</div>' if sub else ""
    st.markdown(f"""
    <div class="kpi-card">
      <div class="kpi-label">{label}</div>
      <div class="kpi-value">{value}</div>
      {sub_html}
    </div>""", unsafe_allow_html=True)


# =============================================================================
# ── MAIN ──────────────────────────────────────────────────────────────────────
# =============================================================================

def main():
    mod = _load_mod()

    # ── Header ────────────────────────────────────────────────────────────────
    hc1, hc2 = st.columns([1, 11])
    with hc1:
        st.markdown("# 🛢️")
    with hc2:
        st.markdown("## Oil Tanker Daughter Vessel Operations — Live Dashboard")
        st.caption(
            "v5 · 8 vessels · 5 storage points A/C/D/E "
            "(Chapel, JasmineS, Westmore, Duke, Starturn) · "
            "3 mother vessels (Bryanston, Alkebulan, GreenEagle) · "
            "Point F Bedford/Balham rotation · Cawthorne Channel routing"
        )
    st.divider()

    # ── Constants from module ─────────────────────────────────────────────────
    cap = mod.STORAGE_CAPACITY_BY_NAME
    MOTHER_CAP      = int(mod.MOTHER_CAPACITY_BBL)
    EXPORT_TRIGGER  = int(mod.MOTHER_EXPORT_TRIGGER)

    # ── Sidebar ───────────────────────────────────────────────────────────────
    with st.sidebar:
        st.markdown("### ⚙️ Simulation Parameters")
        sim_days     = st.slider("Simulation Days", 7, 60, 30)
        prod_rate    = st.number_input("Prod Rate — Chapel & JasmineS (bbl/hr)",
                                        500, 5000, int(mod.PRODUCTION_RATE_BPH), step=100)

        st.markdown("**Starting Storage Levels (bbl)**")
        chapel_v   = st.slider(f"Chapel  (max {cap['Chapel']:,})",   0, cap["Chapel"],   min(int(mod.STORAGE_INIT_BBL), cap["Chapel"]),   5_000)
        jasmines_v = st.slider(f"JasmineS (max {cap['JasmineS']:,})", 0, cap["JasmineS"], min(int(mod.STORAGE_INIT_BBL), cap["JasmineS"]), 5_000)
        westmore_v = st.slider(f"Westmore (max {cap['Westmore']:,})",  0, cap["Westmore"], min(int(mod.STORAGE_INIT_BBL), cap["Westmore"]),  5_000)
        duke_v     = st.slider(f"Duke    (max {cap['Duke']:,})",     0, cap["Duke"],     cap["Duke"]//2,     1_000)
        starturn_v = st.slider(f"Starturn (max {cap['Starturn']:,})",  0, cap["Starturn"], cap["Starturn"]//2,  1_000)
        mother_v   = st.slider("All Mothers (per vessel)",           0, MOTHER_CAP, int(mod.MOTHER_INIT_BBL), 10_000)

        st.markdown("---")
        st.markdown("### 🔄 Auto-Refresh")
        auto_ref  = st.toggle("Enable auto-refresh")
        ref_secs  = st.slider("Interval (s)", 30, 600, 300, disabled=not auto_ref)

        st.markdown("---")
        st.markdown("### 📊 Google Sheets Sync")
        use_gs   = st.toggle("Enable Google Sheets")
        sheet_id = st.text_input("Sheet ID", disabled=not use_gs)
        creds_f  = st.file_uploader("Service Account JSON", type=["json"], disabled=not use_gs)

        with st.expander("📋 Sheets setup guide"):
            st.markdown("""
**Row 1 headers:**
```
timestamp | chapel_bbl | jasmines_bbl | westmore_bbl
duke_bbl  | starturn_bbl | mother_bbl | production_bph | sim_days
```
Each new row = one data update.

1. [console.cloud.google.com](https://console.cloud.google.com) → Enable Sheets API
2. Create Service Account → download JSON key
3. Share sheet with service-account email (Viewer)
4. Paste Sheet ID + upload JSON above
""")
        with st.expander("🚀 Deploy as public link"):
            st.markdown("""
**Streamlit Community Cloud (free):**
1. Push both files + `requirements.txt` to GitHub
2. [share.streamlit.io](https://share.streamlit.io) → New app → `tanker_app.py`
3. Deploy → permanent public URL

`requirements.txt`:
```
streamlit>=1.32.0
pandas>=2.0.0
plotly>=5.18.0
gspread>=6.0.0
google-auth>=2.27.0
```
""")

    # ── Google Sheets override ────────────────────────────────────────────────
    gs = {}
    if use_gs and sheet_id and creds_f:
        gs = gsheets_load(sheet_id, creds_f.read().decode("utf-8"))
        if gs:
            st.sidebar.success("✅ Synced: " + " · ".join(f"{k}={v:,}" for k,v in gs.items()))

    params = dict(
        sim_days=gs.get("sim_days", sim_days),
        chapel=gs.get("chapel", chapel_v),
        jasmines=gs.get("jasmines", jasmines_v),
        westmore=gs.get("westmore", westmore_v),
        duke=gs.get("duke", duke_v),
        starturn=gs.get("starturn", starturn_v),
        mother=gs.get("mother", mother_v),
        prod_rate=gs.get("prod_rate", prod_rate),
    )

    # ── Run ───────────────────────────────────────────────────────────────────
    log_df, tl_df, S = run_sim(**params)
    vnames = S["vessel_names"]

    # ── KPIs ──────────────────────────────────────────────────────────────────
    sec(f"📈 {params['sim_days']}-Day Simulation Summary")
    k1 = st.columns(5)
    with k1[0]: kpi("Total Loadings",   str(S["loadings"]))
    with k1[1]: kpi("Total Discharges", str(S["discharges"]))
    with k1[2]: kpi("Volume Loaded",    f"{S['loaded']:,} bbl")
    with k1[3]: kpi("Volume Exported",  f"{S['exported']:,.0f} bbl")
    with k1[4]: kpi("Export Voyages",   str(S["exports"]))

    st.markdown("<br>", unsafe_allow_html=True)
    k2 = st.columns(5)
    with k2[0]: kpi("Total Produced",   f"{S['produced']:,.0f} bbl")
    with k2[1]: kpi("Total Spilled",    f"{S['spilled']:,.0f} bbl",
                     sub="⚠️ reduce prod or increase lifts" if S["spilled"] > 0 else "✅ no spill")
    with k2[2]: kpi("Overflow Events",  str(S["ovf_events"]))
    all_stor = sum(S.get(f"final_{n}", 0) for n in ["Chapel","JasmineS","Westmore","Duke","Starturn"])
    all_moth = sum(S.get(f"final_{n}", 0) for n in ["Bryanston","Alkebulan","GreenEagle"])
    with k2[3]: kpi("Final All Storage", f"{all_stor:,.0f} bbl")
    with k2[4]: kpi("Final All Mothers", f"{all_moth:,.0f} bbl")

    if S["spilled"] > 0:
        st.markdown(
            f'<div class="warn">⚠️ {S["spilled"]:,.0f} bbl spilled across '
            f'{S["ovf_events"]} overflow events. '
            f'Consider increasing lifting frequency or reducing production rate.</div>',
            unsafe_allow_html=True)

    # ── Storage final levels ───────────────────────────────────────────────────
    sec("📦 Final Storage Levels by Point")
    s_cols = st.columns(5)
    storage_items = [
        ("Chapel","A",270_000),("JasmineS","A",290_000),("Westmore","C",290_000),
        ("Duke","D",90_000),("Starturn","E",70_000),
    ]
    for i,(name,pt,cap_val) in enumerate(storage_items):
        final_val = S.get(f"final_{name}", 0)
        pct = final_val/cap_val*100
        with s_cols[i]:
            kpi(f"{name} (Pt {pt})", f"{final_val:,.0f} bbl",
                sub=f"{pct:.0f}% of {cap_val:,}")

    # ── Storage charts ────────────────────────────────────────────────────────
    sec("📦 Storage Volume Over Time")
    st.plotly_chart(chart_storage_points(tl_df), use_container_width=True)

    oc1, oc2 = st.columns(2)
    with oc1:
        of = chart_overflow(tl_df)
        if of: st.plotly_chart(of, use_container_width=True)
    with oc2:
        st.plotly_chart(chart_util(tl_df), use_container_width=True)

    # ── Mother vessels ────────────────────────────────────────────────────────
    sec("🛢️ Mother Vessels — Point B")
    st.plotly_chart(chart_mothers(tl_df, EXPORT_TRIGGER, MOTHER_CAP),
                    use_container_width=True)

    m_cols = st.columns(3)
    for i,(mn,color) in enumerate([("Bryanston","#16a085"),("Alkebulan","#c0392b"),
                                    ("GreenEagle","#8e44ad")]):
        with m_cols[i]:
            final_val = S.get(f"final_{mn}", 0)
            delta     = final_val - params["mother"]
            arrow     = "▲" if delta >= 0 else "▼"
            col_str   = "#4ade80" if delta >= 0 else "#f87171"
            kpi(mn, f"{final_val:,.0f} bbl",
                sub=f'<span style="color:{col_str}">{arrow} {delta:+,.0f} bbl vs start</span>')

    # ── Gantt ─────────────────────────────────────────────────────────────────
    sec("⛴️ Vessel Activity Timeline (Gantt)")
    st.plotly_chart(chart_gantt(tl_df, vnames), use_container_width=True)

    with st.expander("🎨 Colour key"):
        ck_cols = st.columns(4)
        for i, vn in enumerate(vnames):
            with ck_cols[i % 4]:
                base = VESSEL_COLORS.get(vn, "#aaa")
                st.markdown(
                    f'<span class="pill" style="background:{base};color:#fff">{vn}</span>',
                    unsafe_allow_html=True)
                for st_code, lbl in [
                    ("IDLE_A","Idle"),("LOADING","Loading"),("PF_LOADING","Point F loading"),
                    ("SAILING_AB","Sailing → mother"),("DISCHARGING","Discharging"),
                    ("SAILING_BA","Returning"),("WAITING_DEAD_STOCK","Waiting dead-stock"),
                ]:
                    c = vcolor(vn, st_code)
                    st.markdown(
                        f'<span style="background:{c};padding:1px 8px;border-radius:3px;'
                        f'font-size:11px;">&nbsp;</span> {lbl}', unsafe_allow_html=True)

    # ── Voyage count bars ─────────────────────────────────────────────────────
    sec("📊 Voyage Counts per Vessel")
    vc1, vc2 = st.columns([3, 2])
    with vc1:
        st.plotly_chart(chart_voyage_bars(log_df, vnames), use_container_width=True)
    with vc2:
        # Summary table
        rows_voy = []
        for vn in vnames:
            vl = log_df[log_df.Vessel == vn]
            ld = len(vl[vl.Event == "LOADING_START"])
            dc = len(vl[vl.Event == "DISCHARGE_START"])
            vcap = mod.VESSEL_CAPACITIES.get(vn, mod.DAUGHTER_CARGO_BBL)
            rows_voy.append({
                "Vessel": vn,
                "Loads": ld,
                "Discharges": dc,
                "Vol Loaded (bbl)": f"{ld*vcap:,}",
                "Cargo Cap": f"{vcap:,}",
            })
        st.dataframe(pd.DataFrame(rows_voy), use_container_width=True, hide_index=True)

    # ── Per-vessel tabs ───────────────────────────────────────────────────────
    sec("🚢 Per-Vessel Event Log")
    vtabs = st.tabs(vnames)
    for vtab, vn in zip(vtabs, vnames):
        with vtab:
            vlog  = log_df[log_df.Vessel == vn].copy()
            loads = vlog[vlog.Event == "LOADING_START"]
            discs = vlog[vlog.Event == "DISCHARGE_START"]
            vcap  = mod.VESSEL_CAPACITIES.get(vn, mod.DAUGHTER_CARGO_BBL)

            ml, mr = st.columns([1, 3])
            with ml:
                base = VESSEL_COLORS.get(vn, "#aaa")
                st.markdown(
                    f'<span class="pill" style="background:{base};color:#fff;'
                    f'font-size:15px;padding:5px 16px">{vn}</span><br><br>',
                    unsafe_allow_html=True)
                kpi("Voyages", str(len(loads)))
                st.markdown("<br>",unsafe_allow_html=True)
                kpi("Cargo Capacity", f"{vcap:,} bbl")
                st.markdown("<br>",unsafe_allow_html=True)
                kpi("Vol Loaded", f"{len(loads)*vcap:,} bbl")
                st.markdown("<br>",unsafe_allow_html=True)
                kpi("Vol Discharged", f"{len(discs)*vcap:,} bbl")

                # Which storages used?
                st.markdown("<br>**Storages used:**", unsafe_allow_html=True)
                used = (vlog[vlog.Event=="LOADING_START"]["Detail"]
                        .str.extract(r"Loading \d[,\d]+ bbl \| (\w+):")
                        .dropna()[0].value_counts())
                for sn, cnt in used.items():
                    c = STORAGE_COLORS.get(sn,"#aaa")
                    st.markdown(
                        f'<span class="pill" style="background:{c};color:#fff">{sn} ×{cnt}</span>',
                        unsafe_allow_html=True)

            with mr:
                show = ["Time","Day","Voyage","Event","Detail"]
                extra = [c for c in ["Chapel_bbl","JasmineS_bbl","Duke_bbl","Starturn_bbl","Mother_bbl"]
                         if c in vlog.columns]
                st.dataframe(vlog[show+extra], use_container_width=True, height=380)

    # ── Storage point tabs ────────────────────────────────────────────────────
    sec("📍 Storage Point Breakdown")
    stabs = st.tabs(["Chapel (A)","JasmineS (A)","Westmore (C)","Duke (D)","Starturn (E)"])
    st_info = [
        ("Chapel","Chapel_bbl","Chapel_Overflow_Accum_bbl",270_000,"A",
         sorted(mod.VESSEL_NAMES)),
        ("JasmineS","JasmineS_bbl","JasmineS_Overflow_Accum_bbl",290_000,"A",
         sorted(mod.VESSEL_NAMES)),
        ("Westmore","Westmore_bbl","Westmore_Overflow_Accum_bbl",290_000,"C",
         sorted(mod.WESTMORE_PERMITTED_VESSELS)),
        ("Duke","Duke_bbl","Duke_Overflow_Accum_bbl",90_000,"D",
         sorted(mod.DUKE_PERMITTED_VESSELS)),
        ("Starturn","Starturn_bbl","Starturn_Overflow_Accum_bbl",70_000,"E",
         sorted(mod.STARTURN_PERMITTED_VESSELS)),
    ]
    for stab,(sname,vol_col,ovf_col,cap_val,pt,permitted) in zip(stabs,st_info):
        with stab:
            sf = go.Figure()
            if vol_col in tl_df.columns:
                sf.add_trace(go.Scatter(
                    x=tl_df.Time, y=tl_df[vol_col], name=f"{sname} Volume",
                    fill="tozeroy", fillcolor=STORAGE_COLORS[sname]+"22",
                    line=dict(color=STORAGE_COLORS[sname], width=2)))
            if ovf_col in tl_df.columns:
                sf.add_trace(go.Scatter(
                    x=tl_df.Time, y=tl_df[ovf_col], name="Overflow (accum)",
                    line=dict(color="#ef4444", dash="dot", width=1.5)))
            sf.add_hline(y=cap_val, line=dict(color="#ef4444", dash="dash"),
                         annotation_text=f"Capacity {cap_val:,} bbl")
            sf.update_layout(height=230, **_DARK,
                             yaxis=dict(tickformat=",",**_GRID), xaxis=_GRID,
                             legend=dict(bgcolor="#1e2130"),
                             margin=dict(l=50,r=20,t=20,b=30))
            st.plotly_chart(sf, use_container_width=True)

            sloads = log_df[
                (log_df.Event=="LOADING_START") &
                (log_df.Detail.str.contains(sname, na=False))
            ]
            sc1, sc2 = st.columns(2)
            with sc1:
                kpi(f"Loadings from {sname}", str(len(sloads)),
                    sub=f"Permitted: {', '.join(permitted)}")
            with sc2:
                if not sloads.empty:
                    by_v = sloads.groupby("Vessel").size().reset_index(name="Loads")
                    st.dataframe(by_v, use_container_width=True, hide_index=True)

    # ── Mother discharge sequence ─────────────────────────────────────────────
    sec("🔀 Mother Vessel Discharge Sequence Log")
    seq = log_df[log_df.Event.isin(["BERTHING_START_B","MOTHER_SEQUENCE_ASSIGNMENT",
                                     "MOTHER_PRIORITY_ASSIGNMENT"])]
    if seq.empty:
        st.caption("No berthing events recorded.")
    else:
        st.dataframe(seq[["Time","Day","Vessel","Voyage","Event","Detail"]],
                     use_container_width=True, height=300)

    # ── Point F log ───────────────────────────────────────────────────────────
    sec("🔁 Point F Bedford / Balham Swap Log")
    pf = log_df[log_df.Event.isin(
        ["POINT_F_SWAP_TRIGGER","POINT_F_SWAP_START","POINT_F_SWAP_COMPLETE"])]
    if pf.empty:
        st.caption("No Point F swaps in this simulation period.")
    else:
        st.dataframe(pf[["Time","Day","Vessel","Voyage","Event","Detail"]],
                     use_container_width=True, height=240)

    # ── Full event log ────────────────────────────────────────────────────────
    sec("📋 Full Event Log")
    f1, f2, f3, f4 = st.columns(4)
    all_entities = vnames + ["Chapel","JasmineS","Westmore","Duke","Starturn",
                              "Bryanston","Alkebulan","GreenEagle"]
    with f1: vf = st.multiselect("Vessel / Entity", all_entities, [], key="vf")
    with f2: ef = st.multiselect("Event type", sorted(log_df.Event.dropna().unique()), [], key="ef")
    with f3: dr = st.slider("Day range", 1, params["sim_days"], (1, params["sim_days"]))
    with f4: srch = st.text_input("Search Detail", placeholder="e.g. Chapel, Bryanston…")

    filt = log_df[log_df.Day.between(dr[0], dr[1])].copy()
    if vf:   filt = filt[filt.Vessel.isin(vf)]
    if ef:   filt = filt[filt.Event.isin(ef)]
    if srch: filt = filt[filt.Detail.str.contains(srch, case=False, na=False)]

    show_cols = ["Time","Day","Vessel","Voyage","Event","Detail"]
    extra_bbl = [c for c in ["Chapel_bbl","JasmineS_bbl","Westmore_bbl",
                              "Duke_bbl","Starturn_bbl","Mother_bbl"] if c in filt.columns]
    st.dataframe(filt[show_cols+extra_bbl], use_container_width=True, height=440)
    st.caption(f"Showing {len(filt):,} of {len(log_df):,} events")

    # ── Downloads ─────────────────────────────────────────────────────────────
    sec("⬇️ Download Results")
    d1, d2, d3 = st.columns(3)
    with d1:
        st.download_button("📥 Full Event Log (CSV)",
                           log_df.to_csv(index=False).encode(),
                           "tanker_event_log_v5.csv","text/csv")
    with d2:
        st.download_button("📥 Timeline Snapshots (CSV)",
                           tl_df.to_csv(index=False).encode(),
                           "tanker_timeline_v5.csv","text/csv")
    with d3:
        rows = [
            ["Simulation Days", params["sim_days"]],
            ["Total Loadings", S["loadings"]],
            ["Total Discharges", S["discharges"]],
            ["Volume Loaded (bbl)", S["loaded"]],
            ["Volume Exported (bbl)", S["exported"]],
            ["Volume Produced (bbl)", S["produced"]],
            ["Volume Spilled (bbl)", S["spilled"]],
            ["Overflow Events", S["ovf_events"]],
        ]
        for n,pt,c in storage_items:
            rows.append([f"Final {n} (bbl)", S.get(f"final_{n}",0)])
        for mn in ["Bryanston","Alkebulan","GreenEagle"]:
            rows.append([f"Final {mn} (bbl)", S.get(f"final_{mn}",0)])
        st.download_button("📥 Summary (CSV)",
                           pd.DataFrame(rows,columns=["Metric","Value"]).to_csv(index=False).encode(),
                           "tanker_summary_v5.csv","text/csv")

    # ── Auto-refresh ──────────────────────────────────────────────────────────
    if auto_ref:
        ph = st.empty()
        for rem in range(ref_secs, 0, -1):
            ph.caption(f"🔄 Auto-refreshing in {rem}s…")
            time.sleep(1)
        ph.caption("🔄 Refreshing…")
        st.cache_data.clear()
        st.rerun()

    # ── Footer ────────────────────────────────────────────────────────────────
    st.divider()
    st.caption(
        f"Tanker Operations Simulation v5 · "
        f"Last run: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} · "
        f"Vessels: {', '.join(vnames)}"
    )


if __name__ == "__main__":
    main()
