"""
=============================================================
  OIL TANKER DAUGHTER VESSEL OPERATION — STREAMLIT DASHBOARD
  Wraps: tanker_simulation_v5.py
=============================================================
  Run locally:
      streamlit run tanker_app.py

  Deploy (Streamlit Community Cloud):
      1. Push tanker_app.py + tanker_simulation_v5.py + requirements.txt to GitHub
      2. share.streamlit.io → New app → tanker_app.py → Deploy

  Google Sheets — TWO tabs required:
  ─────────────────────────────────────────────────────────
  Tab 1  "volumes"   — one row per daily 8am update
    Columns: timestamp | sanbarth_bbl | jasmines_bbl | westmore_bbl |
             duke_bbl | starturn_bbl | bryanston_bbl | alkebulan_bbl |
             greeneagle_bbl | production_bph | sim_days

  Tab 2  "fleet"     — one row per vessel per daily 8am update
    Columns: timestamp | vessel | status | location | cargo_bbl | notes
    vessel   → exact name: Sherlock, Laphroaig, Rathbone, SantaMonica,
               Bedford, Balham, Woodstock, Bagshot, Watson, Amyla
    status   → valid code: IDLE_A | LOADING | SAILING_AB | SAILING_CROSS_BW_AC |
               SAILING_BW_TO_FWY | SAILING_AB_LEG2 | SAILING_B_TO_FWY |
               SAILING_FWY_TO_BW | SAILING_CROSS_BW_IN_AC | SAILING_BW_TO_A |
               SAILING_BA | WAITING_RETURN_STOCK | PF_LOADING | PF_SWAP |
               SAILING_D_CHANNEL | SAILING_CH_TO_BW_OUT | SAILING_CROSS_BW_OUT |
               SAILING_B_TO_BW_IN | SAILING_CROSS_BW_IN | SAILING_BW_TO_CH_IN | SAILING_CH_TO_D |
               WAITING_BERTH_B | BERTHING_B | DISCHARGING | CAST_OFF_B |
               WAITING_FAIRWAY | BERTHING_A | HOSE_CONNECT_A | HOSE_CONNECT_B |
               DOCUMENTING | WAITING_DEAD_STOCK | WAITING_CAST_OFF | CAST_OFF
    location → free text, e.g. "Bryanston", "Fairway Buoy",
               "En Route SanBarth→BIA", "SanBarth"
=============================================================
"""

import sys, os, types, colorsys, time, json
import re
import io
import csv
import math
import hashlib
import binascii
import itertools
import datetime as _dt
import unittest.mock as _mock
from datetime import datetime

import streamlit as st
import streamlit.components.v1 as _stc
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

# ── Page config ────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Tanker Ops v5",
    page_icon="🛢️",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown("""
<style>
  /* ── Global light-mode base ─────────────────────────────────────── */
  html, body, [class*="css"] {
    font-family: 'Segoe UI', system-ui, -apple-system, sans-serif;
  }
  .block-container {
    padding-top: 1.2rem;
    padding-bottom: 2rem;
    background: #f8f9fb;
  }

  /* ── Sidebar ────────────────────────────────────────────────────── */
  div[data-testid="stSidebarContent"] {
    background: #1a2744;
    border-right: 1px solid #243460;
  }
  div[data-testid="stSidebarContent"] * {
    color: #c8d6f0 !important;
  }
  div[data-testid="stSidebarContent"] h1,
  div[data-testid="stSidebarContent"] h2,
  div[data-testid="stSidebarContent"] h3,
  div[data-testid="stSidebarContent"] strong,
  div[data-testid="stSidebarContent"] b {
    color: #e8eef8 !important;
  }
  div[data-testid="stSidebarContent"] label {
    color: #9db3d8 !important;
    font-size: 12px !important;
    font-weight: 600 !important;
  }
  div[data-testid="stSidebarContent"] input,
  div[data-testid="stSidebarContent"] div[data-baseweb="input"] {
    background: #243460 !important;
    color: #e8eef8 !important;
    border: 1px solid #344d80 !important;
    border-radius: 6px !important;
  }
  div[data-testid="stSidebarContent"] input::placeholder {
    color: #5a7ab0 !important;
  }
  div[data-testid="stSidebarContent"] button {
    background: #2d4070 !important;
    color: #c8d6f0 !important;
    border: 1px solid #3d5490 !important;
  }
  div[data-testid="stSidebarContent"] button:hover {
    background: #344d80 !important;
  }
  div[data-testid="stSidebarContent"] div[data-testid="stFileUploader"] {
    background: #1f2f58 !important;
    border: 1px dashed #344d80 !important;
    border-radius: 6px !important;
  }
  div[data-testid="stSidebarContent"] div[data-baseweb="select"] div {
    background: #243460 !important;
    color: #e8eef8 !important;
    border-color: #344d80 !important;
  }
  div[data-testid="stSidebarContent"] .stMarkdown p,
  div[data-testid="stSidebarContent"] .stMarkdown li,
  div[data-testid="stSidebarContent"] caption,
  div[data-testid="stSidebarContent"] small {
    color: #8aa4cc !important;
  }
  div[data-testid="stSidebarContent"] hr {
    border-color: #2d4070 !important;
  }

  /* ── KPI cards ──────────────────────────────────────────────────── */
  .kpi-card {
    background: #ffffff;
    border-radius: 10px;
    padding: 14px 10px;
    text-align: center;
    border: 1px solid #e2e8f0;
    margin-bottom: 6px;
    box-shadow: 0 1px 4px rgba(0,0,0,0.07);
  }
  .kpi-label {
    color: #64748b;
    font-size: 10px;
    font-weight: 700;
    text-transform: uppercase;
    letter-spacing: .07em;
    margin-bottom: 4px;
  }
  .kpi-value {
    color: #0f172a;
    font-size: 22px;
    font-weight: 800;
  }
  .kpi-sub {
    color: #94a3b8;
    font-size: 11px;
    margin-top: 3px;
  }

  /* ── Section headers ────────────────────────────────────────────── */
  .sec-hdr {
    background: linear-gradient(90deg, #1a2744 0%, #243460 100%);
    border-left: 4px solid #3b82f6;
    padding: 8px 16px;
    border-radius: 6px;
    margin: 24px 0 12px;
    color: #ffffff;
    font-weight: 700;
    font-size: 14px;
    letter-spacing: .03em;
    box-shadow: 0 2px 6px rgba(26,39,68,0.18);
  }

  /* ── Pill badges ────────────────────────────────────────────────── */
  .pill {
    display: inline-block;
    padding: 2px 10px;
    border-radius: 999px;
    font-size: 11px;
    font-weight: 700;
    margin: 2px;
  }

  /* ── Alert boxes ────────────────────────────────────────────────── */
  .alert-warn {
    background: #fffbeb;
    border: 1px solid #f59e0b;
    border-left: 4px solid #f59e0b;
    border-radius: 6px;
    padding: 10px 14px;
    color: #92400e;
    font-size: 13px;
    margin: 6px 0;
  }
  .alert-info {
    background: #eff6ff;
    border: 1px solid #3b82f6;
    border-left: 4px solid #3b82f6;
    border-radius: 6px;
    padding: 10px 14px;
    color: #1e40af;
    font-size: 13px;
    margin: 6px 0;
  }
  .alert-ok {
    background: #f0fdf4;
    border: 1px solid #22c55e;
    border-left: 4px solid #22c55e;
    border-radius: 6px;
    padding: 10px 14px;
    color: #14532d;
    font-size: 13px;
    margin: 6px 0;
  }

  /* ── Optimizer ──────────────────────────────────────────────────── */
  .opt-best {
    background: linear-gradient(135deg, #f0fdf4, #eff6ff);
    border: 1px solid #22c55e;
    border-radius: 12px;
    padding: 20px 24px;
    margin: 10px 0;
    box-shadow: 0 2px 12px rgba(34,197,94,0.12);
  }
  .opt-score { font-size: 52px; font-weight: 900; color: #16a34a; line-height: 1; }
  .opt-badge {
    display: inline-block;
    background: #16a34a;
    color: #fff;
    border-radius: 5px;
    padding: 2px 10px;
    font-size: 11px;
    font-weight: 700;
    letter-spacing: .05em;
    margin-left: 10px;
    vertical-align: middle;
  }
  .opt-param {
    background: #f1f5f9;
    border: 1px solid #e2e8f0;
    border-radius: 6px;
    padding: 6px 12px;
    display: inline-block;
    margin: 3px;
    font-size: 12px;
    color: #1e40af;
    font-weight: 600;
  }
  .score-bar-wrap { background: #e2e8f0; border-radius: 4px; height: 12px; overflow: hidden; margin: 2px 0; }
  .score-bar { height: 12px; border-radius: 4px; transition: width .3s; }

  /* ── Fleet status cards ─────────────────────────────────────────── */
  .vcard {
    border-radius: 10px;
    padding: 13px 15px;
    border-left-width: 4px;
    border-left-style: solid;
    margin-bottom: 10px;
    background: #ffffff;
    box-shadow: 0 1px 4px rgba(0,0,0,0.08);
  }
  .vcard-name   { font-weight: 700; font-size: 14px; color: #0f172a; margin-bottom: 3px; }
  .vcard-status { font-size: 12px; color: #374151; margin-bottom: 2px; }
  .vcard-loc    { font-size: 11px; color: #64748b; margin-bottom: 6px; }
  .vcard-bar-bg { background: #e2e8f0; border-radius: 4px; height: 6px; }
  .vcard-bar-fg { height: 6px; border-radius: 4px; }

  /* ── Recommendation cards ───────────────────────────────────────── */
  .rec-card {
    background: #ffffff;
    border: 1px solid #e2e8f0;
    border-radius: 12px;
    padding: 18px 20px;
    margin: 8px 0;
    box-shadow: 0 1px 4px rgba(0,0,0,0.06);
  }
  .rec-title  { color: #0f172a; font-size: 13px; font-weight: 700; margin-bottom: 7px; }
  .rec-body   { color: #475569; font-size: 12px; line-height: 1.7; }
  .rec-metric { margin-top: 8px; font-size: 11px; color: #94a3b8; }
  .hl-yellow  { color: #d97706; font-weight: 700; }
  .hl-green   { color: #16a34a; font-weight: 700; }
  .hl-blue    { color: #2563eb; font-weight: 700; }

  /* ── Summary section cards ──────────────────────────────────────── */
  .summary-card {
    background: #ffffff;
    border: 1px solid #e2e8f0;
    border-radius: 12px;
    padding: 20px 22px;
    margin: 8px 0;
    box-shadow: 0 1px 6px rgba(0,0,0,0.06);
  }
  .summary-card h4 {
    color: #1a2744;
    font-size: 13px;
    font-weight: 700;
    text-transform: uppercase;
    letter-spacing: .06em;
    margin: 0 0 10px 0;
    padding-bottom: 8px;
    border-bottom: 2px solid #e2e8f0;
  }
  .summary-card p, .summary-card li {
    color: #374151;
    font-size: 13px;
    line-height: 1.75;
    margin: 4px 0;
  }
  .summary-card ul { padding-left: 18px; margin: 6px 0; }
  .summary-tag {
    display: inline-block;
    padding: 2px 9px;
    border-radius: 4px;
    font-size: 11px;
    font-weight: 700;
    margin: 2px 3px 2px 0;
  }
  .tag-green  { background: #dcfce7; color: #14532d; }
  .tag-amber  { background: #fef9c3; color: #713f12; }
  .tag-red    { background: #fee2e2; color: #7f1d1d; }
  .tag-blue   { background: #dbeafe; color: #1e3a8a; }
  .tag-navy   { background: #1a2744; color: #c8d6f0; }

    /* ── Startup nomination summary ──────────────────────────────── */
    .startup-card {
        background: rgba(255,255,255,0.06);
        border: 1px solid rgba(157,179,216,0.22);
        border-left: 4px solid #3b82f6;
        border-radius: 10px;
        padding: 10px 12px;
        margin: 8px 0 10px;
    }
    .startup-card-head {
        display: flex;
        align-items: center;
        justify-content: space-between;
        gap: 8px;
        margin-bottom: 6px;
    }
    .startup-card-title {
        color: #eef4ff;
        font-size: 12px;
        font-weight: 800;
        letter-spacing: .03em;
    }
    .startup-card-meta {
        color: #8aa4cc;
        font-size: 10px;
        font-weight: 700;
        text-transform: uppercase;
        letter-spacing: .06em;
    }
    .startup-chip {
        display: inline-block;
        padding: 3px 9px;
        border-radius: 999px;
        margin: 3px 5px 0 0;
        font-size: 11px;
        font-weight: 700;
        background: #243460;
        color: #f8fbff;
        border: 1px solid #3d5490;
    }
    .startup-chip-export {
        background: #4c1d95;
        border-color: #8b5cf6;
        color: #f3e8ff;
    }
    .startup-muted {
        color: #8aa4cc;
        font-size: 11px;
    }
    .startup-overall {
        background: rgba(26,39,68,0.72);
        border: 1px solid #344d80;
        border-radius: 10px;
        padding: 10px 12px;
        margin: 10px 0 4px;
    }
    .startup-overall-title {
        color: #eef4ff;
        font-size: 12px;
        font-weight: 800;
        margin-bottom: 6px;
    }

    .context-strip {
        background: linear-gradient(135deg, #ffffff 0%, #f8fbff 100%);
        border: 1px solid #d8e4f4;
        border-radius: 14px;
        padding: 14px 16px;
        margin: 8px 0 16px;
        box-shadow: 0 2px 10px rgba(15, 23, 42, 0.06);
    }
    .context-title {
        color: #10213d;
        font-size: 13px;
        font-weight: 800;
        letter-spacing: .04em;
        text-transform: uppercase;
        margin-bottom: 8px;
    }
    .context-chip {
        display: inline-block;
        margin: 4px 6px 0 0;
        padding: 5px 10px;
        border-radius: 999px;
        font-size: 11px;
        font-weight: 700;
        border: 1px solid #dbeafe;
        background: #eff6ff;
        color: #1e40af;
    }
    .context-chip-ok {
        background: #ecfdf5;
        border-color: #bbf7d0;
        color: #166534;
    }
    .context-chip-warn {
        background: #fff7ed;
        border-color: #fed7aa;
        color: #9a3412;
    }
    .context-chip-risk {
        background: #fef2f2;
        border-color: #fecaca;
        color: #b91c1c;
    }
    .control-shell {
        background: #ffffff;
        border: 1px solid #e2e8f0;
        border-radius: 14px;
        padding: 16px 18px;
        margin: 8px 0 16px;
        box-shadow: 0 1px 8px rgba(15, 23, 42, 0.05);
    }
    .control-title {
        color: #0f172a;
        font-size: 15px;
        font-weight: 800;
        margin-bottom: 4px;
    }
    .control-subtitle {
        color: #64748b;
        font-size: 12px;
        margin-bottom: 12px;
    }
    .quickrun-card {
        background: rgba(255,255,255,0.06);
        border: 1px solid rgba(157,179,216,0.18);
        border-radius: 10px;
        padding: 10px 12px;
        margin: 8px 0;
    }
    .quickrun-label {
        color: #8aa4cc;
        font-size: 10px;
        font-weight: 800;
        text-transform: uppercase;
        letter-spacing: .08em;
        margin-bottom: 4px;
    }
    .quickrun-value {
        color: #eef4ff;
        font-size: 16px;
        font-weight: 800;
    }

  /* ── Main area text contrast ────────────────────────────────────── */
  .stMarkdown p, .stMarkdown li { color: #1e293b; font-size: 13px; }
  .stMarkdown h1, .stMarkdown h2, .stMarkdown h3 { color: #0f172a; }
  .stDataFrame { border-radius: 8px; overflow: hidden; }
  .stCaption, caption { color: #64748b !important; }

  /* ── Executive Summary & Export Schedule Matrix ─────────────────────── */
  .exec-kpi-row { display: flex; gap: 0; margin: 4px 0 14px 0; }
  .exec-kpi {
    background: linear-gradient(135deg, #ffffff 0%, #f4f8ff 100%);
    border: 1px solid #d8e3f5; border-radius: 10px;
    padding: 14px 16px; margin: 0; box-shadow: 0 1px 3px rgba(26,39,68,0.06);
    height: 100%;
  }
  .exec-kpi-label { font-size: 11px; font-weight: 700; letter-spacing: .04em;
    text-transform: uppercase; color: #5a7ab0; }
  .exec-kpi-value { font-size: 26px; font-weight: 800; color: #14213d;
    line-height: 1.15; margin-top: 2px; }
  .exec-kpi-sub { font-size: 11px; color: #7a8aa3; margin-top: 2px; }
  .exec-section-title { font-size: 14px; font-weight: 700; color: #1a2744;
    margin: 16px 0 8px 0; padding-bottom: 4px;
    border-bottom: 2px solid #2E75B6; }

  .exm-wrap { overflow-x: auto; border-radius: 8px;
    border: 1px solid #d8e3f5; }
  .exm-table { border-collapse: collapse; width: 100%; font-size: 12px;
    background: #ffffff; }
  .exm-table th, .exm-table td { padding: 7px 12px; text-align: center;
    border: 1px solid #e2eaf6; white-space: nowrap; }
  .exm-rowlab { text-align: left !important; font-weight: 600;
    color: #1a2744; background: #f4f8ff; position: sticky; left: 0; }
  .exm-vessel { color: #fff; font-weight: 700; font-size: 12px; }
  .exm-bryanston { background: #d89a16; }   /* amber, matching the brief */
  .exm-greeneagle { background: #2E75B6; }  /* blue */
  .exm-alkebulan { background: #6a994e; }   /* green — primary, clone of GreenEagle */
  .exm-monthrow td { background: #f7b32b; color: #1a2744; font-weight: 700; }
  .exm-srcrow td.exm-srcval { color: #14213d; font-variant-numeric: tabular-nums; }
  .exm-srcrow .exm-rowlab { font-weight: 500; color: #2f4163; }
  .exm-srcrow td.exm-zero { color: #b8c4d8; }
  .exm-opening .exm-rowlab { font-style: italic; color: #6b7a96; }
  .exm-opening td.exm-srcval { color: #6b7a96; }
  .exm-totalrow td.exm-val { background: #1a2744; color: #ffffff;
    font-weight: 800; font-variant-numeric: tabular-nums; }
  .exm-totalrow .exm-rowlab { background: #14213d; color: #ffffff; }
  .exm-table tbody tr.exm-srcrow:nth-child(even) td:not(.exm-rowlab) { background: #fbfdff; }
  .exm-foot { font-size: 10.5px; color: #7a8aa3; margin: 6px 2px 0 2px;
    line-height: 1.45; font-style: italic; }

  .exec-readout { margin: 4px 0 2px 0; padding-left: 18px; }
  .exec-readout li { font-size: 13px; color: #243460; margin-bottom: 5px;
    line-height: 1.5; }
</style>
""", unsafe_allow_html=True)

# ── Colour palettes ────────────────────────────────────────────────────────────
VESSEL_COLORS = {
    "Sherlock" : "#ff6b6b",   # coral red     — boosted from #e74c3c
    "Laphroaig": "#2ecc71",   # emerald green — unchanged
    "Rathbone" : "#c77dff",   # light violet  — boosted from #9b59b6
    "SantaMonica": "#6c5ce7", # indigo        — distinct from Rathbone
    "Bedford"  : "#f39c12",   # amber         — unchanged
    "Balham"   : "#1abc9c",   # teal          — unchanged
    "Woodstock": "#ff4d8d",   # hot pink      — boosted from #e91e63
    "Bagshot"  : "#00bcd4",   # cyan          — unchanged
    "Watson"   : "#b0bec5",   # silver        — lightened from #95a5a6
    "Rahama"   : "#fb923c",   # orange
    "Amyla"  : "#7f8c8d",   # steel gray    — Point A-only 63k
    "FatimaZarah": "#84cc16", # lime green    — Point A-only 50k shuttle
    "ZeeZee"   : "#1e3a5f",   # deep navy     — third-party visitor
}
STORAGE_COLORS = {
    "SanBarth"  : "#f1c40f",   # golden yellow  — unchanged
    "JasmineS": "#bf7fff",   # soft lavender  — boosted from #8e44ad
    "Westmore": "#2ecc71",   # emerald green  — brightened from #27ae60
    "Duke"    : "#5dade2",   # sky blue       — lightened from #3498db
    "Starturn": "#f07030",   # vivid orange   — brightened from #d35400
    "PGM"     : "#e91e8c",   # vivid pink     — Point G (SantaMonica only)
}
MOTHER_COLORS = {
    "Bryanston" : "#1abc9c",  # bright teal   — boosted from #16a085
    "GreenEagle": "#c084fc",   # vivid purple
    "Alkebulan" : "#f59e0b",   # warm amber    — primary, clone of GreenEagle
}
STATUS_LIGHTNESS = {
    "IDLE_A":2.0,"WAITING_STOCK":1.8,"WAITING_BERTH_A":1.7,"WAITING_DEAD_STOCK":1.6,
    "BERTHING_A":1.3,"HOSE_CONNECT_A":1.1,"LOADING":1.0,"PF_LOADING":1.0,"PF_SWAP":0.9,
    "DOCUMENTING":0.9,"WAITING_CAST_OFF":0.85,"CAST_OFF":0.8,
    "SAILING_AB":0.68,"SAILING_CROSS_BW_AC":0.72,"SAILING_BW_TO_FWY":0.7,"SAILING_AB_LEG2":0.65,"SAILING_B_TO_FWY":0.68,"SAILING_FWY_TO_BW":0.67,"SAILING_CROSS_BW_IN_AC":0.72,"SAILING_BW_TO_A":0.65,
    "SAILING_D_CHANNEL":0.68,"SAILING_CH_TO_BW_OUT":0.67,"SAILING_CROSS_BW_OUT":0.72,
    "SAILING_B_TO_BW_IN":0.7,"SAILING_CROSS_BW_IN":0.72,
    "SAILING_BW_TO_CH_IN":0.67,"SAILING_CH_TO_D":0.65,
    "WAITING_FAIRWAY":0.6,"WAITING_BERTH_B":0.6,"WAITING_MOTHER_RETURN":0.55,
    "WAITING_MOTHER_CAPACITY":0.5,"WAITING_RETURN_STOCK":0.52,
    "BERTHING_B":0.5,"HOSE_CONNECT_B":0.45,"DISCHARGING":0.4,
    "CAST_OFF_B":0.38,"SAILING_BA":0.5,"IDLE_B":0.55,"SAILING_B_TO_F":0.65,
    "WAITING_DAYLIGHT":1.5,"WAITING_TIDAL":1.45,
}
STATUS_LABELS = {
    # ── At SanBarth / Sego / Awoba / Dawes — Storage ─────────────────────────────────────────
    "IDLE_A"              : "Idle at storage (SanBarth/Sego/Awoba/Dawes)",
    "WAITING_STOCK"       : "SanBarth — Waiting, low stock",
    "WAITING_DEAD_STOCK"  : "SanBarth — Waiting, dead-stock threshold",
    "WAITING_BERTH_A"     : "SanBarth — Waiting for berth",
    "BERTHING_A"          : "SanBarth — Berthing at storage",
    "HOSE_CONNECT_A"      : "SanBarth — Hose connection",
    "LOADING"             : "SanBarth — Loading cargo",
    "DOCUMENTING"         : "SanBarth — Documentation",
    "WAITING_CAST_OFF"    : "Waiting for daylight cast-off window",
    "SAILING_B_TO_F"      : "BIA → Ibom (swap takeover transit)",
    "CAST_OFF"            : "SanBarth — Cast off from storage",
    # ── Ibom (Offshore loading) ────────────────────────────────────────────
    "PF_LOADING"          : "Ibom — Loading at offshore buoy",
    "PF_SWAP"             : "Ibom — Vessel swap in progress",
    "MTO_RECEIVING"       : "BIA — MTO: receiving cargo from shuttle",
    "MTO_DISCHARGING"     : "BIA — MTO: discharging cargo to transient vessel",
    # ── Sailing Point A/C → BIA (4-leg route via breakwater and fairway buoy) ─────────────────
    "SAILING_AB"          : "Sailing Point A/C → Breakwater (1.5h)",
    "SAILING_CROSS_BW_AC"  : "Crossing Breakwater outbound (0.5h, tidal)",
    "SAILING_BW_TO_FWY"    : "After crossing → Fairway Buoy (2h)",
    "SAILING_AB_LEG2"      : "Fairway Buoy → BIA (2h)",
    "SAILING_B_TO_FWY"     : "Returning BIA → Fairway Buoy (2h)",
    "SAILING_FWY_TO_BW"    : "Fairway Buoy → Breakwater (2h)",
    "SAILING_CROSS_BW_IN_AC": "Crossing Breakwater inbound (0.5h, tidal)",
    "SAILING_BW_TO_A"      : "After crossing → Point A/C (1.5h)",
    "WAITING_TIDAL"       : "Waiting — tidal crossing window",
    "WAITING_DAYLIGHT"    : "Waiting — daylight window",
    # ── Sailing SanBarth → Awoba via Cawthorne passage (outbound) ───────────────────────

    # ── At BIA (Mother vessels) ───────────────────────────────────────────
    "WAITING_FAIRWAY"     : "BIA — Holding at fairway buoy",
    "WAITING_BERTH_B"     : "BIA — Waiting for berth at mother",
    "BERTHING_B"          : "BIA — Berthing at mother vessel",
    "HOSE_CONNECT_B"      : "BIA — Hose connection at mother",
    "DISCHARGING"         : "BIA — Discharging to mother",
    "CAST_OFF_B"          : "BIA — Cast off from mother",
    "IDLE_B"              : "BIA — Idle at mother vessel",
    "WAITING_MOTHER_RETURN"   : "BIA — Waiting, mother at export",
    "WAITING_MOTHER_CAPACITY" : "BIA — Waiting, mother full",
    "WAITING_RETURN_STOCK"    : "BIA — Waiting for return assignment",
    # ── Sailing BIA → SanBarth/Sego/Dawes (return via main breakwater) ───────────────────────
    "SAILING_BA"          : "Returning to storage (SanBarth/Sego/Dawes)",
    # ── Sailing Awoba → BIA via Cawthorne passage (inbound) ────────────────────────
    "SAILING_D_CHANNEL"    : "Awoba outbound — Point D → Cawthorne Channel (3h)",
    "SAILING_CH_TO_BW_OUT" : "Awoba outbound — Channel → Breakwater (1h)",
    "SAILING_CROSS_BW_OUT" : "Awoba outbound — Crossing Breakwater (0.5h)",
    "SAILING_B_TO_BW_IN"   : "Awoba return — BIA → clear breakwater (1.5h)",
    "SAILING_CROSS_BW_IN"  : "Awoba return — Crossing Breakwater inbound (0.5h)",
    "SAILING_BW_TO_CH_IN"  : "Awoba return — Breakwater → Cawthorne Channel (1h)",
    "SAILING_CH_TO_D"      : "Awoba return — Cawthorne Channel → Point D (3h)",
}

# Grouped structure for the startup status selector — organised by location
STATUS_GROUPS = [
    ("📍 At SanBarth / Sego / Awoba / Dawes — Storage", [
        ("IDLE_A",           "🟢 Idle at storage"),
        ("WAITING_STOCK",    "⏳ Waiting — low stock"),
        ("WAITING_DEAD_STOCK","⏳ Waiting — dead-stock threshold"),
        ("WAITING_BERTH_A",  "⏳ Waiting for berth"),
        ("BERTHING_A",       "🔗 Berthing at storage"),
        ("HOSE_CONNECT_A",   "🔧 Hose connection"),
        ("LOADING",          "⛽ Loading cargo"),
        ("DOCUMENTING",      "📄 Documentation"),
        ("WAITING_CAST_OFF", "⏳ Waiting — cast-off window"),
        ("CAST_OFF",         "↩️ Cast off from storage"),
    ]),
    ("⚓ Ibom — Offshore Loading", [
        ("PF_LOADING",       "⛽ Loading at offshore buoy"),
        ("PF_SWAP",          "🔁 Vessel swap in progress"),
    ]),
    ("🚢 Sailing Point A/C → BIA (outbound via breakwater)", [
        ("SAILING_AB",          "🚢 Point A/C → Breakwater (1.5h)"),
        ("SAILING_CROSS_BW_AC", "🚢 Crossing Breakwater outbound (0.5h, tidal)"),
        ("SAILING_BW_TO_FWY",   "🚢 Breakwater → Fairway Buoy (2h)"),
        ("SAILING_AB_LEG2",     "🚢 Fairway Buoy → BIA (2h)"),
        ("WAITING_TIDAL",       "🌊 Waiting — tidal crossing window"),
        ("WAITING_DAYLIGHT",    "🌙 Waiting — daylight window"),
        ("WAITING_FAIRWAY",     "⚓ Holding at Fairway Buoy"),
    ]),
    ("🌊 Sailing Awoba (D) → BIA via Cawthorne (outbound)", [
        ("SAILING_D_CHANNEL",    "🚢 Point D → Cawthorne Channel (3h, tidal)"),
        ("SAILING_CH_TO_BW_OUT", "🚢 Channel → Breakwater (1h, tidal)"),
        ("SAILING_CROSS_BW_OUT", "🚢 Crossing Breakwater outbound (0.5h, tidal)"),
        ("WAITING_TIDAL",        "🌊 Waiting — tidal/daylight window"),
    ]),
    ("🛢️ At BIA — Mother Vessels", [
        ("WAITING_FAIRWAY",      "⚓ Holding at fairway buoy"),
        ("WAITING_BERTH_B",      "⏳ Waiting for berth"),
        ("BERTHING_B",           "🔗 Berthing at mother"),
        ("HOSE_CONNECT_B",       "🔧 Hose connection at mother"),
        ("DISCHARGING",          "⬇️ Discharging to mother"),
        ("CAST_OFF_B",           "↩️ Cast off from mother"),
        ("IDLE_B",               "🟢 Idle at mother vessel"),
        ("WAITING_MOTHER_RETURN","⏳ Waiting — mother at export"),
        ("WAITING_MOTHER_CAPACITY","⏳ Waiting — mother full"),
        ("WAITING_RETURN_STOCK", "⏳ Waiting — return assignment"),
    ]),
    ("🔄 Returning BIA → Point A/C (via breakwater)", [
        ("SAILING_B_TO_FWY",       "🔄 BIA → Fairway Buoy (2h)"),
        ("SAILING_FWY_TO_BW",      "🔄 Fairway Buoy → Breakwater (2h)"),
        ("SAILING_CROSS_BW_IN_AC", "🔄 Crossing Breakwater inbound (0.5h, tidal)"),
        ("SAILING_BW_TO_A",        "🔄 Breakwater → Point A/C (1.5h)"),
        ("SAILING_BA",             "🔄 Returning to Starturn/Dawes (direct)"),
        ("WAITING_TIDAL",          "🌊 Waiting — tidal crossing window"),
        ("WAITING_DAYLIGHT",       "🌙 Waiting — daylight window"),
        ("WAITING_FAIRWAY",        "⚓ Holding at Fairway Buoy (return)"),
    ]),
    ("🌊 Returning BIA → Awoba (D) via Cawthorne (inbound)", [
        ("SAILING_B_TO_BW_IN",   "🚢 BIA → clear breakwater (1.5h)"),
        ("SAILING_CROSS_BW_IN",  "🚢 Crossing Breakwater inbound (0.5h, tidal)"),
        ("SAILING_BW_TO_CH_IN",  "🚢 Breakwater → Cawthorne Channel (1h, tidal)"),
        ("SAILING_CH_TO_D",      "🚢 Channel → Point D (3h, tidal)"),
        ("WAITING_TIDAL",        "🌊 Waiting — tidal/daylight window"),
    ]),
]
# ── Location catalogue with status filtering ──────────────────────────────────
# Each location carries:
#   display     – shown in the dropdown
#   sim_value   – what the sim/log uses as the "location" string
#   field_zone  – which storage/area zone this belongs to
#   statuses    – ordered list of (status_code, label) valid at that location
# Groups are ordered: most common first, so default index lands on IDLE_A.

LOCATION_CATALOGUE = [
    # ── SanBarth storage berths ────────────────────────────────────────────────
    # Statuses follow the full lifecycle order: arrive → berth → hose → load → docs → cast-off
    {"display": "SanBarth (Point A)",    "sim_value": "SanBarth",
     "field_zone": "SanBarth",
     "statuses": [
        ("LOADING",            "⛽ Loading — in progress"),
        ("DOCUMENTING",        "📄 Documentation in progress"),
        ("WAITING_CAST_OFF",   "⏳ Loading complete — awaiting cast-off window"),
        ("CAST_OFF",           "↩️ Cast off (departing storage)"),
        ("IDLE_A",             "🟢 Idle at berth — ready to load"),
        ("HOSE_CONNECT_A",     "🔧 Hose connection underway"),
        ("BERTHING_A",         "🔗 Berthing in progress"),
        ("WAITING_BERTH_A",    "⏳ Arrived — waiting for berth slot"),
        ("WAITING_STOCK",      "⏳ At berth — waiting for stock to build"),
        ("WAITING_DEAD_STOCK", "⏳ At berth — stock below dead-stock threshold"),
    ]},
    {"display": "JasmineS (Point A)", "sim_value": "JasmineS",
     "field_zone": "SanBarth",
     "statuses": [
        ("LOADING",            "⛽ Loading — in progress"),
        ("DOCUMENTING",        "📄 Documentation in progress"),
        ("WAITING_CAST_OFF",   "⏳ Loading complete — awaiting cast-off window"),
        ("CAST_OFF",           "↩️ Cast off (departing storage)"),
        ("IDLE_A",             "🟢 Idle at berth — ready to load"),
        ("HOSE_CONNECT_A",     "🔧 Hose connection underway"),
        ("BERTHING_A",         "🔗 Berthing in progress"),
        ("WAITING_BERTH_A",    "⏳ Arrived — waiting for berth slot"),
        ("WAITING_STOCK",      "⏳ At berth — waiting for stock to build"),
        ("WAITING_DEAD_STOCK", "⏳ At berth — stock below dead-stock threshold"),
    ]},
    # ── Sego (Westmore) ────────────────────────────────────────────────────────
    {"display": "Westmore (Sego)",     "sim_value": "Westmore",
     "field_zone": "Sego",
     "statuses": [
        ("LOADING",            "⛽ Loading — in progress"),
        ("DOCUMENTING",        "📄 Documentation in progress"),
        ("WAITING_CAST_OFF",   "⏳ Loading complete — awaiting cast-off window"),
        ("CAST_OFF",           "↩️ Cast off (departing storage)"),
        ("IDLE_A",             "🟢 Idle at berth — ready to load"),
        ("HOSE_CONNECT_A",     "🔧 Hose connection underway"),
        ("BERTHING_A",         "🔗 Berthing in progress"),
        ("WAITING_BERTH_A",    "⏳ Arrived — waiting for berth slot"),
        ("WAITING_STOCK",      "⏳ At berth — waiting for stock to build"),
        ("WAITING_DEAD_STOCK", "⏳ At berth — stock below dead-stock threshold"),
    ]},
    # ── Awoba (Duke) — via Cawthorne ──────────────────────────────────────────
    {"display": "Duke (Awoba)",        "sim_value": "Duke",
     "field_zone": "Awoba",
     "statuses": [
        ("LOADING",            "⛽ Loading — in progress"),
        ("DOCUMENTING",        "📄 Documentation in progress"),
        ("WAITING_CAST_OFF",   "⏳ Loading complete — awaiting cast-off window"),
        ("CAST_OFF",           "↩️ Cast off (departing storage)"),
        ("IDLE_A",             "🟢 Idle at berth — ready to load"),
        ("HOSE_CONNECT_A",     "🔧 Hose connection underway"),
        ("BERTHING_A",         "🔗 Berthing in progress"),
        ("WAITING_BERTH_A",    "⏳ Arrived — waiting for berth slot"),
        ("WAITING_STOCK",      "⏳ At berth — waiting for stock to build"),
        ("WAITING_DEAD_STOCK", "⏳ At berth — stock below dead-stock threshold"),
    ]},
    # ── Dawes (Starturn) ──────────────────────────────────────────────────────
    {"display": "Starturn (Dawes)",    "sim_value": "Starturn",
     "field_zone": "Dawes",
     "statuses": [
        ("LOADING",            "⛽ Loading — in progress"),
        ("DOCUMENTING",        "📄 Documentation in progress"),
        ("WAITING_CAST_OFF",   "⏳ Loading complete — awaiting cast-off window"),
        ("CAST_OFF",           "↩️ Cast off (departing storage)"),
        ("IDLE_A",             "🟢 Idle at berth — ready to load"),
        ("HOSE_CONNECT_A",     "🔧 Hose connection underway"),
        ("BERTHING_A",         "🔗 Berthing in progress"),
        ("WAITING_BERTH_A",    "⏳ Arrived — waiting for berth slot"),
        ("WAITING_STOCK",      "⏳ At berth — waiting for stock to build"),
        ("WAITING_DEAD_STOCK", "⏳ At berth — stock below dead-stock threshold"),
    ]},
    # ── PGM (Point G) ─────────────────────────────────────────────────────────
    {"display": "PGM (Point G)",       "sim_value": "PGM",
     "field_zone": "PGM",
     "statuses": [
        ("LOADING",            "⛽ Loading — in progress"),
        ("DOCUMENTING",        "📄 Documentation in progress"),
        ("WAITING_CAST_OFF",   "⏳ Loading complete — awaiting cast-off window"),
        ("CAST_OFF",           "↩️ Cast off (departing storage)"),
        ("IDLE_A",             "🟢 Idle at berth — ready to load"),
        ("HOSE_CONNECT_A",     "🔧 Hose connection underway"),
        ("BERTHING_A",         "🔗 Berthing in progress"),
        ("WAITING_BERTH_A",    "⏳ Arrived — waiting for berth slot"),
        ("WAITING_STOCK",      "⏳ At berth — waiting for stock to build"),
        ("WAITING_DEAD_STOCK", "⏳ At berth — stock below dead-stock threshold"),
    ]},
    # ── Ibom offshore ─────────────────────────────────────────────────────────
    {"display": "Ibom (Offshore Buoy)","sim_value": "Ibom",
     "field_zone": "Ibom",
     "statuses": [
        ("PF_LOADING",         "⛽ Loading at offshore buoy — in progress"),
        ("PF_SWAP",            "🔁 Vessel swap / handover in progress"),
        ("IDLE_A",             "🟢 Idle / standby at buoy"),
        ("WAITING_DAYLIGHT",   "🌙 Waiting — daylight window"),
        ("WAITING_TIDAL",      "🌊 Waiting — tidal window"),
    ]},
    # ── Rahama (SanBarth / JasmineS + Westmore + Duke — no Starturn, Ibom) ─────────
    {"display": "Returning → SanBarth/JasmineS",        "sim_value": "En Route BIA→Storage",
     "field_zone": "Transit", "target_storage": "SanBarth", "target_mother": None,
     "vessel_filter": ["Rahama"],
     "statuses": [("SAILING_BA", "🚢 Returning — BIA to SanBarth/JasmineS")]},
    # ── En route → BIA — Leg 1: storage → Breakwater ─────────────────────────
    {"display": "Sailing → Bryanston (A/C outbound)", "sim_value": "En Route SanBarth→BIA",
     "field_zone": "Transit", "target_mother": "Bryanston", "target_storage": None,
     "statuses": [
        ("SAILING_AB",          "🚢 Leg 1: Point A/C → Breakwater (1.5h)"),
        ("SAILING_CROSS_BW_AC", "🚢 Leg 2: Crossing Breakwater outbound (0.5h)"),
        ("SAILING_BW_TO_FWY",   "🚢 Leg 3: Breakwater → Fairway Buoy (2h)"),
        ("WAITING_TIDAL",       "🌊 Holding — waiting for tidal window"),
        ("WAITING_DAYLIGHT",    "🌙 Holding — waiting for daylight window"),
        ("WAITING_RETURN_STOCK","⏳ Holding — return destination assignment"),
    ]},
    {"display": "Sailing → GreenEagle (A/C outbound)", "sim_value": "En Route SanBarth→BIA",
     "field_zone": "Transit", "target_mother": "GreenEagle", "target_storage": None,
     "statuses": [
        ("SAILING_AB",          "🚢 Leg 1: Point A/C → Breakwater (1.5h)"),
        ("SAILING_CROSS_BW_AC", "🚢 Leg 2: Crossing Breakwater outbound (0.5h)"),
        ("SAILING_BW_TO_FWY",   "🚢 Leg 3: Breakwater → Fairway Buoy (2h)"),
        ("WAITING_TIDAL",       "🌊 Holding — waiting for tidal window"),
        ("WAITING_DAYLIGHT",    "🌙 Holding — waiting for daylight window"),
        ("WAITING_RETURN_STOCK","⏳ Holding — return destination assignment"),
    ]},
    {"display": "Sailing → Alkebulan (A/C outbound)", "sim_value": "En Route SanBarth→BIA",
     "field_zone": "Transit", "target_mother": "Alkebulan", "target_storage": None,
     "statuses": [
        ("SAILING_AB",          "🚢 Leg 1: Point A/C → Breakwater (1.5h)"),
        ("SAILING_CROSS_BW_AC", "🚢 Leg 2: Crossing Breakwater outbound (0.5h)"),
        ("SAILING_BW_TO_FWY",   "🚢 Leg 3: Breakwater → Fairway Buoy (2h)"),
        ("WAITING_TIDAL",       "🌊 Holding — waiting for tidal window"),
        ("WAITING_DAYLIGHT",    "🌙 Holding — waiting for daylight window"),
        ("WAITING_RETURN_STOCK","⏳ Holding — return destination assignment"),
    ]},
    # ── En route → BIA — Leg 2: at or near Fairway Buoy ──────────────────────
    {"display": "Approaching Bryanston (Fairway Buoy)", "sim_value": "Fairway Buoy",
     "field_zone": "Transit", "target_mother": "Bryanston", "target_storage": None,
     "statuses": [
        ("SAILING_BW_TO_FWY",   "🚢 Breakwater → Fairway Buoy (2h)"),
        ("SAILING_AB_LEG2",     "🚢 Fairway Buoy → BIA (2h)"),
        ("WAITING_FAIRWAY",     "⚓ Arrived after 19:00 — holding at Fairway Buoy overnight"),
        ("WAITING_BERTH_B",     "⏳ Arrived fairway — waiting for mother berth"),
        ("WAITING_MOTHER_RETURN","⏳ Waiting — mother vessel away at export"),
        ("WAITING_MOTHER_CAPACITY","⏳ Waiting — mother vessel full"),
        ("WAITING_DAYLIGHT",    "🌙 Holding — waiting for daylight window"),
    ]},
    {"display": "Approaching GreenEagle (Fairway Buoy)", "sim_value": "Fairway Buoy",
     "field_zone": "Transit", "target_mother": "GreenEagle", "target_storage": None,
     "statuses": [
        ("SAILING_BW_TO_FWY",   "🚢 Breakwater → Fairway Buoy (2h)"),
        ("SAILING_AB_LEG2",     "🚢 Fairway Buoy → BIA (2h)"),
        ("WAITING_FAIRWAY",     "⚓ Arrived after 19:00 — holding at Fairway Buoy overnight"),
        ("WAITING_BERTH_B",     "⏳ Arrived fairway — waiting for mother berth"),
        ("WAITING_MOTHER_RETURN","⏳ Waiting — mother vessel away at export"),
        ("WAITING_MOTHER_CAPACITY","⏳ Waiting — mother vessel full"),
        ("WAITING_DAYLIGHT",    "🌙 Holding — waiting for daylight window"),
    ]},
    {"display": "Approaching Alkebulan (Fairway Buoy)", "sim_value": "Fairway Buoy",
     "field_zone": "Transit", "target_mother": "Alkebulan", "target_storage": None,
     "statuses": [
        ("SAILING_BW_TO_FWY",   "🚢 Breakwater → Fairway Buoy (2h)"),
        ("SAILING_AB_LEG2",     "🚢 Fairway Buoy → BIA (2h)"),
        ("WAITING_FAIRWAY",     "⚓ Arrived after 19:00 — holding at Fairway Buoy overnight"),
        ("WAITING_BERTH_B",     "⏳ Arrived fairway — waiting for mother berth"),
        ("WAITING_MOTHER_RETURN","⏳ Waiting — mother vessel away at export"),
        ("WAITING_MOTHER_CAPACITY","⏳ Waiting — mother vessel full"),
        ("WAITING_DAYLIGHT",    "🌙 Holding — waiting for daylight window"),
    ]},
    # ── Cawthorne outbound (SanBarth → Awoba) — target_mother embedded ────────
    {"display": "Breakwater outbound → Bryanston", "sim_value": "Breakwater (outbound)",
     "field_zone": "Transit", "target_mother": "Bryanston", "target_storage": None,
     "statuses": [
        ("SAILING_CH_TO_BW_OUT", "🚢 Channel → Breakwater (1h, tidal)"),
        ("SAILING_CROSS_BW_OUT", "🚢 Crossing Breakwater outbound (0.5h, tidal)"),
        ("WAITING_TIDAL",        "🌊 Waiting — tidal/daylight window"),
    ]},
    {"display": "Breakwater outbound → GreenEagle", "sim_value": "Breakwater (outbound)",
     "field_zone": "Transit", "target_mother": "GreenEagle", "target_storage": None,
     "statuses": [
        ("SAILING_CH_TO_BW_OUT", "🚢 Channel → Breakwater (1h, tidal)"),
        ("SAILING_CROSS_BW_OUT", "🚢 Crossing Breakwater outbound (0.5h, tidal)"),
        ("WAITING_TIDAL",        "🌊 Waiting — tidal/daylight window"),
    ]},
    {"display": "Breakwater outbound → Alkebulan", "sim_value": "Breakwater (outbound)",
     "field_zone": "Transit", "target_mother": "Alkebulan", "target_storage": None,
     "statuses": [
        ("SAILING_CH_TO_BW_OUT", "🚢 Channel → Breakwater (1h, tidal)"),
        ("SAILING_CROSS_BW_OUT", "🚢 Crossing Breakwater outbound (0.5h, tidal)"),
        ("WAITING_TIDAL",        "🌊 Waiting — tidal/daylight window"),
    ]},
    {"display": "Cawthorne Channel outbound → Bryanston", "sim_value": "Cawthorne Channel (outbound)",
     "field_zone": "Transit", "target_mother": "Bryanston", "target_storage": None,
     "statuses": [
        ("SAILING_D_CHANNEL",    "🚢 Point D → Cawthorne Channel (3h, tidal)"),
        ("SAILING_CH_TO_BW_OUT", "🚢 Channel → Breakwater (1h, tidal)"),
        ("SAILING_CROSS_BW_OUT", "🚢 Crossing Breakwater outbound (0.5h, tidal)"),
        ("WAITING_TIDAL",        "🌊 Waiting — tidal/daylight window"),
    ]},
    {"display": "Cawthorne Channel outbound → GreenEagle", "sim_value": "Cawthorne Channel (outbound)",
     "field_zone": "Transit", "target_mother": "GreenEagle", "target_storage": None,
     "statuses": [
        ("SAILING_D_CHANNEL",    "🚢 Point D → Cawthorne Channel (3h, tidal)"),
        ("SAILING_CH_TO_BW_OUT", "🚢 Channel → Breakwater (1h, tidal)"),
        ("SAILING_CROSS_BW_OUT", "🚢 Crossing Breakwater outbound (0.5h, tidal)"),
        ("WAITING_TIDAL",        "🌊 Waiting — tidal/daylight window"),
    ]},
    {"display": "Cawthorne Channel outbound → Alkebulan", "sim_value": "Cawthorne Channel (outbound)",
     "field_zone": "Transit", "target_mother": "Alkebulan", "target_storage": None,
     "statuses": [
        ("SAILING_D_CHANNEL",    "🚢 Point D → Cawthorne Channel (3h, tidal)"),
        ("SAILING_CH_TO_BW_OUT", "🚢 Channel → Breakwater (1h, tidal)"),
        ("SAILING_CROSS_BW_OUT", "🚢 Crossing Breakwater outbound (0.5h, tidal)"),
        ("WAITING_TIDAL",        "🌊 Waiting — tidal/daylight window"),
    ]},
    # ── At BIA ────────────────────────────────────────────────────────────────
    {"display": "BIA — Fairway Buoy",  "sim_value": "Fairway",
     "field_zone": "BIA",
     "statuses": [
        ("SAILING_AB_LEG2",        "🚢 Inbound — Fairway Buoy → BIA (2h)"),
        ("WAITING_FAIRWAY",        "⚓ Arrived after 19:00 — holding overnight at Fairway Buoy"),
        ("WAITING_BERTH_B",        "⏳ Arrived fairway — waiting for mother berth"),
        ("WAITING_MOTHER_RETURN",  "⏳ Waiting — mother vessel away at export"),
        ("WAITING_MOTHER_CAPACITY","⏳ Waiting — mother vessel full"),
        ("WAITING_RETURN_STOCK",   "⏳ Waiting — return destination assignment"),
        ("WAITING_DAYLIGHT",       "🌙 Waiting — daylight window"),
        ("IDLE_B",                 "🟢 Idle at BIA — no berth assigned yet"),
    ]},
    {"display": "Bryanston (BIA)",     "sim_value": "Bryanston",
     "field_zone": "BIA",
     "statuses": [
        ("HOSE_CONNECT_B",         "🔧 Hose connection underway"),
        ("BERTHING_B",             "🔗 Berthing in progress"),
        ("WAITING_BERTH_B",        "⏳ Arrived — waiting for berth slot"),
        ("WAITING_MOTHER_CAPACITY","⏳ Berthed — waiting for mother capacity"),
        ("CAST_OFF_B",             "↩️ Discharge complete — cast off from mother"),
        ("IDLE_B",                 "🟢 Idle at mother — discharge complete"),
        ("WAITING_CAST_OFF",       "⏳ Discharge complete — awaiting cast-off window"),
        ("WAITING_MOTHER_RETURN",  "⏳ Waiting — mother vessel away at export"),
    ]},
    {"display": "GreenEagle (BIA)",     "sim_value": "GreenEagle",
     "field_zone": "BIA",
     "statuses": [
        ("HOSE_CONNECT_B",         "🔧 Hose connection underway"),
        ("BERTHING_B",             "🔗 Berthing in progress"),
        ("WAITING_BERTH_B",        "⏳ Arrived — waiting for berth slot"),
        ("WAITING_MOTHER_CAPACITY","⏳ Berthed — waiting for mother capacity"),
        ("CAST_OFF_B",             "↩️ Discharge complete — cast off from mother"),
        ("IDLE_B",                 "🟢 Idle at mother — discharge complete"),
        ("WAITING_CAST_OFF",       "⏳ Discharge complete — awaiting cast-off window"),
        ("WAITING_MOTHER_RETURN",  "⏳ Waiting — mother vessel away at export"),
    ]},
    {"display": "Alkebulan (BIA)",     "sim_value": "Alkebulan",
     "field_zone": "BIA",
     "statuses": [
        ("HOSE_CONNECT_B",         "🔧 Hose connection underway"),
        ("BERTHING_B",             "🔗 Berthing in progress"),
        ("WAITING_BERTH_B",        "⏳ Arrived — waiting for berth slot"),
        ("WAITING_MOTHER_CAPACITY","⏳ Berthed — waiting for mother capacity"),
        ("CAST_OFF_B",             "↩️ Discharge complete — cast off from mother"),
        ("IDLE_B",                 "🟢 Idle at mother — discharge complete"),
        ("WAITING_CAST_OFF",       "⏳ Discharge complete — awaiting cast-off window"),
        ("WAITING_MOTHER_RETURN",  "⏳ Waiting — mother vessel away at export"),
    ]},

    # ── MTO (Mid-Transfer Operation) at BIA ─────────────────────────────────
    # Receiver: one entry — operator sets a vessel as the MTO transient.
    # Discharger: one entry per eligible receiver vessel (Lock & Offload → X).
    # The discharger picks their target directly from the location dropdown —
    # no dynamic session-state pairing required.
    {"display": "MTO — Receiver at BIA",
     "sim_value": "MTO_RECEIVER",
     "field_zone": "BIA", "target_mother": None, "target_storage": None,
     "mto_receiver": True,
     "statuses": [
        ("WAITING_BERTH_B", "📦 Lock & Load — receiving from discharger"),
     ]},
    # MTO discharger → Watson
    {"display": "MTO — Lock & Offload → Watson",
     "sim_value": "Watson",
     "field_zone": "BIA", "target_mother": None, "target_storage": None,
     "mto_discharger": True, "mto_target_vessel": "Watson",
     "statuses": [
        ("WAITING_BERTH_B", "🔒 Lock & Offload — queued to pump into Watson"),
        ("HOSE_CONNECT_B",  "🔧 MTO — hose connected, pumping to Watson"),
        ("DISCHARGING",     "⬇️ MTO — pumping cargo to Watson"),
        ("CAST_OFF_B",      "↩️ MTO — complete, casting off from Watson"),
     ]},
    # MTO discharger → Sherlock
    {"display": "MTO — Lock & Offload → Sherlock",
     "sim_value": "Sherlock",
     "field_zone": "BIA", "target_mother": None, "target_storage": None,
     "mto_discharger": True, "mto_target_vessel": "Sherlock",
     "statuses": [
        ("WAITING_BERTH_B", "🔒 Lock & Offload — queued to pump into Sherlock"),
        ("HOSE_CONNECT_B",  "🔧 MTO — hose connected, pumping to Sherlock"),
        ("DISCHARGING",     "⬇️ MTO — pumping cargo to Sherlock"),
        ("CAST_OFF_B",      "↩️ MTO — complete, casting off from Sherlock"),
     ]},
    # MTO discharger → Laphroaig
    {"display": "MTO — Lock & Offload → Laphroaig",
     "sim_value": "Laphroaig",
     "field_zone": "BIA", "target_mother": None, "target_storage": None,
     "mto_discharger": True, "mto_target_vessel": "Laphroaig",
     "statuses": [
        ("WAITING_BERTH_B", "🔒 Lock & Offload — queued to pump into Laphroaig"),
        ("HOSE_CONNECT_B",  "🔧 MTO — hose connected, pumping to Laphroaig"),
        ("DISCHARGING",     "⬇️ MTO — pumping cargo to Laphroaig"),
        ("CAST_OFF_B",      "↩️ MTO — complete, casting off from Laphroaig"),
     ]},
    # MTO discharger → Rathbone
    {"display": "MTO — Lock & Offload → Rathbone",
     "sim_value": "Rathbone",
     "field_zone": "BIA", "target_mother": None, "target_storage": None,
     "mto_discharger": True, "mto_target_vessel": "Rathbone",
     "statuses": [
        ("WAITING_BERTH_B", "🔒 Lock & Offload — queued to pump into Rathbone"),
        ("HOSE_CONNECT_B",  "🔧 MTO — hose connected, pumping to Rathbone"),
        ("DISCHARGING",     "⬇️ MTO — pumping cargo to Rathbone"),
        ("CAST_OFF_B",      "↩️ MTO — complete, casting off from Rathbone"),
     ]},
    # MTO discharger → Balham
    {"display": "MTO — Lock & Offload → Balham",
     "sim_value": "Balham",
     "field_zone": "BIA", "target_mother": None, "target_storage": None,
     "mto_discharger": True, "mto_target_vessel": "Balham",
     "statuses": [
        ("WAITING_BERTH_B", "🔒 Lock & Offload — queued to pump into Balham"),
        ("HOSE_CONNECT_B",  "🔧 MTO — hose connected, pumping to Balham"),
        ("DISCHARGING",     "⬇️ MTO — pumping cargo to Balham"),
        ("CAST_OFF_B",      "↩️ MTO — complete, casting off from Balham"),
     ]},
    # MTO discharger → Bedford
    {"display": "MTO — Lock & Offload → Bedford",
     "sim_value": "Bedford",
     "field_zone": "BIA", "target_mother": None, "target_storage": None,
     "mto_discharger": True, "mto_target_vessel": "Bedford",
     "statuses": [
        ("WAITING_BERTH_B", "🔒 Lock & Offload — queued to pump into Bedford"),
        ("HOSE_CONNECT_B",  "🔧 MTO — hose connected, pumping to Bedford"),
        ("DISCHARGING",     "⬇️ MTO — pumping cargo to Bedford"),
        ("CAST_OFF_B",      "↩️ MTO — complete, casting off from Bedford"),
     ]},
    # MTO discharger → Amyla
    {"display": "MTO — Lock & Offload → Amyla",
     "sim_value": "Amyla",
     "field_zone": "BIA", "target_mother": None, "target_storage": None,
     "mto_discharger": True, "mto_target_vessel": "Amyla",
     "statuses": [
        ("WAITING_BERTH_B", "🔒 Lock & Offload — queued to pump into Amyla"),
        ("HOSE_CONNECT_B",  "🔧 MTO — hose connected, pumping to Amyla"),
        ("DISCHARGING",     "⬇️ MTO — pumping cargo to Amyla"),
        ("CAST_OFF_B",      "↩️ MTO — complete, casting off from Amyla"),
     ]},
    # MTO discharger → Bagshot
    {"display": "MTO — Lock & Offload → Bagshot",
     "sim_value": "Bagshot",
     "field_zone": "BIA", "target_mother": None, "target_storage": None,
     "mto_discharger": True, "mto_target_vessel": "Bagshot",
     "statuses": [
        ("WAITING_BERTH_B", "🔒 Lock & Offload — queued to pump into Bagshot"),
        ("HOSE_CONNECT_B",  "🔧 MTO — hose connected, pumping to Bagshot"),
        ("DISCHARGING",     "⬇️ MTO — pumping cargo to Bagshot"),
        ("CAST_OFF_B",      "↩️ MTO — complete, casting off from Bagshot"),
     ]},
    # MTO discharger → Woodstock
    {"display": "MTO — Lock & Offload → Woodstock",
     "sim_value": "Woodstock",
     "field_zone": "BIA", "target_mother": None, "target_storage": None,
     "mto_discharger": True, "mto_target_vessel": "Woodstock",
     "statuses": [
        ("WAITING_BERTH_B", "🔒 Lock & Offload — queued to pump into Woodstock"),
        ("HOSE_CONNECT_B",  "🔧 MTO — hose connected, pumping to Woodstock"),
        ("DISCHARGING",     "⬇️ MTO — pumping cargo to Woodstock"),
        ("CAST_OFF_B",      "↩️ MTO — complete, casting off from Woodstock"),
     ]},

        # ── Returning from BIA — one entry per storage destination ──────────────
    # SanBarth and JasmineS are both point A: sim picks between them by stock level
    {"display": "Returning → Point A (SanBarth/JasmineS)", "sim_value": "En Route BIA→Storage",
     "field_zone": "Transit", "target_storage": "SanBarth", "target_mother": None,
     "statuses": [
        ("SAILING_B_TO_FWY",       "🔄 Leg 1: BIA → Fairway Buoy (2h)"),
        ("SAILING_FWY_TO_BW",      "🔄 Leg 2: Fairway Buoy → Breakwater (2h)"),
        ("SAILING_CROSS_BW_IN_AC", "🔄 Leg 3: Crossing Breakwater inbound (0.5h)"),
        ("SAILING_BW_TO_A",        "🔄 Leg 4: Breakwater → SanBarth (1.5h)"),
        ("WAITING_TIDAL",          "🌊 Holding — waiting for tidal window"),
        ("WAITING_DAYLIGHT",       "🌙 Holding — waiting for daylight window"),
        ("WAITING_RETURN_STOCK",   "⏳ Holding — return destination assignment"),
    ]},
    {"display": "Returning → Westmore (Sego)", "sim_value": "En Route BIA→Storage",
     "field_zone": "Transit", "target_storage": "Westmore", "target_mother": None,
     "statuses": [
        ("SAILING_B_TO_FWY",       "🔄 Leg 1: BIA → Fairway Buoy (2h)"),
        ("SAILING_FWY_TO_BW",      "🔄 Leg 2: Fairway Buoy → Breakwater (2h)"),
        ("SAILING_CROSS_BW_IN_AC", "🔄 Leg 3: Crossing Breakwater inbound (0.5h)"),
        ("SAILING_BW_TO_A",        "🔄 Leg 4: Breakwater → Westmore (1.5h)"),
        ("WAITING_TIDAL",          "🌊 Holding — waiting for tidal window"),
        ("WAITING_DAYLIGHT",       "🌙 Holding — waiting for daylight window"),
        ("WAITING_RETURN_STOCK",   "⏳ Holding — return destination assignment"),
    ]},
    {"display": "Returning → Starturn (Dawes)", "sim_value": "En Route BIA→Storage",
     "field_zone": "Transit", "target_storage": "Starturn", "target_mother": None,
     "statuses": [
        ("SAILING_B_TO_FWY",       "🔄 Leg 1: BIA → Fairway Buoy (2h)"),
        ("SAILING_FWY_TO_BW",      "🔄 Leg 2: Fairway Buoy → Breakwater (2h)"),
        ("SAILING_CROSS_BW_IN_AC", "🔄 Leg 3: Crossing Breakwater inbound (0.5h)"),
        ("SAILING_BA",             "🔄 Leg 4: Inbound → Starturn (Dawes)"),
        ("WAITING_TIDAL",          "🌊 Holding — waiting for tidal window"),
        ("WAITING_DAYLIGHT",       "🌙 Holding — waiting for daylight window"),
        ("WAITING_RETURN_STOCK",   "⏳ Holding — return destination assignment"),
    ]},
    {"display": "Returning → PGM (Point G)", "sim_value": "En Route BIA→Storage",
     "field_zone": "Transit", "target_storage": "PGM", "target_mother": None,
     "statuses": [
        ("SAILING_BA",             "🔄 Inbound → PGM (Point G, direct 3h)"),
        ("WAITING_TIDAL",          "🌊 Holding — waiting for tidal window"),
        ("WAITING_DAYLIGHT",       "🌙 Holding — waiting for daylight window"),
        ("WAITING_RETURN_STOCK",   "⏳ Holding — return destination assignment"),
    ]},
    # ── Cawthorne inbound (Awoba/Duke → BIA) — target_mother embedded ─────────
    {"display": "Cawthorne Channel → BIA via Bryanston", "sim_value": "Cawthorne Channel (outbound)",
     "field_zone": "Transit", "target_storage": "Duke", "target_mother": "Bryanston",
     "statuses": [
        ("SAILING_D_CHANNEL",    "🚢 Point D → Cawthorne Channel (3h, tidal)"),
        ("SAILING_CH_TO_BW_OUT", "🚢 Channel → Breakwater (1h, tidal)"),
        ("SAILING_CROSS_BW_OUT", "🚢 Crossing Breakwater outbound (0.5h, tidal)"),
        ("WAITING_TIDAL",        "🌊 Waiting — tidal/daylight window"),
    ]},
    {"display": "Cawthorne Channel → BIA via GreenEagle", "sim_value": "Cawthorne Channel (outbound)",
     "field_zone": "Transit", "target_storage": "Duke", "target_mother": "GreenEagle",
     "statuses": [
        ("SAILING_D_CHANNEL",    "🚢 Point D → Cawthorne Channel (3h, tidal)"),
        ("SAILING_CH_TO_BW_OUT", "🚢 Channel → Breakwater (1h, tidal)"),
        ("SAILING_CROSS_BW_OUT", "🚢 Crossing Breakwater outbound (0.5h, tidal)"),
        ("WAITING_TIDAL",        "🌊 Waiting — tidal/daylight window"),
    ]},
    {"display": "Cawthorne Channel → BIA via Alkebulan", "sim_value": "Cawthorne Channel (outbound)",
     "field_zone": "Transit", "target_storage": "Duke", "target_mother": "Alkebulan",
     "statuses": [
        ("SAILING_D_CHANNEL",    "🚢 Point D → Cawthorne Channel (3h, tidal)"),
        ("SAILING_CH_TO_BW_OUT", "🚢 Channel → Breakwater (1h, tidal)"),
        ("SAILING_CROSS_BW_OUT", "🚢 Crossing Breakwater outbound (0.5h, tidal)"),
        ("WAITING_TIDAL",        "🌊 Waiting — tidal/daylight window"),
    ]},
    {"display": "Returning Duke from BIA via Bryanston", "sim_value": "En Route BIA→Storage",
     "field_zone": "Transit", "target_storage": "Duke", "target_mother": "Bryanston",
     "statuses": [
        ("SAILING_B_TO_BW_IN",  "🚢 BIA → clear breakwater (1.5h)"),
        ("SAILING_CROSS_BW_IN", "🚢 Crossing Breakwater inbound (0.5h, tidal)"),
        ("SAILING_BW_TO_CH_IN", "🚢 Breakwater → Cawthorne Channel (1h, tidal)"),
        ("SAILING_CH_TO_D",     "🚢 Channel → Point D (3h, tidal)"),
        ("WAITING_TIDAL",       "🌊 Waiting — tidal/daylight window"),
    ]},
    {"display": "Returning Duke from BIA via GreenEagle", "sim_value": "En Route BIA→Storage",
     "field_zone": "Transit", "target_storage": "Duke", "target_mother": "GreenEagle",
     "statuses": [
        ("SAILING_B_TO_BW_IN",  "🚢 BIA → clear breakwater (1.5h)"),
        ("SAILING_CROSS_BW_IN", "🚢 Crossing Breakwater inbound (0.5h, tidal)"),
        ("SAILING_BW_TO_CH_IN", "🚢 Breakwater → Cawthorne Channel (1h, tidal)"),
        ("SAILING_CH_TO_D",     "🚢 Channel → Point D (3h, tidal)"),
        ("WAITING_TIDAL",       "🌊 Waiting — tidal/daylight window"),
    ]},
    {"display": "Returning Duke from BIA via Alkebulan", "sim_value": "En Route BIA→Storage",
     "field_zone": "Transit", "target_storage": "Duke", "target_mother": "Alkebulan",
     "statuses": [
        ("SAILING_B_TO_BW_IN",  "🚢 BIA → clear breakwater (1.5h)"),
        ("SAILING_CROSS_BW_IN", "🚢 Crossing Breakwater inbound (0.5h, tidal)"),
        ("SAILING_BW_TO_CH_IN", "🚢 Breakwater → Cawthorne Channel (1h, tidal)"),
        ("SAILING_CH_TO_D",     "🚢 Channel → Point D (3h, tidal)"),
        ("WAITING_TIDAL",       "🌊 Waiting — tidal/daylight window"),
    ]},
]

# Pre-built lookups derived from catalogue
LOC_DISPLAY_LIST  = [e["display"]    for e in LOCATION_CATALOGUE]
LOC_BY_DISPLAY    = {e["display"]: e for e in LOCATION_CATALOGUE}

# Vessels restricted to SanBarth and Sego only (Watson)
SANBARTH_LOC_DISPLAYS = [e["display"] for e in LOCATION_CATALOGUE
                          if e["field_zone"] in ("SanBarth", "BIA", "Transit")]

# Zone badges for display
ZONE_BADGE = {
    "SanBarth": ("🟡", "#f1c40f"),
    "Sego":     ("🟢", "#2ecc71"),
    "Awoba":    ("🔵", "#5dade2"),
    "Dawes":    ("🟠", "#f07030"),
    "PGM":      ("🩷", "#e91e8c"),
    "Ibom":     ("🟣", "#bf7fff"),
    "BIA":      ("🔴", "#ff6b6b"),
    "Transit":  ("⚪", "#94a3b8"),
}

STATUS_ICONS = {
    "LOADING":"⛽","PF_LOADING":"⛽","DISCHARGING":"⬇️",
    "SAILING_AB":"🚢","SAILING_CROSS_BW_AC":"🚢","SAILING_BW_TO_FWY":"🚢","SAILING_AB_LEG2":"🚢","SAILING_BA":"🔄","SAILING_B_TO_FWY":"🔄","SAILING_FWY_TO_BW":"🔄","SAILING_CROSS_BW_IN_AC":"🔄","SAILING_BW_TO_A":"🔄",
    "SAILING_D_CHANNEL":"🚢","SAILING_CH_TO_BW_OUT":"🚢","SAILING_CROSS_BW_OUT":"🚢",
    "SAILING_B_TO_BW_IN":"🚢","SAILING_CROSS_BW_IN":"🚢","SAILING_BW_TO_CH_IN":"🚢","SAILING_CH_TO_D":"🚢",
    "WAITING_FAIRWAY":"⚓","WAITING_BERTH_B":"⏳","WAITING_BERTH_A":"⏳",
    "BERTHING_A":"🔗","BERTHING_B":"🔗","HOSE_CONNECT_A":"🔧","HOSE_CONNECT_B":"🔧",
    "IDLE_A":"🟢","IDLE_B":"🟡","CAST_OFF":"↩️","CAST_OFF_B":"↩️","DOCUMENTING":"📄",
    "WAITING_CAST_OFF":"⏳","WAITING_DEAD_STOCK":"⏳","WAITING_RETURN_STOCK":"⏳",
    "PF_SWAP":"🔁","WAITING_DAYLIGHT":"🌙","WAITING_TIDAL":"🌊","WAITING_STOCK":"⏳",
    "WAITING_MOTHER_RETURN":"⏳","WAITING_MOTHER_CAPACITY":"⏳",
}


def _normalize_hex_color(hex_color, fallback="#95a5a6"):
    """Return a safe 6-digit hex color string."""
    raw = str(hex_color or "").strip().lstrip("#")
    if len(raw) == 3:
        raw = "".join(ch * 2 for ch in raw)
    if len(raw) != 6:
        return fallback
    try:
        int(raw, 16)
    except Exception:
        return fallback
    return f"#{raw.lower()}"


def _shade(hex_color, factor):
    h = _normalize_hex_color(hex_color).lstrip("#")
    r, g, b = (int(h[i:i+2], 16)/255 for i in (0, 2, 4))
    hh, l, s = colorsys.rgb_to_hls(r, g, b)
    l2 = max(0.0, min(1.0, l * factor))
    r2, g2, b2 = colorsys.hls_to_rgb(hh, l2, s)
    return "#{:02x}{:02x}{:02x}".format(int(r2*255), int(g2*255), int(b2*255))


def _hex_to_rgba(hex_color, alpha=0.13):
    """Convert '#rrggbb' → 'rgba(r,g,b,alpha)' — compatible with all Plotly versions."""
    h = _normalize_hex_color(hex_color).lstrip("#")
    r, g, b = int(h[0:2],16), int(h[2:4],16), int(h[4:6],16)
    return f"rgba({r},{g},{b},{alpha})"


def vcolor(name, status):
    return _shade(VESSEL_COLORS.get(name, "#95a5a6"), STATUS_LIGHTNESS.get(status, 1.0))


# =============================================================================
# ── SIMULATION ENGINE LOADER ──────────────────────────────────────────────────
# =============================================================================

@st.cache_resource(show_spinner="Loading simulation engine…")
def _load_mod(_file_hash: str = ""):
    """
    _file_hash is derived from the sim file content so Streamlit Cloud
    automatically busts the cache whenever the file is updated on deploy.
    """
    sim_path = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                            "tanker_simulation_v5.py")
    if not os.path.exists(sim_path):
        st.error("❌ tanker_simulation_v5.py not found next to tanker_app.py")
        st.stop()
    source = open(sim_path).read()
    marker = "# -----------------------------------------------------------------\n# RUN SIMULATION"
    if marker in source:
        source = source.split(marker)[0]
    for m in ["matplotlib", "matplotlib.pyplot", "matplotlib.patches"]:
        if m not in sys.modules:
            sys.modules[m] = _mock.MagicMock()
    mod = types.ModuleType("tanker_sim_v5")
    mod.__file__ = sim_path
    exec(compile(source, sim_path, "exec"), mod.__dict__)
    return mod


def _load_mod_current():
    """Return the sim module keyed on current file hash (busts stale cache)."""
    sim_path = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                            "tanker_simulation_v5.py")
    try:
        file_hash = hashlib.md5(open(sim_path, "rb").read()).hexdigest()
    except Exception:
        file_hash = "refined-dispatch-v5-39"
    return _load_mod(file_hash)


@st.cache_data(ttl=0, show_spinner="Running simulation…")
def run_sim(sim_days, sanbarth, jasmines, westmore, duke, starturn, pgm,
            bryanston, alkebulan, greeneagle,
            bryanston_api: float = 0.0,
            alkebulan_api: float = 0.0,
            greeneagle_api: float = 0.0,
            prod_sanbarth=0, prod_jasmines=0, prod_westmore=0,
            prod_duke=0, prod_starturn=0, prod_pgm=0, prod_ibom=0,
            production_overrides_json: str = None,
            vessel_states_json=None,
            tide_csv_bytes: bytes = None,
            sim_start_date: str = None,
            _sim_version: str = "",
            opt_params_json: str = None,
            startup_day_disable_point_b_priority: bool = False,
            startup_day_manual_nominations_json: str = None,
            point_b_startup_seed_json: str = None,
            mother_export_seed_json: str = None,
            mother_export_force_json: str = None,
            export_unavailability_json: str = None,
            custom_vessels_json: str = None,
            vessel_resumption_json: str = None,
            mother_unavailability_json: str = None,
            storage_overrides_json: str = None,
            zeezee_schedule_json: str = None,
            daughter_discharge_overrides_json: str = None,
            multiple_transient_operation: bool = False,
            mto_max_parcels: int = 1,
            enable_variability: bool = False,
            variability_params_json: str = None):
    """
    Run simulation with independent production rates per storage and Ibom.
    vessel_states_json: JSON str of {vessel: {status, cargo_bbl}} or None.
    tide_csv_bytes: raw CSV bytes for tidal constraint (or None to disable).
    sim_start_date: ISO date string (YYYY-MM-DD) — day 0 of the simulation (defaults to today).
    opt_params_json: JSON str of optimizer params to apply (dead_stock_factor,
        ibom_trigger_bbl, export_sail_window_start, berthing_start, berthing_end).
        When provided, these override the sim module constants for this run only.
    production_overrides_json: JSON list of date-window production overrides.
        Each item: {start_date, end_date, rates:{storage_name:bph}}.
    startup_day_disable_point_b_priority: disable Point B auto-priority on Day 1 only.
    startup_day_manual_nominations_json: JSON str of vessel->mother nominations
        used on Day 1 when startup_day_disable_point_b_priority is enabled.
    point_b_startup_seed_json: JSON str of vessel->mother seed map to force
        selected vessels to start fully loaded at Point B (validation mode).
    mother_export_seed_json: JSON str of {mother: days} — mothers that start at
        export for the given number of days, blocking daughter berthing until return.
    mother_unavailability_json: JSON list of {mother, start_date, end_date} —
        scheduled periods when a mother is unavailable.  Daughters are
        rerouted to other mothers during each window.
    """
    mod = _load_mod_current()

    # ── Set simulation epoch ────────────────────────────────────────────
    if sim_start_date:
        _epoch = _dt.date.fromisoformat(sim_start_date)
    else:
        _epoch = _dt.date.today()
    if hasattr(mod, "set_sim_epoch"):
        mod.set_sim_epoch(_epoch)

    # Load tide table if provided
    if tide_csv_bytes is not None:
        import tempfile as _tmpf, os as _os
        with _tmpf.NamedTemporaryFile(delete=False, suffix=".csv") as _tf:
            _tf.write(tide_csv_bytes)
            _tp = _tf.name
        try:
            mod.load_tide_table(_tp)
        except Exception:
            pass
        finally:
            try:
                _os.unlink(_tp)
            except OSError:
                pass
    elif hasattr(mod, "_TIDE_TABLE"):
        mod._TIDE_TABLE = None
    _save_attrs = ["SIMULATION_DAYS","STORAGE_INIT_BBL","MOTHER_INIT_BBL",
                   "PRODUCTION_RATE_BPH","WESTMORE_PRODUCTION_RATE_BPH",
                   "DUKE_PRODUCTION_RATE_BPH","STARTURN_PRODUCTION_RATE_BPH"]
    _runtime_keys = [
        "STARTUP_DAY_DISABLE_POINT_B_PRIORITY",
        "STARTUP_DAY_POINT_B_MANUAL_NOMINATIONS",
        "POINT_B_DISTRIBUTION_TEST_MODE",
        "POINT_B_TEST_STARTUP_FULL_LOAD_NOMINATIONS",
        "PRODUCTION_RATE_OVERRIDES",
        "MULTIPLE_TRANSIENT_OPERATION",
        "MTO_MAX_PARCELS_BEFORE_OFFLOAD",
    ]
    orig = {k: getattr(mod, k) for k in _save_attrs if hasattr(mod, k)}
    for _rk in _runtime_keys:
        if hasattr(mod, _rk):
            orig[_rk] = getattr(mod, _rk)
    orig["_ibom_rate"] = getattr(mod, "IBOM_LOAD_RATE_BPH",
                         getattr(mod, "POINT_F_LOAD_RATE_BPH", 165))

    # ── Custom vessel injection ──────────────────────────────────────────────
    # Save the current _CUSTOM_VESSELS list so it can be restored after the
    # run (Streamlit reruns share module state).  Then repopulate it from the
    # JSON payload passed in by the UI (or leave it empty for a normal run).
    if hasattr(mod, "_CUSTOM_VESSELS"):
        orig["_CUSTOM_VESSELS"] = list(mod._CUSTOM_VESSELS)
        mod._CUSTOM_VESSELS.clear()
    if custom_vessels_json and hasattr(mod, "add_custom_vessel"):
        try:
            _cv_list = json.loads(custom_vessels_json)
        except Exception:
            _cv_list = []
        _valid_storages = {
            getattr(mod, "STORAGE_PRIMARY_NAME",   "SanBarth"),
            getattr(mod, "STORAGE_SECONDARY_NAME", "JasmineS"),
            getattr(mod, "STORAGE_TERTIARY_NAME",  "Westmore"),
            getattr(mod, "STORAGE_QUATERNARY_NAME","Duke"),
            getattr(mod, "STORAGE_QUINARY_NAME",   "Starturn"),
            getattr(mod, "STORAGE_SENARY_NAME",    "PGM"),
        }
        _existing_names = set(getattr(mod, "VESSEL_NAMES", []))
        _seen_names = set()
        for _cv in _cv_list:
            try:
                _cv_name  = str(_cv.get("name", "")).strip()
                _cv_date  = str(_cv.get("join_date", "")).strip()
                _cv_cap   = int(_cv.get("cargo_capacity", 0))
                _cv_perms = [
                    s for s in _cv.get("permitted_storages", [])
                    if s in _valid_storages
                ]
                if (not _cv_name
                        or _cv_name in _existing_names
                        or _cv_name in _seen_names
                        or _cv_cap <= 0
                        or not _cv_date):
                    continue
                mod.add_custom_vessel(
                    name=_cv_name,
                    join_date=_cv_date,
                    cargo_capacity=_cv_cap,
                    permitted_storages=_cv_perms,
                )
                _seen_names.add(_cv_name)
            except Exception as _cv_err:
                import logging as _logging
                _logging.getLogger(__name__).warning(
                    "Custom vessel config parse error: %s", _cv_err)

    # ── Vessel resumption dates ──────────────────────────────────────────────
    # Save, clear, repopulate, then restore — same pattern as _CUSTOM_VESSELS.
    if hasattr(mod, "_VESSEL_RESUMPTION_DATES"):
        orig["_VESSEL_RESUMPTION_DATES"] = dict(mod._VESSEL_RESUMPTION_DATES)
        mod._VESSEL_RESUMPTION_DATES.clear()
    _resumption_seed_by_vessel = {}
    if vessel_resumption_json and hasattr(mod, "set_vessel_resumption"):
        try:
            _vr_list = json.loads(vessel_resumption_json)
        except Exception:
            _vr_list = []
        _valid_storages_vr = {
            getattr(mod, "STORAGE_PRIMARY_NAME",    "SanBarth"),
            getattr(mod, "STORAGE_SECONDARY_NAME",  "JasmineS"),
            getattr(mod, "STORAGE_TERTIARY_NAME",   "Westmore"),
            getattr(mod, "STORAGE_QUATERNARY_NAME", "Duke"),
            getattr(mod, "STORAGE_QUINARY_NAME",    "Starturn"),
            getattr(mod, "STORAGE_SENARY_NAME",     "PGM"),
        }
        _known_vessels_vr = set(getattr(mod, "VESSEL_NAMES", []))
        _INDEFINITE_HOUR = 9_999_999.0   # beyond any sim horizon
        import re as _re_vr
        import datetime as _dt_chk

        def _sanitise_date_str(s):
            """Extract a clean ISO date string from a stored value.

            Guards against Python repr strings like '(datetime.date(2026, 5, 8),)'
            that result from a partial st.date_input range selection being str()-cast.
            """
            if not s or s in ("indefinite", ""):
                return s
            # Already a clean ISO date string?
            try:
                _dt_chk.date.fromisoformat(s)
                return s
            except ValueError:
                pass
            # Extract YYYY-MM-DD from repr like "(datetime.date(2026, 5, 8),)"
            _m = _re_vr.search(r"datetime\.date\((\d{4}),\s*(\d{1,2}),\s*(\d{1,2})\)", s)
            if _m:
                return f"{int(_m.group(1)):04d}-{int(_m.group(2)):02d}-{int(_m.group(3)):02d}"
            return s  # return as-is; fromisoformat will raise and be caught below

        for _vr in _vr_list:
            try:
                _vr_name      = str(_vr.get("name", "")).strip()
                _vr_date      = _sanitise_date_str(str(_vr.get("date", "")).strip())
                _vr_start     = _sanitise_date_str(str(_vr.get("start_date", "")).strip())
                _vr_storage   = str(_vr.get("storage", "")).strip()
                _vr_indef     = bool(_vr.get("indefinite", False))
                if _vr_name not in _known_vessels_vr or _vr_storage not in _valid_storages_vr:
                    continue
                if _vr_indef or _vr_date == "indefinite":
                    # Vessel held idle for the entire run — never wakes.
                    _resumption_seed_by_vessel[_vr_name] = {
                        "date":              "indefinite",
                        "storage":           _vr_storage,
                        "indefinite":        True,
                        "hour":              _INDEFINITE_HOUR,
                        "dormancy_start_hour": 0.0,   # dormant from t=0
                    }
                else:
                    if not _vr_date:
                        continue
                    # Compute the sim-hour at which dormancy begins (08:00 on start_date).
                    # If start_date is missing or equals the sim epoch, dormancy starts
                    # from t=0 (Day 1 08:00) — preserving backward compatibility.
                    _dormancy_start_h = 0.0
                    if _vr_start and _vr_start not in ("", "indefinite"):
                        try:
                            import datetime as _dti2
                            # Use _dorm_epoch (not _epoch) to avoid overwriting the
                            # outer _epoch variable used by set_sim_epoch below.
                            _dorm_epoch = getattr(mod, "_SIM_EPOCH", None)
                            if _dorm_epoch is None:
                                _dorm_epoch = _dt.datetime(_epoch.year, _epoch.month,
                                                           _epoch.day, 8, 0)
                            _sd_obj = _dti2.date.fromisoformat(_vr_start)
                            if _dorm_epoch:
                                _dormancy_start_h = (
                                    _dti2.datetime(_sd_obj.year, _sd_obj.month, _sd_obj.day, 8, 0)
                                    - _dorm_epoch
                                ).total_seconds() / 3600.0
                        except Exception:
                            _dormancy_start_h = 0.0

                    if _dormancy_start_h <= 0.0:
                        # Day-1 dormancy: pre-seed the sim's resumption machinery
                        # so the vessel is held from t=0 until the resumption date.
                        mod.set_vessel_resumption(_vr_name, _vr_date, _vr_storage)
                    # For mid-sim dormancy (_dormancy_start_h > 0), we do NOT call
                    # set_vessel_resumption — that would pre-seed the vessel as
                    # dormant from Day 1.  Instead the dormancy is applied via
                    # dormancy_start_hour / _dormancy_end_hour at runtime.
                    _resumption_seed_by_vessel[_vr_name] = {
                        "date":              _vr_date,
                        "storage":           _vr_storage,
                        "indefinite":        False,
                        "dormancy_start_hour": max(0.0, _dormancy_start_h),
                    }
            except Exception as _vr_err:
                import logging as _logging
                _logging.getLogger(__name__).warning(
                    "Vessel resumption parse error: %s", _vr_err)

    mod.SIMULATION_DAYS                  = sim_days
    mod.STORAGE_INIT_BBL                 = sanbarth
    mod.MOTHER_INIT_BBL                  = 0
    mod.PRODUCTION_RATE_BPH              = prod_sanbarth
    mod.WESTMORE_PRODUCTION_RATE_BPH     = prod_westmore
    mod.DUKE_PRODUCTION_RATE_BPH         = prod_duke
    mod.STARTURN_PRODUCTION_RATE_BPH     = prod_starturn
    if hasattr(mod, "PGM_PRODUCTION_RATE_BPH"): mod.PGM_PRODUCTION_RATE_BPH = prod_pgm
    if hasattr(mod, "IBOM_LOAD_RATE_BPH"):    mod.IBOM_LOAD_RATE_BPH    = prod_ibom
    if hasattr(mod, "POINT_F_LOAD_RATE_BPH"): mod.POINT_F_LOAD_RATE_BPH = prod_ibom

    # ── Stochastic variability settings ──────────────────────────────────────
    # Applied before Simulation() is instantiated so the RNG seed and CV
    # constants are in effect for the entire run.
    if hasattr(mod, "ENABLE_VARIABILITY"):
        mod.ENABLE_VARIABILITY = bool(enable_variability)
    if enable_variability and variability_params_json:
        try:
            _vp = json.loads(variability_params_json)
            _cv_map = {
                "cv_loading":      "VARIABILITY_CV_LOADING",
                "cv_discharge":    "VARIABILITY_CV_DISCHARGE",
                "cv_transit":      "VARIABILITY_CV_TRANSIT",
                "cv_berthing":     "VARIABILITY_CV_BERTHING",
                "cv_hose_connect": "VARIABILITY_CV_HOSE_CONNECT",
                "weather_prob":    "WEATHER_PROB_PER_HOUR",
                "weather_hold_h":  "WEATHER_HOLD_MEAN_H",
                "equip_delay_prob":"EQUIP_DELAY_PROB_PER_LOAD",
                "random_seed":     "VARIABILITY_RANDOM_SEED",
            }
            for key, mod_attr in _cv_map.items():
                if key in _vp and hasattr(mod, mod_attr):
                    setattr(mod, mod_attr, _vp[key])
        except Exception:
            pass

    # Optional custom production windows (date range specific rates)
    _prod_overrides = []
    if production_overrides_json:
        try:
            _raw_overrides = json.loads(production_overrides_json)
        except Exception:
            _raw_overrides = []
        if isinstance(_raw_overrides, list):
            _prod_overrides = _raw_overrides
    if hasattr(mod, "PRODUCTION_RATE_OVERRIDES"):
        mod.PRODUCTION_RATE_OVERRIDES = _prod_overrides

    # ── Runtime control: Day-1 Point B manual nomination exception ────────────
    _mother_names = set(getattr(mod, "MOTHER_NAMES", []))
    _vessel_names = set(getattr(mod, "VESSEL_NAMES", []))
    _manual_nom = {}
    if startup_day_manual_nominations_json:
        try:
            _manual_nom_raw = json.loads(startup_day_manual_nominations_json)
        except Exception:
            _manual_nom_raw = {}
        if isinstance(_manual_nom_raw, dict):
            for _vn, _mn in _manual_nom_raw.items():
                if _vn in _vessel_names and _mn in _mother_names:
                    _manual_nom[_vn] = _mn
    if hasattr(mod, "STARTUP_DAY_DISABLE_POINT_B_PRIORITY"):
        mod.STARTUP_DAY_DISABLE_POINT_B_PRIORITY = False  # startup Day-1 auto-scan is mandatory
    if hasattr(mod, "STARTUP_DAY_POINT_B_MANUAL_NOMINATIONS"):
        mod.STARTUP_DAY_POINT_B_MANUAL_NOMINATIONS = dict(_manual_nom)
    if hasattr(mod, "MULTIPLE_TRANSIENT_OPERATION"):
        mod.MULTIPLE_TRANSIENT_OPERATION = bool(multiple_transient_operation)
    if hasattr(mod, "MTO_MAX_PARCELS_BEFORE_OFFLOAD"):
        mod.MTO_MAX_PARCELS_BEFORE_OFFLOAD = max(1, int(mto_max_parcels))

    # ── Optional startup seed for targeted Point B validation ─────────────────
    _seed_nom = {}
    if point_b_startup_seed_json:
        try:
            _seed_nom_raw = json.loads(point_b_startup_seed_json)
        except Exception:
            _seed_nom_raw = {}
        if isinstance(_seed_nom_raw, dict):
            for _vn, _mn in _seed_nom_raw.items():
                if _vn in _vessel_names and _mn in _mother_names:
                    _seed_nom[_vn] = _mn
    if hasattr(mod, "POINT_B_TEST_STARTUP_FULL_LOAD_NOMINATIONS"):
        mod.POINT_B_TEST_STARTUP_FULL_LOAD_NOMINATIONS = dict(_seed_nom)
    if hasattr(mod, "POINT_B_DISTRIBUTION_TEST_MODE"):
        mod.POINT_B_DISTRIBUTION_TEST_MODE = bool(_seed_nom)

    # ── Optional mother export seed — block mothers away at export on Day 1 ──────
    # mother_export_seed_json: {mother_name: days_at_export}
    # Reserves each named mother's berth for (days × 24) hours at t=0 so the
    # sim treats that mother as unavailable until she returns from export.
    _mother_export = {}
    if mother_export_seed_json:
        try:
            _raw_exp = json.loads(mother_export_seed_json)
        except Exception:
            _raw_exp = {}
        if isinstance(_raw_exp, dict):
            for _mn, _days in _raw_exp.items():
                if _mn in _mother_names and isinstance(_days, (int, float)) and _days > 0:
                    _mother_export[_mn] = float(_days)

    # ── Apply optimizer scenario params (if a specific scenario was selected) ──
    # Save originals so we can restore after the run (run_sim mutates the module).
    _opt_orig = {}
    _OPT_KEYS = [
        ("DEAD_STOCK_FACTOR",           "dead_stock_factor"),
        ("POINT_F_MIN_TRIGGER_BBL",     "ibom_trigger_bbl"),
        ("EXPORT_SAIL_WINDOW_START",    "export_sail_window_start"),
        ("BERTHING_START",              "berthing_start"),
        ("BERTHING_END",                "berthing_end"),
    ]
    if opt_params_json:
        _opt_pr = json.loads(opt_params_json)
        for _mod_key, _pr_key in _OPT_KEYS:
            if hasattr(mod, _mod_key) and _pr_key in _opt_pr:
                _opt_orig[_mod_key] = getattr(mod, _mod_key)
                setattr(mod, _mod_key, _opt_pr[_pr_key])

    # ── Apply JMP loading-point overrides ───────────────────────────────────
    # Save and populate STORAGE_DISPATCH_OVERRIDES on the module so the sim
    # dispatch logic can read it without any architectural change to Simulation().
    _sdo_orig = {}
    if hasattr(mod, "STORAGE_DISPATCH_OVERRIDES"):
        _sdo_orig = dict(mod.STORAGE_DISPATCH_OVERRIDES)
        mod.STORAGE_DISPATCH_OVERRIDES.clear()
    if storage_overrides_json and hasattr(mod, "STORAGE_DISPATCH_OVERRIDES"):
        try:
            _raw_sdo = json.loads(storage_overrides_json)
            # Normalise: keys are vessel names, values are {str(day): payload}
            # payload may be a plain storage string OR a dict:
            #   {"storage": "X", "load_after_hour": h}
            for _vn, _day_map in _raw_sdo.items():
                if isinstance(_day_map, dict):
                    _norm = {}
                    for _d, _s in _day_map.items():
                        if isinstance(_s, dict):
                            # Date-shift override: preserve as dict with int day key
                            _norm[int(_d)] = _s
                        else:
                            _norm[int(_d)] = str(_s)
                    mod.STORAGE_DISPATCH_OVERRIDES[_vn] = _norm
        except Exception as _sdo_err:
            import logging as _logging
            _logging.getLogger(__name__).warning(
                "storage_overrides_json parse error: %s", _sdo_err)

    # ── Apply ZeeZee discharge schedule ─────────────────────────────────────
    _zz_schedule_orig = list(getattr(mod, "ZEEZEE_SCHEDULE", []))
    if hasattr(mod, "ZEEZEE_SCHEDULE"):
        mod.ZEEZEE_SCHEDULE.clear()
    if zeezee_schedule_json and hasattr(mod, "ZEEZEE_SCHEDULE"):
        try:
            _raw_zz = json.loads(zeezee_schedule_json)
            if isinstance(_raw_zz, list):
                mod.ZEEZEE_SCHEDULE.extend(_raw_zz)
        except Exception as _zz_err:
            import logging as _logging
            _logging.getLogger(__name__).warning(
                "zeezee_schedule_json parse error: %s", _zz_err)

    # ── Apply daughter vessel discharge point overrides ───────────────────
    # Supports two formats in the JSON:
    #   Voyage-code keyed (preferred):
    #     {"SHK-001": {"vessel": "Sherlock", "mother": "Bryanston",
    #                  "discharge_date": "YYYY-MM-DD"}, ...}
    #   Legacy vessel/day keyed:
    #     {"Sherlock": {0: "Bryanston"}, ...}
    # ZeeZee is unaffected — her schedule is controlled by ZEEZEE_SCHEDULE.
    _ddo_orig = {}
    if hasattr(mod, "DAUGHTER_DISCHARGE_OVERRIDES"):
        _ddo_orig = dict(mod.DAUGHTER_DISCHARGE_OVERRIDES)
        mod.DAUGHTER_DISCHARGE_OVERRIDES.clear()
    if daughter_discharge_overrides_json and hasattr(mod, "DAUGHTER_DISCHARGE_OVERRIDES"):
        try:
            _raw_ddo = json.loads(daughter_discharge_overrides_json)
            if isinstance(_raw_ddo, dict):
                for _key, _val in _raw_ddo.items():
                    if isinstance(_val, dict):
                        # Voyage-code keyed: key looks like "SHK-001" (has a dash)
                        # or vessel/day-map keyed: value contains int-keyed submap
                        _has_int_keys = any(
                            isinstance(_k, (int, str)) and str(_k).lstrip("-").isdigit()
                            for _k in _val.keys()
                        )
                        if _has_int_keys:
                            # Legacy vessel/day map — parse day keys as ints
                            _parsed_map = {}
                            for _d, _entry in _val.items():
                                if isinstance(_entry, dict):
                                    _parsed_map[int(_d)] = _entry
                                else:
                                    _parsed_map[int(_d)] = str(_entry)
                            mod.DAUGHTER_DISCHARGE_OVERRIDES[_key] = _parsed_map
                        else:
                            # Voyage-code keyed entry — store as-is
                            mod.DAUGHTER_DISCHARGE_OVERRIDES[_key] = _val
        except Exception as _ddo_err:
            import logging as _logging
            _logging.getLogger(__name__).warning(
                "daughter_discharge_overrides_json parse error: %s", _ddo_err)

    # Pass epoch directly into Simulation (requires updated sim file).
    # Falls back gracefully if an older sim file is deployed.
    try:
        sim = mod.Simulation(epoch=_epoch)
    except TypeError:
        # Old sim file: set module global then instantiate
        if hasattr(mod, "set_sim_epoch") and _epoch is not None:
            mod.set_sim_epoch(_epoch)
        sim = mod.Simulation()

    # Seed storage volumes — clamp to capacity and credit any excess as
    # pre-existing overflow so it shows in spill metrics from t=0.
    for _sn, _vol in [("SanBarth", sanbarth), ("JasmineS", jasmines),
                       ("Westmore", westmore), ("Duke", duke), ("Starturn", starturn),
                       ("PGM", pgm)]:
        _cap  = mod.STORAGE_CAPACITY_BY_NAME[_sn]
        _over = max(0, _vol - _cap)
        sim.storage_bbl[_sn]         = min(_vol, _cap)
        if _over > 0:
            sim.storage_overflow_bbl[_sn] = sim.storage_overflow_bbl.get(_sn, 0.0) + _over
            sim.storage_overflow_events  += 1
            sim.total_spilled            += _over
    mod.STORAGE_PRODUCTION_RATE_BY_NAME["SanBarth"]   = prod_sanbarth
    mod.STORAGE_PRODUCTION_RATE_BY_NAME["JasmineS"] = prod_jasmines
    mod.STORAGE_PRODUCTION_RATE_BY_NAME["Westmore"] = prod_westmore
    mod.STORAGE_PRODUCTION_RATE_BY_NAME["Duke"]     = prod_duke
    mod.STORAGE_PRODUCTION_RATE_BY_NAME["Starturn"] = prod_starturn
    mod.STORAGE_PRODUCTION_RATE_BY_NAME["PGM"]      = prod_pgm
    _mother_caps = getattr(mod, "MOTHER_CAPACITY_BY_NAME", {})
    _default_cap = getattr(mod, "MOTHER_CAPACITY_BBL", 550_000)
    sim.mother_bbl["Bryanston"]  = min(bryanston,  int(_mother_caps.get("Bryanston", _default_cap)))
    sim.mother_bbl["GreenEagle"] = min(greeneagle, int(_mother_caps.get("GreenEagle", _default_cap)))
    # Alkebulan — primary mother at Point B, identical spec to GreenEagle.
    # Seeded only when the deployed sim file exposes it in MOTHER_CAPACITY_BY_NAME
    # (guards against running against an older sim that lacks the vessel).
    if "Alkebulan" in sim.mother_bbl:
        sim.mother_bbl["Alkebulan"] = min(alkebulan, int(_mother_caps.get("Alkebulan", _default_cap)))
    if bryanston  > 0 and bryanston_api  > 0:
        sim.mother_api["Bryanston"]  = float(bryanston_api)
    if greeneagle > 0 and greeneagle_api > 0:
        sim.mother_api["GreenEagle"] = float(greeneagle_api)
    if "Alkebulan" in sim.mother_api and alkebulan > 0 and alkebulan_api > 0:
        sim.mother_api["Alkebulan"] = float(alkebulan_api)

    if vessel_states_json:
        vs = json.loads(vessel_states_json)
        _sp_map = getattr(mod, "STORAGE_POINT", {})
        for v in sim.vessels:
            if v.name in _resumption_seed_by_vessel:
                _seed_entry      = _resumption_seed_by_vessel[v.name]
                _seed_storage    = _seed_entry["storage"]
                _seed_indef      = _seed_entry.get("indefinite", False)
                _dorm_start_h    = float(_seed_entry.get("dormancy_start_hour", 0.0))

                if _seed_indef:
                    # Vessel held idle for the entire run — force IDLE_A immediately
                    v.status = "IDLE_A"
                    v.cargo_bbl = 0
                    v.assigned_storage = None
                    v.assigned_load_hours = None
                    v.assigned_mother = None
                    v.target_point = _sp_map.get(_seed_storage, "A")
                    v.resumption_hour     = _INDEFINITE_HOUR
                    v.resumption_storage  = _seed_storage
                    v.resumption_priority = False
                    v.next_event_time = max(v.next_event_time, v.resumption_hour or 0.0)
                    v.current_voyage = 0
                    v._voyage_assigned = False

                elif _dorm_start_h <= 0.0:
                    # Dormancy starts from Day 1 — original behaviour
                    v.status = "IDLE_A"
                    v.cargo_bbl = 0
                    v.assigned_storage = None
                    v.assigned_load_hours = None
                    v.assigned_mother = None
                    v.target_point = _sp_map.get(_seed_storage, "A")
                    v.next_event_time = max(v.next_event_time, v.resumption_hour or 0.0)
                    v.current_voyage = 0
                    v._voyage_assigned = False

                else:
                    # Mid-sim unavailability — vessel runs normally until
                    # dormancy_start_hour, then holds idle until resumption_hour.
                    # Store both the unavailability window and the resumption hour
                    # on the vessel so the run loop can enforce it when t crosses
                    # dormancy_start_hour.
                    v.dormancy_start_hour    = _dorm_start_h
                    v.resumption_storage     = _seed_storage
                    v.resumption_priority    = False
                    v.resumption_hold_logged = False
                    # Flag: if the vessel is loaded (has cargo) when the
                    # unavailability start hour arrives, it must complete its
                    # current BIA discharge before going inactive.  The run-loop
                    # honours this by not activating dormancy while the vessel is
                    # mid-discharge; it only activates after CAST_OFF_B / IDLE_A
                    # at BIA confirms the cargo has been fully cleared.
                    v._dormancy_discharge_first = True

                    # Compute and store the resumption sim-hour from _seed_entry.
                    _res_date_str = _seed_entry.get("date", "")
                    try:
                        _r_epoch = getattr(mod, "_SIM_EPOCH", None)
                        _r_date  = _dt.date.fromisoformat(_res_date_str)
                        if _r_epoch:
                            _res_h = (_dt.datetime(_r_date.year, _r_date.month, _r_date.day, 8, 0)
                                      - _r_epoch).total_seconds() / 3600.0
                            v._dormancy_end_hour = max(0.0, _res_h)
                        else:
                            v._dormancy_end_hour = None
                    except Exception:
                        v._dormancy_end_hour = None
                    # DO NOT continue — fall through to apply vessel_states_json
                    # startup state so the vessel begins the sim in the correct
                    # position (e.g. SAILING_BA returning from BIA) and only goes
                    # inactive when t reaches dormancy_start_hour (or after
                    # cargo discharge completes if vessel is loaded at that time).
            if v.name in vs:
                d = vs[v.name]
                if d.get("status"):
                    v.status = d["status"]
                if d.get("cargo_bbl") is not None:
                    _raw_cargo = int(d["cargo_bbl"])
                    _vcap      = v.cargo_capacity
                    _over      = max(0, _raw_cargo - _vcap)
                    _is_mto_recv_vessel = d.get("is_mto_receiver", False)
                    _mto_cap = getattr(mod, "MTO_TRANSIENT_CAPACITY_BBL", {}).get(
                        v.name, v.cargo_capacity)

                    # ── MTO receiver: honour pre-computed combined cargo ───────
                    # When Case A (active discharger seeding) ran BEFORE this vessel
                    # in the loop, it already computed:
                    #   v.cargo_bbl = receiver_startup_cargo + discharger_cargo
                    # and set v._mto_seeded_cargo to that combined value.
                    # Do NOT overwrite it with the raw startup cargo — that would
                    # erase the discharger's contribution and give the receiver the
                    # wrong total (e.g. 78k instead of 116k).
                    if hasattr(v, "_mto_seeded_cargo"):
                        v.cargo_bbl = v._mto_seeded_cargo
                        if hasattr(v, "_mto_seeded_api") and v._mto_seeded_api > 0:
                            sim.vessel_api[v.name] = v._mto_seeded_api
                        # Attributes consumed — remove so they don't persist
                        del v._mto_seeded_cargo
                        if hasattr(v, "_mto_seeded_api"):
                            del v._mto_seeded_api
                    elif _is_mto_recv_vessel and _raw_cargo <= _mto_cap:
                        # Within MTO capacity — accept full startup cargo, no spill
                        v.cargo_bbl = _raw_cargo
                    elif _is_mto_recv_vessel and _raw_cargo > _mto_cap:
                        # Over MTO cap — clamp to MTO cap (still no spill accounting)
                        v.cargo_bbl = _mto_cap
                    else:
                        # Normal vessel: honour MTO-accumulated cargo that exceeds
                        # the vessel's standard cargo_capacity.
                        # The ONLY legitimate reason a vessel carries more than its
                        # nominal capacity is that it acted as an MTO transient
                        # receiver and accumulated cargo from multiple shuttles.
                        # Use MTO_TRANSIENT_CAPACITY_BBL as the true upper ceiling.
                        # If the seeded cargo is within that range, accept it in full
                        # and set the MTO transient flag so the sim discharges it to
                        # a mother vessel correctly.
                        if _raw_cargo > _vcap:
                            # Cargo exceeds standard capacity — check MTO ceiling
                            _mto_trans_cap = (
                                getattr(mod, "MTO_TRANSIENT_CAPACITY_BBL", {})
                                .get(v.name, _vcap)
                            )
                            if _raw_cargo <= _mto_trans_cap:
                                # Within MTO ceiling — accept as MTO transient cargo
                                v.cargo_bbl = _raw_cargo
                                # Mark as MTO transient so the sim routes discharge
                                # to a mother vessel
                                v._mto_transient_since_day = 0
                                v._mto_parcels_received    = getattr(
                                    v, "_mto_parcels_received", 1)
                                v._is_mto_offload          = True
                                if v.status not in {
                                    "WAITING_BERTH_B", "BERTHING_B",
                                    "HOSE_CONNECT_B", "DISCHARGING",
                                    "WAITING_MOTHER_CAPACITY",
                                }:
                                    v.status = "WAITING_BERTH_B"
                            else:
                                # Even over MTO ceiling — clamp to MTO ceiling
                                v.cargo_bbl = _mto_trans_cap
                                v._mto_transient_since_day = 0
                                v._mto_parcels_received    = getattr(
                                    v, "_mto_parcels_received", 1)
                                v._is_mto_offload          = True
                                if v.status not in {
                                    "WAITING_BERTH_B", "BERTHING_B",
                                    "HOSE_CONNECT_B", "DISCHARGING",
                                    "WAITING_MOTHER_CAPACITY",
                                }:
                                    v.status = "WAITING_BERTH_B"
                        else:
                            # Cargo within standard capacity — normal clamp
                            v.cargo_bbl = _raw_cargo   # _raw_cargo <= _vcap here
                        # Spill accounting only for amounts above MTO ceiling
                        _mto_hard_cap = (
                            getattr(mod, "MTO_TRANSIENT_CAPACITY_BBL", {})
                            .get(v.name, _vcap)
                        )
                        _hard_over = max(0, _raw_cargo - _mto_hard_cap)
                        if _hard_over > 0:
                            _vspill_key = f"vessel_{v.name}"
                            sim.storage_overflow_bbl[_vspill_key] = (
                                sim.storage_overflow_bbl.get(_vspill_key, 0.0) + _hard_over
                            )
                            sim.storage_overflow_events += 1
                            sim.total_spilled           += _hard_over
                # Seed current storage for vessels already at a storage location
                _loc = d.get("location")
                if _loc and _loc in _sp_map:
                    v.target_point     = _sp_map[_loc]
                    v.assigned_storage = _loc
                elif _loc:
                    # Non-storage locations (BIA/Fairway/transit) must not inherit
                    # prior startup storage assignment.
                    if _loc in {"B", "Fairway", "Fairway Buoy"}:
                        v.target_point = "B"
                # Override target_point for transit vessels heading to a specific storage
                _ts = d.get("target_storage")
                if _ts and _ts in _sp_map:
                    v.target_point = _sp_map[_ts]
                    # assigned_storage stays None — vessel not yet arrived
                # Ensure target_point always matches assigned_storage for IDLE_A vessels
                # (guards against default "A" overriding Duke/Starturn placements)
                if v.status == "IDLE_A" and v.assigned_storage and v.assigned_storage in _sp_map:
                    v.target_point = _sp_map[v.assigned_storage]
                _tm = d.get("target_mother")
                if _tm and _tm in getattr(mod, "MOTHER_NAMES", []):
                    v.assigned_mother = _tm

                # ── MTO startup seeding ─────────────────────────────────────
                # Case A — DISCHARGING/HOSE_CONNECT_B/BERTHING_B:
                #   Active discharger at t=0. Directly credit transfer, set timing,
                #   log MTO_TRANSIENT_NOMINATED on receiver + MTO_DISCHARGE_TO_TRANSIENT
                #   on discharger so Day 1 of the journey plan shows correctly.
                # Case B — SAILING_AB_LEG2 with mto_target_vessel:
                #   Queued next discharger (inbound). Flag _mto_target_vessel so the
                #   arrival handler routes to the transient instead of a mother.
                # Case C — is_mto_receiver:
                #   Receiver vessel. Flag _mto_transient_since_day and set WAITING_BERTH_B.
                _mto_tv      = d.get("mto_target_vessel") or ""
                _is_mto_recv = d.get("is_mto_receiver", False)

                if _mto_tv:
                    v.assigned_mother = None  # not a real mother discharge
                    _mto_recv_v = next(
                        (vv for vv in sim.vessels if vv.name == _mto_tv), None
                    )

                    if v.status == "SAILING_AB_LEG2":
                        # Case B — queued inbound discharger
                        v._mto_target_vessel = _mto_tv
                        # Ensure receiver is flagged even if its dict entry hasn't run yet
                        if _mto_recv_v is not None:
                            _mto_recv_v._mto_transient_since_day = 0
                            _mto_recv_v.status = "WAITING_BERTH_B"
                        sim.log_event(
                            0, v.name, "MTO_DISCHARGE_TO_TRANSIENT",
                            f"[MTO Startup seed] {v.name} queued → {_mto_tv}: "
                            f"{v.cargo_bbl:,.0f} bbl inbound — will discharge on arrival",
                        )

                    elif _mto_recv_v is not None:
                        # Case A — active discharger at t=0
                        # (e.g. Rathbone HOSE_CONNECT_B → Bagshot, or Balham DISCHARGING → Watson)
                        _transfer_bbl = v.cargo_bbl   # full remaining volume to pump
                        _xfr_done     = int(d.get("already_transferred_bbl", 0))
                        _dis_api_val  = sim.vessel_api.get(v.name, 0.0)
                        _mto_rate     = getattr(mod, "VESSEL_DISCHARGE_RATE_BPH",
                                                {}).get(v.name, None)
                        _mto_disch_h  = getattr(mod, "DISCHARGE_HOURS", 12.0)
                        _mto_pump_h   = ((_transfer_bbl / _mto_rate) if _mto_rate
                                         else _mto_disch_h)
                        _mto_hose_h   = getattr(mod, "HOSE_CONNECTION_HOURS", 2.0)
                        _mto_bert_h   = getattr(mod, "BERTHING_DELAY_HOURS",  0.5)
                        _mto_coff_h   = getattr(mod, "CAST_OFF_HOURS",        0.2)

                        v._mto_target_vessel = _mto_tv

                        # Flag receiver as active MTO transient
                        _mto_recv_v._mto_transient_since_day = 0
                        _mto_recv_v._mto_parcels_received    = 0

                        # ── Correct receiver startup cargo ────────────────────
                        # The receiver vessel may NOT have been cargo-seeded yet
                        # (vessels are processed in VESSEL_NAMES order; e.g. Rathbone
                        # index 2 runs before Bagshot index 7). Read the receiver's
                        # startup cargo directly from the vessel_states dict `vs`
                        # so we sum the correct total: receiver_startup + discharger.
                        _recv_vs_entry  = vs.get(_mto_recv_v.name, {})
                        _recv_startup_cargo = int(_recv_vs_entry.get("cargo_bbl") or 0)
                        _mto_cap_seed   = getattr(mod, "MTO_TRANSIENT_CAPACITY_BBL", {}).get(
                            _mto_recv_v.name, _mto_recv_v.cargo_capacity)
                        _recv_api_seed  = float(_recv_vs_entry.get("cargo_api", 0.0) or 0.0)

                        # Headroom based on true startup cargo (not the yet-to-be-seeded
                        # vessel.cargo_bbl which is still at its default 0)
                        _headroom_seed  = max(0.0, _mto_cap_seed - _recv_startup_cargo)
                        _xfer_seed      = min(_transfer_bbl, _headroom_seed)

                        # Combine: receiver keeps its startup cargo + incoming transfer
                        _new_recv_cargo = _recv_startup_cargo + _xfer_seed
                        _recv_api_weighted = (
                            (_recv_startup_cargo * _recv_api_seed
                             + _xfer_seed * _dis_api_val) / _new_recv_cargo
                            if _new_recv_cargo > 0 else 0.0
                        )

                        # Write to vessel object now so the log is accurate
                        _mto_recv_v.cargo_bbl = _new_recv_cargo
                        sim.vessel_api[_mto_recv_v.name] = _recv_api_weighted
                        _mto_recv_v._mto_parcels_received = 1 if _xfer_seed > 0 else 0

                        # Mark the receiver with the final combined cargo so that
                        # the cargo-seeding pass (which runs when Bagshot's own dict
                        # entry is processed) knows NOT to overwrite with raw startup cargo.
                        # We store the intended final value on the vessel; the cargo-seeding
                        # block will detect this flag and skip the overwrite.
                        _mto_recv_v._mto_seeded_cargo = _new_recv_cargo
                        _mto_recv_v._mto_seeded_api   = _recv_api_weighted

                        # Compute timing based on current phase
                        if v.status == "DISCHARGING":
                            _time_remaining = _mto_pump_h   # still pumping
                        elif v.status == "HOSE_CONNECT_B":
                            _time_remaining = _mto_hose_h + _mto_pump_h
                        else:  # BERTHING_B / WAITING_BERTH_B
                            _time_remaining = _mto_bert_h + _mto_hose_h + _mto_pump_h

                        # Pump completes at _time_remaining; cast-off after daylight window
                        _pump_done_t = _time_remaining
                        # Compute cast-off sim-hour (respecting daylight restriction)
                        import math as _math
                        _co_start_wall = getattr(mod, "CAST_OFF_START", 6)
                        _co_end_wall   = getattr(mod, "CAST_OFF_END",  17.5)
                        _sim_offset    = getattr(mod, "SIM_HOUR_OFFSET", 8)
                        _wall_at_pump  = (_pump_done_t + _sim_offset) % 24
                        if _co_start_wall <= _wall_at_pump < _co_end_wall:
                            _cast_t = _pump_done_t
                        else:
                            _days_e  = int(_pump_done_t // 24)
                            _co_sim  = _days_e * 24 + (_co_start_wall - _sim_offset)
                            _cast_t  = _co_sim if _pump_done_t <= _co_sim else _co_sim + 24
                        _mto_lock_until = _cast_t + _mto_coff_h

                        # Advance discharger to post-transfer state
                        v.cargo_bbl            = 0
                        sim.vessel_api[v.name] = 0.0
                        v.status               = "CAST_OFF_B"
                        v.next_event_time      = _mto_lock_until
                        v._mto_target_vessel   = None   # transfer complete

                        # Lock receiver berth and set it waiting to berth a mother
                        _mto_recv_v._mto_berth_free_at = _mto_lock_until
                        _mto_recv_v.status             = "WAITING_BERTH_B"
                        _mto_recv_v.next_event_time    = 0.0
                        # Mark receiver ready to offload (MTO complete — no more parcels)
                        _mto_recv_v._mto_transient_since_day = None
                        # Critical: flag the receiver as an MTO offload vessel so the
                        # sim's mother selection treats it as consolidated MTO cargo.
                        _mto_recv_v._is_mto_offload = True

                        # Logging
                        _trn_cap_log = getattr(mod, "MTO_TRANSIENT_CAPACITY_BBL",
                                               {}).get(_mto_recv_v.name,
                                                       _mto_recv_v.cargo_capacity)
                        sim.log_event(
                            0, _mto_recv_v.name, "MTO_TRANSIENT_NOMINATED",
                            f"[MTO Day 1 — transfer at startup] "
                            f"Received {_xfer_seed:,.0f} bbl from {v.name} "
                            f"({_xfr_done:,.0f} bbl already pumped before sim start) "
                            f"@ {_dis_api_val:.2f}° API | on-board: {_mto_recv_v.cargo_bbl:,.0f} bbl "
                            f"(cap {_trn_cap_log:,.0f} bbl) | "
                            f"berth free after {_mto_lock_until:.1f}h",
                            voyage_num=_mto_recv_v.current_voyage,
                        )
                        sim.log_event(
                            0, v.name, "MTO_DISCHARGE_TO_TRANSIENT",
                            f"[MTO Day 1] Startup seed: {_xfer_seed:,.0f} bbl → {_mto_recv_v.name} "
                            f"(cast-off at +{_mto_lock_until:.1f}h) | "
                            f"receiver now has {_mto_recv_v.cargo_bbl:,.0f} bbl",
                            voyage_num=v.current_voyage,
                        )

                if _is_mto_recv:
                    # Case C — receiver vessel.
                    # Only set transient flags if Case A (active discharger) has NOT
                    # already processed this vessel. Case A runs when the discharger
                    # vessel (e.g. Rathbone) is iterated — it sets:
                    #   _mto_transient_since_day = None  (transfer complete, berth mother)
                    #   cargo_bbl += discharger volume   (e.g. 75k + 38k = 113k)
                    #   _mto_berth_free_at = cast-off time
                    # If we blindly re-set _mto_transient_since_day = 0 here, the auto-MTO
                    # gate thinks Bagshot is still accepting dischargers and the berth lock
                    # is cleared — Bagshot would try to berth a mother before Rathbone casts off.
                    _already_processed_by_case_a = (
                        getattr(v, "_mto_berth_free_at", 0.0) > 0
                        or getattr(v, "_mto_transient_since_day", -1) is None
                    )
                    if not _already_processed_by_case_a:
                        # Discharger not yet seeded (e.g. receiver processed first in loop,
                        # or no active discharger paired). Mark as open transient so the sim
                        # can route the discharger to this vessel when it arrives.
                        v._mto_transient_since_day = 0
                    # Always ensure receiver waits at BIA — never proceed to mother alone
                    v.status = "WAITING_BERTH_B"
                    v.next_event_time = max(
                        getattr(v, "_mto_berth_free_at", 0.0),
                        v.next_event_time,
                    )

                # ── Nominated load ceiling (for PF_LOADING startup vessels) ────
                # Prevents the Ibom vessel from jumping to full capacity on Day 1.
                # nominated_load_bbls = total expected load for this startup voyage.
                # The sim only loads up to this value, then triggers swap/departure.
                _nom_load = d.get("nominated_load_bbls")
                if _nom_load is not None and v.status == "PF_LOADING":
                    v._pf_load_ceiling = max(int(_nom_load), v.cargo_bbl)

                # ── Cargo API seeding ─────────────────────────────────────────
                # If the operator supplied an API gravity for the vessel's cargo,
                # seed it directly into vessel_api so blending is accurate.
                _cargo_api_seed = float(d.get("cargo_api", 0.0))
                if _cargo_api_seed > 0:
                    sim.vessel_api[v.name] = _cargo_api_seed

                # ── Partial-discharge resume ──────────────────────────────────
                # If operator entered a volume already transferred to the mother,
                # credit it to the mother now and adjust the vessel's remaining
                # cargo and next_event_time so the sim only pumps the remainder.
                if v.status in {"HOSE_CONNECT_B", "DISCHARGING"}:
                    _xfr = int(d.get("already_transferred_bbl", 0))
                    _full_cargo = v.cargo_bbl             # total cargo this voyage
                    _hose_h     = getattr(mod, "HOSE_CONNECTION_HOURS",  2.0)
                    # Discharge duration derived from per-vessel rate (volume / rate).
                    # The flat DISCHARGE_HOURS fallback is intentionally removed: every
                    # vessel in VESSEL_DISCHARGE_RATE_BPH has an explicit rate, and using
                    # a fixed 12-hour default produces wrong timing for cargoes that are
                    # larger or smaller than the nominal 85k-bbl reference.
                    _vessel_rate = (getattr(mod, "VESSEL_DISCHARGE_RATE_BPH", {}) or {}).get(v.name)
                    if not _vessel_rate:
                        # Safety net for any vessel not yet in the rate table — derive
                        # from full cargo and the module's nominal discharge hours so the
                        # implied rate is always cargo-proportional, never a fixed window.
                        _nominal_h   = getattr(mod, "DISCHARGE_HOURS", 12.0)
                        _vessel_rate = (_full_cargo / _nominal_h) if _full_cargo > 0 else 1.0
                    _mother_names_now = set(getattr(mod, "MOTHER_NAMES", []))
                    _selected_m = v.assigned_mother if v.assigned_mother in _mother_names_now else None
                    if _selected_m is None:
                        # Keep vessel state as provided; sim-side Point B logic now
                        # enforces explicit mother assignment without fallback.
                        continue

                    if _xfr > 0 and _full_cargo > 0:
                        _xfr = min(_xfr, _full_cargo)   # clamp to cargo size
                        # Credit already-transferred volume to mother
                        _prev_mother_bbl = sim.mother_bbl.get(_selected_m, 0.0)
                        _prev_mother_api = sim.mother_api.get(_selected_m, 0.0)
                        _vessel_api_val  = sim.vessel_api.get(v.name, 0.0)
                        sim.mother_bbl[_selected_m] = _prev_mother_bbl + _xfr
                        # Blend vessel API into the volume already on mother
                        if sim.mother_bbl[_selected_m] > 0:
                            sim.mother_api[_selected_m] = (
                                (_prev_mother_bbl * _prev_mother_api + _xfr * _vessel_api_val)
                                / sim.mother_bbl[_selected_m]
                            )
                        # Debit from daughter — only remainder still to pump
                        v.cargo_bbl = _full_cargo - _xfr

                    # Set next_event_time for remaining pump duration.
                    # Both paths now use volume / rate so timing scales correctly
                    # with actual cargo on board, not a fixed hour window.
                    # HOSE_CONNECT_B: pump hasn't started — next_event_time is the
                    #                 remaining hose-connection time only; the full
                    #                 discharge window is reserved in _berth_end below.
                    # DISCHARGING:    already pumping — remaining time = remaining cargo / rate.
                    if v.cargo_bbl <= 0:
                        # Nothing left — go straight to complete
                        v.next_event_time = 0.0
                    elif v.status == "HOSE_CONNECT_B":
                        # Hose still connecting — honour elapsed time from operator input
                        _hose_elapsed = float(d.get("hose_elapsed_hours", 0.0))
                        _hose_remaining = max(0.0, _hose_h - _hose_elapsed)
                        v.next_event_time = _hose_remaining
                    else:
                        # Mid-discharge — remaining time = remaining cargo / vessel rate
                        v.next_event_time = v.cargo_bbl / _vessel_rate

                    # Reserve mother berth for the remaining window.
                    # DISCHARGING:    only the remaining pump time (already pumping).
                    # HOSE_CONNECT_B: hose remaining + full remaining cargo at vessel rate.
                    _full_remaining_pump_h = v.cargo_bbl / _vessel_rate if v.cargo_bbl > 0 else 0.0
                    _berth_end = v.next_event_time + (0 if v.status == "DISCHARGING"
                                                      else _full_remaining_pump_h)
                    sim.mother_berth_free_at[_selected_m] = max(
                        sim.mother_berth_free_at.get(_selected_m, 0.0), _berth_end)
                    sim.next_mother_berthing_start_at = max(
                        sim.next_mother_berthing_start_at, _berth_end)

                # (LOADING partial-cargo resume is handled by the universal
                #  post-processing pass that runs after all defaults are applied)


    # ── Rebuild Point B berth locks after startup vessel overrides ───────────
    # Simulation.__init__ may pre-seed Balham at GreenEagle and reserve the berth.
    # If the operator/CSV startup state moves Balham elsewhere, that old berth lock
    # becomes stale and blocks Woodstock → Bryanston allocation. Recompute locks
    # from the final vessel startup states only.
    if vessel_states_json:
        try:
            _mother_names_now = set(getattr(mod, "MOTHER_NAMES", []))
            sim.mother_berth_free_at = {mn: 0.0 for mn in _mother_names_now}
            sim.next_mother_berthing_start_at = 0.0
            for _vv in sim.vessels:
                _mn = getattr(_vv, "assigned_mother", None)
                if _mn not in _mother_names_now:
                    continue
                if _vv.status not in {"BERTHING_B", "HOSE_CONNECT_B", "DISCHARGING"}:
                    continue
                _rate = getattr(mod, "VESSEL_DISCHARGE_RATE_BPH", {}).get(_vv.name)
                _pump_h = (_vv.cargo_bbl / _rate) if _rate else getattr(mod, "DISCHARGE_HOURS", 12.0)
                _hose_h = getattr(mod, "HOSE_CONNECTION_HOURS", 2.0)
                _bert_h = getattr(mod, "BERTHING_DELAY_HOURS", 0.5)
                if _vv.status == "BERTHING_B":
                    _end_t = _bert_h + _hose_h + _pump_h
                elif _vv.status == "HOSE_CONNECT_B":
                    _end_t = _hose_h + _pump_h
                else:
                    _end_t = _pump_h
                # Respect daylight cast-off extension if helper exists.
                try:
                    _end_t = mod._berth_free_at(_end_t)
                except Exception:
                    pass
                sim.mother_berth_free_at[_mn] = max(sim.mother_berth_free_at.get(_mn, 0.0), _end_t)
                sim.next_mother_berthing_start_at = max(sim.next_mother_berthing_start_at, _end_t)
        except Exception:
            pass

    # ── Mid-sim unavailability: unconditional pass ────────────────────────────
    # The mid-sim dormancy attributes (dormancy_start_hour, _dormancy_end_hour,
    # _dormancy_discharge_first) must be set on vessel objects regardless of
    # whether vessel_states_json was supplied.  The block above only runs inside
    # "if vessel_states_json:" so vessels configured for mid-sim unavailability
    # without manual startup states would be missed.  This pass covers that gap.
    _sp_map_dorm = getattr(mod, "STORAGE_POINT", {})
    for _dv in sim.vessels:
        if _dv.name not in _resumption_seed_by_vessel:
            continue
        _de = _resumption_seed_by_vessel[_dv.name]
        _de_indef   = _de.get("indefinite", False)
        _de_dorm_h  = float(_de.get("dormancy_start_hour", 0.0))
        # Only handle mid-sim unavailability here; Day-1 / indefinite cases are
        # already fully handled inside vessel_states_json block above.
        if _de_indef or _de_dorm_h <= 0.0:
            continue
        # Guard: only set attributes if not already set (vessel_states_json
        # block may have already configured them).
        if not hasattr(_dv, "dormancy_start_hour"):
            _dv.dormancy_start_hour    = _de_dorm_h
            _dv.resumption_storage     = _de["storage"]
            _dv.resumption_priority    = False
            _dv.resumption_hold_logged = False
            _dv._dormancy_discharge_first = True
            _res_date_str2 = _de.get("date", "")
            try:
                _r_epoch2 = getattr(mod, "_SIM_EPOCH", None)
                _r_date2  = _dt.date.fromisoformat(_res_date_str2)
                if _r_epoch2:
                    _res_h2 = (
                        _dt.datetime(_r_date2.year, _r_date2.month, _r_date2.day, 8, 0)
                        - _r_epoch2
                    ).total_seconds() / 3600.0
                    _dv._dormancy_end_hour = max(0.0, _res_h2)
                else:
                    _dv._dormancy_end_hour = None
            except Exception:
                _dv._dormancy_end_hour = None

    # ── Default Bedford / Balham starting state ────────────────────────────────
    # Applied only when the UI has not manually overridden these vessels.
    # Bedford  : PF_LOADING at Ibom, 30k bbl (below 65k swap trigger)
    # Balham   : BERTHING_B at GreenEagle, 85k bbl ready to discharge
    # After discharge Balham sails to SanBarth and runs A→B cycles freely.
    # When Bedford exceeds 65k bbl the swap triggers; Balham then sails B→F
    # to relieve Bedford, who subsequently mirrors the SanBarth A→B cycle.
    _vs_override = json.loads(vessel_states_json) if vessel_states_json else {}
    _resumption_seed_names = set(_resumption_seed_by_vessel)
    _BERTH_DELAY  = getattr(mod, "BERTHING_DELAY_HOURS",   0.5)
    _HOSE_HOURS   = getattr(mod, "HOSE_CONNECTION_HOURS",  2.0)
    _DISCH_HOURS  = getattr(mod, "DISCHARGE_HOURS",        12.0)
    # Pump end = berth_delay + hose + discharge. Cast-off may be deferred past
    # daylight end (18:00) to next morning 06:00, so use _berth_free_at to get
    # the actual berth-release time including overnight cast-off deferral.
    # This ensures berth_start calculations correctly target the next berthing
    # window AFTER GreenEagle is free (not a stale pump-end time).
    _BALHAM_PUMP_END = _BERTH_DELAY + _HOSE_HOURS + _DISCH_HOURS   # ≈14.5 h
    _berth_free_at_fn = getattr(sim, "_berth_free_at",
                                 getattr(mod, "_berth_free_at", None))
    if _berth_free_at_fn is not None:
        try:
            _BALHAM_END = _berth_free_at_fn(_BALHAM_PUMP_END)
        except TypeError:
            _BALHAM_END = _BALHAM_PUMP_END
    else:
        _BALHAM_END = _BALHAM_PUMP_END

    _POINT_B_START_VESSELS = set()   # no longer hardcode any vessels at BIA
    # Bagshot and Watson now start returning from BIA like the other vessels.
    # Their startup state is fully controlled by vessel_states_json from the UI.

    for v in sim.vessels:
        if v.name == "Bedford" and "Bedford" not in _vs_override and v.name not in _resumption_seed_names:
            v.status            = "PF_LOADING"
            v.target_point      = "F"
            v.cargo_bbl         = 20_000   # loading at Ibom — 20k on board
            v.next_event_time   = 0.0
            v._voyage_assigned  = True
            v.current_voyage    = 1
            # Ibom API is constant — set directly so vessel card shows correct value
            sim.vessel_api[v.name] = getattr(mod, "IBOM_API", 32.0)

        elif v.name == "Balham" and "Balham" not in _vs_override and v.name not in _resumption_seed_names:
            v.status            = "LOADING"
            v.target_point      = "A"
            v.assigned_storage  = getattr(mod, "STORAGE_SECONDARY_NAME", "JasmineS")
            v.cargo_bbl         = 0        # loading in progress — no cargo on board yet
            v.next_event_time   = 0.0
            v._voyage_assigned  = True
            v.current_voyage    = 1
            # JasmineS API
            sim.vessel_api[v.name] = getattr(mod, "JASMINES_API", 43.36)

        elif v.name in _POINT_B_START_VESSELS and v.name not in _vs_override and v.name not in _resumption_seed_names:
            # Default: start at BIA waiting for return-stock allocation
            v.status           = "WAITING_RETURN_STOCK"
            v.target_point     = "B"
            v.cargo_bbl        = 0
            v.next_event_time  = 0.0
            v._voyage_assigned = True
            v.current_voyage   = 1

        # Bagshot startup is now fully controlled by vessel_states_json from the UI.
        # The UI defaults to Returning → SanBarth with Leg 1 status, cargo 0.

        # Watson startup is now fully controlled by vessel_states_json from the UI.
        # The UI defaults to Returning → SanBarth with Leg 1 status, cargo 0.

    # ── Sim-level Ibom tracking: derive active loader from seeded vessel states ──
    # Default: Bedford is the active loader. But if the operator has placed Balham
    # at Ibom (PF_LOADING / PF_SWAP / IDLE_A with target_point "F"), Balham is the
    # active loader and Bedford is the SanBarth support vessel — and vice-versa.
    _balham_v  = next((v for v in sim.vessels if v.name == "Balham"),  None)
    _bedford_v = next((v for v in sim.vessels if v.name == "Bedford"), None)
    _ibom_active = None
    for _pf_v in [_balham_v, _bedford_v]:
        if _pf_v is None:
            continue
        if _pf_v.status in {"PF_LOADING", "PF_SWAP"}:
            # Whoever was seeded with an active Ibom status is the loader.
            # Ensure target_point is correct regardless of what location the UI set.
            _pf_v.target_point = "F"
            _ibom_active = _pf_v.name
            break
        if _pf_v.status == "IDLE_A" and _pf_v.target_point == "F":
            _ibom_active = _pf_v.name
            break

    if _ibom_active is None:
        _ibom_active = "Bedford"   # safe default

    sim.point_f_active_loader     = _ibom_active
    sim.point_f_swap_pending_for  = None
    sim.point_f_swap_triggered_by = None

    # Guarantee the active Ibom loader has the correct Ibom API set, regardless
    # of whether the operator filled in the cargo_api field in the UI.
    _ibom_api_const = getattr(mod, "IBOM_API", 32.0)
    _active_loader_v = next((v for v in sim.vessels if v.name == _ibom_active), None)
    if _active_loader_v is not None:
        sim.vessel_api[_ibom_active] = _ibom_api_const

    # Ensure the non-active Point F vessel (SanBarth support) has target_point "A"
    # so the IDLE_A handler dispatches it to SanBarth/JasmineS, not back to the buoy.
    _ibom_support = "Balham" if _ibom_active == "Bedford" else "Bedford"
    _support_v = next((v for v in sim.vessels if v.name == _ibom_support), None)
    if _support_v is not None and _support_v.target_point == "F":
        _support_v.target_point = "A"

    # ── Mother export seed: block mothers away at export from t=0 ─────────────
    # For each mother in _mother_export, reserve her berth AND record her in
    # mother_seeded_away_until so that mother_is_at_point_b() correctly returns
    # False for the full seed duration.
    # We deliberately do NOT touch export_state here — it must only be set by
    # the sim's own DOC→SAILING→HOSE→IN_PORT export machinery.  Setting it
    # externally causes crashes when the state machine tries to read
    # export_end_time which is still None.
    for _exp_mn, _exp_days in _mother_export.items():
        _exp_h = _exp_days * 24.0
        # ── CRITICAL: zero the mother's stock and API.
        # A mother seeded as "away at export" has already left with her cargo.
        # Whatever the operator entered in the stock panel refers to the volume
        # she carried OUT — not what she has on board right now.  She returns
        # empty.  If we leave her stock at the manual input value (e.g. 713k bbl)
        # she will immediately trigger the export machinery again the moment
        # she "returns", causing a spurious second export departure.
        sim.mother_bbl[_exp_mn] = 0.0
        sim.mother_api[_exp_mn] = 0.0
        sim.mother_berth_free_at[_exp_mn] = max(
            sim.mother_berth_free_at.get(_exp_mn, 0.0), _exp_h
        )
        # mother_seeded_away_until is checked by mother_is_at_point_b —
        # blocks daughters from targeting this mother.
        # Use getattr with a fallback dict in case an older sim version is
        # deployed that does not yet have this attribute in __init__.
        if not hasattr(sim, 'mother_seeded_away_until'):
            sim.mother_seeded_away_until = {name: 0.0 for name in getattr(mod, 'MOTHER_NAMES', [])}
        sim.mother_seeded_away_until[_exp_mn] = max(
            sim.mother_seeded_away_until.get(_exp_mn, 0.0), _exp_h
        )
        # mother_available_at ensures the mother is not selected for export
        # documentation until she has "returned" from the seeded voyage.
        # Adding a small return-fendering buffer (2 h) mirrors the real-world
        # procedure and prevents immediate berthing the instant she returns.
        sim.mother_available_at[_exp_mn] = max(
            sim.mother_available_at.get(_exp_mn, 0.0), _exp_h + 2.0
        )
        sim.next_mother_berthing_start_at = max(
            sim.next_mother_berthing_start_at, _exp_h
        )
        # Ensure export_ready stays False — her stock is now 0 so the
        # departure eligibility check (stock >= MOTHER_EXPORT_VOLUME) will
        # not fire as soon as she returns.  The flag is already False from
        # __init__; this line is a defensive guard against re-runs or
        # partial state that might have left it True.
        if hasattr(sim, 'export_ready') and _exp_mn in sim.export_ready:
            sim.export_ready[_exp_mn]       = False
            sim.export_ready_since[_exp_mn] = None
        # Defensive initialisation: ensure export_intake_last_cast_off exists
        # (added alongside EXPORT_INTAKE_BUFFER_HOURS; older sim builds may not
        # have it).  Reset to 0.0 for a mother that is away at export — she
        # returns empty so the intake buffer starts fresh on arrival.
        if not hasattr(sim, 'export_intake_last_cast_off'):
            sim.export_intake_last_cast_off = {
                name: 0.0 for name in getattr(mod, 'MOTHER_NAMES', [])
            }
        sim.export_intake_last_cast_off[_exp_mn] = 0.0

    # ── Fendering gate: post-seeding pass ─────────────────────────────────────
    # vessel_states_json seeding and Balham hard-coding both run before the
    # _mother_export block above sets mother_available_at.  Any vessel that
    # landed in BERTHING_B or HOSE_CONNECT_B at a primary mother that hasn't
    # completed fendering must be downgraded to WAITING_BERTH_B so the sim's
    # own gate (which now checks mother_available_at at BERTHING_B) starts
    # from a clean state.
    for _fv in sim.vessels:
        if _fv.status not in {"BERTHING_B", "HOSE_CONNECT_B"}:
            continue
        _fm = getattr(_fv, "assigned_mother", None)
        if not _fm:
            continue
        _fender_t = sim.mother_available_at.get(_fm, 0.0)
        if _fender_t <= 0:
            continue   # mother already available — no change needed
        # Mother is still fending — vessel cannot be at berth yet
        _fv.status          = "WAITING_BERTH_B"
        _fv.assigned_mother = None
        _fv.next_event_time = _fender_t
        # Clear the stale berth lock this vessel set as BERTHING_B (the vessel
        # hasn't actually started, so the lock was premature).  Reset to
        # mother_available_at unless another active vessel holds a later lock.
        if not any(
            vv is not _fv
            and vv.assigned_mother == _fm
            and vv.status in {"BERTHING_B", "HOSE_CONNECT_B", "DISCHARGING"}
            for vv in sim.vessels
        ):
            sim.mother_berth_free_at[_fm] = _fender_t

    # ── Forced export departure schedule ──────────────────────────────────────
    # mother_export_force_json: JSON list of {mother, date} entries.
    # Each entry forces the named primary mother to begin export documentation
    # (DOC) at the start of the export sail window on the specified calendar
    # date, regardless of her current stock level.  She sails, discharges at
    # the export terminal, and returns empty — identical to a normal export.
    if hasattr(mod, "EXPORT_FORCE_SCHEDULE"):
        mod.EXPORT_FORCE_SCHEDULE.clear()
    _force_schedule: dict = {}
    if mother_export_force_json:
        try:
            _raw_force = json.loads(mother_export_force_json)
        except Exception:
            _raw_force = []
        if isinstance(_raw_force, list):
            _epoch_dt   = mod._SIM_EPOCH
            _sail_start = getattr(mod, "EXPORT_SAIL_WINDOW_START", 6)
            _mother_names_f = set(getattr(mod, "MOTHER_NAMES", []))
            for _item in _raw_force:
                if not isinstance(_item, dict):
                    continue
                _mn   = _item.get("mother", "")
                _date = _item.get("date", "")
                if _mn not in _mother_names_f or not _date:
                    continue
                try:
                    _dep_dt = _dt.datetime.fromisoformat(_date)
                    if _dep_dt.hour == 0 and _dep_dt.minute == 0:
                        _dep_dt = _dep_dt.replace(hour=_sail_start)
                    _dep_h  = (_dep_dt - _epoch_dt).total_seconds() / 3600.0
                    if _dep_h < 0:
                        continue   # departure in the past — skip
                    _force_schedule.setdefault(_mn, []).append(_dep_h)
                except Exception:
                    continue
    if hasattr(mod, "EXPORT_FORCE_SCHEDULE") and _force_schedule:
        for _fmn, _fhrs in _force_schedule.items():
            mod.EXPORT_FORCE_SCHEDULE[_fmn] = sorted(_fhrs)

    # ── Mother unavailability windows ──────────────────────────────────────────
    # Each window is {mother, start_date, end_date} in ISO format.
    # Convert to sim-hours relative to _SIM_EPOCH and register in
    # sim.mother_unavailability_windows so mother_is_at_point_b() returns
    # False for t inside any window.
    _unavail_windows: dict = {}
    if mother_unavailability_json:
        try:
            _raw_unavail = json.loads(mother_unavailability_json)
        except Exception:
            _raw_unavail = []
        if isinstance(_raw_unavail, list):
            _epoch_dt = mod._SIM_EPOCH  # datetime at t=0
            for _item in _raw_unavail:
                if not isinstance(_item, dict):
                    continue
                _mn   = _item.get("mother", "")
                _sd   = _item.get("start_date", "")
                _ed   = _item.get("end_date", "")
                if _mn not in _mother_names or not _sd or not _ed:
                    continue
                try:
                    _sdt = _dt.datetime.fromisoformat(_sd)
                    _edt = _dt.datetime.fromisoformat(_ed)
                except ValueError:
                    continue
                # Convert to sim-hours: hours since _SIM_EPOCH
                _sh = (_sdt - _epoch_dt).total_seconds() / 3600.0
                _eh = (_edt - _epoch_dt).total_seconds() / 3600.0
                if _eh <= _sh:
                    continue   # skip invalid/empty windows
                _unavail_windows.setdefault(_mn, []).append((_sh, _eh))

    if _unavail_windows:
        if not hasattr(sim, 'mother_unavailability_windows'):
            sim.mother_unavailability_windows = {
                name: [] for name in getattr(mod, 'MOTHER_NAMES', [])
            }
        for _mn, _wins in _unavail_windows.items():
            sim.mother_unavailability_windows[_mn] = (
                sim.mother_unavailability_windows.get(_mn, []) + _wins
            )
            # Also reserve berth_free_at for the latest window end so
            # no daughters are slotted into the mother while she is unavailable
            for _sh, _eh in _wins:
                # Only pre-reserve if the window starts near t=0;
                # windows that start mid-sim are handled by the run-loop monitor
                if _sh <= 0.0:
                    sim.mother_berth_free_at[_mn] = max(
                        sim.mother_berth_free_at.get(_mn, 0.0), _eh
                    )
                    sim.mother_available_at[_mn] = max(
                        sim.mother_available_at.get(_mn, 0.0), _eh
                    )

    # ── Export unavailability windows ──────────────────────────────────────────
    # Each window is {label, start_date, end_date}.  During a window, mothers
    # that have reached their export trigger are held at BIA (export_ready is
    # set but departure is suppressed).  Mothers already mid-cycle complete
    # their current export normally.  Converts dates to sim-hours and stores
    # in sim.export_unavailability_windows as a list of (start_h, end_h) tuples.
    _export_unavail_windows: list = []
    if export_unavailability_json:
        try:
            _raw_eu = json.loads(export_unavailability_json)
        except Exception:
            _raw_eu = []
        if isinstance(_raw_eu, list):
            _epoch_dt_eu = mod._SIM_EPOCH
            for _eu_item in _raw_eu:
                if not isinstance(_eu_item, dict):
                    continue
                _eu_sd = _eu_item.get("start_date", "")
                _eu_ed = _eu_item.get("end_date", "")
                if not _eu_sd or not _eu_ed:
                    continue
                try:
                    _eu_sdt = _dt.datetime.fromisoformat(_eu_sd)
                    _eu_edt = _dt.datetime.fromisoformat(_eu_ed)
                except ValueError:
                    try:
                        _eu_sdt = _dt.datetime.combine(_dt.date.fromisoformat(_eu_sd), _dt.time.min)
                        _eu_edt = _dt.datetime.combine(_dt.date.fromisoformat(_eu_ed), _dt.time.min)
                    except Exception:
                        continue
                _eu_sh = (_eu_sdt - _epoch_dt_eu).total_seconds() / 3600.0
                _eu_eh = (_eu_edt - _epoch_dt_eu).total_seconds() / 3600.0
                if _eu_eh > _eu_sh:
                    _export_unavail_windows.append((_eu_sh, _eu_eh))
    sim.export_unavailability_windows = _export_unavail_windows

    # ── Universal LOADING partial-cargo resume pass ────────────────────────
    # For every vessel seeded in LOADING status, treat cargo_bbl as the
    # volume already on board.  Compute remaining load time, deduct only
    # the remaining balance from the assigned storage, credit already-loaded
    # volume to total_loaded, and reserve the storage berth accordingly.
    # This covers both manual overrides (vessel_states_json) and hardcoded
    # defaults (e.g. vessels seeded in LOADING state at a specific storage).
    # Build a rate map from sim module constants (all storages, no fallback gaps)
    _stor_rate_map = {
        getattr(mod, "STORAGE_PRIMARY_NAME",     "SanBarth"):   getattr(mod, "SANBARTH_LOAD_RATE_BPH",   7_083),
        getattr(mod, "STORAGE_SECONDARY_NAME",   "JasmineS"): getattr(mod, "JASMINES_LOAD_RATE_BPH", 7_083),
        getattr(mod, "STORAGE_TERTIARY_NAME",    "Westmore"): getattr(mod, "WESTMORE_LOAD_RATE_BPH", 2_500),
        getattr(mod, "STORAGE_QUATERNARY_NAME",  "Duke"):     getattr(mod, "DUKE_LOAD_RATE_BPH",     3_500),
        getattr(mod, "STORAGE_QUINARY_NAME",     "Starturn"): getattr(mod, "STARTURN_LOAD_RATE_BPH", 2_500),
        getattr(mod, "STORAGE_SENARY_NAME",      "PGM"):      getattr(mod, "PGM_LOAD_RATE_BPH",      1_000),
    }
    _sp_map_post   = getattr(mod, "STORAGE_POINT", {})
    _scap_map      = getattr(mod, "STORAGE_CAPACITY_BY_NAME", {})

    for _v in sim.vessels:
        if _v.status != "LOADING" or not _v.assigned_storage:
            continue
        _stor    = _v.assigned_storage
        # Apply Point A loading cap for Bedford/Balham
        _pt_a_cap_vessels = getattr(mod, "POINT_A_LOAD_CAP_VESSELS", {"Bedford", "Balham"})
        _pt_a_cap_bbl     = getattr(mod, "POINT_A_LOAD_CAP_BBL",     63_000)
        _sp_map_load      = getattr(mod, "STORAGE_POINT", {})
        _is_pt_a = (_sp_map_load.get(_stor) == "A")
        _vcap    = (_pt_a_cap_bbl
                    if (_v.name in _pt_a_cap_vessels and _is_pt_a)
                    else _v.cargo_capacity)
        _loaded  = min(_v.cargo_bbl, _vcap)       # already on board (clamped)
        _remain  = max(0.0, _vcap - _loaded)       # still to load

        # Full and remaining load durations — rate-based for every storage
        _rate     = _stor_rate_map.get(_stor, getattr(mod, "LOAD_HOURS", 12))
        _full_h   = _vcap   / _rate if isinstance(_rate, (int, float)) and _rate > 0 else getattr(mod, "LOAD_HOURS", 12)
        _remain_h = _remain / _rate if isinstance(_rate, (int, float)) and _rate > 0 else _full_h

        _v.assigned_load_hours = _full_h
        _v.next_event_time     = _remain_h         # sim fires when loading completes

        # Deduct only the balance not yet loaded from storage
        _cur_stock = sim.storage_bbl.get(_stor, 0.0)
        _cap_s     = _scap_map.get(_stor, float("inf"))
        sim.storage_bbl[_stor] = max(0.0, min(_cur_stock, _cap_s) - _remain)

        # Credit already-loaded volume toward total_loaded metric
        sim.total_loaded = getattr(sim, "total_loaded", 0) + _loaded

        # Reserve berth for the remaining loading window
        sim.storage_berth_free_at[_stor] = max(
            sim.storage_berth_free_at.get(_stor, 0.0), _remain_h
        )
        # Ensure target_point is consistent with assigned_storage
        if _stor in _sp_map_post:
            _v.target_point = _sp_map_post[_stor]

        _v.cargo_bbl = _loaded   # keep partial value; LOADING handler sets full at completion

        # Emit a synthetic LOADING_START event at t=0 so the JMP and Loading Plan
        # displays can correctly map this vessel to its storage.
        # Guard against duplicate startup logs when the simulator has already
        # emitted the same t=0 LOADING_START for vessels seeded in LOADING state.
        # Set vessel_api to the storage's current API — vessel carries storage API exactly.
        _stor_api_now = getattr(mod, "STORAGE_API", {}).get(_stor, 0.0)
        sim.vessel_api[_v.name] = _stor_api_now
        _stor_stock_now = sim.storage_bbl.get(_stor, 0.0)
        _t0_str = sim.hours_to_dt(0).strftime("%Y-%m-%d %H:%M")
        _startup_loading_exists = any(
            _row.get("Event") == "LOADING_START"
            and _row.get("Vessel") == _v.name
            and _row.get("Time") == _t0_str
            for _row in getattr(sim, "log", [])
        )
        if not _startup_loading_exists:
            # Stamp voyage code on vessel object before logging
            if hasattr(mod, "make_voyage_code") and not getattr(_v, "voyage_code", ""):
                _v.voyage_code = mod.make_voyage_code(_v.name, getattr(_v, "current_voyage", 1))
            sim.log_event(0, _v.name, "LOADING_START",
                          f"Loading {_vcap:,} bbl @ {_stor_api_now:.2f}° API | {_stor}: {_stor_stock_now:,.0f} bbl "
                          f"(started at t=0, {_loaded:,.0f} bbl already on board, "
                          f"remaining {_remain_h:.1f}h)",
                          voyage_num=getattr(_v, "current_voyage", 1))

    # ── Defensive: ensure export_intake_last_cast_off exists before run ──────────
    if not hasattr(sim, 'export_intake_last_cast_off'):
        sim.export_intake_last_cast_off = {
            name: 0.0 for name in getattr(mod, 'MOTHER_NAMES', [])
        }

    # ── Defensive: ensure _point_b_deregister_mother exists ──────────────────────
    # Added in a sim update. Older builds omit it, causing AttributeError when
    # CAST_OFF_COMPLETE_B tries to release the one-per-day day slot.
    if not hasattr(sim, '_point_b_deregister_mother'):
        _pbn_dm = {getattr(mod, 'MOTHER_PRIMARY_NAME',   'Bryanston'),
                   getattr(mod, 'MOTHER_SECONDARY_NAME', 'GreenEagle')}
        _sho_dm = getattr(mod, 'SIM_HOUR_OFFSET', 8)
        def _deregister_mother(mother_name, t,
                               _pbn=_pbn_dm, _sho=_sho_dm,
                               _sim=sim):
            if mother_name not in _pbn:
                return
            _dk = int((t + _sho) // 24)
            _ds = _sim.point_b_day_assigned_mothers.get(_dk)
            if _ds:
                _ds.discard(mother_name)
        sim._point_b_deregister_mother = _deregister_mother

    # ── Phantom-reservation fix for point_b_candidate_slots ──────────────────────
    # Problem: the sim's _committed sum counts ALL vessels assigned to a mother
    # regardless of whether their cargo fits within the mother's live headroom.
    # Vessels that exceed headroom will be turned away by MOTHER_CAPACITY_ABORT —
    # they are phantom reservations. Counting them blocks genuinely fitting
    # vessels (e.g. SantaMonica 7k blocked because RTH 47k + BGT 43k + AMY 63k
    # are all assigned to Bryanston with 28k headroom, pushing projected stock
    # over cap even though none of those three can actually land).
    #
    # Fix: wrap point_b_candidate_slots so that after the original runs, any
    # primary mother that was excluded SOLELY because of phantom committed volume
    # is reinstated when the corrected committed sum (excluding vessels whose
    # cargo alone exceeds live headroom) shows the vessel actually fits.
    #
    # We do NOT re-filter what the original returned — we only add back mothers
    # that were wrongly excluded. This is additive-only and cannot break any
    # other gate the original applies (export_ready, berth_free_at, etc.).
    import types as _rt_types

    _orig_pbc  = sim.point_b_candidate_slots.__func__
    _PRI_NAME  = getattr(mod, 'MOTHER_PRIMARY_NAME',    'Bryanston')
    _SEC_NAME  = getattr(mod, 'MOTHER_SECONDARY_NAME',  'GreenEagle')
    _ALL_MOTHERS = list(getattr(mod, 'MOTHER_NAMES',
                                [_PRI_NAME, _SEC_NAME]))
    _CAP_BY_NAME = dict(getattr(mod, 'MOTHER_CAPACITY_BY_NAME', {}))
    _DEFAULT_CAP = getattr(mod, 'MOTHER_CAPACITY_BBL', 550_000)
    _SHO         = getattr(mod, 'SIM_HOUR_OFFSET', 8)
    _ACTIVE_STS  = {"SAILING_AB_LEG2","WAITING_FAIRWAY","WAITING_BERTH_B",
                    "BERTHING_B","HOSE_CONNECT_B","DISCHARGING",
                    "WAITING_MOTHER_CAPACITY"}

    def _pbc_with_phantom_fix(self_sim, v, at_time):
        # Run original — gets candidates that passed ALL original gates.
        berthing_start, orig_candidates = _orig_pbc(self_sim, v, at_time)

        # Identify which primary mothers are already in the result.
        orig_mothers = {mn for _, _, mn in orig_candidates}

        # For each primary mother NOT in the result, check whether its absence
        # is purely due to phantom committed volume.  A mother passes this test
        # if, with the corrected _committed (only vessels whose cargo ≤ live
        # headroom), the projected stock check would pass.
        extra = []
        for mn in (_PRI_NAME, _SEC_NAME):
            if mn in orig_mothers:
                continue   # already present — nothing to do
            # Skip if mother is export_ready or not at BIA — the original
            # correctly excluded it for one of those real reasons.
            if self_sim.export_ready.get(mn, False):
                continue
            _lookahead = at_time + 24.0
            if not (self_sim.mother_is_at_point_b(mn, at_time)
                    or self_sim.mother_is_at_point_b(mn, _lookahead)):
                continue
            _cap       = _CAP_BY_NAME.get(mn, _DEFAULT_CAP)
            _live_stk  = self_sim.mother_bbl.get(mn, 0.0)
            _live_head = max(0.0, _cap - _live_stk)

            # Corrected committed: only vessels whose cargo ≤ live headroom.
            _committed = sum(
                vv.cargo_bbl for vv in self_sim.vessels
                if vv.assigned_mother == mn
                and vv.status in _ACTIVE_STS
                and vv is not v
                and vv.cargo_bbl <= _live_head
            )
            # With corrected committed, does this vessel's cargo fit?
            if _live_stk + _committed + v.cargo_bbl > _cap:
                continue   # genuinely over capacity even without phantoms

            # Also check the one-per-day rule for the best possible start time.
            _occupant  = self_sim.mother_berth_current_occupant(mn)
            _free_at   = max(self_sim.mother_berth_free_at.get(mn, 0.0),
                             _occupant.next_event_time
                             if _occupant is not None else 0.0)
            _start     = self_sim.next_berthing_window(
                max(at_time, self_sim.mother_available_at.get(mn, 0.0),
                    _free_at if _free_at > at_time else 0.0),
                point="B")
            _day_key   = int((_start + _SHO) // 24)
            _day_set   = self_sim.point_b_day_assigned_mothers.get(_day_key, set())
            if mn in _day_set:
                continue   # one-per-day rule still correctly blocks it

            extra.append((_start, _start, mn))

        return berthing_start, orig_candidates + extra

    sim.point_b_candidate_slots = _rt_types.MethodType(
        _pbc_with_phantom_fix, sim)

    log_df, tl_df = sim.run()

    # Restore module globals AFTER sim.run() — must be here so that
    # SIMULATION_DAYS (and all other overrides) remain in effect for the
    # full duration of the run.  Restoring before sim.run() was the bug
    # that capped every simulation at the module default of 30 days.
    for k, v in orig.items():
        if k == "_ibom_rate":
            continue
        if k == "_CUSTOM_VESSELS":
            if hasattr(mod, "_CUSTOM_VESSELS"):
                mod._CUSTOM_VESSELS.clear()
                mod._CUSTOM_VESSELS.extend(v)
        elif k == "_VESSEL_RESUMPTION_DATES":
            if hasattr(mod, "_VESSEL_RESUMPTION_DATES"):
                mod._VESSEL_RESUMPTION_DATES.clear()
                mod._VESSEL_RESUMPTION_DATES.update(v)
        else:
            setattr(mod, k, v)
    # Always clear EXPORT_FORCE_SCHEDULE after the run so stale entries
    # from a previous call don't carry over to the next Streamlit rerun.
    if hasattr(mod, "EXPORT_FORCE_SCHEDULE"):
        mod.EXPORT_FORCE_SCHEDULE.clear()
    _r = orig["_ibom_rate"]
    if hasattr(mod, "IBOM_LOAD_RATE_BPH"):    mod.IBOM_LOAD_RATE_BPH    = _r
    if hasattr(mod, "POINT_F_LOAD_RATE_BPH"): mod.POINT_F_LOAD_RATE_BPH = _r

    # Restore any optimizer params that were temporarily applied
    for _mod_key, _orig_val in _opt_orig.items():
        setattr(mod, _mod_key, _orig_val)

    # Restore STORAGE_DISPATCH_OVERRIDES to its pre-run state
    if hasattr(mod, "STORAGE_DISPATCH_OVERRIDES"):
        mod.STORAGE_DISPATCH_OVERRIDES.clear()
        mod.STORAGE_DISPATCH_OVERRIDES.update(_sdo_orig)

    # Restore ZEEZEE_SCHEDULE to its pre-run state
    if hasattr(mod, "ZEEZEE_SCHEDULE"):
        mod.ZEEZEE_SCHEDULE.clear()
        mod.ZEEZEE_SCHEDULE.extend(_zz_schedule_orig)

    # Restore DAUGHTER_DISCHARGE_OVERRIDES to its pre-run state
    if hasattr(mod, "DAUGHTER_DISCHARGE_OVERRIDES"):
        mod.DAUGHTER_DISCHARGE_OVERRIDES.clear()
        mod.DAUGHTER_DISCHARGE_OVERRIDES.update(_ddo_orig)


    summary = dict(
        loadings        = int(len(log_df[log_df.Event == "LOADING_START"])),
        discharges      = int(len(log_df[log_df.Event == "DISCHARGE_START"])),
        loaded          = int(sim.total_loaded),
        exported        = float(sim.total_exported),
        produced        = float(sim.total_produced),
        spilled         = float(sim.total_spilled),
        exports         = int(len(log_df[log_df.Event == "EXPORT_COMPLETE"])),
        ovf_events      = int(sim.storage_overflow_events),
        vessel_names    = [v.name for v in sim.vessels],
        spill_by_storage= {k: float(v) for k, v in sim.storage_overflow_bbl.items()},
        **{f"final_{k}": float(v) for k, v in sim.storage_bbl.items()},
        **{f"final_{k}": float(v) for k, v in sim.mother_bbl.items()},
        storage_api     = {k: round(float(v), 2) for k, v in getattr(sim, "final_storage_api", {}).items()},
        mother_api      = {k: round(float(v), 2) for k, v in getattr(sim, "final_mother_api",  {}).items()},
        vessel_api      = {k: round(float(v), 2) for k, v in getattr(sim, "final_vessel_api",  {}).items()},
        avg_exported_api= round(float(getattr(sim, "avg_exported_api", 0.0)), 2),
        vessel_cargo    = {v.name: round(v.cargo_bbl) for v in sim.vessels},
        # Stochastic variability summary
        variability_summary          = getattr(sim, "_variability_summary",
                                               {"enabled": False, "calibration": {}}),
    )
    return log_df, tl_df, summary


@st.cache_data(ttl=3600, show_spinner=False)
def run_optimizer(base_params_json: str):
    """
    Heuristic parameter sweep — no external OptimizationEngine required.
    Sweeps dead_stock_factor, ibom_trigger_bbl, export_sail_window_start,
    berthing_start, berthing_end across a grid and scores each scenario.
    Returns (best_slim_json, results_table_json).

    TIME BUDGET: The sweep runs for at most _OPT_TIME_BUDGET_SECS wall-clock
    seconds (default 90 s).  To get diverse coverage within the budget the
    grid is shuffled with a fixed seed before iteration — this prevents the
    sweep from only ever evaluating the first N combinations of the same
    parameter dimension when time runs out.

    CLEAN BASELINE: The optimizer always uses a clean vessel state (no
    manual positions, no discharge overrides, no forced loadings).  Those
    operator interventions are real-world seeds that are intentionally absent
    from the parameter search — their inclusion would distort every scenario
    in the same direction, preventing the sweep from finding parameter
    combinations that are genuinely superior on a clean slate.
    """
    _OPT_TIME_BUDGET_SECS = 90   # hard wall-clock cap for the full sweep

    base = json.loads(base_params_json)
    _tide_hex   = base.pop("_tide_csv_bytes_hex", None)
    _tide_bytes = None
    if _tide_hex:
        _tide_bytes = binascii.unhexlify(_tide_hex)
    _start_iso  = base.pop("_sim_start_date", None) or ""
    # Strip any operator overrides that were included in base_params_json —
    # the optimizer must run on a clean baseline.
    base.pop("vessel_states_json",                  None)
    base.pop("daughter_discharge_overrides_json",   None)
    base.pop("storage_overrides_json",              None)
    base.pop("mother_export_seed_json",             None)
    base.pop("startup_day_manual_nominations_json", None)
    base.pop("point_b_startup_seed_json",           None)

    # ── Parameter grid ──────────────────────────────────────────────────
    _dead_stock_factors      = [1.50, 1.75, 2.00]
    _ibom_triggers           = [45_000, 55_000, 65_000, 75_000]
    _export_window_starts    = [6, 8, 10]
    _berthing_configs        = [(6, 18), (6, 20), (7, 18)]   # (start, end)
    # MTO: sweep max_parcels 1-3 only when MTO is enabled in the live run.
    # When MTO is off the optimizer always uses mto_max_parcels=1 (irrelevant).
    _mto_enabled_live = bool(base.get("multiple_transient_operation", False))
    _mto_parcels_opts = [1, 2, 3] if _mto_enabled_live else [1]

    _grid = list(itertools.product(
        _dead_stock_factors,
        _ibom_triggers,
        _export_window_starts,
        _berthing_configs,
        _mto_parcels_opts,
    ))
    # Shuffle so the budget covers diverse regions of the parameter space
    # rather than always exhausting one dimension first.
    import random as _rnd
    _rnd.seed(42)
    _rnd.shuffle(_grid)

    mod = _load_mod_current()
    _orig_dsf    = getattr(mod, "DEAD_STOCK_FACTOR",         1.75)
    _orig_pftrig = getattr(mod, "POINT_F_MIN_TRIGGER_BBL",  65_000)
    _orig_expw   = getattr(mod, "EXPORT_SAIL_WINDOW_START",  6)
    _orig_bstart = getattr(mod, "BERTHING_START",            6)
    _orig_bend   = getattr(mod, "BERTHING_END",              18)

    def _restore():
        # run_sim now owns parameter save/restore via opt_params_json.
        # This is kept as a no-op safety net only.
        pass

    def _score(S, log_df, tl_df):
        sim_days  = base.get("sim_days", 14)
        storage_names = ["SanBarth", "JasmineS", "Westmore", "Duke", "Starturn", "PGM"]
        total_loaded  = S.get("loaded", 0)
        total_exported = S.get("exported", 0)
        spilled = S.get("spilled", 0)

        # ── Primary Objective: crash stock drawdown speed ───────────────
        initial_total_stock = sum(float(base.get(sn.lower(), 0)) for sn in storage_names)
        final_total_stock = sum(float(S.get(f"final_{sn}", 0.0)) for sn in storage_names)
        drawdown_bbl = max(0.0, initial_total_stock - final_total_stock)
        drawdown_pct = 100.0 * drawdown_bbl / max(1.0, initial_total_stock)

        if all(sn in tl_df.columns for sn in storage_names) and not tl_df.empty:
            # Use only the storage columns needed — avoids scanning all ~60 cols.
            _stor_subset = tl_df[storage_names]
            total_storage_series = _stor_subset.sum(axis=1)
            # Reward early crash-down in the first 24h window.
            early_steps = max(1, min(len(total_storage_series), 48))
            early_min = float(total_storage_series.iloc[:early_steps].min())
            early_drawdown_pct = 100.0 * max(0.0, initial_total_stock - early_min) / max(1.0, initial_total_stock)
        else:
            early_drawdown_pct = drawdown_pct

        crash_score = min(100.0, 0.65 * drawdown_pct + 0.35 * early_drawdown_pct)

        # ── Safety Objective: suppress overflow + high-risk stock exposure ─
        critical_by_storage = getattr(mod, "STORAGE_CRITICAL_THRESHOLD_BY_NAME", {})
        cap_by_storage = {
            "SanBarth": 400000,
            "JasmineS": 290000,
            "Westmore": 270000,
            "Duke": 228000,
            "Starturn": 228000,
            "PGM": 30000,
        }
        risk_fracs = []
        risk_by_storage = {}
        borderline_fracs = []
        borderline_by_storage = {}
        if not tl_df.empty:
            for sn in storage_names:
                if sn not in tl_df.columns:
                    continue
                s_col = tl_df[sn].astype(float, copy=False)
                crit = float(critical_by_storage.get(sn, 0.0))
                cap = float(cap_by_storage.get(sn, max(1.0, s_col.max())))
                if crit > 0:
                    _rf = float((s_col > crit).mean())
                    risk_fracs.append(_rf)
                    risk_by_storage[sn] = _rf
                _bf = float((s_col >= 0.90 * cap).mean())
                borderline_fracs.append(_bf)
                borderline_by_storage[sn] = _bf
        risk_avg = (sum(risk_fracs) / len(risk_fracs)) if risk_fracs else 0.0
        borderline_avg = (sum(borderline_fracs) / len(borderline_fracs)) if borderline_fracs else 0.0
        max_risk = max(risk_by_storage.values()) if risk_by_storage else 0.0
        max_borderline = max(borderline_by_storage.values()) if borderline_by_storage else 0.0
        persistent_hotspots = sum(
            1 for sn in storage_names
            if risk_by_storage.get(sn, 0.0) > 0.18 or borderline_by_storage.get(sn, 0.0) > 0.28
        )
        spill_penalty = min(85.0, (spilled / max(1.0, total_loaded + 1.0)) * 750.0)
        risk_penalty = min(30.0, risk_avg * 100.0 * 0.55)
        borderline_penalty = min(25.0, borderline_avg * 100.0 * 0.45)
        # Prevent average-masking: punish single-location sustained risk strongly.
        max_risk_penalty = min(20.0, max_risk * 100.0 * 0.22)
        max_borderline_penalty = min(12.0, max_borderline * 100.0 * 0.14)
        hotspot_penalty = min(15.0, float(persistent_hotspots) * 5.0)
        # ── Per-storage production-weighted overflow penalty ──────────────
        # High-production storages (SanBarth, JasmineS) that spend a large
        # fraction of the sim above their critical threshold contribute
        # disproportionately to real-world overflow risk.  Penalise each
        # storage's overflow fraction proportionally to its production rate
        # so the optimizer strongly prefers params that keep fast-filling
        # storages drained.  This prevents the average across all storages
        # from masking a single persistently overflowing tank.
        _prod_rates_opt = {
            "SanBarth":   float(base.get("prod_sanbarth",   0) or 0),
            "JasmineS": float(base.get("prod_jasmines", 0) or 0),
            "Westmore": float(base.get("prod_westmore", 0) or 0),
            "Duke":     float(base.get("prod_duke",     0) or 0),
            "Starturn": float(base.get("prod_starturn", 0) or 0),
            "PGM":      float(base.get("prod_pgm",      0) or 0),
        }
        _max_prod_opt = max(_prod_rates_opt.values()) if _prod_rates_opt else 1.0
        weighted_overflow_penalty = 0.0
        for sn in storage_names:
            _rf_sn  = risk_by_storage.get(sn, 0.0)
            _rate_n = _prod_rates_opt.get(sn, 0.0) / max(1.0, _max_prod_opt)
            # Penalty = fraction_above_critical × production_weight × 25 pts each
            weighted_overflow_penalty += _rf_sn * _rate_n * 25.0
        weighted_overflow_penalty = min(40.0, weighted_overflow_penalty)
        overflow_score = max(
            0.0,
            100.0
            - spill_penalty
            - risk_penalty
            - borderline_penalty
            - max_risk_penalty
            - max_borderline_penalty
            - hotspot_penalty
            - weighted_overflow_penalty,
        )

        # ── Utilisation Objective: avoid idle daughters when stock exists ──
        idle_sts = {
            "IDLE_A", "IDLE_B", "WAITING_BERTH_A", "WAITING_BERTH_B",
            "WAITING_STOCK", "WAITING_DEAD_STOCK", "WAITING_RETURN_STOCK",
            "WAITING_MOTHER_CAPACITY", "WAITING_MOTHER_RETURN"
        }
        vessel_cols = [vn for vn in S.get("vessel_names", []) if vn in tl_df.columns]
        total_slots = 0
        idle_slots = 0
        for vn in vessel_cols:
            col = tl_df[vn]
            total_slots += len(col)
            idle_slots += int(col.isin(idle_sts).sum())
        idle_frac = idle_slots / max(1, total_slots)

        # Penalize idle time specifically when any storage has evac-capable stock.
        min_vcap = min(getattr(mod, "VESSEL_CAPACITIES", {"_": 85000}).values())
        evac_threshold = float(getattr(mod, "DEAD_STOCK_FACTOR", 1.75)) * float(min_vcap)
        if all(sn in tl_df.columns for sn in storage_names) and vessel_cols:
            stock_available = (tl_df[storage_names].max(axis=1) >= evac_threshold)
            any_idle = pd.Series(False, index=tl_df.index)
            for vn in vessel_cols:
                any_idle = any_idle | tl_df[vn].isin(idle_sts)
            idle_with_stock_frac = float((stock_available & any_idle).mean())
        else:
            idle_with_stock_frac = 1.0

        util_base = max(0.0, 100.0 * (1.0 - idle_frac * 1.9))
        idle_with_stock_penalty = min(35.0, idle_with_stock_frac * 100.0 * 0.45)
        idle_score = max(0.0, util_base - idle_with_stock_penalty)

        # ── Fair Allocation Objective: service all storage locations fairly ─
        prod_map = {
            "SanBarth": float(base.get("prod_sanbarth", 0) or 0),
            "JasmineS": float(base.get("prod_jasmines", 0) or 0),
            "Westmore": float(base.get("prod_westmore", 0) or 0),
            "Duke": float(base.get("prod_duke", 0) or 0),
            "Starturn": float(base.get("prod_starturn", 0) or 0),
            "PGM": float(base.get("prod_pgm", 0) or 0),
        }
        total_prod = sum(prod_map.values())
        if total_prod <= 0:
            target_share = {sn: 1.0 / len(storage_names) for sn in storage_names}
        else:
            target_share = {sn: prod_map[sn] / total_prod for sn in storage_names}

        load_counts = {sn: 0 for sn in storage_names}
        if not log_df.empty and "Event" in log_df.columns and "Detail" in log_df.columns:
            _loads = log_df[log_df["Event"] == "LOADING_START"]
            if not _loads.empty:
                _stor = _loads["Detail"].astype(str).str.extract(r"\|\s*([A-Za-z]+):")[0]
                _stor = _stor[_stor.isin(storage_names)]
                if not _stor.empty:
                    vc = _stor.value_counts()
                    for sn in storage_names:
                        load_counts[sn] = int(vc.get(sn, 0))

        total_load_events = sum(load_counts.values())
        if total_load_events <= 0:
            fairness_score = 0.0
        else:
            actual_share = {sn: load_counts[sn] / total_load_events for sn in storage_names}
            # L1-distance fairness (0=perfect match to target share, 1=max mismatch)
            mismatch = 0.5 * sum(abs(actual_share[sn] - target_share[sn]) for sn in storage_names)
            fairness_score = max(0.0, 100.0 * (1.0 - mismatch))
            # Extra penalty for neglecting risky/borderline storages entirely.
            neglected_penalty = 0.0
            for sn in storage_names:
                if load_counts[sn] == 0 and (risk_by_storage.get(sn, 0) > 0.15 or borderline_by_storage.get(sn, 0) > 0.20):
                    _sev = max(risk_by_storage.get(sn, 0.0), borderline_by_storage.get(sn, 0.0))
                    neglected_penalty += 12.0 + (14.0 * _sev)
            fairness_score = max(0.0, fairness_score - min(35.0, neglected_penalty))

        # ── Secondary efficiency metrics ─────────────────────────────────
        n_exports = S.get("exports", 0)
        export_score = min(100.0, n_exports * 20.0)   # 5 exports = 100

        # Avg cycle hours
        cyc_ev = log_df[log_df["Event"] == "ARRIVED_LOADING_POINT"] if not log_df.empty else pd.DataFrame()
        if len(cyc_ev) >= 2:
            times = sorted(cyc_ev["Time"].tolist())
            gaps  = [(pd.Timestamp(times[i+1]) - pd.Timestamp(times[i])).total_seconds()/3600
                     for i in range(len(times)-1) if
                     (pd.Timestamp(times[i+1]) - pd.Timestamp(times[i])).total_seconds()/3600 < 120]
            avg_cycle = math.fsum(gaps)/len(gaps) if gaps else 48.0
        else:
            avg_cycle = 48.0
        turnaround_score = max(0.0, 100.0 - max(0.0, avg_cycle - 24.0) * 2.0)

        # ── MTO Objective: reward scenarios where MTO reduces BIA wait time ─
        # Measures: number of MTO cycles fired, fraction of days where vessels
        # were freed by MTO vs days where they were stuck. Higher = better.
        _mto_nom_events = log_df[log_df["Event"] == "MTO_TRANSIENT_NOMINATED"] if not log_df.empty else pd.DataFrame()
        _mto_dis_events = log_df[log_df["Event"] == "MTO_DISCHARGE_TO_TRANSIENT"] if not log_df.empty else pd.DataFrame()
        n_mto_cycles    = len(_mto_nom_events)
        n_mto_freed     = len(_mto_dis_events)
        # Loading gaps: days where no LOADING_START event occurred at any storage
        _sim_days_count = S.get("sim_days", 14)
        if not log_df.empty and "Day" in log_df.columns:
            _days_with_loading = log_df[log_df["Event"] == "LOADING_START"]["Day"].nunique()
            _loading_gap_days  = max(0, _sim_days_count - _days_with_loading)
        else:
            _loading_gap_days = _sim_days_count
        # Score: reward MTO cycles (each frees a vessel to reload), penalise loading gaps
        _mto_base = min(100.0, n_mto_freed * 15.0)   # up to 100 pts from 7 freed vessels
        _loading_gap_penalty = min(40.0, _loading_gap_days * 5.0)
        mto_score = max(0.0, _mto_base - _loading_gap_penalty)

        # Composite objective — MTO weighted at 5%; reduces idle_score weight by 1%
        # when MTO is enabled so the two objectives don't double-count idle time.
        # ── Mother fill-rate score ─────────────────────────────────────
        # Penalise runs where primary mothers stayed below 40% of their
        # export trigger for extended periods (mother starvation).
        mother_fill_score = 0.0
        mother_names_opt = [getattr(mod, "MOTHER_PRIMARY_NAME", "Bryanston"),
                             getattr(mod, "MOTHER_SECONDARY_NAME", "GreenEagle")]
        _trig_by_mn = getattr(mod, "MOTHER_EXPORT_TRIGGER_BY_NAME",
                              {mn: getattr(mod, "MOTHER_EXPORT_TRIGGER", 450_000)
                               for mn in mother_names_opt})
        _mf_cols = [f"{mn}_bbl" for mn in mother_names_opt
                    if f"{mn}_bbl" in tl_df.columns]
        if _mf_cols and not tl_df.empty:
            _mf_scores = []
            for _mn in mother_names_opt:
                _col = f"{_mn}_bbl"
                if _col not in tl_df.columns:
                    continue
                _trig = float(_trig_by_mn.get(_mn, getattr(mod, "MOTHER_EXPORT_TRIGGER", 450_000)))
                if _trig <= 0:
                    continue
                _fill_frac = (tl_df[_col].astype(float) / _trig).clip(upper=1.0)
                _deficit   = (0.40 - _fill_frac).clip(lower=0.0)
                _mf_scores.append(max(0.0, 100.0 - float(_deficit.mean()) * 250.0))
            mother_fill_score = sum(_mf_scores) / max(1, len(_mf_scores))

        # ── BIA wait score ─────────────────────────────────────────────
        # Penalise runs where vessels spent many hours queued at BIA
        # (WAITING_BERTH_B) beyond the 4h operational baseline.
        bia_wait_score = 100.0
        _bia_sts = {"WAITING_BERTH_B", "WAITING_MOTHER_CAPACITY", "WAITING_MOTHER_RETURN"}
        if vessel_cols and not tl_df.empty:
            _bia_slots = sum(
                int(tl_df[vn].isin(_bia_sts).sum())
                for vn in vessel_cols if vn in tl_df.columns
            )
            _bia_hours = _bia_slots * 0.5   # each slot = 0.5 h
            _bia_baseline = max(1, len(vessel_cols)) * 4.0   # 4h baseline per vessel
            _bia_excess = max(0.0, _bia_hours - _bia_baseline)
            bia_wait_score = max(0.0, 100.0 - _bia_excess * 0.8)

        # ── Composite (weights sum to 1.00) ───────────────────────────
        composite = (
            crash_score      * 0.25
            + overflow_score * 0.35
            + idle_score     * 0.10
            + fairness_score * 0.08
            + mother_fill_score * 0.12
            + bia_wait_score    * 0.06
            + export_score      * 0.02
            + turnaround_score  * 0.02
        )

        # ── Bottlenecks ──────────────────────────────────────────────────
        bottlenecks = []
        if drawdown_pct < 25: bottlenecks.append("Slow stock crash-down")
        if idle_with_stock_frac > 0.20: bottlenecks.append("Idle daughters while stock available")
        if spilled > 0:       bottlenecks.append(f"Storage overflow ({spilled:,.0f} bbl)")
        if risk_avg > 0.25:   bottlenecks.append("Sustained high-risk storage levels")
        if fairness_score < 55: bottlenecks.append("Unfair storage allocation pattern")
        if avg_cycle > 60:    bottlenecks.append("Long cycle times")
        if n_exports == 0:    bottlenecks.append("No exports completed")
        if _loading_gap_days > 2: bottlenecks.append(f"Loading gaps on {_loading_gap_days} day(s) — consider enabling/tuning MTO")
        if mother_fill_score < 50: bottlenecks.append(f"Mother starvation — primary mothers below 40% fill for extended periods (score {mother_fill_score:.0f}/100)")
        if bia_wait_score < 60: bottlenecks.append(f"Excess BIA queue time — vessels waiting too long at Point B (score {bia_wait_score:.0f}/100)")

        # ── Vessel utilisation ───────────────────────────────────────────
        # Statuses that represent genuinely productive work:
        #   - LOADING / berthing at Point A (full-rate cargo loading)
        #   - Sailing outbound with cargo (SAILING_AB family)
        #   - Discharging at BIA (BERTHING_B, HOSE_CONNECT_B, DISCHARGING, CAST_OFF_B)
        #   - Returning to storage (SAILING_BA family)
        # Explicitly NOT productive (treated as idle/non-utilised):
        #   - PF_LOADING / PF_SWAP  — vessel passively accumulates at Ibom buoy
        #     at 165 bbl/hr; this is effectively idle until a real voyage starts
        #   - SAILING_B_TO_F / ARRIVED_IBOM — transit to the Ibom buoy
        #   - All standard idle/waiting states already in idle_sts
        _pf_passive = {"PF_LOADING", "PF_SWAP", "SAILING_B_TO_F", "ARRIVED_IBOM"}
        _non_util_sts = idle_sts | {"IDLE_A", "IDLE_B"} | _pf_passive
        vu = {}
        for vn in S.get("vessel_names", []):
            if vn in tl_df.columns:
                col = tl_df[vn]
                # Only count slots where the vessel is doing real productive work
                active = (~col.isin(_non_util_sts)).sum()
                vu[vn] = round(100.0 * active / max(1, len(col)), 1)

        # ── Storage utilisation ──────────────────────────────────────────
        su = {}
        for sn in ["SanBarth","JasmineS","Westmore","Duke","Starturn","PGM"]:
            if sn in tl_df.columns:
                col = tl_df[sn].dropna()
                cap_col = f"{sn}_cap"
                cap = (400_000 if sn == "SanBarth" else
                       290_000 if sn == "JasmineS" else
                       270_000 if sn == "Westmore" else 228_000)
                su[sn] = {
                    "avg_pct":      round(100.0 * col.mean() / cap, 1),
                    "peak_pct":     round(100.0 * col.max()  / cap, 1),
                    "overflow_bbl": int(S.get("spill_by_storage",{}).get(sn, 0)),
                }

        return dict(
            composite=round(composite,2),
            throughput_score=round(crash_score,2),
            idle_score=round(idle_score,2),
            overflow_score=round(overflow_score,2),
            fairness_score=round(fairness_score,2),
            mother_fill_score=round(mother_fill_score,2),
            bia_wait_score=round(bia_wait_score,2),
            export_score=round(export_score,2),
            turnaround_score=round(turnaround_score,2),
            mto_score=round(mto_score,2),
            mto_cycles=n_mto_cycles,
            mto_freed=n_mto_freed,
            loading_gap_days=_loading_gap_days,
            total_loaded_bbl=int(total_loaded),
            total_exported_bbl=float(total_exported),
            total_spilled_bbl=float(spilled),
            stock_drawdown_bbl=float(drawdown_bbl),
            stock_drawdown_pct=round(drawdown_pct,2),
            early_drawdown_pct=round(early_drawdown_pct,2),
            stock_risk_frac=round(risk_avg,4),
            idle_with_stock_frac=round(idle_with_stock_frac,4),
            avg_cycle_hours=round(avg_cycle,1),
            bottlenecks=bottlenecks,
            vessel_utilisation=vu,
            storage_utilisation=su,
        )

    all_results = []
    _opt_start_wall = time.monotonic()
    _budget_exhausted = False
    try:
        for rank, (dsf, pft, expw, (bstart, bend), mto_parcels) in enumerate(_grid, 1):
            # Time-budget guard: stop sweeping if wall-clock budget is exceeded.
            # We check before each scenario so partial results are still returned.
            if time.monotonic() - _opt_start_wall > _OPT_TIME_BUDGET_SECS:
                _budget_exhausted = True
                break
            try:
                # Pass scenario params via opt_params_json — run_sim applies and
                # restores them safely, no direct module mutation needed.
                _opt_scenario_json = json.dumps(dict(
                    dead_stock_factor        = dsf,
                    ibom_trigger_bbl         = int(pft),
                    export_sail_window_start = int(expw),
                    berthing_start           = int(bstart),
                    berthing_end             = int(bend),
                ))
                _log, _tl, S = run_sim(
                    sim_days            = base.get("sim_days", 14),
                    sanbarth              = base.get("sanbarth",   320_000),   # 80% of 400k
                    jasmines            = base.get("jasmines", 232_000),   # 80% of 290k
                    westmore            = base.get("westmore", 216_000),   # 80% of 270k
                    duke                = base.get("duke",      72_000),   # 80% of 90k
                    starturn            = base.get("starturn",  56_000),   # 80% of 70k
                    pgm                 = base.get("pgm",       22_400),   # 80% of 28k capacity
                    bryanston           = base.get("bryanston", 450_000),
                    alkebulan           = base.get("alkebulan",       0),
                    greeneagle          = base.get("greeneagle",300_000),
                    prod_sanbarth         = base.get("prod_sanbarth",  2500),
                    prod_jasmines       = base.get("prod_jasmines",2500),
                    prod_westmore       = base.get("prod_westmore",2500),
                    prod_duke           = base.get("prod_duke",    500),
                    prod_starturn       = base.get("prod_starturn",350),
                    prod_pgm            = base.get("prod_pgm",     80),
                    prod_ibom           = base.get("prod_ibom",    165),
                    vessel_states_json  = None,
                    tide_csv_bytes      = _tide_bytes,
                    sim_start_date      = _start_iso,
                    _sim_version        = f"opt_{rank}",
                    opt_params_json     = _opt_scenario_json,
                    multiple_transient_operation = _mto_enabled_live,
                    mto_max_parcels              = int(mto_parcels),
                )
                sc = _score(S, _log, _tl)
                all_results.append(dict(
                    rank=rank,
                    label=(f"dsf={dsf:.2f} pft={pft//1000}k expw={expw}h "
                           f"b={bstart}-{bend}"
                           + (f" mto_p={mto_parcels}" if _mto_enabled_live else "")),
                    params=dict(dead_stock_factor=dsf, ibom_trigger_bbl=int(pft),
                                export_sail_window_start=int(expw),
                                berthing_start=int(bstart), berthing_end=int(bend),
                                mto_max_parcels=int(mto_parcels)),
                    score=sc,
                ))
            except Exception:
                continue
    finally:
        _restore()

    if not all_results:
        # Fallback: one default run
        _log, _tl, S = run_sim(
            sim_days=base.get("sim_days",14),
            sanbarth=base.get("sanbarth",320_000), jasmines=base.get("jasmines",232_000),
            westmore=base.get("westmore",216_000), duke=base.get("duke",72_000),
            starturn=base.get("starturn",56_000), pgm=base.get("pgm",22_400),
            bryanston=base.get("bryanston",450_000), alkebulan=0,
            greeneagle=base.get("greeneagle",300_000),
            prod_sanbarth=base.get("prod_sanbarth",2500), prod_jasmines=base.get("prod_jasmines",2500),
            prod_westmore=base.get("prod_westmore",2500), prod_duke=base.get("prod_duke",500),
            prod_starturn=base.get("prod_starturn",350), prod_pgm=base.get("prod_pgm",80),
            prod_ibom=base.get("prod_ibom",165),
            vessel_states_json=None, tide_csv_bytes=_tide_bytes,
            sim_start_date=_start_iso, _sim_version="opt_fallback",
        )
        sc = _score(S, _log, _tl)
        all_results.append(dict(rank=1, label="default",
            params=dict(dead_stock_factor=1.75, ibom_trigger_bbl=65000,
                        export_sail_window_start=6, berthing_start=6, berthing_end=18),
            score=sc))

    all_results.sort(key=lambda r: r["score"]["composite"], reverse=True)
    for i, r in enumerate(all_results, 1):
        r["rank"] = i

    best = all_results[0]
    _wall_elapsed = round(time.monotonic() - _opt_start_wall, 1)
    best_slim = dict(params=best["params"], score=best["score"],
                     rank=best["rank"], label=best["label"],
                     scenarios_evaluated=len(all_results),
                     grid_total=len(_grid),
                     budget_exhausted=_budget_exhausted,
                     wall_seconds=_wall_elapsed)

    rows = []
    for r in all_results:
        sc = r["score"]; pr = r["params"]
        rows.append({
            "Rank": r["rank"], "Score": sc["composite"],
            "Stock Drawdown": sc["throughput_score"], "Fleet Util": sc["idle_score"],
            "Storage Safety": sc["overflow_score"], "Fair Allocation": sc["fairness_score"],
            "Mother Fill": sc.get("mother_fill_score", 0), "BIA Wait": sc.get("bia_wait_score", 0),
            "Export": sc["export_score"], "Turnaround": sc["turnaround_score"],
            "Loaded (bbl)": sc["total_loaded_bbl"],
            "Spilled (bbl)": sc["total_spilled_bbl"],
            "Avg Cycle (h)": sc["avg_cycle_hours"],
            "dead_stock_x": pr["dead_stock_factor"],
            "pf_trigger_k": pr["ibom_trigger_bbl"] // 1000,
            "exp_window_h": pr["export_sail_window_start"],
            "berth_start_h": pr["berthing_start"],
            "berth_end_h": pr["berthing_end"],
        })
    tbl = pd.DataFrame(rows)
    return json.dumps(best_slim), tbl.to_json(orient="records")


# =============================================================================
# ── GOOGLE SHEETS ─────────────────────────────────────────────────────────────
# =============================================================================

def _gs_client(creds_json):
    """
    Create an authenticated gspread client from a service account JSON string.
    Tries three methods in order to support gspread v3 through v6+:
      1. gspread.service_account_from_dict()  — correct for gspread >= 5.x
      2. gspread.Client(auth=creds) + login() — gspread v6 fallback
      3. gspread.authorize(creds)             — gspread < 5 legacy
    """
    import gspread
    from google.oauth2.service_account import Credentials
    _info = json.loads(creds_json)
    _scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive.readonly",
    ]
    # Method 1: service_account_from_dict — cleanest, works gspread 5+
    if hasattr(gspread, "service_account_from_dict"):
        return gspread.service_account_from_dict(_info, scopes=_scopes)
    # Method 2: Client + login — gspread 6 when service_account_from_dict missing
    _creds = Credentials.from_service_account_info(_info, scopes=_scopes)
    try:
        _gc = gspread.Client(auth=_creds)
        _gc.login()   # REQUIRED in gspread v6 — authenticates the HTTP session
        return _gc
    except (TypeError, AttributeError):
        pass
    # Method 3: legacy authorize — gspread < 5
    return gspread.authorize(_creds)


def _gs_raw_to_dicts(all_vals, known_keys):
    """
    Convert a list-of-lists (from get_all_values) into a list of dicts.
    Finds the header row by scanning the first 8 rows for a row that contains
    any of the known_keys.  All header values are normalised: lower-cased,
    stripped of whitespace and everything after the first newline.
    Returns (list_of_dicts, header_row_index).
    """
    header_idx = None
    for i, row in enumerate(all_vals[:8]):
        cleaned = {str(v).split("\n")[0].strip().lower() for v in row if v}
        if cleaned & known_keys:
            header_idx = i
            break
    if header_idx is None:
        return [], None
    raw_headers = all_vals[header_idx]
    headers = [str(h).split("\n")[0].strip().lower() for h in raw_headers]
    result = []
    for row in all_vals[header_idx + 1:]:
        if not any(str(v).strip() for v in row):
            continue   # skip fully empty rows
        d = {}
        for col_i, h in enumerate(headers):
            if h:
                d[h] = row[col_i] if col_i < len(row) else ""
        result.append(d)
    return result, header_idx


def gs_load_volumes(sheet_id, creds_json):
    """
    Load latest data row from the 'volumes' tab using get_all_values() only —
    no get_all_records(), which has breaking API changes across gspread versions.
    Returns dict mapping app param names to integer values.
    """
    try:
        gc = _gs_client(creds_json)
        try:
            ws = gc.open_by_key(sheet_id).worksheet("volumes")
        except Exception:
            ws = gc.open_by_key(sheet_id).sheet1

        all_vals = ws.get_all_values()
        if not all_vals:
            return {}

        KNOWN = {"sanbarth_bbl", "jasmines_bbl", "bryanston_bbl",
                 "timestamp", "sim_days", "prod_sanbarth_bph"}
        rows, hdr_idx = _gs_raw_to_dicts(all_vals, KNOWN)
        if not rows:
            return {}

        # Use the last non-empty row that has at least one numeric volume field
        MAPPING = {
            "sanbarth_bbl":       "sanbarth",
            "jasmines_bbl":     "jasmines",
            "westmore_bbl":     "westmore",
            "duke_bbl":         "duke",
            "starturn_bbl":     "starturn",
            "pgm_bbl":          "pgm",
            "bryanston_bbl":    "bryanston",
            "greeneagle_bbl":  "greeneagle",
            "alkebulan_bbl":   "alkebulan",
            "prod_sanbarth_bph":  "prod_sanbarth",
            "prod_jasmines_bph":"prod_jasmines",
            "prod_westmore_bph":"prod_westmore",
            "prod_duke_bph":    "prod_duke",
            "prod_starturn_bph":"prod_starturn",
            "prod_pgm_bph":     "prod_pgm",
            "prod_ibom_bph":    "prod_ibom",
            "sim_days":         "sim_days",
        }
        # Find last row with at least one recognisable volume value
        latest = None
        for row in reversed(rows):
            if any(str(row.get(k,"")).replace(",","").strip().lstrip("-").replace(".","",1).isdigit()
                   for k in MAPPING):
                latest = row
                break
        if latest is None:
            return {}

        out = {}
        for sheet_key, app_key in MAPPING.items():
            raw = str(latest.get(sheet_key, "")).replace(",", "").strip()
            if raw:
                try:
                    out[app_key] = int(float(raw))
                except (ValueError, OverflowError):
                    pass

        ts = str(latest.get("timestamp", "")).strip()
        if ts and ts != "0":
            out["_timestamp"] = ts
        return out

    except ImportError:
        st.sidebar.warning("Install gspread: pip install gspread google-auth")
        st.warning("⚠️ Missing packages: run `pip install gspread google-auth`.", icon="⚠️")
    except Exception as e:
        st.sidebar.error(f"Sheets (volumes) error: {e}")
        st.error(f"❌ Google Sheets (volumes) error: {e}", icon="❌")
    return {}


def gs_load_fleet(sheet_id, creds_json):
    """
    Load 'fleet' tab using get_all_values() only.
    Returns DataFrame with vessel | status | location | cargo_bbl | notes | mother_status.
    One row per vessel — latest timestamp row wins.
    """
    KNOWN_VESSELS = {
        "sherlock","laphroaig","rathbone","santamonica","bedford",
        "balham","woodstock","bagshot","watson","berners",
    }
    try:
        gc = _gs_client(creds_json)
        try:
            ws = gc.open_by_key(sheet_id).worksheet("fleet")
        except Exception:
            return pd.DataFrame()

        all_vals = ws.get_all_values()
        if not all_vals:
            return pd.DataFrame()

        rows, hdr_idx = _gs_raw_to_dicts(all_vals, {"vessel", "status", "cargo_bbl"})
        if not rows:
            return pd.DataFrame()

        # Keep only rows whose vessel column matches a known vessel name
        clean_rows = []
        for row in rows:
            vname = str(row.get("vessel", "")).strip()
            if vname.lower() in KNOWN_VESSELS:
                # Preserve original capitalisation for the vessel name
                row["vessel"] = vname
                clean_rows.append(row)
        if not clean_rows:
            return pd.DataFrame()

        df = pd.DataFrame(clean_rows)
        # Ensure required columns exist
        for col in ["status", "location", "notes", "mother_status",
                    "already_transferred_bbl", "target_mother"]:
            if col not in df.columns:
                df[col] = ""
        # Latest-timestamp-wins per vessel
        if "timestamp" in df.columns:
            df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
            df = df.sort_values("timestamp", na_position="first")
            df = df.groupby("vessel", as_index=False).last()
        # Numeric coercion
        for col in ["cargo_bbl", "already_transferred_bbl"]:
            if col in df.columns:
                df[col] = pd.to_numeric(
                    df[col].astype(str).str.replace(",", ""), errors="coerce"
                ).fillna(0).astype(int)
        for col in ["status", "location", "notes", "mother_status"]:
            df[col] = df[col].fillna("").astype(str).str.strip()
        return df

    except ImportError:
        pass
    except Exception as e:
        st.sidebar.error(f"Sheets (fleet) error: {e}")
        st.error(f"❌ Google Sheets (fleet) error: {e}", icon="❌")
    return pd.DataFrame()


# =============================================================================
# ── CAPACITY RECOMMENDATION ENGINE ───────────────────────────────────────────
# =============================================================================

def _bia_stall_recommendation(log_df, S, params, mod):
    """Analyse Point-B (BIA) discharge-side stalls and, if the mothers are the
    binding constraint, return recommendation dicts (third mother vessel + levers).

    A "stall" here is a laden daughter / MTO transient held in a waiting state at
    BIA (WAITING_BERTH_B / WAITING_MOTHER_CAPACITY / WAITING_FAIRWAY) unable to
    offload into a mother.  Unlike storage overflow (a source-side symptom), this
    is a sink-side symptom: the two primary mothers cannot absorb cargo fast
    enough, so vessels sit full at the berths.  This can occur even with zero
    storage overflow, so it is evaluated independently of the spill checks.

    Returns [] when stalls are negligible.
    """
    if log_df is None or log_df.empty or "Event" not in log_df.columns:
        return []
    import pandas as _pd
    import re as _re
    sim_days = params["sim_days"]
    df = log_df.copy()
    df["_t"] = _pd.to_datetime(df["Time"], errors="coerce")
    df = df.sort_values("_t")

    WAIT = {"WAITING_BERTH_B", "WAITING_MOTHER_CAPACITY", "WAITING_FAIRWAY"}
    excluded = {"ZeeZee"}

    # Reconstruct per-vessel laden-idle spans from the event log: a laden vessel
    # held in a waiting state at BIA, ended by its next discharge / cast-off / load.
    stall_spans = []
    vessels_affected = set()
    for ves, sub in df.groupby("Vessel"):
        if str(ves) in excluded or str(ves) == "nan":
            continue
        sub = sub.sort_values("_t")
        wait_start = None
        for _, r in sub.iterrows():
            ev = r["Event"]; det = str(r["Detail"])
            is_wait = ev in WAIT or ev == "MTO_PARCEL_LIMIT_REACHED"
            laden_hint = ("bbl on board" in det or "offload" in det.lower()
                          or ev in {"MTO_PARCEL_LIMIT_REACHED", "WAITING_MOTHER_CAPACITY"}
                          or _pd.notna(r.get("Mother")))
            if is_wait and laden_hint:
                if wait_start is None:
                    wait_start = r["_t"]
            elif ev in {"DISCHARGE_START", "CAST_OFF_COMPLETE_B", "LOADING_START"}:
                if wait_start is not None:
                    dur = (r["_t"] - wait_start).total_seconds() / 86400.0
                    if dur >= 1.0:
                        stall_spans.append((str(ves), dur)); vessels_affected.add(str(ves))
                    wait_start = None
        if wait_start is not None:
            dur = (df["_t"].max() - wait_start).total_seconds() / 86400.0
            if dur >= 1.0:
                stall_spans.append((str(ves), dur)); vessels_affected.add(str(ves))

    total_stall_days = sum(d for _, d in stall_spans)
    worst_stall = max((d for _, d in stall_spans), default=0.0)
    n_aff = len(vessels_affected)

    # Peak parcel the mothers must absorb (largest daughter/MTO cargo) — used to
    # size a third mother sensibly.
    peak_parcel = 0.0
    for d in df[df["Event"] == "LOADING_COMPLETE"]["Detail"]:
        m = _re.search(r"Cargo:\s*([\d,]+)", str(d))
        if m:
            peak_parcel = max(peak_parcel, int(m.group(1).replace(",", "")))
    for d in df[df["Event"].astype(str).str.contains("MTO", na=False)]["Detail"]:
        m = _re.search(r"([\d,]{5,})\s*bbl", str(d))
        if m:
            peak_parcel = max(peak_parcel, int(m.group(1).replace(",", "")))

    # Only recommend when discharge-side stalls are genuinely material.
    if total_stall_days < 3.0 and worst_stall < 4.0:
        return []

    if worst_stall >= 8.0 or total_stall_days >= 25.0:
        sev = 3
    elif worst_stall >= 5.0 or total_stall_days >= 12.0:
        sev = 2
    else:
        sev = 1

    # Suggested third-mother capacity: comparable to the larger existing primary.
    try:
        _cap_by = dict(getattr(mod, "MOTHER_CAPACITY_BY_NAME", {}) or {})
    except Exception:
        _cap_by = {}
    _bry_cap = _cap_by.get(getattr(mod, "MOTHER_PRIMARY_NAME", "Bryanston"), 550_000)
    _ge_cap  = _cap_by.get(getattr(mod, "MOTHER_SECONDARY_NAME", "GreenEagle"), 750_000)
    _ref_cap = max(_bry_cap, _ge_cap)
    MOTHER_SIZE_BANDS = [350_000, 450_000, 550_000, 680_000, 750_000, 1_000_000]
    suggested_cap = min(MOTHER_SIZE_BANDS, key=lambda s: abs(s - _ref_cap))

    body = (
        f"Laden vessels are being held at BIA unable to discharge: "
        f"<span class='hl-yellow'>{n_aff} vessel(s)</span> experienced laden-idle "
        f"stalls totalling <span class='hl-yellow'>{total_stall_days:.0f} vessel-days</span>, "
        f"with the worst single stall lasting "
        f"<span class='hl-yellow'>{worst_stall:.1f} days</span>. "
        f"This is a <b>discharge-side (sink) constraint</b>: the two primary mothers "
        f"(<b>{getattr(mod,'MOTHER_PRIMARY_NAME','Bryanston')}</b> and "
        f"<b>{getattr(mod,'MOTHER_SECONDARY_NAME','GreenEagle')}</b>) cannot absorb cargo "
        f"as fast as the fleet delivers it — especially while one mother is away on an "
        f"export voyage (hours each way), during which no daughter may "
        f"berth her. Adding lifting capacity (more daughters) would <i>not</i> help and "
        f"could worsen queueing, because the berths — not the daughters — are the bottleneck.<br><br>"
        f"<b>Recommended: add a third primary mother vessel of "
        f"~<span class='hl-blue'>{suggested_cap:,} bbl</span></b> (comparable to the "
        f"existing primaries so it can take full export loads and absorb the peak parcel "
        f"of ~{peak_parcel:,.0f} bbl that currently queues). A third discharge berth lets "
        f"the fleet keep offloading while either other mother is documenting, sailing to, "
        f"or returning from the export terminal — directly removing the round-trip gap that "
        f"strands laden vessels at BIA. Confirm the new mother's export-trigger volume and "
        f"berth-permission routing against operational constraints."
    )
    rec = dict(
        type="third_mother", severity=sev,
        title=f"🛳️ Add a 3rd mother vessel (~{suggested_cap:,} bbl) — BIA discharge is the bottleneck",
        body=body,
        metric=(f"{worst_stall:.1f}-day worst stall; {total_stall_days:.0f} vessel-days idle "
                f"→ 3rd berth ~{suggested_cap:,} bbl"),
    )
    lever = dict(
        type="discharge_levers", severity=1,
        title="📋 Discharge-side levers (if a third mother is not immediately feasible)",
        body=(
            "<b>1. Stagger export voyages:</b> avoid documenting/sailing both primaries' "
            "exports close together — keep at least one primary at BIA to receive cargo at "
            "all times.<br>"
            "<b>2. Faster export turnaround:</b> any reduction in the export round-trip "
            "(documentation, sail, hose, in-port pumping, return, fendering) shortens the "
            "window during which a mother cannot receive daughters.<br>"
            "<b>3. Raise mother-vessel intake throughput:</b> a faster discharge pump "
            "rate clears the intermediate buffer sooner, freeing it to absorb daughters and "
            "reducing the queue that forms when a primary is away.<br>"
            "<b>4. Smaller / fewer MTO consolidations:</b> very large MTO transients (130k+ "
            "bbl) occupy a berth for a long single discharge and are hard to fit when a "
            "primary is near capacity; smaller parcels berth more flexibly."
        ),
        metric=None,
    )
    return [rec, lever]


def capacity_recommendations(S, params, tl_df, mod, log_df=None):
    """
    Analyse simulation results and produce structured fleet/capacity recommendations.
    Returns list of dicts: {type, severity, title, body, metric}
    severity: 0=ok  1=low  2=medium  3=high

    When `log_df` is supplied, also analyses BIA (Point B) discharge-side stalls —
    laden vessels held at the mothers' berths unable to offload — and, if the
    discharge side is the binding constraint, recommends adding a third mother
    vessel (with a suggested capacity), mirroring the storage-side recommendations.
    """
    recs = []

    total_spilled = S["spilled"]
    ovf_events    = S["ovf_events"]
    sim_days      = params["sim_days"]
    prod_sanbarth   = params.get("prod_sanbarth",   mod.PRODUCTION_RATE_BPH)
    prod_jasmines = params.get("prod_jasmines", mod.PRODUCTION_RATE_BPH)
    prod_westmore = params.get("prod_westmore", mod.WESTMORE_PRODUCTION_RATE_BPH)
    prod_duke     = params.get("prod_duke",     mod.DUKE_PRODUCTION_RATE_BPH)
    prod_starturn = params.get("prod_starturn", mod.STARTURN_PRODUCTION_RATE_BPH)
    prod_pgm      = params.get("prod_pgm",      getattr(mod, "PGM_PRODUCTION_RATE_BPH", 80))
    prod_ibom   = params.get("prod_ibom",   getattr(mod, "IBOM_LOAD_RATE_BPH",
                             getattr(mod, "POINT_F_LOAD_RATE_BPH", 165)))
    total_prod_bpd = (prod_sanbarth + prod_jasmines + prod_westmore + prod_duke + prod_starturn + prod_pgm + prod_ibom) * 24

    loadings     = S["loadings"]
    total_loaded = S["loaded"]

    # ── Discharge-side (BIA) stall analysis ───────────────────────────────────
    # Evaluated up-front because a mother-berth bottleneck can strand laden
    # vessels even when there is zero storage overflow (the no-overflow branch
    # below would otherwise short-circuit before these are added).
    bia_recs = _bia_stall_recommendation(log_df, S, params, mod)

    # ── No overflow ───────────────────────────────────────────────────────────
    if total_spilled <= 0 and ovf_events == 0:
        if bia_recs:
            # Storage side is healthy, but the discharge side is the constraint.
            recs.append(dict(
                type="ok", severity=0,
                title="✅ No overflow — storage/lifting side is sufficient",
                body=(
                    f"The simulation ran {sim_days} days with total production of "
                    f"<span class='hl-green'>{total_prod_bpd:,.0f} bbl/day</span> and recorded "
                    f"zero storage overflow — the daughter fleet is clearing production at "
                    f"source. However, vessels are being delayed on the <b>discharge side at "
                    f"BIA</b> (see below): the constraint is mother-berth capacity, not lifting "
                    f"capacity or storage."
                ),
                metric=None,
            ))
            recs.extend(bia_recs)
            return recs
        recs.append(dict(
            type="ok", severity=0,
            title="✅ No overflow — current fleet is sufficient",
            body=(
                f"The simulation ran {sim_days} days with total production of "
                f"<span class='hl-green'>{total_prod_bpd:,.0f} bbl/day</span> "
                f"across all storage points and recorded zero overflow or spill. "
                f"The fleet completed <b>{loadings} lifts</b> clearing all production. "
                f"No additional assets are required at current production rates."
            ),
            metric=None,
        ))
        return recs

    # ── Derived metrics ───────────────────────────────────────────────────────
    spill_per_day     = total_spilled / sim_days
    spill_pct         = total_spilled / max(total_prod_bpd * sim_days, 1) * 100
    avg_cargo         = total_loaded / loadings if loadings else mod.DAUGHTER_CARGO_BBL
    lifts_per_day     = loadings / sim_days

    # Approximate round-trip cycle time in hours
    # load(12) + doc(4) + cast-off(0.2) + sail SanBarth→BIA(8) + berth(0.5) + hose(2) + disch(12) + cast-off(0.2) + sail BIA→SanBarth(6)
    rt_hours          = 44.9
    trips_per_day     = 24 / rt_hours           # one vessel does ~0.534 round-trips/day

    # Extra throughput gap in bbl/day
    throughput_gap    = spill_per_day

    # How many vessel-equivalents does the gap represent?
    bbl_per_vessel_day = trips_per_day * avg_cargo
    vessel_equivalents = throughput_gap / max(bbl_per_vessel_day, 1)

    # Spill by storage
    spill_by = S.get("spill_by_storage", {})
    worst = sorted([(k,v) for k,v in spill_by.items() if v > 0], key=lambda x:-x[1])

    # ── Rec 0: overflow summary ───────────────────────────────────────────────
    sev = 1 if spill_pct < 2 else (2 if spill_pct < 8 else 3)
    worst_str = "; ".join(f"{k}: {v:,.0f} bbl" for k,v in worst[:3]) or "various"
    recs.append(dict(
        type="overflow_summary", severity=sev,
        title=f"⚠️ {total_spilled:,.0f} bbl overflow in {sim_days} days "
              f"({spill_pct:.1f}% of production)",
        body=(
            f"Average overflow rate: <span class='hl-yellow'>{spill_per_day:,.0f} bbl/day</span>. "
            f"Worst affected storage: <b>{worst_str}</b>. "
            f"Fleet averaged <b>{lifts_per_day:.2f} lifts/day</b> at "
            f"<b>{avg_cargo:,.0f} bbl/lift</b>. "
            f"Each vessel delivers ~<b>{bbl_per_vessel_day:,.0f} bbl/day</b> of throughput "
            f"at a {rt_hours:.0f}h round-trip cycle. "
            f"The throughput gap is equivalent to "
            f"<span class='hl-yellow'>{vessel_equivalents:.2f} vessel-equivalents</span>."
        ),
        metric=f"{total_spilled:,.0f} bbl lost ({spill_pct:.1f}% of production)",
    ))

    # ── Rec 1: additional daughter vessel ─────────────────────────────────────
    # Standard vessel sizes available in the fleet
    STANDARD_SIZES = [42_000, 43_000, 44_000, 63_000, 65_000, 85_000]

    # Raw bbl/vessel needed to close the gap
    raw_gap = throughput_gap / trips_per_day

    if vessel_equivalents <= 1.3:
        # One vessel can close the gap — find the right size
        best_size = min(STANDARD_SIZES, key=lambda s: abs(s - raw_gap))
        # Would it actually close the gap?
        prevented = best_size * trips_per_day * sim_days
        shortfall = max(0, total_spilled - prevented)
        coverage  = min(prevented / total_spilled * 100, 100)

        recs.append(dict(
            type="daughter_vessel", severity=sev,
            title=f"🚢 Add 1 × {best_size:,} bbl daughter vessel",
            body=(
                f"To eliminate the {spill_per_day:,.0f} bbl/day throughput gap, "
                f"one additional <span class='hl-blue'>{best_size:,} bbl daughter vessel</span> "
                f"is recommended. "
                f"At a ~{rt_hours:.0f}h round-trip cycle this vessel would deliver "
                f"~<span class='hl-green'>{best_size * trips_per_day:,.0f} bbl/day</span> "
                f"of extra lifting capacity, covering an estimated "
                f"<span class='hl-green'>{coverage:.0f}%</span> of the projected overflow. "
                + (f"A residual ~{shortfall:,.0f} bbl gap would remain — "
                   f"consider an <b>85,000 bbl vessel</b> for full coverage."
                   if shortfall > 5_000 else
                   "This vessel size is expected to fully eliminate the overflow.")
                + f" Permitted storage points for a new vessel should be confirmed "
                  f"against operational routing constraints."
            ),
            metric=f"{best_size:,} bbl vessel → ~{coverage:.0f}% overflow eliminated",
        ))
    else:
        # Need more than one vessel
        n    = int(vessel_equivalents) + 1
        size = 85_000
        recs.append(dict(
            type="daughter_vessel", severity=3,
            title=f"🚢 Add {n} × 85,000 bbl daughter vessels (significant shortfall)",
            body=(
                f"The throughput gap of <span class='hl-yellow'>{spill_per_day:,.0f} bbl/day</span> "
                f"is equivalent to <span class='hl-yellow'>{vessel_equivalents:.1f} vessel-equivalents</span>. "
                f"A minimum of <span class='hl-blue'>{n} additional 85,000 bbl vessels</span> "
                f"are required to close the gap. Combined they would add "
                f"~<span class='hl-green'>{n * size * trips_per_day:,.0f} bbl/day</span> "
                f"of lifting capacity. If adding {n} vessels is not operationally feasible, "
                f"a storage buffer tanker (see below) may bridge the gap while a permanent "
                f"fleet solution is arranged."
            ),
            metric=f"{n} × 85,000 bbl vessels needed",
        ))

    # ── Rec 2: storage buffer tanker ─────────────────────────────────────────
    if worst:
        top_store, top_spill = worst[0]
        top_pct = top_spill / total_spilled * 100

        # Count overflow hours at worst storage from timeline
        ovf_col     = f"{top_store}_Overflow_Accum_bbl"
        burst_hours = 0
        if ovf_col in tl_df.columns:
            burst_hours = int((tl_df[ovf_col].diff().fillna(0) > 0).sum() * 0.5)

        # Buffer is useful when:
        # (a) one point dominates — it's a local bottleneck not a fleet-wide gap
        # (b) overflow is concentrated in time — it's burst/daylight-window driven
        buffer_useful = top_pct >= 50 or burst_hours > 20

        if buffer_useful:
            # Size the buffer: absorb worst-day overflow × 1.5 safety factor
            worst_day_spill = top_spill / sim_days * 1.5
            BUFFER_SIZES    = [65_000, 85_000, 150_000, 270_000]
            buf_size        = min(BUFFER_SIZES, key=lambda s: abs(s - worst_day_spill))

            # Which vessels could load from a buffer at this point?
            perm_map = {
                "SanBarth"  : list(mod.VESSEL_NAMES),
                "JasmineS": list(mod.VESSEL_NAMES),
                "Westmore": sorted(mod.WESTMORE_PERMITTED_VESSELS),
                "Duke"    : sorted(mod.DUKE_PERMITTED_VESSELS),
                "Starturn": sorted(mod.STARTURN_PERMITTED_VESSELS),
                "PGM"     : sorted(getattr(mod, "PGM_PERMITTED_VESSELS", {"SantaMonica"})),
            }
            permitted = perm_map.get(top_store, [])

            recs.append(dict(
                type="storage_buffer", severity=2,
                title=f"🏗️ Alternative / complement: {buf_size:,} bbl storage buffer tanker at {top_store}",
                body=(
                    f"<b>{top_store}</b> accounts for "
                    f"<span class='hl-yellow'>{top_pct:.0f}%</span> of total overflow "
                    f"({top_spill:,.0f} bbl), with overflow occurring across roughly "
                    f"<b>{burst_hours}h</b> of burst windows. "
                    f"Mooring a <span class='hl-blue'>{buf_size:,} bbl storage buffer tanker</span> "
                    f"at {top_store} would absorb production during windows when no daughter vessel "
                    f"is available — daylight berthing restrictions, queue congestion, or "
                    f"dead-stock wait times. "
                    f"Unlike adding a daughter vessel, a buffer tanker requires no "
                    f"additional round-trip voyages to BIA; it extends hold time at source "
                    f"and is offloaded later by the existing fleet "
                    f"(permitted vessels: {', '.join(permitted)}). "
                    f"Best deployed <b>alongside</b> an additional daughter vessel "
                    f"for full coverage, or as a standalone measure if peak-burst overflow "
                    f"is the primary driver."
                ),
                metric=f"{buf_size:,} bbl buffer covers ~{burst_hours}h of overflow windows",
            ))
        else:
            recs.append(dict(
                type="storage_buffer", severity=1,
                title="🏗️ Storage buffer tanker: lower priority in this scenario",
                body=(
                    f"Overflow is distributed across multiple storage points "
                    f"({', '.join(k for k,_ in worst)}), indicating a fleet-wide "
                    f"throughput shortfall rather than a single-point bottleneck. "
                    f"A storage buffer tanker at one point would not address the root cause. "
                    f"Prioritise the additional daughter vessel recommendation above."
                ),
                metric=None,
            ))

    # ── Rec 3: operational levers ─────────────────────────────────────────────
    dead_stock_ratio = getattr(mod, "DEAD_STOCK_FACTOR", 1.75)
    recs.append(dict(
        type="operational", severity=1,
        title="📋 Operational levers — no new assets required",
        body=(
            f"<b>1. Reduce dead-stock threshold:</b> Currently set at "
            f"<b>×{dead_stock_ratio}</b> of cargo volume. Reducing to ×1.5 allows loading "
            f"to begin ~{(dead_stock_ratio-1.5)*avg_cargo/max(prod_sanbarth+prod_jasmines,1):.1f}h earlier per voyage, "
            f"increasing effective throughput without new vessels.<br>"
            f"<b>2. Extend operational window:</b> Berthing is restricted to "
            f"{getattr(mod,'BERTHING_START',6):02d}:00–{getattr(mod,'BERTHING_END',18):02d}:00. "
            f"Extending by even 1h each side adds ~{lifts_per_day * 2 / 24 * avg_cargo:,.0f} bbl/day "
            f"of additional capacity.<br>"
            f"<b>3. Reduce documentation time:</b> Current 4h documentation + 12h load = 16h at berth. "
            f"A 1h reduction in documentation time frees ~{1/rt_hours*24*avg_cargo:,.0f} bbl/vessel/day.<br>"
            f"<b>4. Review mother export timing:</b> Ensure export voyages are not blocking "
            f"discharge berths during peak loading windows at storage points."
        ),
        metric=None,
    ))

    # Discharge-side (BIA) stall recommendations — appended after the storage/
    # overflow recommendations so both source-side and sink-side constraints are
    # surfaced together when both are present.
    if bia_recs:
        recs.extend(bia_recs)

    return recs


def render_recommendations(recs):
    SEV_BORDER = {0:"#238636", 1:"#9e6a03", 2:"#bd561d", 3:"#6e1616"}
    SEV_BADGE  = {0:"✅ OK", 1:"ℹ️ LOW", 2:"⚠️ MEDIUM", 3:"🔴 HIGH"}
    for rec in recs:
        border  = SEV_BORDER.get(rec["severity"], "#e2e8f0")
        badge   = SEV_BADGE.get(rec["severity"], "")
        met_html = (
            f'<div class="rec-metric">Estimated impact: '
            f'<span class="hl-yellow">{rec["metric"]}</span></div>'
        ) if rec.get("metric") else ""
        st.markdown(f"""
        <div class="rec-card" style="border-color:{border}">
          <div style="display:flex;justify-content:space-between;align-items:flex-start">
            <div class="rec-title">{rec["title"]}</div>
            <div style="font-size:10px;color:#484f58;margin-left:12px;white-space:nowrap">{badge}</div>
          </div>
          <div class="rec-body">{rec["body"]}</div>
          {met_html}
        </div>
        """, unsafe_allow_html=True)


# =============================================================================
# ── CHARTS ────────────────────────────────────────────────────────────────────
# =============================================================================

_DARK = dict(paper_bgcolor="#ffffff", plot_bgcolor="#f8f9fb",
             font=dict(color="#1e293b"))
_MARGIN = dict(l=60, r=20, t=46, b=30)   # default margin — override per chart
_GRID = dict(gridcolor="#e2e8f0")


def chart_storage(tl_df):
    fig = make_subplots(
        rows=2, cols=1, shared_xaxes=True,
        subplot_titles=("Point A — SanBarth & JasmineS",
                        "Sego / Awoba / Dawes — Westmore · Duke · Starturn"),
        vertical_spacing=0.1,
    )
    for name, col, dash in [("SanBarth","SanBarth_bbl","solid"),
                              ("JasmineS","JasmineS_bbl","dot")]:
        fig.add_trace(go.Scatter(x=tl_df.Time, y=tl_df[col], name=name,
            line=dict(color=STORAGE_COLORS[name], width=2, dash=dash)), row=1, col=1)
    for name, col in [("Westmore","Westmore_bbl"),
                       ("Duke","Duke_bbl"),("Starturn","Starturn_bbl"),("PGM","PGM_bbl")]:
        fig.add_trace(go.Scatter(x=tl_df.Time, y=tl_df[col], name=name,
            line=dict(color=STORAGE_COLORS[name], width=2)), row=2, col=1)
    fig.update_layout(height=460, margin=_MARGIN, **_DARK, legend=dict(bgcolor="#ffffff", bordercolor="#e2e8f0"))
    fig.update_yaxes(tickformat=",", **_GRID, title_text="bbl")
    fig.update_xaxes(**_GRID)
    return fig


def chart_overflow(tl_df):
    ovf_cols = [c for c in tl_df.columns if "Overflow_Accum" in c]
    if not ovf_cols:
        return None
    name_map = {
        "SanBarth_Overflow_Accum_bbl":"SanBarth","JasmineS_Overflow_Accum_bbl":"JasmineS",
        "Westmore_Overflow_Accum_bbl":"Westmore","Duke_Overflow_Accum_bbl":"Duke",
        "Starturn_Overflow_Accum_bbl":"Starturn","PGM_Overflow_Accum_bbl":"PGM",
        "Ibom_Overflow_Accum_bbl":"Ibom",
    }
    fig = go.Figure()
    for col in ovf_cols:
        fig.add_trace(go.Scatter(x=tl_df.Time, y=tl_df[col],
                                  name=name_map.get(col, col),
                                  stackgroup="o", line=dict(width=1.5)))
    fig.update_layout(height=240, margin=_MARGIN, title="Cumulative Overflow — all storage points",
                      **_DARK, legend=dict(bgcolor="#ffffff"))
    fig.update_yaxes(tickformat=",", **_GRID, title_text="bbl")
    fig.update_xaxes(**_GRID)
    return fig


def chart_util(tl_df):
    items = [("SanBarth","SanBarth_bbl",400_000),("JasmineS","JasmineS_bbl",290_000),
             ("Westmore","Westmore_bbl",270_000),("Duke","Duke_bbl",90_000),
             ("Starturn","Starturn_bbl",70_000),("PGM","PGM_bbl",28_000)]
    fig = go.Figure()
    for name, col, c in items:
        if col in tl_df.columns:
            fig.add_trace(go.Scatter(x=tl_df.Time, y=(tl_df[col]/c*100).round(1),
                name=name, line=dict(color=STORAGE_COLORS[name], width=1.8)))
    fig.add_hline(y=90, line=dict(color="#ef4444", dash="dash"),
                  annotation_text="90%", annotation_font_color="#ef4444")
    fig.update_layout(title="Storage Utilisation %", height=240, margin=_MARGIN, **_DARK,
                      yaxis=dict(**_GRID, title_text="%", range=[0,105]),
                      xaxis=_GRID, legend=dict(bgcolor="#ffffff"))
    return fig


def chart_mothers(tl_df, export_trigger, cap_by_name, export_trigger_by_name=None):
    fills = {"Bryanston" :"rgba(26,188,156,0.12)",
             "GreenEagle":"rgba(192,132,252,0.12)",
             "Alkebulan" :"rgba(245,158,11,0.12)"}
    fig = go.Figure()
    for name, col in [("Bryanston","Bryanston_bbl"),
                       ("GreenEagle","GreenEagle_bbl"),
                       ("Alkebulan","Alkebulan_bbl")]:
        if col in tl_df.columns:
            fig.add_trace(go.Scatter(x=tl_df.Time, y=tl_df[col], name=name,
                fill="tozeroy", fillcolor=fills.get(name, "rgba(0,0,0,0.05)"),
                line=dict(color=MOTHER_COLORS.get(name, "#888888"), width=2)))
    # Export trigger lines
    _trig_by = export_trigger_by_name or {}
    _bry_trig = int(_trig_by.get("Bryanston", export_trigger))
    _ge_trig  = int(_trig_by.get("GreenEagle", export_trigger))
    fig.add_hline(y=_bry_trig,
                  line=dict(color="#ff5555", dash="dash", width=1.5),
                  annotation_text=f"Bryanston export trigger ({_bry_trig:,} bbl)",
                  annotation_font_color="#e74c3c")
    if _ge_trig != _bry_trig:
        fig.add_hline(y=_ge_trig,
                      line=dict(color="#c084fc", dash="dash", width=1.5),
                      annotation_text=f"GreenEagle export trigger ({_ge_trig:,} bbl)",
                      annotation_font_color="#c084fc")
    _cap_bry = int(cap_by_name.get("Bryanston", 550_000))
    _cap_ge  = int(cap_by_name.get("GreenEagle", _cap_bry))
    fig.add_hline(y=_cap_bry, line=dict(color="#7f1d1d", dash="dot"),
                  annotation_text=f"Bryanston capacity ({_cap_bry:,} bbl)",
                  annotation_font_color="#fca5a5")
    fig.add_hline(y=_cap_ge, line=dict(color="#64748b", dash="dot"),
                  annotation_text=f"GreenEagle capacity ({_cap_ge:,} bbl)",
                  annotation_font_color="#cbd5e1")
    fig.update_layout(height=320, margin=_MARGIN,
                      title="BIA Mother Vessels — Volume",
                      **_DARK, legend=dict(bgcolor="#ffffff"))
    fig.update_yaxes(tickformat=",", **_GRID, title_text="bbl")
    fig.update_xaxes(**_GRID)
    return fig


def chart_gantt(tl_df, vessel_names, log_df=None):
    """
    Proper continuous-span Gantt.
    Each block of consecutive same-category status slots is collapsed into a
    single horizontal bar, making the chart readable across any horizon.
    """
    _LOAD_ST   = {"LOADING","BERTHING_A","HOSE_CONNECT_A","CAST_OFF","DOCUMENTING",
                  "WAITING_CAST_OFF"}
    _SAIL_OUT  = {"SAILING_AB","SAILING_CROSS_BW_AC","SAILING_BW_TO_FWY","SAILING_AB_LEG2",
                  "SAILING_D_CHANNEL","SAILING_CH_TO_BW_OUT","SAILING_CROSS_BW_OUT",
                  "SAILING_B_TO_F"}
    _BIA_ST    = {"DISCHARGING","BERTHING_B","HOSE_CONNECT_B","CAST_OFF_B","IDLE_B","WAITING_CAST_OFF"}
    _RETURN_ST = {"SAILING_BA","SAILING_BW_TO_A","SAILING_B_TO_FWY","SAILING_FWY_TO_BW","SAILING_CROSS_BW_IN_AC","SAILING_B_TO_BW_IN","SAILING_CROSS_BW_IN",
                  "SAILING_BW_TO_CH_IN","SAILING_CH_TO_D"}
    _WAIT_ST   = {"WAITING_STOCK","WAITING_DEAD_STOCK","WAITING_BERTH_A",
                  "WAITING_BERTH_B","WAITING_MOTHER_RETURN","WAITING_MOTHER_CAPACITY",
                  "WAITING_RETURN_STOCK","WAITING_FAIRWAY","WAITING_TIDAL",
                  "WAITING_DAYLIGHT"}
    _IBOM_ST   = {"PF_LOADING","PF_SWAP"}
    # Activity category -> display label, color
    _CATS = {
        "Loading"      : ("#2ecc71", "⛽ Loading"),
        "Outbound"     : ("#3b82f6", "🚢 Outbound (→BIA)"),
        "At BIA"       : ("#a855f7", "⚓ At BIA / Discharging"),
        "Returning"    : ("#14b8a6", "↩️ Returning"),
        "Waiting"      : ("#f59e0b", "⏳ Waiting"),
        "Ibom"         : ("#f97316", "🛢️ Ibom Offshore"),
        "Idle"         : ("#475569", "💤 Idle"),
    }

    def _cat(st):
        if st in _LOAD_ST:   return "Loading"
        if st in _SAIL_OUT:  return "Outbound"
        if st in _BIA_ST:    return "At BIA"
        if st in _RETURN_ST: return "Returning"
        if st in _WAIT_ST:   return "Waiting"
        if st in _IBOM_ST:   return "Ibom"
        return "Idle"

    fig = go.Figure()

    vessels_ordered = list(reversed(vessel_names))   # top vessel = first in list
    y_pos = {n: i for i, n in enumerate(vessels_ordered)}
    _legend_added = set()
    SLOTS_PER_DAY = 48  # 30-min intervals

    for vn in vessel_names:
        if vn not in tl_df.columns:
            continue
        vc  = VESSEL_COLORS.get(vn, "#95a5a6")
        sub = tl_df[["Day", "Time", vn]].dropna(subset=[vn]).copy()
        if sub.empty:
            continue

        sub["xf"]  = (sub["Day"] - 1) + sub["Time"].apply(
            lambda d: (d.hour + d.minute / 60) / 24
        )
        sub["cat"] = sub[vn].apply(_cat)
        sub["blk"] = (sub["cat"] != sub["cat"].shift()).cumsum()

        yi = y_pos[vn]

        for (cat, _blk), grp in sub.groupby(["cat","blk"], sort=False):
            x0  = float(grp["xf"].iloc[0])
            x1  = float(grp["xf"].iloc[-1]) + (1.0 / SLOTS_PER_DAY)
            dur = x1 - x0
            col = vc if cat == "Loading" else _CATS[cat][0]

            rep_st  = grp[vn].mode().iloc[0]
            lbl     = STATUS_LABELS.get(rep_st, rep_st)
            d0, d1  = int(grp["Day"].iloc[0]), int(grp["Day"].iloc[-1])
            day_lbl = f"Day {d0}" if d0 == d1 else f"Day {d0}–{d1}"
            t0      = grp["Time"].iloc[0].strftime("%H:%M")
            t1      = grp["Time"].iloc[-1].strftime("%H:%M")
            hover   = (
                f"<b>{vn}</b>  ·  <b>{cat}</b><br>"
                f"{lbl}<br>"
                f"{day_lbl}  {t0} → {t1}<br>"
                f"Duration: {dur*24:.1f} h"
            )

            show_leg = cat not in _legend_added
            if show_leg:
                _legend_added.add(cat)

            fig.add_trace(go.Bar(
                x=[dur], y=[yi], base=[x0],
                orientation="h", width=0.60,
                marker=dict(
                    color=col, opacity=0.90,
                    line=dict(color="rgba(0,0,0,0.3)", width=0.6),
                ),
                name=_CATS[cat][1],
                legendgroup=cat,
                showlegend=show_leg,
                hovertemplate=hover + "<extra></extra>",
            ))

    # Day grid lines + alternating week shading
    max_day = int(tl_df["Day"].max()) if not tl_df.empty else 1
    _gantt_shapes = []
    for d in range(0, max_day + 2):
        _gantt_shapes.append(dict(
            type="line", xref="x", yref="paper",
            x0=d, x1=d, y0=0, y1=1,
            line=dict(color="rgba(255,255,255,0.07)", width=1)
        ))
    for w in range(0, (max_day // 7) + 2):
        if w % 2 == 1:
            _gantt_shapes.append(dict(
                type="rect", xref="x", yref="paper",
                x0=w*7, x1=min((w+1)*7, max_day+1), y0=0, y1=1,
                fillcolor="rgba(255,255,255,0.025)", line_width=0, layer="below"
            ))

    tick_step = (1 if max_day <= 10 else
                 2 if max_day <= 20 else
                 7 if max_day <= 90 else
                 14 if max_day <= 180 else 30)
    tick_vals = list(range(0, max_day + 1, tick_step))

    # All rows: daughter vessels
    _all_rows_ordered = list(vessels_ordered)
    _all_y_vals       = [y_pos[n] for n in vessels_ordered]
    _all_tick_text    = [
        "<span style=\"color:{};font-weight:700\">{}</span>".format(
            VESSEL_COLORS.get(n, "#e2e8f0"), n
        )
        for n in vessels_ordered
    ]
    _total_rows = len(vessel_names)

    fig.update_layout(
        shapes=_gantt_shapes,
        height=max(360, 54 * _total_rows + 90),
        barmode="overlay", bargap=0,
        plot_bgcolor="#0f1a35", paper_bgcolor="#0f1a35",
        margin=dict(l=110, r=20, t=50, b=50),
        legend=dict(
            orientation="h", yanchor="bottom", y=1.01,
            xanchor="left", x=0,
            bgcolor="rgba(15,26,53,0.85)", bordercolor="#344d80", borderwidth=1,
            font=dict(color="#e2e8f0", size=11),
        ),
        xaxis=dict(
            title=dict(text="Simulation Day", font=dict(color="#94a3b8", size=12)),
            tickvals=tick_vals, ticktext=[str(v) for v in tick_vals],
            tickfont=dict(color="#94a3b8", size=10),
            range=[-0.5, max_day + 0.5],
            gridcolor="rgba(255,255,255,0.06)", zerolinecolor="rgba(255,255,255,0.1)",
            showgrid=True,
        ),
        yaxis=dict(
            tickvals=_all_y_vals,
            ticktext=_all_tick_text,
            tickfont=dict(size=12),
            gridcolor="rgba(255,255,255,0.07)",
            range=[-0.6, _total_rows - 0.4],
            showgrid=True,
        ),
        hoverlabel=dict(
            bgcolor="#1e3a5f", bordercolor="#3b82f6",
            font=dict(color="#f1f5f9", size=12),
        ),
        font=dict(color="#e2e8f0"),
    )
    return fig


def chart_voyage_bars(log_df, vessel_names):
    ld = log_df[log_df.Event=="LOADING_START"].groupby("Vessel").size().reindex(vessel_names, fill_value=0)
    dc = log_df[log_df.Event=="DISCHARGE_START"].groupby("Vessel").size().reindex(vessel_names, fill_value=0)
    fig = go.Figure([
        go.Bar(name="Loadings",   x=vessel_names, y=ld.values, opacity=0.9,
         marker_color=[_normalize_hex_color(VESSEL_COLORS.get(n, "#95a5a6")) for n in vessel_names]),
        go.Bar(name="Discharges", x=vessel_names, y=dc.values, opacity=0.9,
         marker_color=[_shade(VESSEL_COLORS.get(n, "#95a5a6"),0.55) for n in vessel_names]),
    ])
    fig.update_layout(barmode="group", title="Voyages per Vessel",
                      height=260, margin=_MARGIN, **_DARK, yaxis=_GRID, legend=dict(bgcolor="#ffffff"))
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


def _int(v, fallback=0):
    """Safe int conversion."""
    try:
        return int(float(str(v).replace(",","")))
    except Exception:
        return fallback


def _extract_cargo_bbl(detail):
    if detail is None or (isinstance(detail, float) and math.isnan(detail)):
        return 0
    m = re.search(r"([\d,]+) bbl", str(detail))
    return int(m.group(1).replace(",", "")) if m else 0


def _safe_sum_cargo(series):
    """Sum cargo bbls from a Detail Series, safely returning 0 for empty/string Series."""
    if series.empty:
        return 0
    return int(series.map(_extract_cargo_bbl).sum())


def _effective_load_cap(vessel_name, storage_name, mod):
    base_cap = getattr(mod, "VESSEL_CAPACITIES", {}).get(
        vessel_name, getattr(mod, "DAUGHTER_CARGO_BBL", 85_000)
    )
    if not storage_name or storage_name == "__any__":
        return base_cap
    cap = int(round(base_cap))
    if storage_name == getattr(mod, "STORAGE_SECONDARY_NAME", "JasmineS"):
        cap = int(round(base_cap * getattr(mod, "JASMINES_LOAD_CAP_MULTIPLIER", 1.08)))
    elif storage_name == getattr(mod, "STORAGE_TERTIARY_NAME", "Westmore"):
        cap = max(0, int(round(base_cap)) - getattr(mod, "WESTMORE_LOAD_CAP_OFFSET_BBL", 1_000))
    if (vessel_name in getattr(mod, "POINT_A_LOAD_CAP_VESSELS", {"Bedford", "Balham"})
            and getattr(mod, "STORAGE_POINT", {}).get(storage_name) == "A"):
        cap = min(cap, getattr(mod, "POINT_A_LOAD_CAP_BBL", 63_000))
    return max(0, cap)


# =============================================================================
# ── FLEET STATUS RENDERING ────────────────────────────────────────────────────
# =============================================================================

def render_fleet_cards(vessel_names, fleet_df, manual_states, mod):
    """Render one status card per daughter vessel in a 4-column grid."""
    _sp_map           = getattr(mod, "STORAGE_POINT", {})
    cols = st.columns(4)
    for i, vn in enumerate(vessel_names):
        base  = VESSEL_COLORS.get(vn, "#95a5a6")
        vcap  = mod.VESSEL_CAPACITIES.get(vn, mod.DAUGHTER_CARGO_BBL)

        # Resolve data source: Sheets > manual > default
        if not fleet_df.empty and vn in fleet_df["vessel"].values:
            row    = fleet_df[fleet_df["vessel"]==vn].iloc[0]
            status = str(row.get("status","IDLE_A"))
            loc    = str(row.get("location","—"))
            cargo  = _int(row.get("cargo_bbl", 0))
            notes  = str(row.get("notes",""))
            badge  = '<span style="font-size:10px;color:#56d364">● live</span>'
        elif vn in manual_states:
            ms     = manual_states[vn]
            status = ms.get("status","IDLE_A")
            loc    = ms.get("location","—")
            cargo  = ms.get("cargo_bbl", 0)
            notes  = ms.get("notes","")
            badge  = '<span style="font-size:10px;color:#8b949e">● manual</span>'
        else:
            status = "IDLE_A"; loc = "—"; cargo = 0; notes = ""; badge = ""

        # Show the effective storage-adjusted voyage size rather than only
        # the physical vessel nameplate capacity.
        _loc_storage = (manual_states.get(vn, {}).get("location") or
                        (fleet_df[fleet_df["vessel"]==vn].iloc[0].get("location","")
                         if not fleet_df.empty and vn in fleet_df["vessel"].values else ""))
        if _loc_storage in _sp_map:
            vcap = _effective_load_cap(vn, _loc_storage, mod)
        icon     = STATUS_ICONS.get(status, "❓")
        label    = STATUS_LABELS.get(status, status)
        pct      = max(4, min(100, int(cargo/vcap*100))) if vcap else 0
        bar_col  = vcolor(vn, status)
        notes_h  = (f'<div style="font-size:10px;color:#484f58;margin-top:3px">'
                    f'{notes}</div>') if notes else ""

        with cols[i % 4]:
            st.markdown(f"""
            <div class="vcard" style="border-left-color:{base}">
              <div style="display:flex;justify-content:space-between">
                <span class="vcard-name" style="color:{base}">{vn}</span>
                {badge}
              </div>
              <div class="vcard-status">{icon} {label}</div>
              <div class="vcard-loc">📍 {loc}</div>
              <div style="font-size:11px;color:#484f58;margin-bottom:5px">
                {cargo:,} / {vcap:,} bbl
              </div>
              <div class="vcard-bar-bg">
                <div class="vcard-bar-fg" style="background:{bar_col};width:{pct}%"></div>
              </div>
              {notes_h}
            </div>
            """, unsafe_allow_html=True)


def render_mother_cards(gs_vols, manual_mother, mod):
    """Render one status card per mother vessel in a 4-column row."""
    _mother_caps = getattr(mod, "MOTHER_CAPACITY_BY_NAME", {})
    _default_cap = getattr(mod, "MOTHER_CAPACITY_BBL", 550_000)
    cols = st.columns(3)
    for i, (mn, mk) in enumerate([
        ("Bryanston","bryanston"), ("GreenEagle","greeneagle"),
        ("Alkebulan","alkebulan")
    ]):
        bbl   = gs_vols.get(mk) or manual_mother.get(mk, 0)
        cap   = int(_mother_caps.get(mn, _default_cap))
        exp_t = mod.MOTHER_EXPORT_TRIGGER
        pct   = max(4, min(100, int(bbl/cap*100))) if cap else 0
        color = MOTHER_COLORS.get(mn,"#aaa")
        above = bbl >= exp_t
        flag  = ('<span style="color:#e74c3c;font-size:11px">▲ above export trigger</span>'
                 if above else
                 '<span style="color:#56d364;font-size:11px">▼ below export trigger</span>')

        with cols[i]:
            st.markdown(f"""
            <div class="vcard" style="border-left-color:{color}">
              <div class="vcard-name" style="color:{color}">🛢️ {mn}</div>
              <div style="font-size:22px;font-weight:700;color:#f0f6fc;margin:4px 0">
                {bbl:,} <span style="font-size:12px;color:#484f58">bbl</span>
              </div>
              <div style="font-size:11px;color:#484f58;margin-bottom:5px">
                {pct}% of {cap:,} bbl capacity
              </div>
              <div class="vcard-bar-bg" style="height:8px;margin-bottom:6px">
                <div class="vcard-bar-fg" style="background:{color};width:{pct}%;height:8px"></div>
              </div>
              {flag}
            </div>
            """, unsafe_allow_html=True)


# =============================================================================
# ── EXECUTIVE SUMMARY & ANALYTICS ────────────────────────────────────────────
#  Purely additive presentation layer: reads the same log_df / tl_df / S that
#  every other section uses and renders a board-level overview at the top of the
#  results.  Does not run the simulation, mutate state, or change any KPI — it
#  only reformats results already computed by run_sim().
# =============================================================================

def _exec_attribute_sources(log_df, opening):
    """Mass-balance attribution of each export cycle's volume to production sources.

    Tracks a composition vector {source: bbl} for every vessel and each mother.
    Cargo inflows add to a vector; outflows draw proportionally (correctly
    handling MTO blending of mixed sources).  Opening mother stock is labelled
    'Opening'; cargo loaded
    on vessels before the sim window (startup state, no LOADING_START) is
    labelled 'Startup'.  Returns a list of (mother, doc_time, {source: bbl})
    in chronological order, each reconciling to that cycle's export volume.
    """
    import re as _re
    from collections import defaultdict as _dd
    if log_df is None or log_df.empty or "Event" not in log_df.columns:
        return []
    ev = log_df.copy()
    ev["_t"] = pd.to_datetime(ev["Time"], errors="coerce")
    ev = ev.sort_values("_t")

    MOTHERS = ("Bryanston", "GreenEagle", "Alkebulan")
    # Point-F (Ibom offshore buoy) loaders.  Cargo loaded at Ibom is committed
    # incrementally via PF_LOADING with no LOADING_START event, so it would
    # otherwise fall through to the generic "Startup" bucket.  We detect it by
    # the vessel being a Point-F loader AND the cargo carrying the constant
    # Ibom API at discharge, and attribute it to the dedicated "Ibom" source.
    IBOM_VESSELS = {"Bedford", "Balham"}
    try:
        _IBOM_API = float(getattr(_load_mod_current(), "IBOM_API", 32.0))
    except Exception:
        _IBOM_API = 32.0

    def _src(detail):
        m = _re.search(r"@ [\d.]+\D+API \| (\w+):", str(detail))
        return m.group(1) if m else None
    def _num(detail, pat):
        m = _re.search(pat, str(detail))
        return int(m.group(1).replace(",", "")) if m else 0
    def _api(detail):
        m = _re.search(r"@ ([\d.]+)\D+API", str(detail))
        return float(m.group(1)) if m else None
    def _untracked_label(ves, detail):
        # Decide how to label cargo a vessel carries that was never tracked to a
        # LOADING_START source: Ibom buoy crude vs genuine pre-sim startup cargo
        # (the latter tagged with the vessel name so it can be listed per cycle).
        api = _api(detail)
        if ves in IBOM_VESSELS and api is not None and abs(api - _IBOM_API) < 0.05:
            return "Ibom"
        return "Startup\u241f" + str(ves)

    comp = _dd(lambda: _dd(float))
    moth_accum = {m: _dd(float) for m in MOTHERS}
    for m in MOTHERS:
        moth_accum[m]["Opening"] += float(opening.get(m, 0) or 0)

    def _ensure(vec, amount, label):
        tot = sum(vec.values())
        if amount > tot + 1e-6:
            vec[label] += (amount - tot)
    def _draw(vec, amount):
        tot = sum(vec.values()); out = _dd(float)
        if tot <= 0:
            out["Opening"] += amount; return out
        frac = min(1.0, amount / tot)
        for s, v in list(vec.items()):
            t = v * frac; out[s] += t; vec[s] -= t
        return out

    rows = []
    for _, r in ev.iterrows():
        e = r["Event"]; ves = str(r["Vessel"]); det = r["Detail"]; moth = str(r["Mother"])
        if e == "LOADING_START":
            src = _src(det); vol = _num(det, r"Loading ([\d,]+)")
            if src:
                comp[ves] = _dd(float); comp[ves][src] += vol
        elif e == "MTO_TRANSIENT_NOMINATED":
            vol = _num(det, r"Received ([\d,]+) bbl"); frm_m = _re.search(r"from (\w+)", str(det))
            if vol and frm_m:
                frm = frm_m.group(1); _ensure(comp[frm], vol, _untracked_label(frm, det))
                for s, v in _draw(comp[frm], vol).items():
                    comp[ves][s] += v
        elif e == "DISCHARGE_START":
            vol = _num(det, r"Discharging ([\d,]+)")
            if vol <= 0:
                continue
            _ensure(comp[ves], vol, _untracked_label(ves, det))
            if moth in MOTHERS:
                for s, v in _draw(comp[ves], vol).items():
                    moth_accum[moth][s] += v
        elif e == "EXPORT_DOC_START":
            mother = ves if ves in MOTHERS else moth
            if mother in MOTHERS:
                rows.append((mother, r["_t"], dict(moth_accum[mother])))
                moth_accum[mother] = _dd(float)
    return rows


def _exec_extract_export_cycles(log_df, tl_df, opening=None):
    """Reconstruct per-mother export cycles with per-source volume breakdown.

    Returns a list of dicts (one per export cycle):
      { mother, doc_date, sail_date, return_date, volume_bbl, month,
        sources: {source: bbl} }
    volume_bbl is the mother's stock at EXPORT_DOC_START (the load she sails
    with); sources is the mass-balance attribution of that volume to the
    production storages that fed it (see _exec_attribute_sources).
    """
    if log_df is None or log_df.empty or "Event" not in log_df.columns:
        return []
    ev = log_df.copy()
    ev["_t"] = pd.to_datetime(ev["Time"], errors="coerce")

    moth_cols = {"Bryanston": "Bryanston_bbl", "GreenEagle": "GreenEagle_bbl",
                 "Alkebulan": "Alkebulan_bbl"}
    tl = None
    if tl_df is not None and not tl_df.empty and "Time" in tl_df.columns:
        tl = tl_df.copy()
        tl["_t"] = pd.to_datetime(tl["Time"], errors="coerce")
        tl = tl.sort_values("_t")

    def _stock_before(mother, ts):
        col = moth_cols.get(mother)
        if tl is None or col is None or col not in tl.columns:
            return None
        prior = tl[tl["_t"] <= ts]
        return float(prior.iloc[-1][col]) if not prior.empty else None

    def _stock_from_logrow(row, mother):
        col = moth_cols.get(mother)
        if col and col in row and pd.notna(row.get(col)):
            try:
                return float(row[col])
            except Exception:
                return None
        return None

    # Per-cycle source attribution (chronological list, one entry per export).
    att007 = _exec_attribute_sources(log_df, opening or {})
    attr_by_mother = {}
    for mother, t, snap in att007:
        attr_by_mother.setdefault(mother, []).append((t, snap))

    cycles = []
    for mother in ("Bryanston", "GreenEagle", "Alkebulan"):
        _vcol = ev["Vessel"].astype(str) if "Vessel" in ev.columns else None
        _mcol = ev["Mother"].astype(str) if "Mother" in ev.columns else None
        if _vcol is not None and _mcol is not None:
            m_ev = ev[(_vcol == mother) | (_mcol == mother)]
        elif _vcol is not None:
            m_ev = ev[_vcol == mother]
        else:
            m_ev = ev[_mcol == mother]
        docs   = m_ev[m_ev["Event"] == "EXPORT_DOC_START"].sort_values("_t")
        sails  = m_ev[m_ev["Event"] == "EXPORT_SAIL_START"].sort_values("_t")
        rets   = m_ev[m_ev["Event"] == "EXPORT_RETURN_ARRIVE"].sort_values("_t")
        _attr_list = attr_by_mother.get(mother, [])
        for _i, (_, d) in enumerate(docs.iterrows()):
            d_t = d["_t"]
            s_after = sails[sails["_t"] >= d_t]
            r_after = rets[rets["_t"] >= d_t]
            sail_t = s_after.iloc[0]["_t"] if not s_after.empty else None
            ret_t  = r_after.iloc[0]["_t"] if not r_after.empty else None
            vol = _stock_from_logrow(d, mother)
            if vol is None:
                vol = _stock_before(mother, d_t)
            sources = _attr_list[_i][1] if _i < len(_attr_list) else {}
            # Collapse composite "Startup\u241f<vessel>" keys into a single
            # "Startup" total, and surface the contributing vessels (largest
            # first) so the UI can list them inside the Startup cell.
            _clean_sources = {}
            _startup_by_vessel = {}
            for _k, _v in (sources or {}).items():
                if isinstance(_k, str) and _k.startswith("Startup\u241f"):
                    _clean_sources["Startup"] = _clean_sources.get("Startup", 0.0) + _v
                    _vn = _k.split("\u241f", 1)[1]
                    _startup_by_vessel[_vn] = _startup_by_vessel.get(_vn, 0.0) + _v
                else:
                    _clean_sources[_k] = _clean_sources.get(_k, 0.0) + _v
            _startup_vessels = [
                _n for _n, _a in sorted(_startup_by_vessel.items(), key=lambda x: -x[1])
                if _a > 1
            ]
            cycles.append({
                "mother":      mother,
                "doc_date":    d_t,
                "sail_date":   sail_t,
                "return_date": ret_t,
                "volume_bbl":  vol,
                "month":       d_t.strftime("%b-%y") if pd.notna(d_t) else "",
                "sources":     _clean_sources,
                "startup_vessels": _startup_vessels,
            })
    cycles.sort(key=lambda c: (c["doc_date"] if pd.notna(c["doc_date"]) else pd.Timestamp.max))
    return cycles


def _exec_schedule_image_bytes(cycles, present, SRC_LABEL, show_breakdown, mother_colors):
    """Render the Mother Vessel Export Schedule as a styled PNG and return its
    bytes.  Uses Pillow (a hard Streamlit dependency, available everywhere) so it
    works across environments without relying on matplotlib's table API.  Mirrors
    the on-screen HTML table and respects the collapse toggle."""
    from PIL import Image, ImageDraw, ImageFont
    import io as _io

    def _fmt(dtv):
        try:
            return dtv.strftime("%d-%b-%y") if (dtv is not None and pd.notna(dtv)) else "\u2014"
        except Exception:
            return "\u2014"

    def _font(sz, bold=False):
        _cands = (["/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
                   "DejaVuSans-Bold.ttf"] if bold else
                  ["/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
                   "DejaVuSans.ttf"])
        for _p in _cands:
            try:
                return ImageFont.truetype(_p, sz)
            except Exception:
                continue
        try:
            return ImageFont.load_default(sz)
        except Exception:
            return ImageFont.load_default()

    f_reg, f_bold, f_small = _font(15), _font(15, True), _font(12)

    # Row model: (label, [values], style, [subtexts]|None)
    rows = [("Vessel", [c["mother"] for c in cycles], "header", None),
            ("Month",  [c.get("month", "") for c in cycles], "month", None)]
    if show_breakdown:
        for s in present:
            vals, subs = [], []
            for c in cycles:
                amt = (c.get("sources") or {}).get(s, 0)
                if amt > 0:
                    vals.append(f"{amt:,.0f}")
                    subs.append(", ".join(c.get("startup_vessels") or []) if s == "Startup" else "")
                else:
                    vals.append("\u2014"); subs.append("")
            style = "startup" if s == "Startup" else ("opening" if s == "Opening" else "src")
            rows.append((SRC_LABEL.get(s, s), vals, style, subs if any(subs) else None))
    rows.append(("Total Export Volume (bbl)",
                 [f"{c['volume_bbl']:,.0f}" if c.get("volume_bbl") is not None else "\u2014" for c in cycles],
                 "total", None))
    rows.append(("Export Documentation", [_fmt(c["doc_date"]) for c in cycles], "date", None))
    rows.append(("Commence Export Discharge", [_fmt(c["sail_date"]) for c in cycles], "date", None))
    rows.append(("Complete Discharge / Return to BIA", [_fmt(c["return_date"]) for c in cycles], "date", None))

    # Measurement helper (textbbox with graceful fallbacks across Pillow versions)
    _probe = ImageDraw.Draw(Image.new("RGB", (10, 10)))
    def _tw(text, font):
        try:
            bb = _probe.textbbox((0, 0), text, font=font); return bb[2] - bb[0], bb[3] - bb[1]
        except Exception:
            try:
                return font.getsize(text)
            except Exception:
                return (len(text) * 7, 14)

    ncyc = len(cycles)
    pad, title_h, base_row_h = 12, 38, 34
    # Dynamic column widths so nothing clips.
    label_w = max(170, min(360, max(_tw(r[0], f_bold)[0] for r in rows) + 22))
    col_w = max(110, *(_tw(v, f_bold)[0] + 22 for r in rows for v in r[1]))
    row_heights = [base_row_h + (16 if (r[3] and any(r[3])) else 0) for r in rows]

    W = label_w + col_w * ncyc + pad * 2
    H = title_h + sum(row_heights) + pad * 2
    img = Image.new("RGB", (int(W), int(H)), "white")
    d = ImageDraw.Draw(img)
    d.text((pad, pad), "Mother Vessel Export Schedule", fill="#0f172a", font=_font(18, True))

    def _center(text, x0, x1, yc, font, fill):
        tw, th = _tw(text, font)
        d.text(((x0 + x1) / 2 - tw / 2, yc - th / 2), text, font=font, fill=fill)
    def _left(text, x0, yc, font, fill):
        _, th = _tw(text, font)
        d.text((x0, yc - th / 2), text, font=font, fill=fill)

    y = pad + title_h
    for ri, (lab, vals, style, subs) in enumerate(rows):
        h = row_heights[ri]
        lab_bg = {"header": "#1a2744", "month": "#f59e0b", "total": "#0f1a35"}.get(style, "#f1f5f9")
        lab_fg = "white" if style in ("header", "total") else ("#1f2937" if style == "month" else "#0f172a")
        d.rectangle([pad, y, pad + label_w, y + h], fill=lab_bg, outline="#d0d7e2")
        _left(lab, pad + 8, y + h / 2, (f_bold if style in ("header", "total", "month") else f_reg), lab_fg)
        for ci in range(ncyc):
            x0 = pad + label_w + col_w * ci; x1 = x0 + col_w
            if style == "header":
                bg, fg, font = mother_colors.get(vals[ci], "#1a2744"), "white", f_bold
            elif style == "month":
                bg, fg, font = "#f59e0b", "#1f2937", f_bold
            elif style == "total":
                bg, fg, font = "#0f1a35", "white", f_bold
            elif style in ("opening", "startup"):
                bg, fg, font = "#f8fafc", "#475569", f_reg
            else:
                bg, fg, font = "#ffffff", "#0f172a", f_reg
            d.rectangle([x0, y, x1, y + h], fill=bg, outline="#d0d7e2")
            if subs and subs[ci]:
                _center(vals[ci], x0, x1, y + h / 2 - 8, font, fg)
                _center(subs[ci], x0, x1, y + h / 2 + 11, f_small, "#6366f1")
            else:
                _center(vals[ci], x0, x1, y + h / 2, font, fg)
        y += h

    buf = _io.BytesIO()
    img.save(buf, "PNG")
    return buf.getvalue()


def render_executive_summary(log_df, tl_df, S, params, start_iso):
    """Board-level executive summary: headline KPIs, export-schedule matrix,
    and a concise analytics readout.  Rendered first, in an expander so it never
    pushes the existing operational sections out of reach."""
    with st.expander("🧭  Executive Summary & Analytics", expanded=True):
        # ── Headline KPI strip ────────────────────────────────────────────────
        loaded   = S.get("loaded", 0)
        exported = S.get("exported", 0.0)
        produced = S.get("produced", 0.0)
        spilled  = S.get("spilled", 0.0)
        exports  = S.get("exports", 0)
        disch    = S.get("discharges", 0)
        evac_rate = (100.0 * exported / produced) if produced else 0.0
        spill_rate = (100.0 * spilled / produced) if produced else 0.0

        st.markdown('<div class="exec-kpi-row">', unsafe_allow_html=True)
        _k = st.columns(5)
        _kpis = [
            ("Crude Evacuated", f"{exported:,.0f}", "bbl exported to terminal"),
            ("Evacuation Rate", f"{evac_rate:.1f}%", "of production cleared"),
            ("Export Voyages",  f"{exports}", "completed mother sailings"),
            ("Volume Loaded",   f"{loaded:,.0f}", f"across {disch} discharges"),
            ("Spill / Overflow",f"{spilled:,.0f}", f"{spill_rate:.2f}% of production"),
        ]
        for col, (lab, val, sub) in zip(_k, _kpis):
            with col:
                st.markdown(
                    f'<div class="exec-kpi"><div class="exec-kpi-label">{lab}</div>'
                    f'<div class="exec-kpi-value">{val}</div>'
                    f'<div class="exec-kpi-sub">{sub}</div></div>',
                    unsafe_allow_html=True,
                )
        st.markdown('</div>', unsafe_allow_html=True)

        # ── Export schedule matrix (mother × cycle) ───────────────────────────
        cycles = _exec_extract_export_cycles(log_df, tl_df, opening={
            "Bryanston":  params.get("bryanston", 0),
            "GreenEagle": params.get("greeneagle", 0),
            "Alkebulan":  params.get("alkebulan", 0),
        })
        st.markdown('<div class="exec-section-title">⛴ Mother Vessel Export Schedule</div>',
                    unsafe_allow_html=True)
        if not cycles:
            st.caption("No completed export cycles in this run window.")
        else:
            def _fmt(dtv, fmt="%d-%b-%y"):
                return dtv.strftime(fmt) if (dtv is not None and pd.notna(dtv)) else "—"

            # Build an HTML matrix: columns = export cycles; rows = attributes.
            head_vessels = "".join(
                f'<th class="exm-vessel exm-{c["mother"].lower()}">{c["mother"]}</th>'
                for c in cycles)
            head_month = "".join(
                f'<td class="exm-month">{c["month"]}</td>' for c in cycles)

            # ── Per-source production breakdown rows (matching the brief) ──────
            # Display sources in operational order; only show a row if at least
            # one cycle drew from it, so the matrix stays compact.
            SRC_ORDER = ["SanBarth", "JasmineS", "Westmore", "Duke", "Starturn",
                         "PGM", "Ibom", "ZeeZee", "Opening", "Startup"]
            SRC_LABEL = {
                "SanBarth": "SanBarth Production", "JasmineS": "JasmineS Production",
                "Westmore": "Westmore Production", "Duke": "Duke Production",
                "Starturn": "Starturn Production", "PGM": "PGM Production",
                "Ibom": "Ibom Production", "ZeeZee": "ZeeZee 3rd Party",
                "Opening": "Opening Mother Stock", "Startup": "Startup Vessel Cargo",
            }
            present = [s for s in SRC_ORDER
                       if any((c.get("sources") or {}).get(s, 0) > 1 for c in cycles)]

            # ── Collapse toggle: hide the per-source production breakdown so the
            #    matrix shows only the headline rows (Month, Total, dates). ──────
            _show_breakdown = st.toggle(
                "Show production breakdown",
                value=True, key="exm_show_breakdown",
                help="Turn off to collapse the per-source production rows and see "
                     "only the Total Export Volume and the export dates.",
            )

            src_rows_html = ""
            if _show_breakdown:
                for s in present:
                    if s == "Startup":
                        # Startup row: show the volume AND list the vessels whose
                        # pre-sim cargo accounts for it, inside the same cell.
                        cells = ""
                        for c in cycles:
                            _amt = (c.get("sources") or {}).get(s, 0)
                            if _amt > 0:
                                _vs = c.get("startup_vessels") or []
                                _vtag = (f'<div style="font-size:10px;font-weight:600;'
                                         f'color:#6366f1;margin-top:2px;line-height:1.25">'
                                         f'{", ".join(_vs)}</div>') if _vs else ""
                                cells += f'<td class="exm-srcval">{_amt:,.0f}{_vtag}</td>'
                            else:
                                cells += '<td class="exm-srcval exm-zero">—</td>'
                    else:
                        cells = "".join(
                            (f'<td class="exm-srcval">{(c.get("sources") or {}).get(s, 0):,.0f}</td>'
                             if (c.get("sources") or {}).get(s, 0) > 0
                             else '<td class="exm-srcval exm-zero">—</td>')
                            for c in cycles)
                    _cls = "exm-srcrow exm-opening" if s in ("Opening", "Startup") else "exm-srcrow"
                    src_rows_html += f'<tr class="{_cls}"><td class="exm-rowlab">{SRC_LABEL.get(s, s)}</td>{cells}</tr>'

            row_total = "".join(
                f'<td class="exm-val">{c["volume_bbl"]:,.0f}</td>'
                if c["volume_bbl"] is not None else '<td class="exm-val">—</td>'
                for c in cycles)
            row_doc = "".join(f'<td>{_fmt(c["doc_date"])}</td>' for c in cycles)
            row_sail = "".join(f'<td>{_fmt(c["sail_date"])}</td>' for c in cycles)
            row_ret = "".join(f'<td>{_fmt(c["return_date"])}</td>' for c in cycles)

            # Assemble the whole table as a single, flush-left HTML string with no
            # blank lines.  Previously the f-string was indented and contained a
            # {src_rows_html} placeholder on its own line; when the breakdown was
            # collapsed that placeholder became an empty (whitespace) line, which
            # made Streamlit's Markdown engine close the HTML block early and dump
            # the remaining rows as raw text.  Total + date rows are always part of
            # the table, so collapsing only removes the per-source rows.
            _foot = ('Per-source figures are a mass-balance attribution of each export to the '
                     'production storages that fed it (blending through the '
                     'MTO transients is resolved proportionally). \u201cOpening\u201d/\u201cStartup\u201d '
                     'denote crude already on board at simulation start. Columns reconcile exactly '
                     'to Total Export Volume.')
            _table_html = (
                '<div class="exm-wrap"><table class="exm-table">'
                f'<thead><tr><th class="exm-rowlab">Vessel</th>{head_vessels}</tr></thead>'
                '<tbody>'
                f'<tr class="exm-monthrow"><td class="exm-rowlab">Month</td>{head_month}</tr>'
                f'{src_rows_html}'
                f'<tr class="exm-totalrow"><td class="exm-rowlab">Total Export Volume (bbl)</td>{row_total}</tr>'
                f'<tr><td class="exm-rowlab">Export Documentation</td>{row_doc}</tr>'
                f'<tr><td class="exm-rowlab">Commence Export Discharge</td>{row_sail}</tr>'
                f'<tr><td class="exm-rowlab">Complete Discharge / Return to BIA</td>{row_ret}</tr>'
                '</tbody></table></div>'
                f'<div class="exm-foot">{_foot}</div>'
            )
            st.markdown(_table_html, unsafe_allow_html=True)

            # ── Download the schedule as a PNG image ──────────────────────────
            try:
                _exm_png = _exec_schedule_image_bytes(
                    cycles, present, SRC_LABEL, _show_breakdown, MOTHER_COLORS
                )
                st.download_button(
                    "🖼️ Download schedule as image",
                    data=_exm_png,
                    file_name="mother_vessel_export_schedule.png",
                    mime="image/png",
                    key="exm_dl_img",
                )
            except Exception as _exm_e:
                st.caption(f"Image export unavailable: {_exm_e}")

        # ── Narrative analytics readout ───────────────────────────────────────
        st.markdown('<div class="exec-section-title">📊 Operational Readout</div>',
                    unsafe_allow_html=True)
        bullets = []
        if produced:
            bullets.append(
                f"The fleet cleared <b>{evac_rate:.1f}%</b> of the "
                f"{produced:,.0f} bbl produced over the planning horizon "
                f"({exported:,.0f} bbl exported across {exports} mother voyages).")
        if cycles:
            by_m = {}
            for c in cycles:
                by_m.setdefault(c["mother"], 0)
                by_m[c["mother"]] += 1
            cadence = ", ".join(f"{m}: {n} cycle{'s' if n!=1 else ''}"
                                for m, n in by_m.items())
            bullets.append(f"Export cadence — {cadence}.")
        if spilled and produced:
            sev = ("negligible" if spill_rate < 0.5 else
                   "minor" if spill_rate < 2 else "material")
            bullets.append(
                f"Storage overflow was <b>{sev}</b> at {spilled:,.0f} bbl "
                f"({spill_rate:.2f}% of production) across "
                f"{S.get('ovf_events', 0)} event(s).")
        else:
            bullets.append("No storage overflow occurred — production was fully absorbed.")
        all_moth = sum(v for k, v in S.items()
                       if k.startswith("final_") and any(
                           m in k for m in ("Bryanston", "GreenEagle")))
        bullets.append(
            f"Closing mother-vessel inventory stands at <b>{all_moth:,.0f} bbl</b> "
            f"available for the next export window.")
        st.markdown(
            '<ul class="exec-readout">' +
            "".join(f"<li>{b}</li>" for b in bullets) +
            '</ul>', unsafe_allow_html=True)


# =============================================================================
# ── MAIN ──────────────────────────────────────────────────────────────────────
# =============================================================================

def main():
    mod = _load_mod_current()

    # ── Auto-clear cache when a new sim version is deployed ────────────
    _deployed_ver = getattr(mod, "SIM_VERSION", "unknown")
    if st.session_state.get("_sim_version_loaded") != _deployed_ver:
        st.cache_data.clear()
        st.session_state["_sim_version_loaded"] = _deployed_ver


    # Constants
    SCAP        = mod.STORAGE_CAPACITY_BY_NAME
    MOTHER_CAP_BY_NAME = getattr(mod, "MOTHER_CAPACITY_BY_NAME", {
        "Bryanston": int(mod.MOTHER_CAPACITY_BBL),
        "GreenEagle": int(getattr(mod, "GREENEAGLE_CAPACITY_BBL", mod.MOTHER_CAPACITY_BBL)),
        "Alkebulan": int(getattr(mod, "ALKEBULAN_CAPACITY_BBL",
                                 getattr(mod, "GREENEAGLE_CAPACITY_BBL", mod.MOTHER_CAPACITY_BBL))),
    })
    EXPORT_TRIG = int(mod.MOTHER_EXPORT_TRIGGER)
    ALL_VESSELS = list(mod.VESSEL_NAMES)
    ALL_STATUS  = [code for _, items in STATUS_GROUPS
                   for code, _ in items]  # ordered by operational flow

    # ── Header ────────────────────────────────────────────────────────────────
    h1, h2 = st.columns([1, 11])
    with h1: st.markdown("# 🛢️")
    with h2:
        st.markdown("## Oil Tanker Daughter Vessel Operations — Live Dashboard")
        st.caption(
            "v5 · 10 vessels · 5 storage points SanBarth/Sego/Awoba/Dawes · "
            "3 mother vessels (Bryanston, GreenEagle, MT SanBarth) · "
            "Ibom Bedford/Balham · Cawthorne Channel routing"
        )
    st.divider()

    # ── Quick-run sidebar ────────────────────────────────────────────────────
    with st.sidebar:
        st.markdown("### ⚙️ Quick Run")
        _today = _dt.date.today()
        sim_start_date = st.date_input(
            "📅 Simulation Start Date",
            value=_today,
            min_value=_dt.date(2020, 1, 1),
            max_value=_dt.date(2035, 12, 31),
            format="DD/MM/YYYY",
            help=(
                "Day 1 of the forecast. Defaults to today.\n\n"
                "All event timestamps, chart axes and tidal lookups are anchored to this date."
            ),
            key="sim_start_date",
        )
        _dur_presets = {
            "1 day": 1,
            "3 days": 3,
            "1 week": 7,
            "2 weeks": 14,
            "1 month": 30,
            "2 months": 60,
            "3 months": 90,
            "6 months": 180,
            "9 months": 270,
            "12 months": 365,
            "Custom…": None,
        }
        _dur_sel = st.selectbox(
            "Simulation Duration",
            list(_dur_presets.keys()),
            index=4,
            key="dur_preset",
        )
        if _dur_presets[_dur_sel] is None:
            sim_days = st.number_input(
                "Custom days (1 – 365)", min_value=1, max_value=365,
                value=30, step=1, key="dur_custom"
            )
        else:
            sim_days = _dur_presets[_dur_sel]
        st.markdown(
            f'''<div class="quickrun-card">
            <div class="quickrun-label">Current Horizon</div>
            <div class="quickrun-value">{sim_days} day{'s' if sim_days != 1 else ''}</div>
            </div>''',
            unsafe_allow_html=True,
        )

        st.markdown("---")
        st.markdown("### ⚡ Run Behavior")
        auto_ref = st.toggle("Enable auto-refresh")
        ref_secs = st.slider("Interval (s)", 30, 600, 300, disabled=not auto_ref)
        run_opt = st.toggle(
            "Run Optimizer",
            help=(
                "Evaluates the parameter sweep and auto-selects the highest-scoring configuration."
            ),
        )
        if run_opt:
            st.caption("⏱️ ~60–120s first run · cached thereafter")

        st.markdown("---")
        st.markdown("### 🧹 Maintenance")
        if st.button("🗑️ Clear simulation cache", use_container_width=True):
            st.cache_data.clear()
            st.cache_resource.clear()
            st.session_state.pop("_sim_version_loaded", None)
            st.success("✅ Cache cleared — next run will be fully fresh.", icon="✅")

    st.caption("🔒 Daylight-operation constraints (06:00–18:00) are non-negotiable")

    _mother_opts = list(getattr(mod, "MOTHER_NAMES", ["Bryanston", "GreenEagle", "Alkebulan"]))
    _nom_vessels = list(getattr(mod, "VESSEL_NAMES", []))
    _EXPORT_TOKEN  = "Export Operation"
    _daughter_opts = _nom_vessels + [_EXPORT_TOKEN]
    _mother_defaults = {
        "Bryanston": ["Sherlock"],
        "GreenEagle": ["Laphroaig"],
        "GreenEagle": ["Watson"],
    }
    startup_day_manual_nominations = {}
    mother_export_seed = {}
    _selected_by_mother = {}

    _cv_storage_opts = [
        getattr(mod, "STORAGE_PRIMARY_NAME",    "SanBarth"),
        getattr(mod, "STORAGE_SECONDARY_NAME",  "JasmineS"),
        getattr(mod, "STORAGE_TERTIARY_NAME",   "Westmore"),
        getattr(mod, "STORAGE_QUATERNARY_NAME", "Duke"),
        getattr(mod, "STORAGE_QUINARY_NAME",    "Starturn"),
    ]
    _cv_existing_names = set(getattr(mod, "VESSEL_NAMES", []))

    sec("🎛️ Control Center")
    st.markdown(
        '<div class="control-shell">'
        '<div class="control-title">Operational setup and scenario inputs</div>'
        '<div class="control-subtitle">High-frequency forecasting actions stay visible here in the main workspace. The sidebar is now reserved for quick-run controls.</div>'
        '</div>',
        unsafe_allow_html=True,
    )

    _ctrl_day1, _ctrl_inputs, _ctrl_data, _ctrl_help, _ctrl_jmp = st.tabs([
        "Day 1 Startup",
        "Scenario Inputs",
        "Data & Constraints",
        "Reference & Help",
        "🗺️ JMP & Tides",
    ])

    with _ctrl_day1:
        startup_day_disable_point_b_priority = st.toggle(
            "Disable Point B auto-priority on startup day (Day 1 only)",
            value=False,
            help=(
                "Recommended OFF: Day 1 arriving vessels scan all available Point B mothers "
                "and auto-allocate to the best operational target, especially the mother closest "
                "to export volume. Manual nominations are no longer forced on startup day."
            ),
            key="startup_day_disable_point_b_priority",
        )

        # ── Nominations are derived from the vessel row UI below ──────────────
        # Each BIA vessel row has a "Nominated mother" / export selector.
        # We rebuild startup_day_manual_nominations and mother_export_seed here
        # so this tab always reflects the current vessel row state.
        for _vn2 in _nom_vessels:
            _row_is_exp   = st.session_state.get(f"vbia_export_{_vn2}", False)
            _row_mother   = st.session_state.get(f"vbia_mother_{_vn2}")
            _row_expmother= st.session_state.get(f"vbia_expmother_{_vn2}")
            _row_exp_days = st.session_state.get(f"vbia_expdays_{_vn2}", 3)
            if _row_is_exp and _row_expmother:
                mother_export_seed[_row_expmother] = int(_row_exp_days)
            elif _row_mother:
                startup_day_manual_nominations[_vn2] = _row_mother

        # ── Summary panel ─────────────────────────────────────────────────────
        with st.expander("📋 Day 1 Point B Summary (set in vessel rows below)",
                         expanded=False):
            st.caption(
                "Nominations and export seeds are configured per vessel in the "
                "'Enter 08:00 vessel positions' section. "
                "This panel shows the compiled Day 1 picture."
            )
            _seeded_total2  = len(startup_day_manual_nominations)
            _export_total2  = len(mother_export_seed)
            _unassigned2 = sorted(
                _vn for _vn in _nom_vessels
                if _vn not in startup_day_manual_nominations
                and not st.session_state.get(f"vbia_export_{_vn}", False)
            )
            _by_mother_disp2 = {}
            for _vn2, _mn2 in startup_day_manual_nominations.items():
                _by_mother_disp2.setdefault(_mn2, []).append(_vn2)
            _sum_html2 = []
            for _mn2, _vns2 in _by_mother_disp2.items():
                _mc2  = MOTHER_COLORS.get(_mn2, "#3b82f6")
                _chps = " ".join(
                    f'<span class="startup-chip">{_v2}</span>' for _v2 in _vns2)
                _sum_html2.append(
                    f'<div class="startup-card" style="border-left-color:{_mc2};margin-bottom:4px">'
                    f'<b style="font-size:11px">{_mn2}</b> '
                    f'<span style="font-size:10px;color:#64748b">({len(_vns2)} daughter(s))</span>'
                    f'<div style="margin-top:2px">{_chps}</div></div>'
                )
            for _mn2, _days2 in mother_export_seed.items():
                _mc2 = MOTHER_COLORS.get(_mn2, "#3b82f6")
                _sum_html2.append(
                    f'<div class="startup-card" style="border-left-color:{_mc2};'
                    f'background:#fff7ed;margin-bottom:4px">'
                    f'<b style="font-size:11px">{_mn2}</b> '
                    f'<span style="font-size:10px;color:#b45309">⛴ Export: {_days2} day(s)</span></div>'
                )
            if _sum_html2:
                st.markdown("".join(_sum_html2), unsafe_allow_html=True)
            st.markdown(
                f'<div class="startup-overall">'
                f'<span class="startup-chip">Nominated: {_seeded_total2}</span>'
                f'<span class="startup-chip">Export seeds: {_export_total2}</span>'
                f'<span class="startup-chip">Unassigned: {len(_unassigned2)}</span>'
                f'</div>',
                unsafe_allow_html=True,
            )
            if _unassigned2:
                st.info("Not yet assigned: " + ", ".join(_unassigned2), icon="ℹ️")
            else:
                st.success("All BIA vessels have a Day 1 assignment.", icon="✅")

        st.markdown("---")
        st.markdown("#### 🔄 Point B Congestion Relief")
        multiple_transient_operation = st.toggle(
            "Multiple Transient Operation",
            value=True,
            help=(
                "When ON: if 2 or more shuttle vessels are simultaneously stuck at Point B "
                "waiting because no mother vessel or roving storage is available to receive "
                "their cargo, a mid-day (12:00) nomination fires once per calendar day.\n\n"
                "**Transient vessel** — the largest-capacity waiting shuttle is held at Point B "
                "as temporary floating storage. It receives the cargo from the smallest waiting "
                "shuttle and is given absolute discharge priority to the mother vessel the "
                "following day.\n\n"
                "**Discharger** — the smallest waiting shuttle transfers its cargo to the "
                "transient vessel and is immediately freed to return and reload.\n\n"
                "**Constraints:** one transient nomination per calendar day only; skipped if "
                "any primary mother currently has space at BIA."
            ),
            key="multiple_transient_operation",
        )
        if multiple_transient_operation:
            _mto_col1, _mto_col2 = st.columns([1, 1])
            with _mto_col1:
                _mto_max_parcels = st.number_input(
                    "Max top-ups before no more accepted",
                    min_value=1, max_value=5, value=st.session_state.get("mto_max_parcels", 1),
                    step=1,
                    help=(
                        "How many additional shuttle cargoes the transient vessel may receive "
                        "on subsequent congested days **while it is still waiting for a mother "
                        "berth**.\n\n"
                        "This is **not** a 'fill before offload' target. The transient discharges "
                        "to the mother **opportunistically** — as soon as any berth window opens, "
                        "regardless of how much volume it is carrying. It does NOT wait to fill up.\n\n"
                        "**1** = after receiving one parcel, no further top-ups accepted "
                        "(transient still offloads whenever a berth opens).\n"
                        "**2-5** = allows additional shuttles to top up the transient on further "
                        "congested days, up to its capacity ceiling. Useful when the mother is "
                        "away at export for multiple days.\n\n"
                        "The optimizer sweeps this value when MTO is enabled."
                    ),
                    key="mto_max_parcels",
                )
            with _mto_col2:
                st.markdown(
                    '<div style="font-size:11px;font-weight:700;color:#166534;margin-bottom:4px">'
                    '🔄 MTO Transient Capacities (bbl)</div>'
                    '<div style="font-size:10px;color:#374151;line-height:1.8">'
                    'Balham / Bedford / Amyla / Bagshot: <b>125,000</b><br>'
                    'Laphroaig / Sherlock / Watson: <b>230,000</b><br>'
                    'Rathbone: <b>78,000</b> · Woodstock: <b>95,000</b><br>'
                    'Santa Monica: <b>35,000</b>'
                    '</div>',
                    unsafe_allow_html=True,
                )
            st.info(
                "**Multiple Transient Operation is ON.** "
                f"Transient vessels accumulate up to **{_mto_max_parcels} parcel(s)** before "
                "seeking an offloading window. "
                "MTO events: **MTO_TRANSIENT_NOMINATED**, **MTO_DISCHARGE_TO_TRANSIENT**, "
                "**MTO_TRANSIENT_PRIORITY_BERTH**.",
                icon="🔄",
            )

        st.markdown("---")
        enable_point_b_startup_seed = st.toggle(
            "Validation Seed: Start nominated BIA vessels at HOSE_CONNECT_B (full load)",
            value=False,
            help=(
                "When enabled, every vessel with a nominated mother in the vessel rows "
                "starts at t=0 in HOSE_CONNECT_B with full cargo at their assigned mother. "
                "Hose connection time is drawn from the 'Hose elapsed' field in each vessel row. "
                "Use this to validate Day 1 discharge sequencing without sailing time."
            ),
            key="enable_point_b_startup_seed",
        )
        point_b_startup_seed = {}
        if enable_point_b_startup_seed:
            point_b_startup_seed = dict(startup_day_manual_nominations)
            if not point_b_startup_seed and not mother_export_seed:
                st.warning(
                    "Validation seed enabled, but no BIA vessels have been nominated below.",
                    icon="⚠️",
                )

        # ── Variability & Realism ─────────────────────────────────────────────
        st.markdown("---")
        st.markdown("### 🎲 Variability & Realism")
        st.caption(
            "Enable stochastic variability to convert fixed-duration operations "
            "to probabilistic ones, reflecting real-world offshore logistics uncertainty. "
            "Disable for deterministic (reproducible) runs."
        )
        _var_col1, _var_col2 = st.columns([1, 2])
        with _var_col1:
            enable_variability = st.toggle(
                "Enable Operational Variability",
                value=st.session_state.get("enable_variability", False),
                key="enable_variability",
                help=(
                    "When ON: loading, discharge, transit, berthing and hose-connection "
                    "durations are sampled from calibrated triangular distributions. "
                    "Weather holds and equipment delays are injected probabilistically. "
                    "A calibration report is generated after each run showing planned "
                    "vs actual operation durations."
                ),
            )
        with _var_col2:
            if enable_variability:
                st.info(
                    "🎲 **Variability ON** — Operations will include stochastic delays. "
                    "Each run may differ slightly. Use a fixed Random Seed below for "
                    "reproducible results.",
                    icon="🎲",
                )
            else:
                st.caption("Deterministic mode — all durations are fixed nominal values.")

        _variability_params_json = None
        if enable_variability:
            with st.expander("⚙️ Variability Parameters (Calibration)", expanded=False):
                st.caption(
                    "Coefficient of Variation (CV = σ/μ): 0.05 = low, 0.15 = medium, "
                    "0.25 = high, 0.40 = severe. Calibrate against historical port records."
                )
                _vc1, _vc2, _vc3 = st.columns(3)
                with _vc1:
                    _cv_loading  = st.slider("Loading CV",      0.0, 0.5, 0.15, 0.01, key="cv_loading",
                                             help="Pump-rate uncertainty at storage point")
                    _cv_discharge= st.slider("Discharge CV",    0.0, 0.5, 0.12, 0.01, key="cv_discharge",
                                             help="Hose/valve variability at BIA")
                    _cv_transit  = st.slider("Transit CV",      0.0, 0.5, 0.10, 0.01, key="cv_transit",
                                             help="Current, weather and speed variability")
                with _vc2:
                    _cv_berthing = st.slider("Berthing CV",     0.0, 0.5, 0.20, 0.01, key="cv_berthing",
                                             help="Pilot availability, traffic separation")
                    _cv_hose     = st.slider("Hose Connect CV", 0.0, 0.5, 0.18, 0.01, key="cv_hose",
                                             help="Crew readiness, equipment condition")
                    _weather_prob= st.slider("Weather P (per hour)", 0.0, 0.20, 0.02, 0.005, key="weather_prob",
                                             help="Probability of weather hold per hour at sea")
                with _vc3:
                    _weather_hold= st.slider("Weather Hold Mean (h)", 0.5, 12.0, 3.0, 0.5, key="weather_hold",
                                             help="Mean duration of a weather hold event")
                    _equip_prob  = st.slider("Equipment Delay P", 0.0, 0.30, 0.08, 0.01, key="equip_prob",
                                             help="Probability of an inspection/equipment delay per load")
                    _rand_seed   = st.number_input("Random Seed (blank = random)", value=0,
                                                   min_value=0, max_value=99999, step=1,
                                                   key="var_rand_seed",
                                                   help="Set to a fixed number for reproducible stochastic runs. 0 = random.")
                _variability_params_json = json.dumps({
                    "cv_loading":      _cv_loading,
                    "cv_discharge":    _cv_discharge,
                    "cv_transit":      _cv_transit,
                    "cv_berthing":     _cv_berthing,
                    "cv_hose_connect": _cv_hose,
                    "weather_prob":    _weather_prob,
                    "weather_hold_h":  _weather_hold,
                    "equip_delay_prob":_equip_prob,
                    "random_seed":     int(_rand_seed) if _rand_seed else None,
                })
                st.session_state["_variability_params_json"] = _variability_params_json
        _day1_left, _day1_right = st.columns(2)
        with _day1_left:
            with st.container():
                st.markdown("#### 🚢 Add Custom Daughter Vessel(s)")
                st.caption(
                    "Schedule one or more new daughter vessels to join the fleet "
                    "on a specific date mid-simulation. Each vessel is dispatched "
                    "through the normal A→B cycle from its join date onward."
                )

            # Session-state list stores vessel dicts while the user builds the table
            if "custom_vessels" not in st.session_state:
                st.session_state["custom_vessels"] = []

            _cv_list = st.session_state["custom_vessels"]

            # ── Add-vessel form ──────────────────────────────────────────────
            with st.container():
                st.markdown("**Add a vessel**")
                _cva_col1, _cva_col2 = st.columns(2)
                with _cva_col1:
                    _cv_name_input = st.text_input(
                        "Vessel name",
                        key="cv_name_input",
                        placeholder="e.g. Aldgate",
                        help="Must be unique and not match any existing vessel name.",
                    )
                with _cva_col2:
                    _cv_cap_input = st.number_input(
                        "Capacity (bbl)",
                        min_value=1_000,
                        max_value=500_000,
                        value=85_000,
                        step=1_000,
                        key="cv_cap_input",
                        help="Maximum cargo per voyage in barrels.",
                    )
                _cv_date_input = st.date_input(
                    "Join date",
                    value=sim_start_date + _dt.timedelta(days=7),
                    min_value=sim_start_date,
                    max_value=sim_start_date + _dt.timedelta(days=365),
                    format="DD/MM/YYYY",
                    key="cv_date_input",
                    help="Calendar date the vessel becomes active (08:00 on that day).",
                )
                _cv_perms_input = st.multiselect(
                    "Permitted loading points",
                    options=_cv_storage_opts,
                    default=[_cv_storage_opts[0], _cv_storage_opts[1]],  # SanBarth + JasmineS
                    key="cv_perms_input",
                    help=(
                        "Storages this vessel may load from. "
                        "Select at least one. Defaults to SanBarth and JasmineS."
                    ),
                )
                _cv_add_clicked = st.button("➕ Add vessel", key="cv_add_btn")
                if _cv_add_clicked:
                    _cv_err = None
                    _cv_clean_name = _cv_name_input.strip()
                    if not _cv_clean_name:
                        _cv_err = "Please enter a vessel name."
                    elif _cv_clean_name in _cv_existing_names:
                        _cv_err = f"'{_cv_clean_name}' already exists in the fleet."
                    elif any(v["name"] == _cv_clean_name for v in _cv_list):
                        _cv_err = f"'{_cv_clean_name}' is already in the custom list."
                    elif not _cv_perms_input:
                        _cv_err = "Select at least one permitted loading point."
                    if _cv_err:
                        st.error(_cv_err, icon="❌")
                    else:
                        _cv_list.append({
                            "name":              _cv_clean_name,
                            "join_date":         _cv_date_input.isoformat(),
                            "cargo_capacity":    int(_cv_cap_input),
                            "permitted_storages": list(_cv_perms_input),
                        })
                        st.session_state["custom_vessels"] = _cv_list
                        st.rerun()

            # ── Vessel table ─────────────────────────────────────────────────
            if _cv_list:
                st.markdown("**Scheduled custom vessels**")
                for _cvi, _cvr in enumerate(_cv_list):
                    _cv_row_cols = st.columns([3, 2, 3, 1])
                    with _cv_row_cols[0]:
                        st.markdown(
                            f"**{_cvr['name']}**  \n"
                            f"<span style='font-size:0.82em;color:#888'>joins {_cvr['join_date']}</span>",
                            unsafe_allow_html=True,
                        )
                    with _cv_row_cols[1]:
                        st.markdown(
                            f"<span style='font-size:0.88em'>{_cvr['cargo_capacity']:,} bbl</span>",
                            unsafe_allow_html=True,
                        )
                    with _cv_row_cols[2]:
                        st.markdown(
                            f"<span style='font-size:0.82em;color:#555'>"
                            f"{', '.join(_cvr['permitted_storages'])}</span>",
                            unsafe_allow_html=True,
                        )
                    with _cv_row_cols[3]:
                        if st.button("✕", key=f"cv_remove_{_cvi}",
                                     help=f"Remove {_cvr['name']}"):
                            _cv_list.pop(_cvi)
                            st.session_state["custom_vessels"] = _cv_list
                            st.rerun()
                if st.button("🗑️ Clear all", key="cv_clear_all"):
                    st.session_state["custom_vessels"] = []
                    st.rerun()
            else:
                st.info(
                    "No custom vessels configured. "
                    "Use the form above to add a vessel.",
                    icon="ℹ️",
                )

        with _day1_right:
            with st.container():
                st.info(
                    "⏸️ **Shuttle vessel unavailability** settings have moved to the "
                    "**Availability Windows** section below — use the unified panel "
                    "to schedule downtime for both shuttle and mother vessels.",
                    icon="ℹ️",
                )

    _custom_vessels_json = (
        json.dumps(st.session_state.get("custom_vessels", []))
        if st.session_state.get("custom_vessels")
        else None
    )
    _vessel_resumption_json = (
        json.dumps(list(st.session_state.get("vessel_resumptions", {}).values()))
        if st.session_state.get("vessel_resumptions")
        else None
    )

    with _ctrl_inputs:
        st.markdown("**📦 Production Rates (bbl/hr)**")
        _r1c1, _r1c2, _r1c3 = st.columns(3)
        with _r1c1:
            prod_sanbarth = st.number_input("SanBarth (Point A)", 0, 5000, int(mod.PRODUCTION_RATE_BPH), step=50, key="pr_sanbarth")
        with _r1c2:
            prod_jasmines = st.number_input("JasmineS (SanBarth)", 0, 5000, int(mod.PRODUCTION_RATE_BPH), step=50, key="pr_jasmines")
        with _r1c3:
            prod_westmore = st.number_input("Westmore (Sego)", 0, 2000, int(mod.WESTMORE_PRODUCTION_RATE_BPH), step=50, key="pr_westmore")
        _r2c1, _r2c2, _r2c3 = st.columns(3)
        with _r2c1:
            prod_duke = st.number_input("Duke (Awoba)", 0, 1000, int(mod.DUKE_PRODUCTION_RATE_BPH), step=10, key="pr_duke")
        with _r2c2:
            prod_starturn = st.number_input("Starturn (Dawes)", 0, 500, int(mod.STARTURN_PRODUCTION_RATE_BPH), step=10, key="pr_starturn")
        with _r2c3:
            prod_pgm = st.number_input("PGM (Point G)", 0, 200,
                int(getattr(mod, "PGM_PRODUCTION_RATE_BPH", 80)), step=5, key="pr_pgm")
        _r3c1, _r3c2, _r3c3 = st.columns(3)
        with _r3c1:
            prod_ibom = st.number_input(
                "Ibom", 0, 500,
                int(getattr(mod, "IBOM_LOAD_RATE_BPH", getattr(mod, "POINT_F_LOAD_RATE_BPH", 165))),
                step=10, key="pr_ibom"
            )

        st.markdown("---")
        st.markdown("### Production Override Window")
        enable_prod_window_override = st.toggle(
            "Apply custom production rates for a date range",
            value=False,
            help=(
                "Use this to temporarily override storage production rates within a specific calendar window (inclusive). Example: set all rates to 0 bph from Mar 10 to Mar 18."
            ),
            key="enable_prod_window_override",
        )
        production_overrides = []
        if enable_prod_window_override:
            _pw_c1, _pw_c2 = st.columns(2)
            with _pw_c1:
                prod_window_start = st.date_input("Override start date", value=sim_start_date, key="prod_window_start", format="DD/MM/YYYY")
            with _pw_c2:
                prod_window_end = st.date_input("Override end date", value=sim_start_date, key="prod_window_end", format="DD/MM/YYYY")

            st.caption("Override rates (bbl/hr) applied only inside this date range.")
            _pw_r1c1, _pw_r1c2, _pw_r1c3 = st.columns(3)
            with _pw_r1c1:
                ovr_sanbarth = st.number_input("SanBarth override", 0, 5000, int(prod_sanbarth), step=50, key="ovr_pr_sanbarth")
            with _pw_r1c2:
                ovr_jasmines = st.number_input("JasmineS override", 0, 5000, int(prod_jasmines), step=50, key="ovr_pr_jasmines")
            with _pw_r1c3:
                ovr_westmore = st.number_input("Westmore override", 0, 2000, int(prod_westmore), step=50, key="ovr_pr_westmore")
            _pw_r2c1, _pw_r2c2, _pw_r2c3 = st.columns(3)
            with _pw_r2c1:
                ovr_duke = st.number_input("Duke override", 0, 1000, int(prod_duke), step=10, key="ovr_pr_duke")
            with _pw_r2c2:
                ovr_starturn = st.number_input("Starturn override", 0, 500, int(prod_starturn), step=10, key="ovr_pr_starturn")
            with _pw_r2c3:
                ovr_pgm = st.number_input("PGM override", 0, 200, int(prod_pgm), step=5, key="ovr_pr_pgm")

            _start_d = prod_window_start
            _end_d = prod_window_end
            if _end_d < _start_d:
                _start_d, _end_d = _end_d, _start_d
            production_overrides = [{
                "start_date": _start_d.isoformat(),
                "end_date": _end_d.isoformat(),
                "rates": {
                    "SanBarth": ovr_sanbarth,
                    "JasmineS": ovr_jasmines,
                    "Westmore": ovr_westmore,
                    "Duke": ovr_duke,
                    "Starturn": ovr_starturn,
                    "PGM": ovr_pgm,
                },
            }]
            st.caption(f"Override active: {_start_d.strftime('%d/%m/%Y')} to {_end_d.strftime('%d/%m/%Y')}")

        # ── Vessel Availability Windows ───────────────────────────────────────
        st.markdown("---")
        st.markdown("### 🚫 Vessel Availability Windows")
        st.caption(
            "Schedule planned unavailability windows and resumption dates for any vessel — "
            "both **mother vessels** (Bryanston, GreenEagle, MT SanBarth) and **shuttle daughters**. "
            "During a mother's window she cannot receive daughters — "
            "traffic is automatically rerouted. A shuttle vessel resumes at 08:00 on the "
            "resumption date and takes loading priority at the assigned storage."
        )

        # ── TAB 1: Mother Vessel Unavailability Windows ───────────────────────
        _avail_tab1, _avail_tab2 = st.tabs(["🛢️ Mother Vessel Unavailability", "⏸️ Shuttle Vessel Unavailability"])

        with _avail_tab1:
            st.caption(
                "Mark a primary mother vessel unavailable for a date range (e.g. dry-dock, "
                "maintenance, redeployment). The vessel resumes normal operations when the window ends."
            )

            if "mother_unavailability_windows" not in st.session_state:
                st.session_state["mother_unavailability_windows"] = []

            _muv_list = st.session_state["mother_unavailability_windows"]
            _all_muv_mothers = list(_mother_opts)

            _muv_c1, _muv_c2, _muv_c3, _muv_c4 = st.columns([2, 2, 2, 1])
            with _muv_c1:
                _muv_mother = st.selectbox(
                    "Mother vessel",
                    _all_muv_mothers,
                    key="muv_mother_sel",
                    label_visibility="collapsed",
                    placeholder="Select mother…",
                )
            with _muv_c2:
                _muv_start = st.date_input(
                    "Unavailable from",
                    value=sim_start_date,
                    format="DD/MM/YYYY",
                    key="muv_start_sel",
                    label_visibility="collapsed",
                )
            with _muv_c3:
                _muv_end = st.date_input(
                    "Available again from",
                    value=sim_start_date + _dt.timedelta(days=7),
                    format="DD/MM/YYYY",
                    key="muv_end_sel",
                    label_visibility="collapsed",
                )
            with _muv_c4:
                if st.button("➕ Add", key="muv_add_btn"):
                    _muv_err = None
                    if _muv_end <= _muv_start:
                        _muv_err = "End date must be after start date."
                    if _muv_err:
                        st.error(_muv_err, icon="❌")
                    else:
                        _muv_list.append({
                            "mother":     _muv_mother,
                            "start_date": _muv_start.isoformat(),
                            "end_date":   _muv_end.isoformat(),
                        })
                        st.session_state["mother_unavailability_windows"] = _muv_list
                        st.rerun()

            if _muv_list:
                _muvh_c1, _muvh_c2, _muvh_c3, _muvh_c4 = st.columns([2, 2, 2, 1])
                with _muvh_c1: st.caption("Mother")
                with _muvh_c2: st.caption("Unavailable from")
                with _muvh_c3: st.caption("Available again from")
                with _muvh_c4: st.caption("")

                for _muvi, _muvr in enumerate(_muv_list):
                    _mc = MOTHER_COLORS.get(_muvr["mother"], "#94a3b8")
                    _muv_row_c1, _muv_row_c2, _muv_row_c3, _muv_row_c4 = st.columns([2, 2, 2, 1])
                    _sd_fmt = _dt.date.fromisoformat(_muvr["start_date"]).strftime("%d %b %Y")
                    _ed_fmt = _dt.date.fromisoformat(_muvr["end_date"]).strftime("%d %b %Y")
                    _dur = (_dt.date.fromisoformat(_muvr["end_date"]) -
                            _dt.date.fromisoformat(_muvr["start_date"])).days
                    with _muv_row_c1:
                        st.markdown(
                            f'<span style="display:inline-block;background:{_mc};color:#fff;'
                            f'font-weight:700;font-size:12px;padding:2px 10px;border-radius:4px">'
                            f'{_muvr["mother"]}</span>',
                            unsafe_allow_html=True,
                        )
                    with _muv_row_c2:
                        st.markdown(f"**{_sd_fmt}**")
                    with _muv_row_c3:
                        st.markdown(f"**{_ed_fmt}** · {_dur}d window")
                    with _muv_row_c4:
                        if st.button("✕", key=f"muv_remove_{_muvi}"):
                            _muv_list.pop(_muvi)
                            st.session_state["mother_unavailability_windows"] = _muv_list
                            st.rerun()

                if st.button("🗑️ Clear all windows", key="muv_clear_all"):
                    st.session_state["mother_unavailability_windows"] = []
                    st.rerun()
            else:
                st.info(
                    "No unavailability windows configured. Use the row above to add one.",
                    icon="ℹ️",
                )

        # ── TAB 2: Shuttle Vessel Unavailability ──────────────────────────────
        with _avail_tab2:
            st.caption(
                "Select a vessel and choose its unavailability window (start → end date). "
                "The vessel operates normally up to the start date, is excluded from the model "
                "during the window, then resumes at 08:00 on the end date at the chosen priority storage. "
                "Use **Indefinite** to exclude a vessel for the entire simulation run "
                "(dry-dock, repair, or redeployment). "
                "**Note:** if the vessel is loaded with cargo on the unavailability start date, "
                "it will first complete its BIA discharge, then become unavailable."
            )

            if "vessel_resumptions" not in st.session_state:
                st.session_state["vessel_resumptions"] = {}

            _vr_dict = st.session_state["vessel_resumptions"]

            _all_vessels = list(getattr(mod, "VESSEL_NAMES", []))
            _storage_opts = [
                getattr(mod, "STORAGE_PRIMARY_NAME",    "SanBarth"),
                getattr(mod, "STORAGE_SECONDARY_NAME",  "JasmineS"),
                getattr(mod, "STORAGE_TERTIARY_NAME",   "Westmore"),
                getattr(mod, "STORAGE_QUATERNARY_NAME", "Duke"),
                getattr(mod, "STORAGE_QUINARY_NAME",    "Starturn"),
            ]
            _sim_start = st.session_state.get("sim_start_date")
            _sim_end   = (
                _sim_start + _dt.timedelta(days=sim_days - 1)
                if _sim_start else None
            )

            _vr_c1, _vr_c2 = st.columns([2, 2])
            with _vr_c1:
                _vr_vessel = st.selectbox(
                    "Vessel", _all_vessels, key="vr_vessel_sel",
                    label_visibility="collapsed"
                )
            with _vr_c2:
                _vr_storage = st.selectbox(
                    "Priority storage on resumption", _storage_opts, key="vr_storage_sel",
                    label_visibility="collapsed"
                )

            _vr_indefinite = st.checkbox(
                "♾️ Indefinite (vessel excluded from JMP for entire run)",
                key="vr_indefinite",
                help=(
                    "When ticked, the vessel is held idle indefinitely — it will never "
                    "load or appear in the JMP output for this simulation run. "
                    "Useful for vessels under repair, dry-dock, or otherwise unavailable "
                    "for the full planning period."
                ),
            )

            _vr_c3, _vr_c4 = st.columns([3, 1])
            with _vr_c3:
                if not _vr_indefinite:
                    # Date range: (unavailability_start, resumption_date)
                    _vr_range = st.date_input(
                        "Unavailability window (start → resumption date)",
                        value=(_sim_start, _sim_start + _dt.timedelta(days=3)) if _sim_start else None,
                        min_value=_sim_start,
                        max_value=_sim_end,
                        key="vr_date_sel",
                        label_visibility="collapsed",
                        help=(
                            "Select the unavailability window. The vessel operates normally "
                            "up to the **start date**, then becomes unavailable. "
                            "If it is loaded with cargo on the start date, it first completes "
                            "its BIA discharge before going inactive. "
                            "It resumes loading at 08:00 on the **end date**."
                        ),
                    )
                    # date_input in range mode may return:
                    #   (date, date) — both ends chosen (normal)
                    #   (date,)      — only start chosen (user still selecting end)
                    #   date         — non-range single date
                    # Always extract raw datetime.date objects before calling .isoformat()
                    # so we never store the repr() of the tuple as a string.
                    if isinstance(_vr_range, (list, tuple)):
                        if len(_vr_range) >= 2:
                            _vr_start_date = _vr_range[0]
                            _vr_end_date   = _vr_range[1]
                        elif len(_vr_range) == 1:
                            # Partial selection — only start picked, end not yet chosen.
                            # Hold off storing until both ends are selected.
                            _vr_start_date = _vr_range[0]
                            _vr_end_date   = None   # signals incomplete range
                        else:
                            _vr_start_date = _vr_end_date = None
                    else:
                        # Single date object — treat as same start/end
                        _vr_start_date = _vr_end_date = _vr_range
                else:
                    _vr_start_date = _vr_end_date = None
                    st.markdown(
                        '<div style="font-size:11px;color:#7c3aed;padding-top:6px">'
                        '♾️ No resumption date — vessel sits out the entire run.</div>',
                        unsafe_allow_html=True,
                    )
            with _vr_c4:
                if st.button("➕ Set", key="vr_add_btn", use_container_width=True):
                    if _vr_indefinite:
                        _vr_dict[_vr_vessel] = {
                            "name":         _vr_vessel,
                            "start_date":   "indefinite",
                            "date":         "indefinite",   # backend key (resumption date)
                            "storage":      _vr_storage,
                            "indefinite":   True,
                        }
                    elif _vr_end_date is None:
                        # Incomplete date range — user only picked the start date.
                        # Show a warning and do not save to avoid storing an
                        # unparseable repr string like "(datetime.date(2026, 5, 8),)".
                        st.warning(
                            "⚠️ Please select both the **start** and **end** dates "
                            "of the unavailability window before clicking Set.",
                            icon="📅",
                        )
                    else:
                        # Always use .isoformat() on the raw date objects — never str()
                        # on a tuple, which would produce an unparseable repr string.
                        _vr_start_iso = (
                            _vr_start_date.isoformat()
                            if hasattr(_vr_start_date, "isoformat")
                            else str(_vr_start_date)
                        )
                        _vr_end_iso = (
                            _vr_end_date.isoformat()
                            if hasattr(_vr_end_date, "isoformat")
                            else str(_vr_end_date)
                        )
                        _vr_dict[_vr_vessel] = {
                            "name":         _vr_vessel,
                            "start_date":   _vr_start_iso,
                            "date":         _vr_end_iso,   # resumption date → backend
                            "storage":      _vr_storage,
                            "indefinite":   False,
                        }
                    st.session_state["vessel_resumptions"] = _vr_dict
                    st.rerun()

            if _vr_dict:
                st.markdown("---")
                _col_h1, _col_h2, _col_h3, _col_h4, _col_h5 = st.columns([2, 2, 2, 2, 1])
                with _col_h1: st.caption("Vessel")
                with _col_h2: st.caption("Unavailable from")
                with _col_h3: st.caption("Resumes on")
                with _col_h4: st.caption("Priority storage")
                with _col_h5: st.caption("")

                for _vr_key, _vr_entry in list(_vr_dict.items()):
                    _ta, _tb, _tc, _td, _te = st.columns([2, 2, 2, 2, 1])
                    with _ta:
                        _vc3 = VESSEL_COLORS.get(_vr_entry["name"], "#64748b")
                        st.markdown(
                            f'<span style="background:{_vc3};color:#fff;border-radius:4px;'
                            f'padding:2px 8px;font-size:11px;font-weight:700">'
                            f'{_vr_entry["name"]}</span>',
                            unsafe_allow_html=True,
                        )
                    with _tb:
                        if _vr_entry.get("indefinite"):
                            st.markdown(
                                '<span style="font-size:11px;color:#7c3aed;font-weight:600">'
                                '♾️ Entire run</span>',
                                unsafe_allow_html=True,
                            )
                        else:
                            _sd = _vr_entry.get("start_date", _vr_entry.get("date", ""))
                            try:
                                st.markdown(_dt.date.fromisoformat(_sd).strftime("%d %b %Y"))
                            except Exception:
                                st.markdown(_sd)
                    with _tc:
                        if _vr_entry.get("indefinite"):
                            st.markdown(
                                '<span style="font-size:11px;color:#7c3aed;font-weight:600">'
                                '♾️ Never</span>',
                                unsafe_allow_html=True,
                            )
                        else:
                            _rd = _vr_entry.get("date", "")
                            try:
                                _rd_fmt = _dt.date.fromisoformat(_rd).strftime("%d %b %Y")
                                _sd2 = _vr_entry.get("start_date", _rd)
                                try:
                                    _dur = (_dt.date.fromisoformat(_rd) -
                                            _dt.date.fromisoformat(_sd2)).days
                                    st.markdown(f"**{_rd_fmt}** · {_dur}d")
                                except Exception:
                                    st.markdown(f"**{_rd_fmt}**")
                            except Exception:
                                st.markdown(_rd)
                    with _td:
                        st.markdown(_vr_entry["storage"])
                    with _te:
                        if st.button("✕", key=f"vr_rm_{_vr_key}", use_container_width=True):
                            del _vr_dict[_vr_key]
                            st.session_state["vessel_resumptions"] = _vr_dict
                            st.rerun()
                if st.button("🗑️ Clear all unavailability windows", key="vr_clear_all"):
                    st.session_state["vessel_resumptions"] = {}
                    st.rerun()
            else:
                st.info("No shuttle vessel unavailability windows set.", icon="ℹ️")

    with _ctrl_data:

        # ======================================================================
        # ── PDF STOCK REPORT IMPORTER ─────────────────────────────────────────
        # ======================================================================
        st.markdown("### 📄 Load from Daily Stock Report (CSV)")
        st.caption(
            "Export the Daily Stock Update Excel as CSV (File → Save As → CSV), then upload here. "
            "Data is extracted instantly — no API key or internet connection needed."
        )

        # ── CSV parser (no AI, no API key — pure column-position parsing) ──────
        def _parse_stock_csv(file_bytes):
            """Parse the Daily Stock Update CSV and return the same dict structure
            as the old PDF extractor, so all downstream code works unchanged."""
            import csv, io, re, datetime

            def _clean(v):
                v = str(v).strip().replace("\xa0", "").replace("?", "").strip()
                v = v.replace(",", "")
                if v.startswith("(") and v.endswith(")"):
                    v = "-" + v[1:-1]
                return v

            def _num(rows, r, c):
                try:
                    return int(float(_clean(rows[r][c])))
                except Exception:
                    return 0

            def _cell(rows, r, c):
                try:
                    return _clean(rows[r][c])
                except Exception:
                    return ""

            text = file_bytes.decode("utf-8", errors="replace")
            rows = list(csv.reader(io.StringIO(text)))

            # ── Report date (row 1, col 4) ────────────────────────────────────
            raw_date = _cell(rows, 1, 4)  # "20/5/2026 07:00:00" or "05/04/2026 07:00:00"
            report_date = ""
            _date_part = raw_date.split()[0] if raw_date else ""
            # The report uses DD/M/YYYY (day first).  Try that first, then
            # the US-style MM/DD/YYYY variant used in some older reports,
            # then a plain YYYY-MM-DD fallback.
            for _fmt in ("%d/%m/%Y", "%m/%d/%Y", "%Y-%m-%d"):
                try:
                    report_date = datetime.datetime.strptime(_date_part, _fmt).date().isoformat()
                    break
                except Exception:
                    pass
            if not report_date:
                # Last resort: return whatever is there; downstream will show a warning
                report_date = _date_part

            # ── Storage vessels (rows 7-13) ──────────────────────────────────
            # Actual opening stock:
            #   Westmore (r7)  → col 8   JasmineS (r8)  → col 8
            #   SanBarth   (r9)  → col 8   Duke     (r10) → col 7
            #   Ibom     (r11) → col 7   Asaramatoru (r12) → col 7
            #   Starturn (r13) → col 7
            _SV_ROWS = {
                "Westmore":  (7,  8),
                "JasmineS":  (8,  8),
                "SanBarth":    (9,  8),
                "Duke":      (10, 7),
                "Starturn":  (13, 7),
                "PGM":       (12, 7),   # Asaramatoru field = PGM storage in the sim
            }
            storage_volumes = {k: abs(_num(rows, r, c)) for k, (r, c) in _SV_ROWS.items()}

            # Ibom actual (row 11, col 7) — used to detect which vessel is loading
            _ibom_stock = abs(_num(rows, 11, 7))

            # Ibom comment (row 11, col 9) — names the loading vessel
            # Ibom loader is named in the IBOM row next-day comment (col 22)
            # e.g. "Discharge of cargo into MT Bedford is ongoing."
            _ibom_comment     = _cell(rows, 11, 9).lower()   # current comment
            _ibom_nextcomment = _cell(rows, 11, 22).lower()  # next-day comment
            _ibom_both        = _ibom_comment + " " + _ibom_nextcomment
            _ibom_loader  = ""
            for _vn in ["bedford", "balham", "watson", "woodstock", "bagshot",
                        "rathbone", "sherlock", "laphroaig", "santa monica"]:
                if _vn in _ibom_both:
                    _ibom_loader = _vn.title().replace(" ", "")
                    break

            # ── Mother vessels (rows 29-31, col 9 = actual ROB) ─────────────
            # Row layout (with Woodstock now at row 28):
            #   row 29 = MT Bryanston
            #   row 31 = MT Green Eagle
            mother_volumes = {
                "Bryanston": abs(_num(rows, 29, 9)),
                "GreenEagle": abs(_num(rows, 31, 9)),
            }

            # ── Daughter vessels (rows 18-27) ────────────────────────────────
            # col 9  = actual ROB (cargo on board)
            # col 10 = comment text
            # Loading-from: col 11=SanBarth, 12=JasmineS, 13=Westmore, 14=Duke,
            #               15=Ibom, 16=Petralon54, 17=Asaramatoru
            # Discharging-to: col 18=GreenEagle, 19=Bryanston
            # Scheduled Loading cols: 11=SanBarth 12=JasmineS 13=Westmore
            #                        14=Duke   15=Ibom     17=Asaramatoru(not a sim storage)
            _STOR_COLS = {
                11: "SanBarth", 12: "JasmineS", 13: "Westmore", 14: "Duke", 15: "Ibom",
                17: "PGM",   # col 17 = Asaramatoru field barge = PGM storage in sim
            }
            # Discharge @ BIA cols: 18=GreenEagle  19=Bryanston
            _MOM_COLS = {18: "GreenEagle", 19: "Bryanston"}

            # ── MTO (Mid-Transfer Operation) detection ────────────────────────
            # Col 21 = "Trans. Storage 2 - MT Watson" (or whichever vessel acts as MTO)
            # Col 22 = "Trans. Storage 3" (spare)
            # Positive value in col 21 → that vessel IS the MTO receiver
            # Negative value in col 21 → that vessel is DISCHARGING into the MTO receiver
            _ALL_DAUGHTER_NAMES = ["Sherlock","Laphroaig","Watson","Bedford","Balham",
                                   "Amyla","Bagshot","Rahama","Rathbone","SantaMonica","Woodstock"]
            _MTO_SCAN_ROWS = list(range(18, 29))   # rows 18-28 inclusive (Woodstock now at row 28)

            # Find the MTO receiver: positive value in col 21
            _mto_receiver_name = ""
            _mto_receiver_row  = None
            for _mr in _MTO_SCAN_ROWS:
                _v21 = _num(rows, _mr, 21)
                if _v21 > 0:
                    _raw = _cell(rows, _mr, 2).lower()
                    for _dn in _ALL_DAUGHTER_NAMES:
                        if _dn.lower() in _raw:
                            _mto_receiver_name = _dn
                            _mto_receiver_row  = _mr
                            break
                if _mto_receiver_name:
                    break

            # ── Fallback: over-capacity cargo detection ───────────────────────
            # When the operator leaves the Trans. Storage col-21 blank (as on
            # 20 May 2026), detect the MTO receiver from the cargo alone:
            # any vessel carrying more than her rated capacity must be an MTO
            # transient (the only operational explanation for over-capacity cargo).
            if not _mto_receiver_name:
                _NOMINAL_CAPS = {
                    "Sherlock": 85_000, "Laphroaig": 85_000, "Watson": 85_000,
                    "Bedford":  66_000, "Balham":    66_000, "Amyla":  67_000,
                    "Bagshot":  45_000, "Rahama":    35_000, "Rathbone": 46_000,
                    "SantaMonica": 28_000, "Woodstock": 42_000,
                }
                # A vessel is only an MTO receiver when she carries SUBSTANTIALLY
                # more than her rated capacity — not a trivial overload from storage.
                # Threshold: cargo must exceed nominal by at least 30% AND 15,000 bbl.
                # This correctly catches Watson at 201k (237% of 85k cap) while
                # ignoring Woodstock at 45k (108% of 42k cap = normal over-load).
                _MTO_EXCESS_PCT  = 1.30   # 30% above nominal minimum
                _MTO_EXCESS_ABS  = 15_000  # AND at least 15,000 bbl absolute margin
                for _mr in _MTO_SCAN_ROWS:
                    _cargo9 = abs(_num(rows, _mr, 9))
                    if _cargo9 == 0:
                        continue
                    _raw = _cell(rows, _mr, 2).lower()
                    for _dn in _ALL_DAUGHTER_NAMES:
                        if _dn.lower() in _raw:
                            _nom = _NOMINAL_CAPS.get(_dn, 85_000)
                            if (_cargo9 > _nom * _MTO_EXCESS_PCT
                                    and _cargo9 > _nom + _MTO_EXCESS_ABS):
                                # Substantially over capacity → MTO receiver
                                _mto_receiver_name = _dn
                                _mto_receiver_row  = _mr
                            break
                    if _mto_receiver_name:
                        break

            # Find MTO dischargers: negative values in col 21 or col 20
            # (both are "Trans. Storage" columns).  Negative = vessel discharged
            # into the transient.
            # When col-21 is blank (as on 20 May) the operator may still have
            # written discharger volumes into col-20, or the volumes are simply
            # not recorded — in the latter case fall back to inferring from
            # the receiver's over-capacity cargo amount.
            _mto_dischargers = {}  # {vessel_name: volume_discharged_bbl}
            for _mr in _MTO_SCAN_ROWS:
                if _mr == _mto_receiver_row:
                    continue
                # Check both col 20 and col 21
                for _mto_col in (20, 21):
                    _vmt = _num(rows, _mr, _mto_col)
                    if _vmt < 0:
                        _raw = _cell(rows, _mr, 2).lower()
                        for _dn in _ALL_DAUGHTER_NAMES:
                            if _dn.lower() in _raw:
                                # Use largest negative value across both cols
                                _mto_dischargers[_dn] = max(
                                    _mto_dischargers.get(_dn, 0), abs(_vmt))
                                break
                        break

            _DAUGHTER_ROWS = {
                "Sherlock":    18,   # CSV row 18
                "Laphroaig":   19,   # CSV row 19
                "Watson":      20,   # CSV row 20
                "Bedford":     21,   # CSV row 21
                "Balham":      22,   # CSV row 22
                "Amyla":       23,   # CSV row 23 — must come before Bagshot
                "Bagshot":     24,   # CSV row 24
                "Rahama":      25,   # CSV row 25
                "Rathbone":    26,   # CSV row 26
                "SantaMonica": 27,   # CSV row 27
                "Woodstock":   28,   # CSV row 28
            }

            daughter_vessels = []
            for _vname, _r in _DAUGHTER_ROWS.items():
                _cargo        = abs(_num(rows, _r, 9))
                _comment      = _cell(rows, _r, 10).lower()
                _next_comment = _cell(rows, _r, 25).lower()  # next-day comment col

                # Assigned storage from Scheduled Loading columns.
                # Accept any non-empty cell value including sentinel "1" (written
                # by the corrected CSV builder for zero-cargo vessels at a storage).
                _stor = ""
                for _c, _sn in _STOR_COLS.items():
                    _raw_cv = _cell(rows, _r, _c).strip()
                    if _raw_cv and _raw_cv != "0" and _raw_cv != "":
                        _stor = _sn
                        break

                # Target mother from Discharge @ BIA columns
                # A non-zero value (positive OR negative) in the daughter row means
                # this vessel is actively discharging INTO that mother.
                # Negative values arise when the CSV records the transfer as a
                # debit on the daughter side (e.g. Watson col-18 = -80,598 → GreenEagle).
                _mom = ""
                for _c, _mn in _MOM_COLS.items():
                    v = _num(rows, _r, _c)
                    if v != 0:   # non-zero (pos or neg) = discharging into this mother
                        _mom = _mn
                        break

                # Special: Rahama goes to Bryanston for decanted water
                if _vname == "Rahama" and not _mom:
                    if ("bryanston" in _next_comment or "decanted" in _next_comment
                            or "bryanston" in _comment):
                        _mom = "Bryanston"

                # Derive status from comment + cargo + assignments
                if (_vname == _ibom_loader
                        or _vname.lower() in _ibom_both
                        or "ibom" in _next_comment or "5-buoy" in _next_comment
                        or "ibom field" in _next_comment):
                    # Vessel loading at Ibom buoy — ROB shows 0, use field stock
                    _status = "PF_LOADING"
                    _stor   = "Ibom"
                    _cargo  = _ibom_stock
                elif _vname == _mto_receiver_name:
                    # MTO receiver — accumulating cargo from dischargers
                    _status = "PF_SWAP"
                    _stor   = ""
                    _mom    = ""
                elif (_vname in _mto_dischargers
                      and _mto_receiver_name
                      and ("moor alongside mt " + _mto_receiver_name.lower()) in _next_comment.lower()
                      and "ongoing" not in _comment):
                    # In mto_dischargers but NOT actively pumping — sailing inbound to discharge next
                    _status = "SAILING_AB_LEG2"
                    _mom    = ""   # mto_target_vessel carries the pairing
                    _stor   = ""
                elif _vname in _mto_dischargers and ("ongoing" in _comment or "discharge" in _comment):
                    # Actively discharging to MTO receiver right now — show as hose connection
                    _status = "HOSE_CONNECT_B"
                    _mom    = _mto_receiver_name
                    _stor   = ""
                elif _vname in _mto_dischargers:
                    # In dischargers list but unclear status — show as hose connection
                    _status = "HOSE_CONNECT_B"
                    _mom    = _mto_receiver_name
                    _stor   = ""
                elif "loading is still ongoing" in _comment and _stor:
                    _status = "LOADING"
                elif "loading" in _comment and "ongoing" in _comment and _stor:
                    _status = "LOADING"
                elif "discharge" in _comment and "ongoing" in _comment and _mom:
                    _status = "HOSE_CONNECT_B"
                elif ("en route to bonny" in _comment or "underway to bia" in _comment
                      or "from the fairway buoy to drop anchor" in _comment):
                    _status = "SAILING_AB_LEG2"
                elif ("sail inbound" in _comment or "inbound to sanbarth" in _comment
                      or "inbound to belema" in _comment or "weigh anchor" in _comment):
                    _status = "SAILING_AB"
                elif "outbound to cawthorne" in _comment or "cawthorne channel" in _comment:
                    _status = "SAILING_D_CHANNEL"
                elif ("sail outbound" in _comment or "unmoor from" in _comment
                      or "cast off from mt bryanston" in _comment
                      or "proceed to" in _comment
                      or "sail outbound" in _next_comment or "unmoor from" in _next_comment
                      or "cast off from mt bryanston" in _next_comment
                      or "proceed to" in _next_comment
                      or "weigh anchor" in _next_comment):
                    _status = "SAILING_BA"
                elif "drop anchor" in _comment or "moor alongside" in _comment:
                    _status = "WAITING_BERTH_B"
                elif "receive decanted water" in _comment:
                    _status = "WAITING_BERTH_B"
                    if not _mom:
                        _mom = "Bryanston"
                elif _mom and _cargo > 0:
                    _status = "HOSE_CONNECT_B"
                elif _stor and _cargo > 0:
                    _status = "LOADING"
                elif _stor and _cargo == 0:
                    _status = "IDLE_A"
                else:
                    _status = "SAILING_AB_LEG2" if _cargo > 0 else "IDLE_A"

                # ── Direct status override from col 10 ─────────────────────
                # The corrected CSV builder writes the confirmed status code into
                # col 10.  When the value is a known sim status code, use it
                # directly and skip the comment-heuristic inferred status.
                # This ensures JasmineS/Westmore vessels with CAST_OFF, IDLE_A,
                # HOSE_CONNECT_A etc. round-trip exactly as confirmed.
                _DIRECT_STATUSES = {
                    "LOADING","IDLE_A","HOSE_CONNECT_A","BERTHING_A","CAST_OFF","DOCUMENTING",
                    "WAITING_STOCK","WAITING_DEAD_STOCK","WAITING_BERTH_A","WAITING_CAST_OFF",
                    "HOSE_CONNECT_B","BERTHING_B","WAITING_BERTH_B","WAITING_MOTHER_CAPACITY",
                    "CAST_OFF_B","IDLE_B","SAILING_AB","SAILING_AB_LEG2","SAILING_BA",
                    "SAILING_D_CHANNEL","PF_LOADING","PF_SWAP",
                }
                # BIA discharge states in col 10 for the MTO receiver mean she has
                # FINISHED collecting and is now offloading her MTO cargo to a primary
                # mother. Demote her from accumulating-receiver to transient-offloader
                # so that: (a) col-10 override is applied, (b) she is seeded as
                # HOSE_CONNECT_B/WAITING_BERTH_B at the correct primary mother, and
                # (c) _mto_receiver_name is cleared so the remaining vessels are not
                # misclassified as dischargers delivering to her.
                _BIA_DISCHARGE_STATUSES = {
                    "HOSE_CONNECT_B", "BERTHING_B", "DISCHARGING", "WAITING_BERTH_B",
                    "CAST_OFF_B", "WAITING_MOTHER_CAPACITY",
                }
                _col10_raw = _cell(rows, _r, 10)
                if (_vname == _mto_receiver_name
                        and _col10_raw in _BIA_DISCHARGE_STATUSES):
                    # MTO receiver is now in offload mode — treat as transient discharger
                    _status = _col10_raw
                    # Infer primary mother from col 19 (positive = Bryanston discharge vol)
                    # or col 18 (GreenEagle); use Bryanston as default since it has 0 stock.
                    if _num(rows, _r, 19) > 0:
                        _mom = "Bryanston"
                    elif _num(rows, _r, 18) > 0:
                        _mom = "GreenEagle"
                    else:
                        _mom = "Bryanston"   # safest default: Bryanston is empty
                    # Clear MTO receiver so other vessels aren't misclassified
                    _mto_receiver_name = ""
                    _mto_receiver_row  = None
                if _col10_raw in _DIRECT_STATUSES:
                    # Only override when MTO-specific logic hasn't already taken over
                    # (MTO receiver/discharger assignments are controlled above)
                    if not (_vname == _mto_receiver_name or _vname in _mto_dischargers):
                        _status = _col10_raw
                        # Re-derive storage/mother from storage cols when status overridden
                        # (ensures _stor and _mom are consistent with overridden status)
                        if _status in {"HOSE_CONNECT_B","BERTHING_B","WAITING_BERTH_B",
                                       "WAITING_MOTHER_CAPACITY","CAST_OFF_B","IDLE_B"}:
                            # BIA status — storage col doesn't apply, keep _mom from cols
                            pass
                        # For storage statuses, _stor is already set from _STOR_COLS scan

                _is_recv = (_vname == _mto_receiver_name)
                # mto_target_vessel set for both active dischargers AND inbound queued dischargers
                _is_queued_mto = (
                    bool(_mto_receiver_name)
                    and _status == "SAILING_AB_LEG2"
                    and ("moor alongside mt " + _mto_receiver_name.lower()) in _next_comment.lower()
                )
                _mto_tv  = _mto_receiver_name if (_vname in _mto_dischargers or _is_queued_mto) else ""

                # For actively discharging MTO vessels, compute how much was already pumped
                # prev_prediction (col 5) - current_rob (col 9) = volume already transferred
                _already_xfr = 0
                if _status in {"DISCHARGING", "HOSE_CONNECT_B"} and _mto_tv:
                    _prev_pred = abs(_num(rows, _r, 5))
                    _already_xfr = max(0, _prev_pred - _cargo)

                daughter_vessels.append({
                    "name":              _vname,
                    "cargo_bbl":         _cargo,
                    "status":            _status,
                    "assigned_storage":  _stor,
                    "target_mother":     _mom if not _mto_tv else "",
                    "mto_target_vessel": _mto_tv,
                    "is_mto_receiver":   _is_recv,
                    "already_transferred_bbl": _already_xfr,
                    "nominated_load_bbls": None,
                    "notes":             _cell(rows, _r, 10)[:80],
                })

            return {
                "report_date":        report_date,
                "storage_volumes":    storage_volumes,
                "mother_volumes":     mother_volumes,
                "daughter_vessels":   daughter_vessels,
                "ibom_loading_vessel": _ibom_loader,
                "extraction_notes":   (
                    f"Parsed directly from CSV. Ibom stock: {_ibom_stock:,} bbl. "
                    + (f"MTO: {_mto_receiver_name} receiving from {', '.join(_mto_dischargers.keys())}." if _mto_receiver_name else "")
                ),
                "mto_receiver":  _mto_receiver_name,
                "mto_dischargers": list(_mto_dischargers.keys()),
            }

        # ── File uploader ─────────────────────────────────────────────────────
        _stock_csv = st.file_uploader(
            "Daily Stock Update CSV",
            type=["csv"],
            key="stock_report_csv",
            help=(
                "Export the Daily Stock Update Excel as CSV: "
                "File → Save As → CSV (Comma delimited). "
                "Upload the CSV here — extraction is instant."
            ),
        )

        if _stock_csv is not None:
            _csv_bytes = _stock_csv.read()
            _csv_hash  = hashlib.md5(_csv_bytes).hexdigest()

            if st.session_state.get("_pdf_last_hash") != _csv_hash:
                try:
                    _extracted = _parse_stock_csv(_csv_bytes)
                    st.session_state["_pdf_extracted"]     = _extracted
                    st.session_state["_pdf_last_hash"]     = _csv_hash
                    st.session_state["_pdf_import_status"] = {
                        "type": "ok",
                        "msg":  (
                            f"✅ CSV parsed — report dated {_extracted.get('report_date', '?')}. "
                            "Review and adjust the data below, then press **Confirm & Apply**."
                        ),
                    }
                except Exception as _csv_err:
                    st.session_state["_pdf_import_status"] = {
                        "type": "error",
                        "msg":  f"Could not parse CSV: {_csv_err}. Make sure you uploaded the correct file.",
                    }

        _imp_clear_col, _imp_status_col = st.columns([1, 3])
        with _imp_clear_col:
            if st.button("🗑️ Clear import", key="pdf_clear_btn", use_container_width=True):
                for _k in ["_pdf_extracted", "_pdf_last_hash", "_pdf_import_status",
                           "_pdf_apply_summary",
                           "vp_vessel_states", "vp_mother_vols", "vp_mother_apis", "vp_confirmed"]:
                    st.session_state.pop(_k, None)
                st.rerun()

        # ── Status banner ─────────────────────────────────────────────────────
        _imp_st = st.session_state.get("_pdf_import_status")
        if _imp_st:
            if _imp_st["type"] == "ok":
                st.success(_imp_st["msg"], icon="✅")
            elif _imp_st["type"] == "warn":
                st.warning(_imp_st["msg"], icon="⚠️")
            else:
                st.error(_imp_st["msg"], icon="❌")

        # Confirmed-apply persistent green banner
        if st.session_state.get("vp_confirmed") and st.session_state.get("_pdf_apply_summary"):
            st.success(
                f"✅ Stock report applied — {st.session_state['_pdf_apply_summary']}. "
                "Storage volumes, mother stocks and vessel positions are loaded. "
                "Scroll down to **Enter 08:00 vessel positions** to verify.",
                icon="✅",
            )
            # ── Download corrected/confirmed stock CSV ────────────────────────
            # Build a re-uploadable CSV from the confirmed session state so the
            # operator can export corrected volumes + vessel positions for reuse.
            try:
                _ex_conf = st.session_state.get("_pdf_extracted", {})
                _sv_conf = _ex_conf.get("storage_volumes", {})
                _mv_conf = _ex_conf.get("mother_volumes", {})
                _dv_conf = _ex_conf.get("daughter_vessels", [])
                _rd_conf = _ex_conf.get("report_date", _dt.date.today().isoformat())

                # Override storage & mother volumes from widget state (post-edit)
                for _svn in ["SanBarth","JasmineS","Westmore","Duke","Starturn","PGM"]:
                    _wv = st.session_state.get(f"sv_{_svn}")
                    if _wv is not None:
                        _sv_conf[_svn] = int(_wv)
                for _mvn, _mvk in [("Bryanston","mv_Bryanston"),("GreenEagle","mv_GreenEagle"),("Alkebulan","mv_Alkebulan")]:
                    _wv = st.session_state.get(_mvk)
                    if _wv is not None:
                        _mv_conf[_mvn] = int(_wv)

                try:
                    _conf_date_dt = _dt.datetime.strptime(_rd_conf, "%Y-%m-%d")
                    _conf_date_str = _conf_date_dt.strftime("%m/%d/%Y") + " 07:00:00"
                except Exception:
                    _conf_date_str = _rd_conf + " 07:00:00"

                _cbuf = io.StringIO()
                _cw   = csv.writer(_cbuf)

                # Row 0: title
                _cw.writerow(["Daily Stock Report — Corrected & Confirmed Export"])
                # Row 1: date in col 4
                _cr1 = [""] * 10; _cr1[4] = _conf_date_str
                _cw.writerow(_cr1)
                for _ in range(5): _cw.writerow([""])  # rows 2-6 spacers

                # Rows 7-13: storage vessels (col 2=name, col 7&8=volume)
                for _svn in ["Westmore","JasmineS","SanBarth","Duke","Ibom","PGM","Starturn"]:
                    _vol = _sv_conf.get(_svn, 0)
                    _sr  = [""] * 10
                    _sr[2] = _svn; _sr[7] = _vol; _sr[8] = _vol
                    _cw.writerow(_sr)

                for _ in range(4): _cw.writerow([""])  # rows 14-17

                # Rows 18-28: daughter vessels
                # Source from vp_vessel_states (user-confirmed edits) not raw parse,
                # so MTO receiver/discharger selections and all manual edits are captured.
                _vp_conf  = st.session_state.get("vp_vessel_states", {})
                # Fall back to raw extracted list for vessels not in vp_vessel_states
                _dv_lookup_raw = {d["name"]: d for d in _dv_conf}

                # Column mapping — matches _parse_stock_csv expectations exactly:
                #   col 11=SanBarth  12=JasmineS  13=Westmore  14=Duke  15=Ibom  17=PGM(Asaramatoru)
                #   col 18=GreenEagle  19=Bryanston
                #   col 21=MTO: positive = receiver, negative = discharger
                _stor_col = {"SanBarth":11,"JasmineS":12,"Westmore":13,"Duke":14,"Ibom":15,"PGM":17}
                _mom_col  = {"GreenEagle":18,"Bryanston":19}
                _ALL_DVS  = ["Sherlock","Laphroaig","Watson","Bedford","Balham",
                             "Amyla","Bagshot","Rahama","Rathbone","SantaMonica","Woodstock"]

                for _dvn in _ALL_DVS:
                    # Prefer confirmed state; fall back to raw parse
                    _vp  = _vp_conf.get(_dvn, {})
                    _raw = _dv_lookup_raw.get(_dvn, {})

                    _cargo    = int(_vp.get("cargo_bbl", _raw.get("cargo_bbl", 0)) or 0)
                    _stor     = _vp.get("target_storage") or _raw.get("assigned_storage", "") or ""
                    _mom      = _vp.get("target_mother")  or _raw.get("target_mother",   "") or ""
                    _evt      = _vp.get("status",         _raw.get("status", "")) or ""
                    _is_recv  = bool(_vp.get("is_mto_receiver",  _raw.get("is_mto_receiver",  False)))
                    _mto_tv   = _vp.get("mto_target_vessel", _raw.get("mto_target_vessel", "")) or ""

                    _dr = [""] * 26
                    _dr[2]  = _dvn
                    _dr[9]  = _cargo
                    _dr[10] = _evt

                    # Storage loading columns.
                    # Write sentinel "1" when cargo=0 but storage is assigned — the
                    # parser's _cell() != "0" check detects it and sets _stor correctly,
                    # ensuring zero-cargo vessels (IDLE_A, CAST_OFF etc.) round-trip.
                    if _stor in _stor_col:
                        _dr[_stor_col[_stor]] = _cargo if _cargo > 0 else 1

                    # Mother discharge columns (normal BIA discharge)
                    if _mom in _mom_col and not _is_recv and not _mto_tv:
                        _dr[_mom_col[_mom]] = _cargo

                    # MTO col 21:
                    #   MTO receiver (Watson holding consolidated cargo):  positive cargo value
                    #   MTO discharger (Amyla pumping into receiver):      negative cargo value
                    if _is_recv:
                        _dr[21] = _cargo          # positive → parser sees this as the receiver
                    elif _mto_tv:
                        _dr[21] = -_cargo         # negative → parser sees this as a discharger

                    _cw.writerow(_dr)

                # Rows 29-31: mothers
                for _mvn, _lbl in [("Bryanston","Bryanston"),("GreenEagle","Green Eagle")]:
                    _mr = [""] * 26; _mr[2] = _lbl; _mr[9] = _mv_conf.get(_mvn, 0)
                    _cw.writerow(_mr)

                _corr_csv_bytes = _cbuf.getvalue().encode("utf-8")
                _corr_fname = f"stock_report_corrected_{_rd_conf}.csv"

                st.download_button(
                    "📥 Download Corrected Stock CSV",
                    data=_corr_csv_bytes,
                    file_name=_corr_fname,
                    mime="text/csv",
                    help=(
                        "Downloads a re-uploadable Daily Stock Report CSV containing "
                        "the corrected and confirmed volumes for all storage tanks, "
                        "mother vessels, and daughter vessel ROBs. "
                        "Upload this CSV in the Daily Stock Report section of another "
                        "simulation session to reuse these corrected positions."
                    ),
                    key="download_corrected_csv_btn",
                )
            except Exception as _dce:
                st.caption(f"CSV export unavailable: {_dce}")

        if st.session_state.get("_pdf_extracted"):
            _ex_preview = st.session_state["_pdf_extracted"]
            _ex_totals = (
                f"Storage: {sum(_ex_preview.get('storage_volumes', {}).values()):,} bbl · "
                f"Mothers: {sum(_ex_preview.get('mother_volumes', {}).values()):,} bbl · "
                f"Daughters: {sum(v.get('cargo_bbl', 0) for v in _ex_preview.get('daughter_vessels', [])):,} bbl"
            )
            st.caption(f"📦 Extracted totals — {_ex_totals}")

        # ── Review / edit panel ────────────────────────────────────────────────
        if st.session_state.get("_pdf_extracted"):
            _ex = st.session_state["_pdf_extracted"]

            with st.expander("📝 Review & Adjust Extracted Data", expanded=True):
                # ── Report date ────────────────────────────────────────────
                _ed_c1, _ed_c2 = st.columns([1, 3])
                with _ed_c1:
                    _ex_date_str = _ex.get("report_date", "")
                    try:
                        _ex_date_val = _dt.date.fromisoformat(_ex_date_str)
                    except Exception:
                        _ex_date_val = _dt.date.today()
                    _new_date = st.date_input(
                        "Report date", value=_ex_date_val,
                        format="DD/MM/YYYY", key="pdf_edit_date",
                        help="The date of this stock report."
                    )
                    _ex["report_date"] = _new_date.isoformat()

                with _ed_c2:
                    if _ex.get("extraction_notes"):
                        st.caption(f"ℹ️ {_ex['extraction_notes']}")
                    # MTO pairing callout
                    _mto_recv = _ex.get("mto_receiver", "")
                    _mto_dsch = _ex.get("mto_dischargers", [])
                    if _mto_recv:
                        _mto_dsch_str = ", ".join(_mto_dsch) if _mto_dsch else "—"
                        st.info(
                            f"**MTO detected** — **{_mto_recv}** is acting as MTO receiver. "
                            f"Discharger(s): **{_mto_dsch_str}**. "
                            f"{_mto_recv} status set to `PF_SWAP`; "
                            f"discharger(s) set to `DISCHARGING → {_mto_recv}`.",
                            icon="🔄",
                        )

                st.markdown("**Storage vessel opening stocks (bbl TOV)**")
                # 6 storage vessels — 3 per row
                _sv_specs = [
                    ("SanBarth",   400_000), ("JasmineS", 290_000), ("Westmore", 270_000),
                    ("Duke",      90_000), ("Starturn",  70_000), ("PGM",      28_000),
                ]
                _sv_names = [s for s, _ in _sv_specs]
                _sv_row1 = st.columns(3)
                _sv_row2 = st.columns(3)
                _sv_col_map = {_sv_specs[i][0]: (_sv_row1 if i < 3 else _sv_row2)[i % 3]
                               for i in range(len(_sv_specs))}
                for _svn, _sv_cap in _sv_specs:
                    with _sv_col_map[_svn]:
                        _sv_val = int(_ex.get("storage_volumes", {}).get(_svn, 0))
                        _ex["storage_volumes"][_svn] = st.number_input(
                            _svn, 0, _sv_cap * 2, _sv_val,
                            step=5_000, key=f"pdf_sv_{_svn}",
                        )
                        if _sv_val > _sv_cap:
                            st.caption(f"⚠️ {_sv_val - _sv_cap:,} bbl over capacity → overflow")

                st.markdown("**Mother vessel stocks (bbl TOV)**")
                # Capacities from sim: Bryanston 550k, GreenEagle 750k, Alkebulan 750k
                _mv_cap_map = getattr(mod, "MOTHER_CAPACITY_BY_NAME", {})
                _mv_specs = [
                    ("Bryanston",  "Bryanston",  int(_mv_cap_map.get("Bryanston",  550_000))),
                    ("GreenEagle", "GreenEagle", int(_mv_cap_map.get("GreenEagle", 750_000))),
                    ("Alkebulan",  "Alkebulan",  int(_mv_cap_map.get("Alkebulan",  750_000))),
                ]
                _mv_cols = st.columns(3)
                for _mvi, (_mvk, _mvl, _mv_cap) in enumerate(_mv_specs):
                    with _mv_cols[_mvi]:
                        _mv_val = int(_ex.get("mother_volumes", {}).get(_mvk, 0))
                        _ex["mother_volumes"][_mvk] = st.number_input(
                            _mvl, 0, _mv_cap * 2, _mv_val,
                            step=10_000, key=f"pdf_mv_{_mvk}",
                        )
                        if _mv_val > _mv_cap:
                            st.caption(f"⚠️ {_mv_val - _mv_cap:,} bbl over capacity → excess load")

                st.markdown("**Daughter vessel positions**")
                # Header: Vessel | Cargo | Nom.Load | Location | Status | API
                _dv_hdrs = ["Vessel", "Cargo (bbl)", "Nom. Load", "Location", "Status", "API (°)"]
                _dv_hdr_cols = st.columns([2, 2, 2, 3, 3, 1.5])
                for _dhc, _dhl in zip(_dv_hdr_cols, _dv_hdrs):
                    _dhc.markdown(
                        f'<div style="font-size:10px;font-weight:700;color:#64748b;text-transform:uppercase">{_dhl}</div>',
                        unsafe_allow_html=True
                    )

                # Build location display list from LOCATION_CATALOGUE (same as startup form)
                _ALL_LOC_DISPLAYS = [e["display"] for e in LOCATION_CATALOGUE]

                _LOADING_STATUSES = {"LOADING","PF_LOADING","HOSE_CONNECT_A","BERTHING_A","WAITING_BERTH_A","WAITING_STOCK"}
                _CARGO_STATUSES   = {"LOADING","PF_LOADING","SAILING_AB","SAILING_CROSS_BW_AC",
                                     "SAILING_BW_TO_FWY","SAILING_AB_LEG2","DISCHARGING","HOSE_CONNECT_B",
                                     "BERTHING_B","WAITING_BERTH_B","WAITING_MOTHER_RETURN",
                                     "WAITING_MOTHER_CAPACITY","CAST_OFF_B","IDLE_B","WAITING_CAST_OFF",
                                     "SAILING_D_CHANNEL","SAILING_CH_TO_BW_OUT","SAILING_CROSS_BW_OUT",
                                     "HOSE_CONNECT_A","BERTHING_A","DOCUMENTING","WAITING_DEAD_STOCK",
                                     "PF_SWAP"}

                # Build reverse lookup: (status_code, sim_value) → display string
                # Used to find which catalogue entry matches extracted state
                _sv_status_to_disp = {}
                for _lce in LOCATION_CATALOGUE:
                    for _sc, _sl in _lce["statuses"]:
                        _key = (_sc, _lce["sim_value"])
                        if _key not in _sv_status_to_disp:
                            _sv_status_to_disp[_key] = _lce["display"]

                for _dvi, _dv in enumerate(_ex.get("daughter_vessels", [])):
                    _dc = st.columns([2, 2, 2, 3, 3, 1.5])
                    _vcolor_hex = VESSEL_COLORS.get(_dv["name"], "#94a3b8")
                    _dv_st_cur  = _dv.get("status", "IDLE_A")
                    _dv_cap     = int(getattr(mod, "VESSEL_CAPACITIES", {}).get(
                                    _dv["name"], getattr(mod, "DAUGHTER_CARGO_BBL", 85_000)))

                    # ── Resolve current location display from extracted state ──────────
                    # Try (status, storage), (status, mother), (status, mto_target), sim_value
                    _dv_stor  = _dv.get("assigned_storage", "") or ""
                    _dv_mom   = _dv.get("target_mother", "") or ""
                    _dv_mto   = _dv.get("mto_target_vessel", "") or ""
                    _is_recv  = _dv.get("is_mto_receiver", False)

                    # Pick best sim_value to match against catalogue
                    if _is_recv:
                        _match_sv = _dv["name"]   # MTO receiver uses own name as sim_value
                    elif _dv_mto:
                        _match_sv = _dv_mto       # MTO discharger uses target vessel as sim_value
                    elif _dv_mom:
                        _match_sv = _dv_mom
                    elif _dv_stor:
                        _match_sv = _dv_stor
                    else:
                        _match_sv = ""

                    _resolved_disp = (
                        _sv_status_to_disp.get((_dv_st_cur, _match_sv))
                        or next((e["display"] for e in LOCATION_CATALOGUE
                                 if e["sim_value"] == _match_sv), None)
                        or _ALL_LOC_DISPLAYS[0]
                    )
                    _loc_idx = (_ALL_LOC_DISPLAYS.index(_resolved_disp)
                                if _resolved_disp in _ALL_LOC_DISPLAYS else 0)

                    # Col 0: Vessel name
                    with _dc[0]:
                        st.markdown(
                            f'<div style="padding-top:28px;font-weight:700;font-size:13px;'
                            f'color:{_vcolor_hex}">{_dv["name"]}</div>',
                            unsafe_allow_html=True
                        )

                    # Col 1: Current cargo on board
                    with _dc[1]:
                        _dv_cargo_val = int(_dv.get("cargo_bbl", 0))
                        # max_value must never be less than the current cargo —
                        # MTO transient vessels carry more than 2× nominal capacity.
                        # Also raise the ceiling to the vessel's MTO transient
                        # capacity so an MTO receiver can be set to its full volume.
                        _dv_mto_cap = int(getattr(mod, "MTO_TRANSIENT_CAPACITY_BBL", {}).get(_dv.get("name", ""), 0))
                        _cargo_max = max(_dv_cap * 2, _dv_cargo_val, _dv_mto_cap)
                        _dv["cargo_bbl"] = st.number_input(
                            "cargo",
                            min_value=0, max_value=_cargo_max, value=_dv_cargo_val,
                            step=1_000, key=f"pdf_dv_cargo_{_dvi}",
                            label_visibility="collapsed",
                            help="Current cargo on board at 08:00.",
                        )
                        if _dv_cargo_val > _dv_cap:
                            st.caption(f"⚠️ {_dv_cargo_val - _dv_cap:,} bbl over capacity → excess load")

                    # Col 2: Nominated load ceiling (loading vessels only)
                    with _dc[2]:
                        if _dv_st_cur in _LOADING_STATUSES:
                            # min_value must never exceed value: when cargo already
                            # overflows vessel capacity (e.g. MTO receiver with 78k
                            # on a 43k vessel), clamp min to 0 and default to the
                            # larger of capacity or current cargo to avoid
                            # StreamlitValueBelowMinError.
                            _nom_min = 0
                            _nom_cur = max(
                                int(_dv.get("nominated_load_bbls") or _dv_cap),
                                _dv_cargo_val,
                            )
                            _nom_key = f"pdf_dv_nom_{_dvi}"
                            if _nom_key not in st.session_state:
                                st.session_state[_nom_key] = _nom_cur
                            elif st.session_state[_nom_key] < _nom_min:
                                st.session_state[_nom_key] = _nom_cur
                            # Guard: max must not be below the current value
                            _nom_max = max(_dv_cap * 2,
                                           st.session_state.get(_nom_key, _nom_cur))
                            _dv["nominated_load_bbls"] = st.number_input(
                                "nom",
                                min_value=_nom_min, max_value=_nom_max,
                                step=1_000, key=_nom_key,
                                label_visibility="collapsed",
                                help="**Nominated load** — total volume this vessel should load before departing.",
                            )
                        else:
                            st.markdown(
                                '<div style="padding-top:28px;font-size:11px;color:#94a3b8">—</div>',
                                unsafe_allow_html=True
                            )

                    # Col 3: Location (matches startup form catalogue exactly, inc. MTO entries)
                    with _dc[3]:
                        _sel_disp = st.selectbox(
                            "location", _ALL_LOC_DISPLAYS, index=_loc_idx,
                            key=f"pdf_dv_loc_{_dvi}",
                            label_visibility="collapsed",
                            help="Same location options as the startup form, including MTO entries.",
                        )
                        _sel_entry = LOC_BY_DISPLAY.get(_sel_disp, LOCATION_CATALOGUE[0])
                        _sel_sv    = _sel_entry["sim_value"]
                        _sel_statuses = _sel_entry["statuses"]   # [(code, label), ...]
                        _sel_stat_labels = [lbl for _, lbl in _sel_statuses]
                        _sel_stat_codes  = {lbl: code for code, lbl in _sel_statuses}

                        # Derive storage/mother/MTO from the selected location entry
                        _dv["assigned_storage"]  = _sel_entry.get("target_storage") or (
                            _sel_sv if _sel_sv in {"SanBarth","JasmineS","Westmore","Duke","Starturn","PGM","Ibom"} else ""
                        )
                        _dv["target_mother"]      = _sel_entry.get("target_mother") or (
                            _sel_sv if _sel_sv in {"Bryanston","GreenEagle","Alkebulan"} else ""
                        )
                        _dv["is_mto_receiver"]    = bool(_sel_entry.get("mto_receiver", False))
                        _dv["mto_target_vessel"]  = _sel_entry.get("mto_target_vessel", "")

                    # Col 4: Status (filtered to selected location — same as startup form)
                    with _dc[4]:
                        # Find the current status label within the location's status list
                        _cur_stat_lbl = next(
                            (lbl for code, lbl in _sel_statuses if code == _dv_st_cur),
                            _sel_stat_labels[0] if _sel_stat_labels else _dv_st_cur
                        )
                        _stat_idx = (_sel_stat_labels.index(_cur_stat_lbl)
                                     if _cur_stat_lbl in _sel_stat_labels else 0)
                        _sel_stat_lbl = st.selectbox(
                            "status", _sel_stat_labels, index=_stat_idx,
                            key=f"pdf_dv_status_{_dvi}",
                            label_visibility="collapsed",
                            help="Only statuses valid at the selected location are shown.",
                        )
                        _dv["status"] = _sel_stat_codes.get(_sel_stat_lbl, _dv_st_cur)

                    # Col 5: Cargo API (for loaded/transit vessels)
                    with _dc[5]:
                        _dv_cargo_now = int(_dv.get("cargo_bbl", 0))
                        if _dv_cargo_now > 0 and _dv["status"] in _CARGO_STATUSES:
                            _api_cur = float(_dv.get("cargo_api", 0.0) or 0.0)
                            _dv["cargo_api"] = st.number_input(
                                "API", 10.0, 60.0, max(10.0, _api_cur) if _api_cur > 0 else 29.0,
                                step=0.5, key=f"pdf_dv_api_{_dvi}",
                                label_visibility="collapsed",
                                help="Cargo API gravity (°API).",
                            )
                        else:
                            st.markdown(
                                '<div style="padding-top:28px;font-size:11px;color:#94a3b8">—</div>',
                                unsafe_allow_html=True
                            )

                # Ibom loading vessel
                _ibom_opts = ["", "Bedford", "Balham"]
                _cur_ibom = _ex.get("ibom_loading_vessel", "") or ""
                _ibom_idx = _ibom_opts.index(_cur_ibom) if _cur_ibom in _ibom_opts else 0
                _ex["ibom_loading_vessel"] = st.selectbox(
                    "Vessel currently loading at Ibom offshore buoy",
                    _ibom_opts, index=_ibom_idx, key="pdf_ibom_vessel",
                    help="Indicated in the IBOM row comment on page 1 of the report.",
                )

                # ── MT SanBarth discharge pairing ──────────────────────────

                st.markdown("---")

                # ── Confirm button ─────────────────────────────────────────
                _confirm_col, _clear_col = st.columns([2, 1])
                with _confirm_col:
                    if st.button(
                        "✅ Confirm & Apply to Simulation",
                        key="pdf_confirm_btn",
                        type="primary",
                        use_container_width=True,
                        help="Applies extracted volumes and vessel positions to the simulation startup fields.",
                    ):
                        # ── Populate storage number inputs via session_state ────
                        for _svn in _sv_names:
                            st.session_state[f"sv_{_svn}"] = int(_ex.get("storage_volumes", {}).get(_svn, 0))

                        # ── Populate mother number inputs ──────────────────────
                        for _mvk in ["Bryanston", "GreenEagle", "Alkebulan"]:
                            st.session_state[f"mv_{_mvk}"] = int(_ex.get("mother_volumes", {}).get(_mvk, 0))

                        # ── Build vessel_states for companion-page pathway ─────
                        _sp_map_pdf = getattr(mod, "STORAGE_POINT", {
                            "SanBarth": "A", "JasmineS": "A", "Westmore": "C",
                            "Duke": "D", "Starturn": "E", "PGM": "G", "Ibom": "F",
                        })
                        _vp_states_pdf = {}
                        for _dv in _ex.get("daughter_vessels", []):
                            _vname    = _dv["name"]
                            _vstatus  = _dv.get("status", "IDLE_A")
                            _vcargo   = int(_dv.get("cargo_bbl", 0))
                            _vstor    = _dv.get("assigned_storage", "") or ""
                            _vmom     = _dv.get("target_mother", "") or ""
                            _vmto_tv  = _dv.get("mto_target_vessel", "") or ""
                            _vis_recv = bool(_dv.get("is_mto_receiver", False))
                            _vnom     = _dv.get("nominated_load_bbls")

                            # Derive location (sim_value) from status/storage/MTO
                            if _vis_recv:
                                # MTO receiver — location is the vessel's own name
                                _vloc = _vname
                            elif _dv.get("location") == "MTO_RECEIVER":
                                # Unified MTO_RECEIVER catalogue entry — location = vessel name
                                _vloc = _vname
                            elif _vmto_tv:
                                # MTO discharger — location is the target shuttle vessel
                                _vloc = _vmto_tv
                            elif _vstatus in {"LOADING", "IDLE_A", "HOSE_CONNECT_A", "BERTHING_A",
                                              "WAITING_BERTH_A", "WAITING_STOCK", "WAITING_DEAD_STOCK",
                                              "DOCUMENTING", "WAITING_CAST_OFF", "CAST_OFF"}:
                                _vloc = _vstor if _vstor else "SanBarth"
                            elif _vstatus in {"PF_LOADING", "PF_SWAP"}:
                                _vloc = "Ibom"
                            elif _vstatus in {"DISCHARGING", "HOSE_CONNECT_B", "BERTHING_B",
                                              "WAITING_BERTH_B", "IDLE_B", "CAST_OFF_B",
                                              "WAITING_MOTHER_RETURN", "WAITING_MOTHER_CAPACITY",
                                              "WAITING_RETURN_STOCK"}:
                                _vloc = _vmom if _vmom else "Bryanston"
                            elif _vstatus in {"SAILING_D_CHANNEL", "SAILING_CH_TO_BW_OUT", "SAILING_CROSS_BW_OUT"}:
                                _vloc = "Cawthorne Channel (outbound)"
                            elif _vstatus in {"SAILING_B_TO_BW_IN", "SAILING_CROSS_BW_IN",
                                              "SAILING_BW_TO_CH_IN", "SAILING_CH_TO_D"}:
                                _vloc = "En Route BIA→Storage"
                            elif _vstatus in {"WAITING_FAIRWAY", "SAILING_AB_LEG2", "SAILING_BW_TO_FWY"}:
                                _vloc = "Fairway Buoy"
                            elif _vstatus in {"SAILING_AB", "SAILING_CROSS_BW_AC"}:
                                _vloc = "En Route SanBarth→BIA"
                            elif _vstatus in {"SAILING_BA", "SAILING_B_TO_FWY", "SAILING_FWY_TO_BW",
                                              "SAILING_CROSS_BW_IN_AC", "SAILING_BW_TO_A"}:
                                _vloc = "En Route BIA→Storage"
                            else:
                                _vloc = _vstor or "SanBarth"

                            _vp_states_pdf[_vname] = {
                                "status":             _vstatus,
                                "cargo_bbl":          _vcargo,
                                "location":           _vloc,
                                "target_storage":     _vstor if _vstatus not in {
                                    "DISCHARGING", "HOSE_CONNECT_B", "BERTHING_B",
                                    "WAITING_BERTH_B", "IDLE_B", "CAST_OFF_B",
                                } else None,
                                "target_mother":      _vmom or None,
                                # MTO fields — critical for correct startup form routing
                                "mto_target_vessel":  _vmto_tv,
                                "is_mto_receiver":    _vis_recv,
                                # Nominated load ceiling for Ibom/loading vessels
                                "nominated_load_bbls": int(_vnom) if _vnom is not None else None,
                                "cargo_api":          float(_dv.get("cargo_api", 0.0) or 0.0),
                                "hose_elapsed_hours": 0.0,
                                "already_transferred_bbl": 0,
                                "notes":              _dv.get("notes", ""),
                            }

                        # Override Ibom loading vessel cargo from field stock
                        _ibom_loader = _ex.get("ibom_loading_vessel", "")
                        if _ibom_loader and _ibom_loader in _vp_states_pdf:
                            _vp_states_pdf[_ibom_loader]["status"]         = "PF_LOADING"
                            _vp_states_pdf[_ibom_loader]["location"]       = "Ibom"
                            _vp_states_pdf[_ibom_loader]["target_storage"] = "Ibom"


                        # Push into companion-page session state pathway
                        st.session_state["vp_vessel_states"] = _vp_states_pdf
                        st.session_state["vp_mother_vols"]   = {
                            "bryanston":  int(_ex.get("mother_volumes", {}).get("Bryanston", 0)),
                            "greeneagle": int(_ex.get("mother_volumes", {}).get("GreenEagle", 0)),
                            "alkebulan":  int(_ex.get("mother_volumes", {}).get("Alkebulan", 0)),
                            }
                        st.session_state["vp_mother_apis"]  = {
                            "bryanston": 33.0, "greeneagle": 38.0, "alkebulan": 38.0,
                        }
                        st.session_state["vp_confirmed"]     = True
                        # One-shot flag: seed the editable widget keys from this
                        # extraction exactly once, on the rerun triggered just below.
                        # Without this, the seeding block re-ran on every rerun and
                        # overwrote the operator's manual edits.
                        st.session_state["_vp_apply_pending"] = True

                        # Also set the sim start date to match the report date
                        try:
                            _rep_date = _dt.date.fromisoformat(_ex.get("report_date", ""))
                            st.session_state["sim_start_date"] = _rep_date
                        except Exception:
                            pass

                        # Build a compact summary for the persistent green banner
                        _loading_v = [v["name"] for v in _ex.get("daughter_vessels", []) if v.get("status") == "LOADING"]
                        _transit_v = [v["name"] for v in _ex.get("daughter_vessels", []) if v.get("status", "").startswith("SAILING")]
                        _disch_v   = [v["name"] for v in _ex.get("daughter_vessels", []) if v.get("status") in {"DISCHARGING", "HOSE_CONNECT_B"}]
                        _apply_parts = []
                        if _loading_v:  _apply_parts.append(f"loading: {', '.join(_loading_v)}")
                        if _transit_v:  _apply_parts.append(f"in transit: {', '.join(_transit_v)}")
                        if _disch_v:    _apply_parts.append(f"discharging: {', '.join(_disch_v)}")
                        _apply_summary = (
                            f"{len(_vp_states_pdf)} vessel positions · report date {_ex.get('report_date', '?')}"
                            + (f" · {'; '.join(_apply_parts)}" if _apply_parts else "")
                        )
                        st.session_state["_pdf_apply_summary"] = _apply_summary
                        st.session_state["_pdf_import_status"] = {
                            "type": "ok",
                            "msg": (
                                f"✅ Applied — {len(_vp_states_pdf)} vessel positions loaded "
                                f"from report dated {_ex.get('report_date', '?')}. "
                                "Scroll down to 'Enter 08:00 vessel positions' to verify."
                            )
                        }
                        st.rerun()

                with _clear_col:
                    if st.button("🗑️ Clear import", key="pdf_clear_btn2", use_container_width=True):
                        for _k in ["_pdf_extracted", "_pdf_last_hash", "_pdf_import_status",
                                   "_pdf_apply_summary",
                                   "vp_vessel_states", "vp_mother_vols", "vp_mother_apis", "vp_confirmed"]:
                            st.session_state.pop(_k, None)
                        st.rerun()

        st.markdown("---")

        # ── Tidal constraint only (Google Sheets sync removed) ─────────────
        st.markdown("### 🌊 Tidal Constraint")
        st.caption("Upload the tidal prediction CSV to enforce the breakwater crossing rule.")
        tide_file = st.file_uploader(
            "Tidal CSV  (Date · Time · Tide_Height_m)",
            type=["csv"], key="tide_uploader",
            help=(
                "Required columns: Date (DD/MM/YYYY) | Time (HH:MM) | Tide_Height_m\n\n"
                "Vessels cross the breakwater only if tide is above 1.6 m within daylight (06:00-18:00).\n"
                "• SanBarth→BIA: breakwater is 2 h from SanBarth\n"
                "• BIA→SanBarth: breakwater is 4 h from BIA"
            )
        )
        if tide_file is not None:
            # Cache the IMMUTABLE bytes (via getvalue(), which does NOT advance the
            # read pointer) rather than the UploadedFile object.  Previously the
            # object was stored and later re-read with .read(); after the first
            # read its buffer sat at EOF, so every subsequent rerun (e.g. clicking
            # "Confirm & Apply") re-read empty bytes and silently dropped the tidal
            # constraint.  Caching bytes makes the tidal data survive every rerun.
            st.session_state["_tide_csv_bytes"] = tide_file.getvalue()
            st.success("✅ Tidal data loaded — breakwater constraint active")
            st.caption("🌊 Breakwater crossing: tide >1.6 m · daylight only (06:00-18:00)")
        elif st.session_state.get("_tide_csv_bytes"):
            # A file was uploaded earlier this session and is still cached — keep
            # the constraint active even though the uploader widget shows empty
            # (e.g. after navigating tabs or other reruns).
            st.success("✅ Tidal data loaded — breakwater constraint active")
            st.caption("🌊 Breakwater crossing: tide >1.6 m · daylight only (06:00-18:00)")
        else:
            st.info("ℹ️ No tidal file uploaded — daylight-only rule applies")
    # Google Sheets sync removed — gs_vols and fleet_df are always empty
    gs_vols    = {}
    fleet_df   = pd.DataFrame()
    use_gs     = False

    # Tidal data comes from the Data & Constraints tab uploader, cached as
    # immutable bytes in session_state (see the uploader handler above).
    _tide_bytes_cached = st.session_state.get("_tide_csv_bytes")

    # ── Pre-populate UI widget keys from vp_vessel_states BEFORE widgets render ──
    # Seed the per-vessel / storage / mother widget keys from the confirmed
    # extraction EXACTLY ONCE — on the rerun immediately after the operator
    # clicks "Apply".  Re-seeding on every rerun (the previous behaviour, while
    # vp_confirmed stayed True) overwrote manual edits to cargo, storage levels
    # and mother volumes each time any unrelated control triggered a rerun
    # (e.g. adding a vessel-unavailability window) — which made the form appear
    # to constantly "reset to default".  The one-shot _vp_apply_pending flag
    # (set at Apply time, consumed here) keeps the extraction authoritative on
    # apply while letting subsequent edits persist.
    if (st.session_state.pop("_vp_apply_pending", False)
            and st.session_state.get("vp_confirmed")
            and st.session_state.get("vp_vessel_states")):
        _vp_st  = st.session_state["vp_vessel_states"]
        _vp_mv  = st.session_state.get("vp_mother_vols", {})

        # Build two lookups from LOCATION_CATALOGUE:
        # 1. (status_code, sim_value) → display string  (most specific)
        # 2. sim_value → first matching display string  (fallback)
        _status_loc_to_display = {}
        _simval_to_display     = {}
        for _lce in LOCATION_CATALOGUE:
            _sv   = _lce["sim_value"]
            _disp = _lce["display"]
            if _sv not in _simval_to_display:
                _simval_to_display[_sv] = _disp
            for _sc, _sl in _lce["statuses"]:
                if (_sc, _sv) not in _status_loc_to_display:
                    _status_loc_to_display[(_sc, _sv)] = _disp

        for _vn, _vd in _vp_st.items():
            if not isinstance(_vd, dict):
                continue
            _vstatus  = _vd.get("status", "IDLE_A")
            _vloc     = _vd.get("location", "")
            _vcargo   = int(_vd.get("cargo_bbl", 0))
            _vmom     = _vd.get("target_mother") or ""
            _vstor    = _vd.get("target_storage") or ""
            _vmto_tv  = _vd.get("mto_target_vessel", "") or ""
            _vis_recv = bool(_vd.get("is_mto_receiver", False))

            # For MTO vessels, location is set to the shuttle name (receiver or target)
            # so the lookup finds the MTO catalogue entry, not a mother entry
            _lookup_sv = _vloc  # already set correctly by the build step above

            # Resolve the best display string — most specific first
            _disp = (
                _status_loc_to_display.get((_vstatus, _lookup_sv))
                or _status_loc_to_display.get((_vstatus, _vmom))
                or _status_loc_to_display.get((_vstatus, _vstor))
                or _simval_to_display.get(_lookup_sv)
                or _simval_to_display.get(_vmom)
                or _simval_to_display.get(_vstor)
            )
            if _disp:
                # Only seed session state if not already set — avoids Streamlit 1.57
                # "widget created with default AND set via Session State API" warning
                # which triggers infinite rerun loops (keepalive timeout).
                if f"vl_{_vn}" not in st.session_state:
                    st.session_state[f"vl_{_vn}"] = _disp
                # Status label: only seed on first render too
                _loc_entry = LOC_BY_DISPLAY.get(_disp, {})
                _stat_map  = {code: lbl for code, lbl in _loc_entry.get("statuses", [])}
                _stat_lbl  = _stat_map.get(_vstatus)
                if _stat_lbl and f"vs_{_vn}" not in st.session_state:
                    st.session_state[f"vs_{_vn}"] = _stat_lbl

            st.session_state[f"vc_{_vn}"] = _vcargo   # always overwrite cargo

            # Pre-populate nominated load for Ibom/loading vessels
            _vnom = _vd.get("nominated_load_bbls")
            if _vnom is not None:
                st.session_state[f"vnom_{_vn}"] = int(_vnom)

        # Pre-populate storage volume widget keys — always overwrite
        _vp_sv = st.session_state.get("_pdf_extracted", {}).get("storage_volumes", {})
        for _svn, _svv in _vp_sv.items():
            st.session_state[f"sv_{_svn}"] = int(_svv)

        # Pre-populate mother volume widget keys — always overwrite
        # mv_ keys use the Title-case name shown in the UI
        _mv_key_map = {
            "bryanston":  "Bryanston",
            "greeneagle": "GreenEagle",
            "alkebulan":  "Alkebulan",
        }
        for _mk, _mv in _vp_mv.items():
            _title_key = _mv_key_map.get(_mk, _mk.title())
            st.session_state[f"mv_{_title_key}"] = int(_mv)

    sheets_has_all = (not fleet_df.empty
                      and all(vn in fleet_df["vessel"].values for vn in ALL_VESSELS))

    missing_vessels = [vn for vn in ALL_VESSELS
                       if fleet_df.empty or vn not in fleet_df["vessel"].values]

    # Always show storage + mother volume entry if not fully from Sheets
    manual_storage    = {}   # always defined before expander
    manual_mother     = {}   # initialise before expander so always defined
    manual_mother_api = {}
    manual_states     = {}   # vessel startup states — populated by position entry form
    fleet_df          = pd.DataFrame()  # no GS feed — always empty

    if not gs_vols:
        with st.expander("✏️ Enter 08:00 storage & mother volumes",
                          expanded=True):
            st.markdown("**Storage Volumes at 08:00 (bbl)**")
            st.caption("You may enter volumes above capacity — the excess will be credited to overflow at simulation start.")
            sv = st.columns(6)
            manual_storage = {}
            for j,(sn,cv,init_pct) in enumerate([
                ("SanBarth",   400_000, 0.80),
                ("JasmineS", 290_000, 0.80),
                ("Westmore", 270_000, 0.80),
                ("Duke",      90_000, 0.80),
                ("Starturn",  70_000, 0.80),
                ("PGM",       28_000, 0.80),
            ]):
                with sv[j]:
                    _default_vol = int(cv * init_pct)
                    _sv_key = f"sv_{sn}"
                    # Initialise session state ONCE (first run, no CSV import).
                    # After this, Streamlit owns the value — do NOT pass value= to
                    # number_input, otherwise Streamlit 1.57 warns about the conflict.
                    if _sv_key not in st.session_state:
                        st.session_state[_sv_key] = _default_vol
                    _entered = st.number_input(
                        sn, 0, cv * 2, step=5_000, key=_sv_key,
                        help=f"Capacity: {cv:,} bbl. Default: {_default_vol:,} bbl (80%). Values above capacity are treated as pre-existing overflow.")
                    manual_storage[sn] = _entered
                    if _entered > cv:
                        st.caption(f"⚠️ {_entered - cv:,} bbl over capacity → overflow")

            st.markdown("**Mother Vessel Volumes at 08:00 (bbl)**")
            st.caption("You may enter volumes above capacity — the excess will be noted as an excess load.")
            mv = st.columns(4)
            # Default startup: Bryanston 450k @ 33°, GreenEagle 300k @ 38°, Alkebulan 300k @ 38°
            _mother_vol_defaults = {"Bryanston": 450_000, "GreenEagle": 300_000, "Alkebulan": 300_000}
            for j,mn in enumerate(["Bryanston","GreenEagle","Alkebulan"]):
                with mv[j]:
                    _mother_cap_ui = int(MOTHER_CAP_BY_NAME.get(mn, mod.MOTHER_CAPACITY_BBL))
                    _mv_key = f"mv_{mn}"
                    _mv_default = _mother_vol_defaults.get(mn, 0)
                    # Initialise session state ONCE (first run, no CSV import).
                    # Do NOT pass value= to number_input — avoids Streamlit 1.57 conflict warning.
                    if _mv_key not in st.session_state:
                        st.session_state[_mv_key] = _mv_default
                    _mv_entered = st.number_input(
                        mn, 0, _mother_cap_ui * 2,
                        step=10_000, key=_mv_key,
                        help=f"Capacity: {_mother_cap_ui:,} bbl. Values above capacity are treated as excess load.")
                    manual_mother[mn.lower()] = _mv_entered
                    if _mv_entered > _mother_cap_ui:
                        st.caption(f"⚠️ {_mv_entered - _mother_cap_ui:,} bbl over capacity → excess load")

            st.markdown("**Mother Vessel Stock API Gravity at 08:00 (°API)**")
            st.caption("Set the API gravity of existing stock on each mother vessel. Ignored when stock is zero.")
            _mapi_ui_cols = st.columns(4)
            _mother_api_defaults = {"Bryanston": 33.00, "GreenEagle": 38.00, "Alkebulan": 38.00}
            manual_mother_api = {}
            for _j2, _mn2 in enumerate(["Bryanston", "GreenEagle", "Alkebulan"]):
                with _mapi_ui_cols[_j2]:
                    _stock_vol = manual_mother.get(_mn2.lower(), 0)
                    # Seed the API widget's state ONCE, then let Streamlit own it.
                    # Passing value= alongside key= (the previous pattern) triggers a
                    # Streamlit conflict warning and can reset the field on rerun.
                    _mapi_key = f"mapi_{_mn2}"
                    if _mapi_key not in st.session_state:
                        st.session_state[_mapi_key] = float(_mother_api_defaults[_mn2])
                    _api_val = st.number_input(
                        f"{_mn2} API°",
                        min_value=0.0, max_value=60.0,
                        step=0.1, format="%.2f",
                        key=_mapi_key,
                        help=f"API gravity of stock currently on {_mn2}. Only used when stock > 0 bbl.",
                        disabled=(_stock_vol == 0),
                    )
                    manual_mother_api[_mn2.lower()] = _api_val
                    if _stock_vol > 0:
                        st.caption(f"{_stock_vol:,} bbl · {_api_val:.2f}°API")
    else:
        manual_storage = {}  # gs_vols will be used directly

    if missing_vessels:
        with st.expander(
            f"✏️ Enter 08:00 vessel positions ({len(missing_vessels)} vessels not in Sheets)",
            expanded=True
        ):
            st.caption(
                "Select each vessel's location and the simulation will automatically "
                "show only the statuses valid at that location.")

            # ── Zone legend ───────────────────────────────────────────────────
            _zone_html = " ".join(
                f'<span style="display:inline-flex;align-items:center;gap:4px;' +
                f'background:{_zc[1]}22;border:1px solid {_zc[1]}55;' +
                f'border-radius:5px;padding:2px 8px;font-size:11px;font-weight:600;' +
                f'color:{_zc[1]};margin:2px">{_zc[0]} {_zn}</span>'
                for _zn, _zc in ZONE_BADGE.items()
            )
            st.markdown(
                f'<div style="margin:6px 0 14px;line-height:2">{_zone_html}</div>',
                unsafe_allow_html=True)

            # ── Column headers ────────────────────────────────────────────────
            _hdr_lbl = '<div style="font-size:11px;font-weight:700;color:#64748b;text-transform:uppercase;letter-spacing:.06em;padding-bottom:4px">'
            _hc = st.columns([2, 4, 3, 2])
            with _hc[0]: st.markdown(_hdr_lbl + 'Vessel</div>', unsafe_allow_html=True)
            with _hc[1]: st.markdown(_hdr_lbl + 'Location</div>', unsafe_allow_html=True)
            with _hc[2]: st.markdown(_hdr_lbl + 'Status (filtered by location)</div>', unsafe_allow_html=True)
            with _hc[3]: st.markdown(_hdr_lbl + 'Cargo (bbl)</div>', unsafe_allow_html=True)
            st.markdown('<hr style="margin:0 0 8px;border-color:#e2e8f0">', unsafe_allow_html=True)

            for vn in missing_vessels:
                _base_vcap = mod.VESSEL_CAPACITIES.get(vn, mod.DAUGHTER_CARGO_BBL)
                vcap = _base_vcap
                vcol = VESSEL_COLORS.get(vn, "#aaa")

                # Watson loads from Point A (SanBarth) and Point C (Sego/Westmore)
                _is_watson       = (vn == "Watson")
                _is_ibom_vessel  = (vn in ("Bedford", "Balham"))
                _is_santamonica  = (vn == "SantaMonica")
                # These 7 vessels start the simulation returning empty from BIA
                # toward SanBarth storage.  Default them to the Transit location
                # "Returning → SanBarth" with Leg 1 status and 0 cargo.
                _is_returning_vessel = (vn in (
                    "Sherlock", "Laphroaig", "Rathbone",
                    "Bagshot", "Watson", "Amyla", "SantaMonica",
                ))
                _is_point_b_default = _is_returning_vessel   # kept for legacy references
                if _is_watson:
                    _loc_opts = [e["display"] for e in LOCATION_CATALOGUE
                                 if e["field_zone"] in ("SanBarth", "Sego", "BIA", "Transit")]
                elif _is_ibom_vessel:
                    # Bedford/Balham support Point A (SanBarth/JasmineS) when Ibom
                    # swap is not active; so allow all locations except Sego/Awoba/Dawes
                    _loc_opts = [e["display"] for e in LOCATION_CATALOGUE
                                 if e["field_zone"] in ("SanBarth", "Ibom", "BIA", "Transit")]
                elif _is_santamonica:
                    # SantaMonica loads from Starturn (E) and PGM (G) only
                    # and may only offload to Bryanston / GreenEagle — no MTO discharger entries
                    _loc_opts = [e["display"] for e in LOCATION_CATALOGUE
                                 if e["field_zone"] in ("Dawes", "PGM", "BIA", "Transit")
                                 and not e.get("mto_discharger", False)]
                else:
                    _loc_opts  = LOC_DISPLAY_LIST

                rc = st.columns([2, 4, 3, 2])

                # ── Col 0: Vessel pill ────────────────────────────────────────
                with rc[0]:
                    _zone = LOC_BY_DISPLAY.get(
                        st.session_state.get(f"vl_{vn}", _loc_opts[0]), {}
                    ).get("field_zone", "Transit")
                    _zbadge, _zcol = ZONE_BADGE.get(_zone, ("⚪", "#94a3b8"))
                    st.markdown(
                        f'<div style="padding-top:32px">' +
                        f'<span class="pill" style="background:{vcol};color:#fff;' +
                        f'font-size:12px;padding:4px 12px">{vn}</span>' +
                        f'<br><span style="font-size:10px;color:{_zcol};font-weight:600;' +
                        f'margin-top:4px;display:block">{_zbadge} {_zone}</span>' +
                        f'</div>',
                        unsafe_allow_html=True)

                # ── Col 1: Location dropdown ──────────────────────────────────
                with rc[1]:
                    _default_loc_i = 0
                    if _is_ibom_vessel and vn == "Bedford":
                        # Bedford: default to Ibom offshore buoy
                        try: _default_loc_i = _loc_opts.index("Ibom (Offshore Buoy)")
                        except ValueError: pass
                    elif _is_ibom_vessel and vn == "Balham":
                        # Balham: default to JasmineS loading (SanBarth support on Day 1)
                        try: _default_loc_i = _loc_opts.index("JasmineS (SanBarth)")
                        except ValueError: pass
                    elif _is_returning_vessel:
                        # Default: returning empty from BIA toward Point A (SanBarth/JasmineS)
                        try: _default_loc_i = _loc_opts.index("Returning → Point A (SanBarth/JasmineS)")
                        except ValueError: pass
                    # Seed the session_state default ONCE before the widget is
                    # rendered — never pass index= alongside a pre-seeded key.
                    # Streamlit 1.57 raises a conflict warning when both are present.
                    if f"vl_{vn}" not in st.session_state:
                        st.session_state[f"vl_{vn}"] = _loc_opts[_default_loc_i]
                    _sel_loc = st.selectbox(
                        "Location", _loc_opts,
                        key=f"vl_{vn}",
                        label_visibility="collapsed",
                        help=(
                            "Choose where this vessel is at 08:00. "
                            "The status list will update to show only valid options "
                            "for that location.\n\n"
                            "Watson loads from Point A (SanBarth/JasmineS) and Point C (Sego/Westmore) to Point B."
                            if _is_watson else
                            "Bedford/Balham support Point A (SanBarth/JasmineS) when Ibom "
                            "swap is not active. During an active swap trigger they are "
                            "held at Point A awaiting the Ibom handover."
                            if _is_ibom_vessel else
                            "Choose where this vessel is at 08:00. "
                            "The status list updates to show only valid options "
                            "for that location."
                        )
                    )
                    _loc_entry    = LOC_BY_DISPLAY[_sel_loc]
                    lc            = _loc_entry["sim_value"]
                    _loc_statuses = _loc_entry["statuses"]
                    _cap_storage  = lc if lc in getattr(mod, "STORAGE_POINT", {}) else _loc_entry.get("target_storage")
                    _cargo_cap    = _effective_load_cap(vn, _cap_storage, mod) if _cap_storage else _base_vcap
                    # Zone badge under the dropdown
                    _z  = _loc_entry["field_zone"]
                    _zb, _zc2 = ZONE_BADGE.get(_z, ("⚪","#94a3b8"))
                    st.markdown(
                        f'<div style="font-size:10px;color:{_zc2};font-weight:600;' +
                        f'margin-top:2px">{_zb} {_z} zone · load cap {_cargo_cap:,} bbl</div>',
                        unsafe_allow_html=True)

                # ── Col 2: Status dropdown (location-filtered) ────────────────
                with rc[2]:
                    _stat_labels = [lbl for _, lbl in _loc_statuses]
                    _stat_codes  = {lbl: code for code, lbl in _loc_statuses}
                    # Default Point B vessels to "Waiting — return stock low";
                    # Default status for returning vessels: Leg 1 (BIA → Fairway Buoy)
                    _stat_default_i = 0
                    if _is_returning_vessel:
                        _leg1_lbl = "🔄 Leg 1: BIA → Fairway Buoy (2h)"
                        if _leg1_lbl in _stat_labels:
                            _stat_default_i = _stat_labels.index(_leg1_lbl)
                    elif _is_ibom_vessel and vn == "Balham":
                        # Balham defaults to Loading — in progress at JasmineS
                        _load_lbl = "⛽ Loading — in progress"
                        if _load_lbl in _stat_labels:
                            _stat_default_i = _stat_labels.index(_load_lbl)
                    # Seed the status widget's state ONCE (and re-seed only if the
                    # stored status is no longer valid for the current location,
                    # e.g. after the location dropdown changed the option list).
                    # This avoids passing index= alongside key= — that combination
                    # conflicts with the Session State value and can reset the field
                    # on rerun.
                    _vs_key = f"vs_{vn}"
                    if (_vs_key not in st.session_state
                            or st.session_state[_vs_key] not in _stat_labels):
                        st.session_state[_vs_key] = _stat_labels[_stat_default_i]
                    _sel_stat    = st.selectbox(
                        "Status", _stat_labels,
                        key=_vs_key,
                        label_visibility="collapsed",
                        help="Only statuses valid at the selected location are shown."
                    )
                    st_v = _stat_codes.get(_sel_stat, _loc_statuses[0][0])

                # ── Col 3: Cargo ──────────────────────────────────────────────
                with rc[3]:
                    _cargo_default = (
                        0 if _is_returning_vessel   # returning empty from BIA
                        else 20_000 if (vn == "Bedford" and "Loading" in _sel_stat)        # Bedford: 20k on board at Ibom on startup day
                        else 0 if (vn == "Balham" and _is_ibom_vessel and "Loading" in _sel_stat)  # Balham loading at JasmineS — nothing on board yet
                        else _cargo_cap if ("Discharging" in _sel_stat or "Loading" in _sel_stat)
                        else 0
                    )
                    _vc_key = f"vc_{vn}"
                    if _vc_key not in st.session_state:
                        st.session_state[_vc_key] = _cargo_default
                    # Cargo ceiling: normally 2× the effective load cap (overflow
                    # allowance), but raised to the vessel's MTO transient capacity
                    # when that is higher — so a shuttle acting as an MTO receiver can
                    # be seeded up to the full volume it may legitimately hold at BIA
                    # (e.g. Sherlock 230,000 bbl).  Non-MTO vessels are unaffected.
                    _mto_cap = int(getattr(mod, "MTO_TRANSIENT_CAPACITY_BBL", {}).get(vn, 0))
                    _cargo_input_max = max(_cargo_cap * 2, _mto_cap)
                    cg = st.number_input(
                        "Cargo", 0, _cargo_input_max,
                        step=1_000, key=_vc_key,
                        label_visibility="collapsed",
                        help=(
                            f"Current cargo on board at 08:00. "
                            f"Effective load cap: {_cargo_cap:,} bbl. Base vessel capacity: {_base_vcap:,} bbl. "
                            + (f"MTO transient capacity: {_mto_cap:,} bbl. " if _mto_cap > 0 else "")
                            + "Values above the effective cap are treated as pre-existing overflow."
                        )
                    )
                    if cg > _cargo_cap:
                        st.caption(f"⚠️ {cg - _cargo_cap:,} bbl over effective cap → overflow")

                # ── Col 3b: Nominated load (LOADING/PF_LOADING only) ──────────
                _is_loading_status = st_v in {
                    "LOADING", "PF_LOADING", "HOSE_CONNECT_A", "BERTHING_A",
                    "WAITING_BERTH_A", "WAITING_STOCK",
                }
                _nominated_load_bbl = None
                if _is_loading_status:
                    with rc[3]:
                        # min_value must not exceed value — when cargo already overflows
                        # capacity (cg > _cargo_cap), clamp min to 0 to avoid
                        # StreamlitValueBelowMinError: value(_cargo_cap) < min_value(cg).
                        _nom_min     = min(cg, _cargo_cap)
                        _nom_default = max(_cargo_cap, cg)  # nominated load >= current cargo
                        _vnom_key    = f"vnom_{vn}"
                        # Initialise session state only on first render — do NOT pass
                        # value= when key is already in session_state (avoids Streamlit
                        # 1.57 conflict warning that triggers rerun loops).
                        if _vnom_key not in st.session_state:
                            st.session_state[_vnom_key] = _nom_default
                        else:
                            # Clamp stored value to valid range in case cargo changed
                            _stored = st.session_state[_vnom_key]
                            if _stored < _nom_min:
                                st.session_state[_vnom_key] = _nom_default
                        _nominated_load_bbl = st.number_input(
                            "Nom. load (bbl)",
                            min_value=_nom_min,
                            max_value=_cargo_cap * 2,
                            step=1_000,
                            key=_vnom_key,
                            help=(
                                "**Nominated load volume** — the total cargo this vessel should "
                                "load on startup day before departing. The sim will stop loading "
                                "when cargo reaches this ceiling.\n\n"
                                f"• **Current cargo** (above): {cg:,} bbl already on board.\n"
                                f"• **Remaining to load**: {max(0, _nom_default - cg):,} bbl.\n"
                                "Set lower than vessel capacity to reflect a partial nomination."
                            ),
                        )
                        if _nominated_load_bbl < cg:
                            st.caption("⚠️ Nominated load < current cargo — will be set to current cargo")
                            _nominated_load_bbl = cg

                # ── BIA-only extended fields (nominated mother, hose elapsed,
                #    export operation, cargo API) ─────────────────────────────
                _is_bia_loc = (_loc_entry.get("field_zone") == "BIA")
                _bia_sim_val = lc   # e.g. "Bryanston", "GreenEagle", …

                _bia_nominated_mother = None
                _bia_hose_elapsed     = 0.0
                _bia_is_export        = False
                _bia_export_mother    = None
                _bia_export_days      = 3
                _cargo_api_val        = 0.0

                _is_mto_receiver   = _loc_entry.get("mto_receiver", False)
                _is_mto_discharger = _loc_entry.get("mto_discharger", False)
                _mto_target_vessel = _loc_entry.get("mto_target_vessel", "") or ""
                # For the new unified MTO_RECEIVER entry, treat lc as the vessel's own name
                if _is_mto_receiver and lc == "MTO_RECEIVER":
                    lc = vn
                _bia_mto_target    = None   # shuttle vessel selected as MTO target/receiver

                if _is_bia_loc:
                    _all_mothers = list(_mother_opts)
                    # SantaMonica may only discharge to primary mothers
                    if vn == "SantaMonica":
                        _sm_primary = {
                            getattr(mod, "MOTHER_PRIMARY_NAME",   "Bryanston"),
                            getattr(mod, "MOTHER_SECONDARY_NAME", "GreenEagle"),
                        }
                        _all_mothers = [m for m in _all_mothers if m in _sm_primary]

                    # Check if this is a status where the vessel has cargo
                    _has_cargo_status = st_v in {
                        "HOSE_CONNECT_B", "DISCHARGING", "BERTHING_B",
                        "WAITING_BERTH_B", "WAITING_MOTHER_CAPACITY",
                        "WAITING_MOTHER_RETURN", "CAST_OFF_B",
                        "WAITING_CAST_OFF", "PF_SWAP",
                    }
                    _show_nom = st_v in {
                        "HOSE_CONNECT_B", "DISCHARGING", "BERTHING_B",
                        "WAITING_BERTH_B", "WAITING_MOTHER_CAPACITY",
                        "WAITING_MOTHER_RETURN",
                    }

                    if _is_mto_receiver:
                        # MTO RECEIVER — show simple info only.
                        # The DISCHARGER sets their own location to
                        # "MTO — Lock & Offload → {vn}" — no selectbox here.
                        st.info(
                            f"📦 **Lock & Load** — {vn} is set as MTO receiver.\n\n"
                            f"Set the discharger vessel's **Location** to "
                            f"'MTO — Lock & Offload → {vn}' in their row.",
                            icon="🔗",
                        )

                    elif _is_mto_discharger:
                        # DISCHARGER — target receiver is in the catalogue entry.
                        _bia_mto_target = _mto_target_vessel
                        _dis_rate = (getattr(mod, "VESSEL_DISCHARGE_RATE_BPH", {}) or {}).get(vn)
                        _est_hrs  = (f"{cg / _dis_rate:.1f} h" if _dis_rate and cg else "~12 h")
                        st.info(
                            f"🔒 **Lock & Offload → {_mto_target_vessel}**\n\n"
                            f"Full cargo pumped into **{_mto_target_vessel}** at BIA.\n"
                            f"Rate: {f'{_dis_rate:,} bph' if _dis_rate else 'default'}"
                            f" · Est. pump time: {_est_hrs}",
                            icon="⬇️",
                        )

                    else:
                        # Normal mother discharge — show mother selector
                        _show_nom_field = _show_nom or (_bia_sim_val in _all_mothers)
                        if _show_nom_field:
                            _nom_default_idx = 0
                            if _bia_sim_val in _all_mothers:
                                _nom_default_idx = _all_mothers.index(_bia_sim_val)
                            _bia_nominated_mother = st.selectbox(
                                "Nominated mother",
                                options=_all_mothers,
                                index=_nom_default_idx,
                                key=f"vbia_mother_{vn}",
                                label_visibility="visible",
                                help=(
                                    "Which mother vessel this daughter discharges to on Day 1. "
                                    "Used for manual nominations and the validation seed."
                                ),
                            )
                            if _bia_nominated_mother:
                                startup_day_manual_nominations[vn] = _bia_nominated_mother

                        # Hose elapsed time — only for HOSE_CONNECT_B
                        if st_v == "HOSE_CONNECT_B":
                            _hose_max = float(getattr(mod, "HOSE_CONNECTION_HOURS", 2.0))
                            _bia_hose_elapsed = st.number_input(
                                "Hose elapsed (h)",
                                min_value=0.0, max_value=_hose_max,
                                value=0.0, step=0.25, key=f"vbia_hose_{vn}",
                                label_visibility="visible",
                                help=(
                                    f"Hours of hose connection already completed (0 – {_hose_max:.1f} h). "
                                    "The sim will only run the remaining connection time."
                                ),
                            )

                    # Cargo API — show when vessel has cargo on board (all BIA types)
                    if cg > 0 and _has_cargo_status:
                        _cargo_api_val = st.number_input(
                            "Cargo API (°)",
                            min_value=10.0, max_value=60.0,
                            value=29.0, step=0.5,
                            key=f"vbia_api_{vn}",
                            label_visibility="visible",
                            help="API gravity of the cargo currently on board. Used for blending calculations.",
                        )

                # ── Assemble manual state for this vessel ─────────────────────
                manual_states[vn] = {
                    "status":                st_v,
                    "cargo_bbl":             cg,
                    "cargo_api":             _cargo_api_val,
                    "location":              lc,
                    "target_storage":        _loc_entry.get("target_storage"),
                    "target_mother":         _loc_entry.get("target_mother")
                                             or (_bia_nominated_mother if _is_bia_loc and not _is_mto_discharger else None),
                    # MTO-specific fields — carried through to vessel_states_json
                    # Discharger: mto_target_vessel = receiver name (from catalogue entry)
                    # Receiver:   mto_target_vessel = "" (the link is on the discharger side)
                    "mto_target_vessel":     _bia_mto_target or "",
                    "is_mto_receiver":       _is_mto_receiver,
                    "notes":                 "",
                    "hose_elapsed_hours":    _bia_hose_elapsed,
                    # Nominated load ceiling for loading vessels (Challenges 1 & 2)
                    "nominated_load_bbls":   _nominated_load_bbl,
                }
                st.markdown('<div style="height:4px"></div>', unsafe_allow_html=True)

    # ── Standalone Export Operation section ──────────────────────────────────
    # Mother export seeds: which primary mothers are currently away at export
    # and how many days until they return. Kept as a single panel below the
    # vessel list because it is about mother vessel availability, not daughter
    # vessel state — and must not repeat for every daughter row.
    st.markdown("---")
    st.markdown("**⛴ Mother Vessel Export Operations**")
    st.caption(
        "If one or more primary mothers are currently away at export, specify them here. "
        "The simulation will treat each named mother as unavailable until she returns. "
        "Returning mothers arrive with zero cargo."
    )

    _exp_primary_mothers = list(getattr(mod, "MOTHER_NAMES",
                                        ["Bryanston", "GreenEagle", "Alkebulan"]))

    # Session-state list for export operations
    if "export_operations" not in st.session_state:
        st.session_state["export_operations"] = {}

    _exp_ops = st.session_state["export_operations"]

    # ── Add row ───────────────────────────────────────────────────────────────
    _expo_c1, _expo_c2, _expo_c3 = st.columns([3, 2, 1])
    with _expo_c1:
        _exp_sel_mother = st.selectbox(
            "Mother vessel", _exp_primary_mothers,
            key="exp_mother_sel", label_visibility="collapsed",
            help="Primary mother currently away at export.",
        )
    with _expo_c2:
        _exp_sel_days = st.number_input(
            "Export days", min_value=1, max_value=30, value=3, step=1,
            key="exp_days_sel", label_visibility="collapsed",
            help="Days until this mother returns (she returns with zero cargo).",
        )
    with _expo_c3:
        if st.button("➕ Add", key="exp_add_btn", use_container_width=True):
            _exp_ops[_exp_sel_mother] = int(_exp_sel_days)
            st.session_state["export_operations"] = _exp_ops
            st.rerun()

    # ── Active export operations ──────────────────────────────────────────────
    if _exp_ops:
        for _em, _ed in list(_exp_ops.items()):
            _ec1, _ec2, _ec3 = st.columns([3, 2, 1])
            _mc = MOTHER_COLORS.get(_em, "#3b82f6")
            with _ec1:
                st.markdown(
                    f'<span style="background:{_mc};color:#fff;border-radius:4px;'
                    f'padding:2px 10px;font-size:11px;font-weight:700">{_em}</span>'
                    f'<span style="font-size:11px;color:#b45309;margin-left:6px">'
                    f'⛴ at export · returns in {_ed} day(s) · empty</span>',
                    unsafe_allow_html=True,
                )
            with _ec3:
                if st.button("✕", key=f"exp_rm_{_em}", use_container_width=True):
                    del _exp_ops[_em]
                    st.session_state["export_operations"] = _exp_ops
                    st.rerun()
        if st.button("🗑️ Clear all export ops", key="exp_clear_all"):
            st.session_state["export_operations"] = {}
            st.rerun()
    else:
        st.caption("No export operations active.")

    # Merge session-state export ops into mother_export_seed for simulation
    mother_export_seed.update(st.session_state.get("export_operations", {}))

    st.markdown("---")

    # ── Render fleet cards ────────────────────────────────────────────────────
    render_fleet_cards(ALL_VESSELS, fleet_df, manual_states, mod)

    st.markdown("<br>**🛢️ Mother Vessels at 08:00**", unsafe_allow_html=True)
    render_mother_cards(gs_vols, manual_mother, mod)

    # ==========================================================================
    # ── Resolve simulation parameters ─────────────────────────────────────────
    # ==========================================================================
    def _p(gs_key, man_dict, man_key, fallback):
        return gs_vols.get(gs_key) or man_dict.get(man_key, fallback)

    params = dict(
        sim_days      = gs_vols.get("sim_days",       sim_days),
        prod_sanbarth   = gs_vols.get("prod_sanbarth",    prod_sanbarth),
        prod_jasmines = gs_vols.get("prod_jasmines",  prod_jasmines),
        prod_westmore = gs_vols.get("prod_westmore",  prod_westmore),
        prod_duke     = gs_vols.get("prod_duke",      prod_duke),
        prod_starturn = gs_vols.get("prod_starturn",  prod_starturn),
        prod_pgm      = gs_vols.get("prod_pgm",       prod_pgm),
        prod_ibom   = gs_vols.get("prod_ibom",    prod_ibom),
        # Storage defaults: 80% of each tank's capacity
        sanbarth    = _p("sanbarth",    manual_storage, "SanBarth",   int(SCAP["SanBarth"]   * 0.80)),
        jasmines  = _p("jasmines",  manual_storage, "JasmineS", int(SCAP["JasmineS"] * 0.80)),
        westmore  = _p("westmore",  manual_storage, "Westmore", int(SCAP["Westmore"] * 0.80)),
        duke      = _p("duke",      manual_storage, "Duke",     int(SCAP["Duke"]     * 0.80)),
        starturn  = _p("starturn",  manual_storage, "Starturn", int(SCAP["Starturn"] * 0.80)),
        pgm       = _p("pgm",       manual_storage, "PGM",      int(SCAP.get("PGM", 28_000) * 0.80)),
        # Mother defaults: Bryanston 450k@33°, GreenEagle 300k@38°, Alkebulan 300k@38°
        bryanston = _p("bryanston", manual_mother, "bryanston", 450_000),
        alkebulan = _p("alkebulan", manual_mother, "alkebulan", 0),
        greeneagle= _p("greeneagle",manual_mother, "greeneagle",300_000),
        bryanston_api  = gs_vols.get("bryanston_api",  manual_mother_api.get("bryanston",  33.0)),
        alkebulan_api  = gs_vols.get("alkebulan_api",  manual_mother_api.get("alkebulan",   0.0)),
        greeneagle_api = gs_vols.get("greeneagle_api", manual_mother_api.get("greeneagle", 38.0)),
    )

    # ── Pull confirmed positions from the vessel_positions page (pages/ companion) ─
    # vessel_positions.py writes to these session_state keys when the operator
    # presses "Confirm & Send to Simulation" on that page.
    _vp_states = st.session_state.get("vp_vessel_states")
    _vp_mvols  = st.session_state.get("vp_mother_vols", {})
    _vp_mapis  = st.session_state.get("vp_mother_apis", {})
    if _vp_states and st.session_state.get("vp_confirmed"):
        for _vn, _vd in _vp_states.items():
            if not isinstance(_vd, dict):
                continue
            # Always apply — CSV import is authoritative over UI widget defaults
            if fleet_df.empty or _vn not in fleet_df["vessel"].values:
                _vstatus = _vd.get("status", "IDLE_A")
                _vstor   = _vd.get("target_storage") or ""
                _vmom    = _vd.get("target_mother") or ""
                _vloc    = _vd.get("location", "")

                # Resolve location to a sim_value the sim understands
                # For storage vessels, location IS the storage name
                # For BIA vessels, location IS the mother name
                _sim_loc = (
                    _vloc if _vloc in {
                        "SanBarth","JasmineS","Westmore","Duke","Starturn","PGM",
                        "Ibom","Bryanston","GreenEagle","Alkebulan",
                        "Fairway Buoy","Fairway",
                        "En Route SanBarth→BIA","En Route BIA→Storage",
                        "Breakwater (outbound)","Cawthorne Channel (outbound)",
                    }
                    else _vstor or _vmom or "SanBarth"
                )

                _mto_tv   = _vd.get("mto_target_vessel", "") or ""
                _is_recv  = bool(_vd.get("is_mto_receiver", False))
                _nom_load = _vd.get("nominated_load_bbls")
                manual_states[_vn] = {
                    "status":                 _vstatus,
                    "cargo_bbl":              int(_vd.get("cargo_bbl", 0)),
                    "cargo_api":              float(_vd.get("cargo_api", 0.0)),
                    "already_transferred_bbl": int(_vd.get("already_transferred_bbl", 0)),
                    "hose_elapsed_hours":     float(_vd.get("hose_elapsed_hours", 0.0)),
                    "location":               _sim_loc,
                    "target_storage":         _vstor or (_sim_loc if _vstatus in {
                        "LOADING","IDLE_A","PF_LOADING",
                        "HOSE_CONNECT_A","BERTHING_A","WAITING_BERTH_A",
                        "WAITING_STOCK","WAITING_DEAD_STOCK","DOCUMENTING",
                        "WAITING_CAST_OFF","CAST_OFF",
                    } else None),
                    "target_mother":          (_vmom or None) if not _mto_tv else None,
                    "mto_target_vessel":      _mto_tv,
                    "is_mto_receiver":        _is_recv,
                    "nominated_load_bbls":    int(_nom_load) if _nom_load is not None else None,
                    "notes":                  _vd.get("notes", ""),
                }
        for _mk, _mv in _vp_mvols.items():
            # Always apply — override UI widget defaults
            manual_mother[_mk] = _mv
        for _mk, _ma in _vp_mapis.items():
            manual_mother_api[_mk] = _ma
        st.sidebar.success("🚢 Startup data loaded from Daily Stock Report", icon="✅")

    # Build vessel_states_json
    vs_dict = {}
    for vn in ALL_VESSELS:
        # Manual UI entries must override any fleet sheet defaults.
        if vn in manual_states:
            ms = manual_states[vn]
            vs_dict[vn] = {
                "status":                ms.get("status", "IDLE_A"),
                "cargo_bbl":             ms.get("cargo_bbl", 0),
                "cargo_api":             ms.get("cargo_api", 0.0),
                "already_transferred_bbl": ms.get("already_transferred_bbl", 0),
                "hose_elapsed_hours":    ms.get("hose_elapsed_hours", 0.0),
                "location":              ms.get("location"),
                "target_storage":        ms.get("target_storage"),
                "target_mother":         ms.get("target_mother"),
                "mto_target_vessel":     ms.get("mto_target_vessel", ""),
                "is_mto_receiver":       ms.get("is_mto_receiver", False),
                "nominated_load_bbls":   ms.get("nominated_load_bbls"),
            }
        elif not fleet_df.empty and vn in fleet_df["vessel"].values:
            row = fleet_df[fleet_df["vessel"]==vn].iloc[0]
            vs_dict[vn] = {"status": str(row.get("status","IDLE_A")),
                           "cargo_bbl": _int(row.get("cargo_bbl",0))}

    vessel_states_json = json.dumps(vs_dict) if vs_dict else None

    # ==========================================================================
    # ── Run simulation ────────────────────────────────────────────────────────
    # ==========================================================================
    _tide_bytes = _tide_bytes_cached if _tide_bytes_cached else None
    # Serialise to ISO string — ensures reliable @st.cache_data hashing
    _start_iso_str = sim_start_date.isoformat() if hasattr(sim_start_date, "isoformat") else _dt.date.today().isoformat()

    # Use selected optimizer scenario params if one was chosen, otherwise use best.
    # NOTE: run_optimizer() runs AFTER this point (in the display section), so best_pr
    # is not yet defined here. We persist it in session_state so it is available on the
    # next Streamlit rerun after the optimizer completes.
    _sel_scen    = st.session_state.get("selected_opt_scenario")
    _cached_best = st.session_state.get("_best_opt_params")
    if run_opt and _sel_scen:
        _opt_params_for_run = json.dumps({
            "dead_stock_factor":        _sel_scen["dead_stock_factor"],
            "ibom_trigger_bbl":         _sel_scen["ibom_trigger_bbl"],
            "export_sail_window_start": _sel_scen["export_sail_window_start"],
            "berthing_start":           _sel_scen["berthing_start"],
            "berthing_end":             _sel_scen["berthing_end"],
            "mto_max_parcels":          _sel_scen.get("mto_max_parcels", 1),
        })
    elif run_opt and _cached_best:
        # Use best params from the previous optimizer run (persisted across reruns)
        _opt_params_for_run = json.dumps(_cached_best)
    else:
        _opt_params_for_run = None

    _startup_nom_json = json.dumps(startup_day_manual_nominations) if startup_day_manual_nominations else None
    _point_b_seed_json = json.dumps(point_b_startup_seed) if point_b_startup_seed else None
    _mother_export_seed_json = json.dumps(mother_export_seed) if mother_export_seed else None
    _production_overrides_json = json.dumps(production_overrides) if production_overrides else None
    _mother_unavailability_json = (
        json.dumps(st.session_state.get("mother_unavailability_windows", []))
        if st.session_state.get("mother_unavailability_windows")
        else None
    )
    _mother_export_force_json = (
        json.dumps(st.session_state.get("forced_export_departures", []))
        if st.session_state.get("forced_export_departures")
        else None
    )
    _export_unavailability_json = (
        json.dumps(st.session_state.get("export_unavailability_windows", []))
        if st.session_state.get("export_unavailability_windows")
        else None
    )

    log_df, tl_df, S = run_sim(
        sim_days            = params["sim_days"],
        sanbarth              = params["sanbarth"],
        jasmines            = params["jasmines"],
        westmore            = params["westmore"],
        duke                = params["duke"],
        starturn            = params["starturn"],
        pgm                 = params["pgm"],
        bryanston           = params["bryanston"],
        alkebulan           = params["alkebulan"],
        greeneagle          = params["greeneagle"],
        bryanston_api       = params["bryanston_api"],
        alkebulan_api       = params["alkebulan_api"],
        greeneagle_api      = params["greeneagle_api"],
        prod_sanbarth         = params["prod_sanbarth"],
        prod_jasmines       = params["prod_jasmines"],
        prod_westmore       = params["prod_westmore"],
        prod_duke           = params["prod_duke"],
        prod_starturn       = params["prod_starturn"],
        prod_pgm            = params["prod_pgm"],
        prod_ibom           = params["prod_ibom"],
        production_overrides_json = _production_overrides_json,
        vessel_states_json  = vessel_states_json,
        tide_csv_bytes      = _tide_bytes,
        sim_start_date      = _start_iso_str,
        _sim_version        = getattr(mod, "SIM_VERSION", ""),
        opt_params_json     = _opt_params_for_run,
        startup_day_disable_point_b_priority = startup_day_disable_point_b_priority,
        startup_day_manual_nominations_json  = _startup_nom_json,
        point_b_startup_seed_json            = _point_b_seed_json,
        mother_export_seed_json              = _mother_export_seed_json,
        mother_export_force_json             = _mother_export_force_json,
        export_unavailability_json           = _export_unavailability_json,
        custom_vessels_json                  = _custom_vessels_json,
        vessel_resumption_json               = _vessel_resumption_json,
        mother_unavailability_json           = _mother_unavailability_json,
        storage_overrides_json               = json.dumps(
            st.session_state.get("jmp_storage_overrides", {})
        ) if st.session_state.get("jmp_storage_overrides") else None,
        zeezee_schedule_json                 = json.dumps(
            st.session_state.get("zeezee_schedule", [])
        ) if st.session_state.get("zeezee_schedule") else None,
        daughter_discharge_overrides_json    = json.dumps(
            st.session_state.get("daughter_discharge_overrides", {})
        ) if st.session_state.get("daughter_discharge_overrides") else None,
        multiple_transient_operation         = multiple_transient_operation,
        mto_max_parcels                      = st.session_state.get("mto_max_parcels", 1),
        enable_variability                   = st.session_state.get("enable_variability", False),
        variability_params_json              = st.session_state.get("_variability_params_json"),
    )
    vnames = S["vessel_names"]


    # ==========================================================================
    # ── SECTION 1a: EXECUTIVE SUMMARY & ANALYTICS (board-level overview) ──────
    # ==========================================================================
    try:
        render_executive_summary(log_df, tl_df, S, params, _start_iso_str)
    except Exception as _exec_err:
        st.caption(f"Executive summary unavailable: {_exec_err}")


    # ==========================================================================
    # ── SECTION 1b: TODAY'S VESSEL SCHEDULE SUMMARY ───────────────────────────
    # ==========================================================================
    sec("📋 Today's Vessel Schedule Summary")


    try:
        _today_date = _dt.date.fromisoformat(_start_iso_str)
    except Exception:
        _today_date = _dt.date.today()

    # ── Status categorisation sets ─────────────────────────────────────────────
    _LOAD_ST    = {"LOADING","BERTHING_A","HOSE_CONNECT_A","WAITING_BERTH_A","IDLE_A",
                   "WAITING_STOCK","WAITING_DEAD_STOCK","WAITING_CAST_OFF","CAST_OFF",
                   "DOCUMENTING","PF_LOADING","PF_SWAP"}
    _RETURN_ST  = {"SAILING_BA","SAILING_BW_TO_A","SAILING_B_TO_FWY","SAILING_FWY_TO_BW","SAILING_CROSS_BW_IN_AC","SAILING_B_TO_BW_IN","SAILING_CROSS_BW_IN","SAILING_BW_TO_CH_IN","SAILING_CH_TO_D"}
    _TRANSIT_ST = {"SAILING_AB","SAILING_CROSS_BW_AC","SAILING_BW_TO_FWY","SAILING_AB_LEG2","WAITING_TIDAL","WAITING_DAYLIGHT","SAILING_D_CHANNEL","SAILING_CH_TO_BW_OUT","SAILING_CROSS_BW_OUT","WAITING_FAIRWAY","SAILING_B_TO_F"}
    _BIA_ST     = {"BERTHING_B","HOSE_CONNECT_B","DISCHARGING","CAST_OFF_B","IDLE_B",
                   "WAITING_BERTH_B","WAITING_MOTHER_RETURN","WAITING_MOTHER_CAPACITY",
                   "WAITING_RETURN_STOCK","WAITING_CAST_OFF"}

    # ── Get 08:00 vessel statuses from timeline ────────────────────────────────
    _d1_tl = tl_df[tl_df["Day"] == 1]
    # t=0 is now 08:00 — the very first timeline slot IS 08:00 (index 0)
    _t08_idx = 0
    _t08     = _d1_tl.iloc[_t08_idx] if not _d1_tl.empty else None

    _d1_log  = log_df[log_df["Day"] == 1]

    def _st08(vn):
        if _t08 is not None and vn in _t08.index:
            return str(_t08[vn])
        return "IDLE_A"

    def _pcargo(detail):
        m = re.search(r"([\d,]+) bbl", detail)
        return int(m.group(1).replace(",", "")) if m else 0

    def _pstorage_from_detail(detail):
        m = re.search(r"\| (\w+):", detail)
        return m.group(1) if m else "?"

    def _status_short(st):
        _map = {
            "LOADING": "Loading", "HOSE_CONNECT_A": "Hose Connect",
            "BERTHING_A": "Berthing", "WAITING_BERTH_A": "Waiting Berth",
            "IDLE_A": "Idle", "DOCUMENTING": "Documentation",
            "WAITING_CAST_OFF": "Awaiting Cast-off", "CAST_OFF": "Cast off",
            "WAITING_STOCK": "Waiting (Low Stock)", "WAITING_DEAD_STOCK": "Waiting (Dead Stock)",
            "PF_LOADING": "Loading (Ibom)", "PF_SWAP": "Vessel Swap",
            "SAILING_AB":           "Point A/C → Breakwater (1.5h)",
            "SAILING_CROSS_BW_AC":  "Crossing Breakwater",
            "SAILING_BW_TO_FWY":    "Breakwater → Fairway (2h)",
            "SAILING_AB_LEG2":      "Fairway → BIA (2h)",
            "SAILING_B_TO_FWY":     "BIA → Fairway (2h)",
            "SAILING_FWY_TO_BW":    "Fairway → Breakwater (2h)",
            "SAILING_CROSS_BW_IN_AC": "Crossing Breakwater",
            "SAILING_BW_TO_A":      "Breakwater → Point A/C (1.5h)",
            "WAITING_TIDAL": "Waiting (Tidal)", "WAITING_DAYLIGHT": "Waiting (Daylight)",
            "WAITING_FAIRWAY": "Holding Fairway", "SAILING_BA": "Returning",
            "BERTHING_B": "Berthing", "HOSE_CONNECT_B": "Hose Connect",
            "DISCHARGING": "Discharging", "CAST_OFF_B": "Cast off",
            "IDLE_B": "Idle at Mother", "WAITING_BERTH_B": "Waiting Berth",
            "WAITING_MOTHER_RETURN": "Waiting (Mother Away)",
            "WAITING_MOTHER_CAPACITY": "Waiting (Mother Full)",
            "WAITING_CAST_OFF": "Waiting — Night Cast-off Hold",
            "SAILING_B_TO_F":        "Sailing BIA → Ibom (swap)",
            "SAILING_D_CHANNEL":    "Awoba → Channel (3h)",
            "SAILING_CH_TO_BW_OUT": "Channel → Breakwater (1h)",
            "SAILING_CROSS_BW_OUT": "Crossing Breakwater",
            "SAILING_B_TO_BW_IN":   "BIA → Breakwater (1.5h)",
            "SAILING_CROSS_BW_IN":  "Crossing Breakwater",
            "SAILING_BW_TO_CH_IN":  "Breakwater → Channel (1h)",
            "SAILING_CH_TO_D":      "Channel → Point D (3h)",
        }
        return _map.get(st, st.replace("_", " ").title())

    # ── Build four section lists ───────────────────────────────────────────────
    _loading, _returning, _transit, _discharging = [], [], [], []

    for _vn in ALL_VESSELS:
        _st = _st08(_vn)

        # Loading events today
        _lev = _d1_log[(_d1_log["Vessel"] == _vn) & (_d1_log["Event"] == "LOADING_START")]
        _bev = _d1_log[(_d1_log["Vessel"] == _vn) & (_d1_log["Event"].isin(["BERTHING_START_A","WAITING_BERTH_A"]))]
        # Mother assignment — first try MOTHER_PRIORITY_ASSIGNMENT (normal voyage),
        # then fall back to HOSE_CONNECTION_START_B / DISCHARGE_START for vessels
        # that start the sim already alongside a mother (Bagshot, Watson).
        _mev = log_df[( log_df["Vessel"] == _vn) & ( log_df["Event"] == "MOTHER_PRIORITY_ASSIGNMENT")]
        if _mev.empty:
            _mev = log_df[(log_df["Vessel"] == _vn) &
                          (log_df["Event"].isin(["HOSE_CONNECTION_START_B", "DISCHARGE_START"]))]
        # Return allocation (first in whole log)
        _rev = log_df[( log_df["Vessel"] == _vn) & ( log_df["Event"] == "RETURN_POINT_ALLOCATED")]
        # Fairway ETA (first in whole log)
        _fev = log_df[( log_df["Vessel"] == _vn) & ( log_df["Event"] == "ARRIVED_FAIRWAY")]
        # Discharge events today
        _dev = _d1_log[(_d1_log["Vessel"] == _vn) & (_d1_log["Event"] == "DISCHARGE_START")]

        def _vapi_at08(vn):
            """Return vessel cargo API at 08:00 from timeline, or 0.0."""
            if _t08 is not None:
                _col = f"{vn}_api"
                if _col in _t08.index:
                    return round(float(_t08[_col]), 2)
            return 0.0

        if _st in _LOAD_ST:
            _storage = "?"
            _cargo   = 0
            _slabel  = _status_short(_st)

            # Primary: LOADING_START event on Day 1
            if not _lev.empty:
                _d = _lev.iloc[0]["Detail"]
                _storage = _pstorage_from_detail(_d)
                _cargo   = _pcargo(_d)
                _slabel  = "Loading"

            # Fallback A: LOADING_START anywhere in the full log before/at 08:00 Day 1
            # (catches vessels whose load started at t=0, i.e. Day 0)
            elif _st in {"LOADING", "DOCUMENTING", "CAST_OFF", "WAITING_CAST_OFF"}:
                _lev_all = log_df[
                    (log_df["Vessel"] == _vn) &
                    (log_df["Event"]  == "LOADING_START")
                ]
                # Most-recent load event at or before 08:00 Day 1
                _lev_before = _lev_all[
                    ((_lev_all["Day"] == 1) & (_lev_all["Hour"] <= "08:00")) |
                    (_lev_all["Day"] < 1)
                ]
                _lev_use = (_lev_before if not _lev_before.empty else _lev_all)
                if not _lev_use.empty:
                    _d = _lev_use.iloc[-1]["Detail"]
                    _storage = _pstorage_from_detail(_d)
                    _cargo   = _pcargo(_d)
                    _slabel  = "Loading"

            # Fallback B: BERTHING_START_A / WAITING_BERTH_A — Day 1 first, then full log
            if _storage == "?" and not _bev.empty:
                _d = _bev.iloc[0]["Detail"]
                _m = re.search(r"(?:at|window at) (\w+)", _d)
                if _m: _storage = _m.group(1)
                _slabel = _status_short(_bev.iloc[0]["Event"])

            if _storage == "?" and _st in {"BERTHING_A", "HOSE_CONNECT_A",
                                            "WAITING_BERTH_A", "WAITING_STOCK",
                                            "WAITING_DEAD_STOCK", "IDLE_A"}:
                _bev_all = log_df[
                    (log_df["Vessel"] == _vn) &
                    (log_df["Event"].isin(["BERTHING_START_A", "WAITING_BERTH_A"]))
                ]
                _bev_before = _bev_all[
                    ((_bev_all["Day"] == 1) & (_bev_all["Hour"] <= "08:00")) |
                    (_bev_all["Day"] < 1)
                ]
                _bev_use = (_bev_before if not _bev_before.empty else _bev_all)
                if not _bev_use.empty:
                    _d = _bev_use.iloc[-1]["Detail"]
                    _m = re.search(r"(?:at|window at) (\w+)", _d)
                    if _m: _storage = _m.group(1)
                    _slabel = _status_short(_bev_use.iloc[-1]["Event"])

            # Fallback C: PF_LOADING
            if _st == "PF_LOADING":
                _storage = "Ibom"
                _slabel  = "Loading (Ibom)"

            _loading.append({"vessel": _vn, "storage": _storage,
                              "status": _slabel, "cargo": _cargo,
                              "api": _vapi_at08(_vn)})

        elif _st in _RETURN_ST:
            _ret_stor = "?"
            _eta_s    = "TBD"

            # ── Find the most-recent RETURN_POINT_ALLOCATED before/at 08:00 Day 1 ──
            # The log is sorted by time; pick the last allocation ≤ 08:00 Day 1
            _rev_all = log_df[(log_df["Vessel"] == _vn) &
                               (log_df["Event"]  == "RETURN_POINT_ALLOCATED")]
            if not _rev_all.empty:
                # Filter to events whose time ≤ 08:00 Day 1 (i.e. Day==1, Hour<=08:00,
                # OR any earlier day) — take the last one
                _rev_before = _rev_all[
                    ((_rev_all["Day"] == 1) & (_rev_all["Hour"] <= "08:00")) |
                    (_rev_all["Day"] < 1)
                ]
                _rev_use = (_rev_before if not _rev_before.empty else _rev_all).iloc[-1]
                _d = _rev_use["Detail"]
                _m2 = re.search(r"eligible storage: (\w+)", _d)
                if _m2:
                    _ret_stor = _m2.group(1)
                else:
                    # Fallback: derive storage from target_point in detail
                    _mp = re.search(r"Allocated to Point ([A-F])", _d)
                    _pt_map = {"A": "SanBarth", "C": "Westmore", "D": "Duke",
                               "E": "Starturn", "F": "Ibom", "G": "PGM"}
                    if _mp:
                        _ret_stor = _pt_map.get(_mp.group(1), "?")

            # ── ETA: find first ARRIVED_LOADING_POINT for this vessel AFTER 08:00 ──
            _eta_all = log_df[(log_df["Vessel"] == _vn) &
                               (log_df["Event"]  == "ARRIVED_LOADING_POINT")]
            _eta_fut = _eta_all[
                ((_eta_all["Day"] == 1) & (_eta_all["Hour"] > "08:00")) |
                (_eta_all["Day"] > 1)
            ]
            if not _eta_fut.empty:
                _eta_row  = _eta_fut.iloc[0]
                _eta_day  = int(_eta_row["Day"])
                _eta_time = _eta_row["Time"][11:16]
                # If arrival is on a future day, show day+time
                _eta_s = _eta_time if _eta_day == 1 else f"D{_eta_day} {_eta_time}"
            else:
                # ── Fallback: estimate from tl_df — find when status leaves _RETURN_ST ──
                _tl_v = tl_df[[c for c in tl_df.columns if c in (_vn,) or c in ("Day","Time","Hour")]]
                if _vn in tl_df.columns:
                    _ret_rows = tl_df[(tl_df["Day"] == 1) & (tl_df[_vn].isin(_RETURN_ST))]
                    if not _ret_rows.empty:
                        _arr_idx = _ret_rows.index[-1] + 1
                        if _arr_idx < len(tl_df):
                            _arr_row = tl_df.loc[_arr_idx]
                            _arr_day = int(_arr_row["Day"])
                            _eta_s   = (_arr_row["Time"].strftime("%H:%M")
                                        if _arr_day == 1
                                        else f"D{_arr_day} {_arr_row['Time'].strftime('%H:%M')}")

            _returning.append({"vessel": _vn, "storage": _ret_stor, "eta": _eta_s})

        elif _st in _TRANSIT_ST:
            _mother  = "TBD"
            _eta_bia = "TBD"
            if not _mev.empty:
                _detail = _mev.iloc[0]["Detail"]
                # MOTHER_PRIORITY_ASSIGNMENT: "...assigned to Bryanston..."
                # HOSE_CONNECTION_START_B:    "Hose connected at GreenEagle..."
                # DISCHARGE_START:            "Discharging N bbl | GreenEagle: ..."
                _m = (re.search(r"assigned to (\w+)", _detail)
                      or re.search(r"Hose connected at (\w+)", _detail)
                      or re.search(r"\|\s*(\w+):", _detail))
                if _m: _mother = _m.group(1)
            if not _fev.empty:
                _eta_bia = _fev.iloc[0]["Time"][11:16]
            _transit.append({"vessel": _vn, "mother": _mother,
                              "eta_bia": _eta_bia, "status": _status_short(_st)})

        elif _st in _BIA_ST:
            _mother = "?"
            if not _mev.empty:
                _detail = _mev.iloc[0]["Detail"]
                # MOTHER_PRIORITY_ASSIGNMENT: "...assigned to Bryanston..."
                # HOSE_CONNECTION_START_B:    "Hose connected at GreenEagle..."
                # DISCHARGE_START:            "Discharging N bbl | GreenEagle: ..."
                _m = (re.search(r"assigned to (\w+)", _detail)
                      or re.search(r"Hose connected at (\w+)", _detail)
                      or re.search(r"\|\s*(\w+):", _detail))
                if _m: _mother = _m.group(1)
            _slabel = "Discharging" if not _dev.empty else _status_short(_st)
            _cargo  = _pcargo(_dev.iloc[0]["Detail"]) if not _dev.empty else 0
            _discharging.append({"vessel": _vn, "mother": _mother,
                                  "status": _slabel, "cargo": _cargo,
                                  "api": _vapi_at08(_vn)})

    # ── Colour helpers ─────────────────────────────────────────────────────────
    def _vc(vn):
        return VESSEL_COLORS.get(vn, "#94a3b8")

    def _mc(mn):
        return MOTHER_COLORS.get(mn, "#94a3b8")

    def _sc(sn):
        return STORAGE_COLORS.get(sn, "#94a3b8")

    def _kk(bbl):
        if not bbl: return ""
        if bbl >= 1000: return f"{bbl//1000}k bbl"
        return f"{bbl} bbl"

    # ── HTML render ────────────────────────────────────────────────────────────
    _ncols = max(len(_loading), len(_returning), len(_transit), len(_discharging), 5)

    def _pill(vn, extra=""):
        c = _vc(vn)
        return (f'<span style="display:inline-block;background:{c};color:#fff;'
                f'font-weight:700;font-size:11px;padding:3px 10px;border-radius:4px;'
                f'letter-spacing:.02em">{vn}</span>'
                + (f' <span style="font-size:10px;color:#374151;font-weight:500">{extra}</span>' if extra else ""))

    def _mpill(mn):
        c = _mc(mn)
        return (f'<span style="display:inline-block;background:{c}22;color:{c};'
                f'border:1.5px solid {c};font-weight:700;font-size:11px;'
                f'padding:3px 10px;border-radius:4px">{mn}</span>')

    def _spill(sn):
        c = _sc(sn)
        return (f'<span style="display:inline-block;background:{c}22;color:{c};'
                f'border:1.5px solid {c};font-weight:600;font-size:11px;'
                f'padding:2px 8px;border-radius:4px">{sn}</span>')

    def _badge(txt, bg="#e2e8f0", fg="#374151"):
        return (f'<span style="display:inline-block;background:{bg};color:{fg};'
                f'font-size:10px;font-weight:600;padding:2px 7px;'
                f'border-radius:3px;white-space:nowrap">{txt}</span>')

    def _tdv(vn):
        """Vessel name cell — coloured left border."""
        c = _vc(vn)
        return (f'<td style="border-left:4px solid {c};padding:6px 10px;'
                f'background:#fff;white-space:nowrap">'
                f'<span style="font-weight:700;font-size:12px;color:#0f172a">{vn}</span></td>')

    def _tde(content=""):
        return f'<td style="padding:6px 10px;background:#fff">{content}</td>'

    def _tde_alt(content=""):
        return f'<td style="padding:6px 10px;background:#f8f9fb">{content}</td>'

    # ── Section header style ───────────────────────────────────────────────────
    _SEC_STYLES = {
        "loading":    ("#1a6b3c", "#d1fae5", "#bbf7d0"),  # green
        "returning":  ("#92400e", "#fef3c7", "#fde68a"),  # amber
        "transit":    ("#1e3a8a", "#dbeafe", "#bfdbfe"),  # blue
        "discharging":("#5b21b6", "#ede9fe", "#ddd6fe"),  # purple
        "mto":        ("#be185d", "#fce7f3", "#fbcfe8"),  # pink — MTO
    }

    def _sec_hdr(label, key, cols):
        dark, light, mid = _SEC_STYLES[key]
        return (f'<th colspan="{cols}" style="background:{dark};color:#fff;'
                f'text-align:center;padding:7px 12px;font-size:12px;'
                f'font-weight:800;letter-spacing:.08em;text-transform:uppercase;'
                f'border:1px solid {dark}">{label}</th>')

    def _col_hdr(label, key):
        dark, light, mid = _SEC_STYLES[key]
        return (f'<th style="background:{mid};color:{dark};text-align:left;'
                f'padding:5px 10px;font-size:10px;font-weight:700;'
                f'letter-spacing:.06em;text-transform:uppercase;'
                f'border:1px solid #e2e8f0;white-space:nowrap">{label}</th>')

    # ── CSS ────────────────────────────────────────────────────────────────────
    _tss_css = """
<style>
.tss-wrap{overflow-x:auto;border-radius:8px;box-shadow:0 2px 12px rgba(0,0,0,.08);margin:4px 0 16px}
.tss-title{text-align:center;font-size:15px;font-weight:800;color:#0f172a;
           padding:10px 16px;background:linear-gradient(135deg,#f8fafc,#e2e8f0);
           border-bottom:2px solid #cbd5e1;letter-spacing:.04em}
.tss-table{border-collapse:collapse;width:100%;font-family:'Segoe UI',system-ui,sans-serif}
.tss-table td{vertical-align:middle;border:1px solid #e2e8f0;min-width:90px}
.tss-table tr:hover td{filter:brightness(.97)}
.tss-empty td{background:#f8f9fb!important}
.tss-divider{width:6px;background:#e2e8f0;padding:0!important;border:none!important}
</style>"""
    st.markdown(_tss_css, unsafe_allow_html=True)

    # ── Build MTO list from startup manual_states ─────────────────────────────
    _mto_plan = []
    for _mto_vn, _mto_sd in manual_states.items():
        _mto_role = None
        _mto_partner = ""
        if _mto_sd.get("is_mto_receiver"):
            _mto_role = "receiver"
        elif _mto_sd.get("mto_target_vessel"):
            _mto_role = "discharger"
            _mto_partner = _mto_sd["mto_target_vessel"]
        if _mto_role:
            _mto_plan.append({
                "vessel":  _mto_vn,
                "role":    _mto_role,
                "partner": _mto_partner,
                "cargo":   int(_mto_sd.get("cargo_bbl", 0)),
                "status":  _mto_sd.get("status", ""),
            })

    # ── Build the table ────────────────────────────────────────────────────────
    _rows = max(len(_loading), len(_transit), len(_discharging), len(_mto_plan))
    _rows = max(_rows, 5)   # minimum 5 rows so empty sections show

    def _pad(lst, n):
        return lst + [None] * (n - len(lst))

    _L   = _pad(_loading,    _rows)
    _T   = _pad(_transit,    _rows)
    _D   = _pad(_discharging, _rows)
    _MTO = _pad(_mto_plan,   _rows)

    _html = ['<div class="tss-wrap">']
    _html.append(f'<div class="tss-title">📋 Today\'s Vessel Schedule Summary &nbsp;|&nbsp; '
                 f'<span style="font-size:12px;font-weight:600;color:#475569">'
                 f'{_today_date.strftime("%A, %-d %B %Y")}</span></div>')
    _html.append('<table class="tss-table">')

    # Row 1: section mega-headers
    _html.append(
        '<tr>'
        + _sec_hdr("🟢 Loading Plan",    "loading",    3)
        + '<td class="tss-divider"></td>'
        + _sec_hdr("🔵 Transit to BIA",  "transit",    3)
        + '<td class="tss-divider"></td>'
        + _sec_hdr("🟣 Discharging Plan","discharging",3)
        + '<td class="tss-divider"></td>'
        + _sec_hdr("🔄 MTO Plan",        "mto",        3)
        + '</tr>'
    )
    # Row 2: column sub-headers
    _html.append(
        '<tr>'
        + _col_hdr("Daughter Vessel", "loading")
        + _col_hdr("Storage",         "loading")
        + _col_hdr("Status",          "loading")
        + '<td class="tss-divider"></td>'
        + _col_hdr("Daughter Vessel", "transit")
        + _col_hdr("Mother Allocation","transit")
        + _col_hdr("ETA to BIA",      "transit")
        + '<td class="tss-divider"></td>'
        + _col_hdr("Daughter Vessel", "discharging")
        + _col_hdr("Mother Vessel",   "discharging")
        + _col_hdr("Status",          "discharging")
        + '<td class="tss-divider"></td>'
        + _col_hdr("Vessel",          "mto")
        + _col_hdr("Role",            "mto")
        + _col_hdr("Partner Vessel",  "mto")
        + '</tr>'
    )

    # Data rows
    for _i in range(_rows):
        _l = _L[_i]
        _t = _T[_i]
        _d = _D[_i]
        _bg = "#fff" if _i % 2 == 0 else "#f8f9fb"

        def _cell(content, bg=_bg):
            return f'<td style="padding:7px 10px;background:{bg};vertical-align:middle;border:1px solid #e2e8f0">{content}</td>'

        def _vcell(vn, bg=_bg):
            c = _vc(vn)
            return (f'<td style="padding:7px 10px;background:{bg};border-left:4px solid {c};'
                    f'border-top:1px solid #e2e8f0;border-bottom:1px solid #e2e8f0;'
                    f'border-right:1px solid #e2e8f0;vertical-align:middle">'
                    f'<span style="font-weight:700;font-size:12px;color:#0f172a">{vn}</span></td>')

        _row = "<tr>"
        _m = _MTO[_i]

        # ── Loading section ────────────────────────────────────────────────────
        if _l:
            _row += _vcell(_l["vessel"])
            _row += _cell(_spill(_l["storage"]) if _l["storage"] != "?" else "—")
            # Status badge
            _st_bg = {"Loading": "#d1fae5", "Hose Connect": "#fef9c3",
                      "Berthing": "#dbeafe", "Waiting Berth": "#fde8d8",
                      "Loading (Ibom)": "#d1fae5"}.get(_l["status"], "#f1f5f9")
            _st_fg = {"Loading": "#14532d", "Hose Connect": "#713f12",
                      "Berthing": "#1e3a8a", "Waiting Berth": "#9a3412",
                      "Loading (Ibom)": "#14532d"}.get(_l["status"], "#374151")
            _stxt = _l["status"]
            if _l["cargo"]: _stxt += f" | {_kk(_l['cargo'])}"
            if _l.get("api"): _stxt += f" | API {_l['api']:.2f}°"
            _row += _cell(_badge(_stxt, _st_bg, _st_fg))
        else:
            _row += _cell("") + _cell("") + _cell("")

        _row += '<td class="tss-divider" style="width:6px;background:#e2e8f0;border:none"></td>'

        # ── Transit section ────────────────────────────────────────────────────
        if _t:
            _row += _vcell(_t["vessel"])
            _mc_col = _mc(_t["mother"])
            _mother_disp = (_mpill(_t["mother"]) if _t["mother"] != "TBD"
                           else _badge("TBD", "#f1f5f9", "#64748b"))
            _row += _cell(_mother_disp)
            _row += _cell(_badge(_t["eta_bia"], "#dbeafe", "#1e3a8a") if _t["eta_bia"] != "TBD" else _badge("TBD", "#f1f5f9", "#64748b"))
        else:
            _row += _cell("") + _cell("") + _cell("")

        _row += '<td class="tss-divider" style="width:6px;background:#e2e8f0;border:none"></td>'

        # ── Discharging section ────────────────────────────────────────────────
        if _d:
            _row += _vcell(_d["vessel"])
            _row += _cell(_mpill(_d["mother"]) if _d["mother"] != "?" else "—")
            _ds_bg = {"Discharging": "#ede9fe", "Hose Connect": "#fef9c3",
                      "Berthing": "#dbeafe", "Waiting Berth": "#fde8d8",
                      "Idle at Mother": "#f0fdf4"}.get(_d["status"], "#f1f5f9")
            _ds_fg = {"Discharging": "#5b21b6", "Hose Connect": "#713f12",
                      "Berthing": "#1e3a8a", "Waiting Berth": "#9a3412",
                      "Idle at Mother": "#14532d"}.get(_d["status"], "#374151")
            _dstxt = _d["status"]
            if _d["cargo"]: _dstxt += f" | {_kk(_d['cargo'])}"
            if _d.get("api"): _dstxt += f" | API {_d['api']:.2f}°"
            _row += _cell(_badge(_dstxt, _ds_bg, _ds_fg))
        else:
            _row += _cell("") + _cell("") + _cell("")

        _row += '<td class="tss-divider" style="width:6px;background:#e2e8f0;border:none"></td>'

        # ── MTO section ────────────────────────────────────────────────────────
        if _m:
            _mto_bg = "#fce7f3" if _m["role"] == "receiver" else "#fff1f2"
            _mto_role_badge = (
                _badge("📦 Receiver", "#fce7f3", "#be185d") if _m["role"] == "receiver"
                else _badge("⬇️ Discharger", "#fff1f2", "#9f1239")
            )
            _row += _vcell(_m["vessel"])
            _row += _cell(_mto_role_badge)
            _mto_partner_disp = (_vcell(_m["partner"]).replace('<td ', '<td ') if _m["partner"]
                                 else _cell("—"))
            # Inline partner vessel with color
            if _m["partner"]:
                _pc = _vc(_m["partner"])
                _row += _cell(
                    f'<span style="font-weight:700;color:{_pc}">{_m["partner"]}</span>'
                )
            else:
                _row += _cell("—")
        else:
            _row += _cell("") + _cell("") + _cell("")

        _row += "</tr>"
        _html.append(_row)

    _html.append("</table></div>")
    _tss_html = "\n".join(_html)
    st.markdown(_tss_html, unsafe_allow_html=True)

    # ── Quick legend row ───────────────────────────────────────────────────────
    _leg = '<div style="display:flex;flex-wrap:wrap;gap:6px;margin:0 0 12px;align-items:center">'
    _leg += '<span style="font-size:11px;font-weight:700;color:#475569">Vessels:</span>'
    for _vn2 in ALL_VESSELS:
        _c2 = _vc(_vn2)
        _leg += (f'<span style="background:{_c2};color:#fff;border-radius:4px;'
                 f'padding:2px 9px;font-size:10px;font-weight:700">{_vn2}</span>')
    _leg += '<span style="font-size:11px;font-weight:700;color:#475569;margin-left:10px">Storage:</span>'
    for _sn2, _sc2 in STORAGE_COLORS.items():
        _leg += (f'<span style="background:{_sc2}22;color:{_sc2};border:1px solid {_sc2};'
                 f'border-radius:4px;padding:2px 9px;font-size:10px;font-weight:700">{_sn2}</span>')
    _leg += '<span style="font-size:11px;font-weight:700;color:#475569;margin-left:10px">Mothers:</span>'
    for _mn2, _mc2 in MOTHER_COLORS.items():
        _leg += (f'<span style="background:{_mc2}22;color:{_mc2};border:1.5px solid {_mc2};'
                 f'border-radius:4px;padding:2px 9px;font-size:10px;font-weight:700">{_mn2}</span>')
    _leg += '</div>'
    st.markdown(_leg, unsafe_allow_html=True)

    # ── Export buttons ─────────────────────────────────────────────────────────
    _ex1, _ex2 = st.columns([1,1])

    # Self-contained HTML for download / print-to-image
    _full_tss = f"""<!DOCTYPE html>
<html><head><meta charset="utf-8">
<title>Today's Vessel Schedule — {_today_date.strftime('%d %b %Y')}</title>
<style>
body{{margin:20px;background:#fff;font-family:'Segoe UI',Arial,sans-serif}}
{_tss_css.replace('<style>','').replace('</style>','')}
.tss-table td,.tss-table th{{min-width:80px}}
</style>
</head><body>
{_tss_html}
<div style="margin-top:10px;font-size:9px;color:#94a3b8">
Generated {_dt.datetime.now().strftime('%Y-%m-%d %H:%M')} | Tanker Operations Simulation v5
</div>
</body></html>"""

    with _ex1:
        st.download_button(
            "📥 Download Schedule (HTML → open in browser to save as image/PDF)",
            data=_full_tss.encode("utf-8"),
            file_name=f"vessel_schedule_{_today_date.isoformat()}.html",
            mime="text/html",
            help="Download as HTML. Open in Chrome/Edge → Ctrl+P → Save as PDF, or use the browser screenshot tool for PNG."
        )
    with _ex2:
        # CSV of the schedule
        _sched_rows = []
        for _x in _loading:
            _sched_rows.append({"Section":"Loading Plan","Vessel":_x["vessel"],"Storage":_x["storage"],"Status":_x["status"],"Cargo_bbl":_x["cargo"],"Mother":"","ETA":""})
        for _x in _transit:
            _sched_rows.append({"Section":"Transit to BIA","Vessel":_x["vessel"],"Storage":"","Status":_x["status"],"Cargo_bbl":"","Mother":_x["mother"],"ETA":_x["eta_bia"]})
        for _x in _discharging:
            _sched_rows.append({"Section":"Discharging","Vessel":_x["vessel"],"Storage":"","Status":_x["status"],"Cargo_bbl":_x["cargo"],"Mother":_x["mother"],"ETA":""})
        for _x in _mto_plan:
            _sched_rows.append({"Section":"MTO Plan","Vessel":_x["vessel"],"Storage":"","Status":_x["role"],"Cargo_bbl":_x["cargo"],"Mother":_x.get("partner",""),"ETA":""})
        st.download_button(
            "📥 Download Schedule (CSV)",
            data=pd.DataFrame(_sched_rows).to_csv(index=False).encode(),
            file_name=f"vessel_schedule_{_today_date.isoformat()}.csv",
            mime="text/csv"
        )






    # ==========================================================================
    # ── SECTION: JMP & TIDAL PREDICTION (5th top-level tab) ─────────────────
    # ==========================================================================
    with _ctrl_jmp:
        _jmp_tab, _tide_tab = st.tabs([
            "🗺️ Journey Management Plan",
            "🌊 Tidal Prediction",
        ])


        with _jmp_tab:
            sec("🗺️ Journey Management Plan")


            # ── Loading-Point Override Panel ──────────────────────────────────────────
            # Lets the operator manually reassign a Point A/E vessel to a different
            # permitted storage on a specific day, then re-run the simulation with
            # that forced assignment to see the true downstream effect.
            _sp_map_jmp    = getattr(mod, "STORAGE_POINT", {})
            _all_storages  = list(getattr(mod, "STORAGE_NAMES", _sp_map_jmp.keys()))

            # Pull every permission set from the sim module so the UI mirrors the
            # sim exactly — no duplication of business rules in the app layer.
            _pt_ao          = set(getattr(mod, "POINT_A_ONLY_VESSELS",      {"Amyla"}))
            _westmore_perm  = set(getattr(mod, "WESTMORE_PERMITTED_VESSELS", set()))
            _duke_perm      = set(getattr(mod, "DUKE_PERMITTED_VESSELS",     set()))
            _starturn_perm  = set(getattr(mod, "STARTURN_PERMITTED_VESSELS", set()))
            _pgm_perm       = set(getattr(mod, "PGM_PERMITTED_VESSELS",      {"SantaMonica"}))
            _sm_perm        = set(getattr(mod, "SANTAMONICA_PERMITTED_STORAGES", ()))
            _watson_perm    = set(getattr(mod, "WATSON_PERMITTED_STORAGES",   ()))
            _storage_pt     = _sp_map_jmp   # {storage_name: point_letter}
            _stor_primary   = getattr(mod, "STORAGE_PRIMARY_NAME",   "SanBarth")
            _stor_secondary = getattr(mod, "STORAGE_SECONDARY_NAME", "JasmineS")
            _stor_tertiary  = getattr(mod, "STORAGE_TERTIARY_NAME",  "Westmore")
            _stor_quaternary= getattr(mod, "STORAGE_QUATERNARY_NAME","Duke")
            _stor_quinary   = getattr(mod, "STORAGE_QUINARY_NAME",   "Starturn")
            _stor_senary    = getattr(mod, "STORAGE_SENARY_NAME",    "PGM")

            def _allowed_ae_storages(vessel):
                """All storages this vessel is permitted to load from.

                Mirrors storage_allowed_for_vessel() in the sim exactly so the UI
                and the simulation always agree on what is and is not permitted.
                Covers all six storages (SanBarth, JasmineS, Westmore, Duke, Starturn, PGM).
                """
                result = []
                for _s in _all_storages:
                    _pt = _storage_pt.get(_s, "A")
                    # SantaMonica and Watson use dedicated allowlists
                    if vessel == "SantaMonica":
                        if _s in _sm_perm:
                            result.append(_s)
                        continue
                    if vessel == "Watson":
                        if _s in _watson_perm:
                            result.append(_s)
                        continue
                    # Point-A-only vessels may never leave Point A
                    if vessel in _pt_ao and _pt != "A":
                        continue
                    # Storage-specific permission gates
                    if _s == _stor_tertiary  and vessel not in _westmore_perm:
                        continue
                    if _s == _stor_quaternary and vessel not in _duke_perm:
                        continue
                    if _s == _stor_quinary   and vessel not in _starturn_perm:
                        continue
                    if _s == _stor_senary    and vessel not in _pgm_perm:
                        continue
                    result.append(_s)
                return sorted(result)

            # Build vessel list: any vessel that has at least one permitted
            # storage (excludes vessels with no valid override targets).
            _override_vessels = sorted(
                v for v in (S.get("vessel_names", []) or vnames)
                if bool(_allowed_ae_storages(v))
            )

            _existing_overrides = st.session_state.get("jmp_storage_overrides", {})

            # ── Helper: compute sim-hour from a calendar date ─────────────────────────
            def _date_to_sim_hour(cal_date):
                """Convert a calendar date to the sim-hour at 08:00 on that day."""
                try:
                    _epoch = _dt.date.fromisoformat(_start_iso_str)
                except Exception:
                    _epoch = _dt.date.today()
                delta_days = (cal_date - _epoch).days
                # t=0 is 08:00 on day 1; 08:00 on day N is t=(N-1)*24 hours
                return delta_days * 24  # 08:00 on that calendar date

            with st.expander(
                "🔀 Loading-Point Override Panel" +
                (f" · {sum(len(v) for v in _existing_overrides.values())} active override(s)"
                 if _existing_overrides else ""),
                expanded=bool(_existing_overrides),
            ):
                st.markdown(
                    '<div style="font-size:12px;color:#64748b;margin-bottom:10px">'
                    'Force any vessel to a specific loading storage on a given day. '
                    'Optionally set a <b>Load date</b> to hold the vessel until that day — '
                    'it will wait idle and load once the storage is free on the target date. '
                    'The simulation re-runs with the override applied, immune to reassessment, '
                    'so all downstream timings update accurately.</div>',
                    unsafe_allow_html=True,
                )

                # ── Add a new override ────────────────────────────────────────────────
                _ov_r1c1, _ov_r1c2, _ov_r1c3 = st.columns([2, 1, 2])
                with _ov_r1c1:
                    _ov_vessel = st.selectbox(
                        "Vessel", options=_override_vessels,
                        key="ov_vessel",
                        help="Vessel to force to a specific storage.",
                    )
                with _ov_r1c2:
                    _ov_day = st.number_input(
                        "Trigger day", min_value=1,
                        max_value=params.get("sim_days", 30),
                        value=1, step=1,
                        key="ov_day",
                        help=(
                            "Day the vessel becomes idle and the override activates. "
                            "If Load date is set, the vessel is held at this trigger day "
                            "then dispatched to the storage on the Load date."
                        ),
                    )
                _allowed = _allowed_ae_storages(_ov_vessel) if _ov_vessel else []
                with _ov_r1c3:
                    _ov_storage = st.selectbox(
                        "Storage",
                        options=_allowed,
                        key="ov_storage",
                        help="Only storages this vessel is permitted to load from are shown.",
                    ) if _allowed else st.selectbox(
                        "Storage", options=[], key="ov_storage",
                    )

                # ── Second row: optional date-shift ───────────────────────────────────
                _ov_r2c1, _ov_r2c2, _ov_r2c3 = st.columns([2, 2, 1])
                with _ov_r2c1:
                    _ov_use_date = st.checkbox(
                        "📅 Load on a specific date (date-shift)",
                        key="ov_use_date",
                        help=(
                            "Enable to hold the vessel idle after the trigger day and "
                            "force loading from the selected storage on the chosen date. "
                            "The vessel waits patiently until the storage becomes idle "
                            "within the daylight berthing window on that date."
                        ),
                    )
                with _ov_r2c2:
                    if _ov_use_date:
                        try:
                            _ov_epoch = _dt.date.fromisoformat(_start_iso_str)
                        except Exception:
                            _ov_epoch = _dt.date.today()
                        _ov_load_date = st.date_input(
                            "Load date",
                            value=_ov_epoch + _dt.timedelta(days=int(_ov_day)),
                            min_value=_ov_epoch + _dt.timedelta(days=int(_ov_day) - 1),
                            max_value=_ov_epoch + _dt.timedelta(
                                days=params.get("sim_days", 30) - 1),
                            key="ov_load_date",
                            help="The vessel will wait until this date then load from the storage above.",
                        )
                    else:
                        _ov_load_date = None
                with _ov_r2c3:
                    st.markdown("<br>", unsafe_allow_html=True)
                    if st.button("➕ Add", key="ov_add_btn", use_container_width=True):
                        if _ov_vessel and _ov_storage:
                            _upd = dict(st.session_state.get("jmp_storage_overrides", {}))
                            if _ov_use_date and _ov_load_date is not None:
                                # Date-shift: store as dict with load_after_hour
                                _lah = _date_to_sim_hour(_ov_load_date)
                                _entry = {"storage": _ov_storage, "load_after_hour": _lah,
                                          "load_date_iso": _ov_load_date.isoformat()}
                            else:
                                # Plain override: store as storage string
                                _entry = _ov_storage
                            _upd.setdefault(_ov_vessel, {})[str(int(_ov_day))] = _entry
                            st.session_state["jmp_storage_overrides"] = _upd
                            st.session_state.pop("_jmp_full_key", None)
                            st.rerun()

                # ── Show active overrides with remove buttons ─────────────────────────
                if _existing_overrides:
                    st.markdown("**Active overrides** (click ✖ to remove):")
                    _ov_rows = []
                    for _ov_v, _ov_days in sorted(_existing_overrides.items()):
                        for _ov_d, _ov_s in sorted(_ov_days.items(), key=lambda x: int(x[0])):
                            _trig_date = (
                                (_dt.date.fromisoformat(_start_iso_str)
                                 + _dt.timedelta(days=int(_ov_d) - 1)).strftime("%d %b")
                                if _start_iso_str else f"Day {_ov_d}"
                            )
                            # Unpack entry — plain string or date-shift dict
                            if isinstance(_ov_s, dict):
                                _stor_label = _ov_s.get("storage", "?")
                                _load_iso   = _ov_s.get("load_date_iso")
                                _load_label = (
                                    _dt.date.fromisoformat(_load_iso).strftime("%d %b")
                                    if _load_iso else "?"
                                )
                                _ov_desc = f" → load at <b>{_stor_label}</b> on {_load_label} 📅"
                            else:
                                _stor_label = _ov_s
                                _ov_desc = f" → <b>{_stor_label}</b>"
                            _ov_rows.append((_ov_v, _ov_d, _ov_s, _stor_label, _trig_date, _ov_desc))
                    for _r_idx, (_r_v, _r_d, _r_s_raw, _r_stor, _r_date, _r_desc) in enumerate(_ov_rows):
                        _rc1, _rc2 = st.columns([6, 1])
                        _vc = VESSEL_COLORS.get(_r_v, "#64748b")
                        with _rc1:
                            st.markdown(
                                f'<span style="background:{_vc};color:#fff;border-radius:4px;'
                                f'padding:2px 8px;font-size:11px;font-weight:700">{_r_v}</span> '
                                f'<span style="font-size:12px;color:#374151">'
                                f' Day {_r_d} ({_r_date}){_r_desc}</span>',
                                unsafe_allow_html=True,
                            )
                        with _rc2:
                            if st.button("✖", key=f"ov_del_{_r_idx}", use_container_width=True):
                                _upd2 = dict(st.session_state.get("jmp_storage_overrides", {}))
                                if _r_v in _upd2 and str(_r_d) in _upd2[_r_v]:
                                    del _upd2[_r_v][str(_r_d)]
                                    if not _upd2[_r_v]:
                                        del _upd2[_r_v]
                                st.session_state["jmp_storage_overrides"] = _upd2
                                st.session_state.pop("_jmp_full_key", None)
                                st.rerun()
                    if st.button("🗑️ Clear all overrides", key="ov_clear_all"):
                        st.session_state["jmp_storage_overrides"] = {}
                        st.session_state.pop("_jmp_full_key", None)
                        st.rerun()
                else:
                    st.caption("No overrides active. Add one above to force a vessel reallocation.")

            # ── Daughter Vessel Discharge Point Override Panel ────────────────────────
            # Voyage-code keyed: {voyage_code: {vessel, mother, discharge_date}}
            # If the vessel arrives at BIA before discharge_date, she waits at BIA.
            # When the date is reached, she displaces any incumbent at that mother.
            _ddo_state = st.session_state.get("daughter_discharge_overrides", {})
            # Count voyage-code keyed rules (top-level entries with dict values containing "mother")
            def _ddo_count_rules(s):
                count = 0
                for k, v in s.items():
                    if isinstance(v, dict) and "mother" in v:
                        count += 1          # voyage-code keyed
                    elif isinstance(v, dict):
                        count += len(v)     # legacy vessel/day map
                return count
            _ddo_count = _ddo_count_rules(_ddo_state)

            # ── Build voyage-code lookup table from the most recent sim log ───────────
            # Maps voyage_code -> {vessel, day_1based, date} for lookup and display.
            _vcode_map: dict = {}
            try:
                if not log_df.empty and "VoyageCode" in log_df.columns:
                    _ls = log_df[log_df["Event"] == "LOADING_START"].copy()
                    _sim_start_iso = sim_start_date.isoformat() if hasattr(sim_start_date, "isoformat") else ""
                    for _, _lrow in _ls.iterrows():
                        _vc_key = str(_lrow.get("VoyageCode", "")).strip()
                        _vc_vn  = str(_lrow.get("Vessel", "")).strip()
                        _vc_day = int(_lrow.get("Day", 1))
                        _vc_time_str = str(_lrow.get("Time", ""))[:10]   # "YYYY-MM-DD"
                        if _vc_key and _vc_vn and _vc_key not in _vcode_map:
                            _vcode_map[_vc_key] = {
                                "vessel": _vc_vn,
                                "day":    _vc_day,
                                "date":   _vc_time_str,
                            }
            except Exception:
                pass

            with st.expander(
                "🔀 Daughter Vessel Discharge Point Override" +
                (f" · {_ddo_count} rule(s) active" if _ddo_count else ""),
                expanded=bool(_ddo_count),
            ):
                st.markdown(
                    '<div style="font-size:12px;color:#64748b;margin-bottom:10px">'
                    'Force a daughter vessel identified by its <b>Voyage Code</b> (e.g. '
                    '<code>SHK-001</code>) to discharge to a specific <b>mother vessel</b> '
                    'on a chosen <b>discharge date</b>. '
                    'If the vessel arrives at BIA early, she will wait there until the target date. '
                    'When the date is reached, she takes priority and any incumbent vessel at '
                    'that mother berth is displaced to find another slot. '
                    'This panel does <b>not</b> affect ZeeZee — use the ZeeZee panel below.</div>',
                    unsafe_allow_html=True,
                )

                _ddo_all_vessels = list(getattr(mod, "VESSEL_NAMES", []))
                _ddo_mother_opts = list(getattr(mod, "MOTHER_NAMES",
                                                ["Bryanston", "GreenEagle", "Alkebulan"]))
                _ddo_sim_days    = params.get("sim_days", 30)
                _ddo_sim_start   = sim_start_date if isinstance(sim_start_date, _dt.date) else _dt.date.today()

                # ── Voyage Code input (primary — resolves vessel automatically) ──────
                if _vcode_map:
                    _info_html = (
                        '<div style="background:#f0f9ff;border:1px solid #38bdf8;border-radius:6px;'
                        'padding:8px 12px;margin-bottom:10px">'
                        '<span style="font-size:11px;font-weight:700;color:#0369a1">🔖 VOYAGE CODE</span>'
                        '<span style="font-size:11px;color:#64748b;margin-left:6px">— enter a code '
                        f'({len(_vcode_map)} available from last run) to resolve the vessel automatically. '
                        'Then pick the discharge date and mother vessel.</span></div>'
                    )
                else:
                    _info_html = (
                        '<div style="background:#fafafa;border:1px solid #e2e8f0;border-radius:6px;'
                        'padding:8px 12px;margin-bottom:10px">'
                        '<span style="font-size:11px;color:#64748b">'
                        '💡 Run the simulation once to populate voyage codes, then enter one here.</span></div>'
                    )
                st.markdown(_info_html, unsafe_allow_html=True)

                _vc_col1, _vc_col2 = st.columns([2, 4])
                with _vc_col1:
                    _vc_input = st.text_input(
                        "Voyage Code",
                        value="",
                        key="ddo_voyage_code_lookup",
                        placeholder="e.g. SHK-001",
                        help="7-character voyage code from the JMP (e.g. SHK-001). "
                             "The vessel is resolved automatically from this code.",
                    ).strip().upper()

                # Resolve vessel from voyage code
                _vc_resolved_info = _vcode_map.get(_vc_input, {}) if _vc_input else {}
                _vc_vessel        = _vc_resolved_info.get("vessel", "")
                _vc_load_date     = _vc_resolved_info.get("date", "")   # YYYY-MM-DD of loading

                with _vc_col2:
                    if _vc_input and _vc_vessel:
                        _vc_bg = VESSEL_COLORS.get(_vc_vessel, "#64748b")
                        # Suggest discharge date = load date + 1 day
                        try:
                            _sug_date = (_dt.date.fromisoformat(_vc_load_date)
                                         + _dt.timedelta(days=1)).isoformat() if _vc_load_date else ""
                        except Exception:
                            _sug_date = ""
                        st.markdown(
                            f'<div style="padding-top:26px">'
                            f'<span style="background:{_vc_bg};color:#fff;border-radius:4px;'
                            f'padding:3px 9px;font-size:12px;font-weight:700">{_vc_vessel}</span>'
                            f'<span style="font-size:12px;color:#374151;margin-left:8px">'
                            f'loaded {_vc_load_date}'
                            + (f' · suggested discharge <b>{_sug_date}</b>' if _sug_date else "")
                            + '</span></div>',
                            unsafe_allow_html=True,
                        )
                    elif _vc_input:
                        st.markdown(
                            '<div style="padding-top:26px;font-size:12px;color:#ef4444">'
                            '⚠️ Code not found — run the simulation first.</div>',
                            unsafe_allow_html=True,
                        )
                    else:
                        st.markdown(
                            '<div style="padding-top:26px;font-size:12px;color:#94a3b8">'
                            'Enter a voyage code above to identify the vessel.</div>',
                            unsafe_allow_html=True,
                        )

                st.markdown("**Set discharge rule:**")
                _ddo_c2, _ddo_c3, _ddo_c4 = st.columns([2, 2, 1])

                with _ddo_c2:
                    # Discharge date — default to load-date + 1, or sim start + 1
                    try:
                        _default_disc_date = (
                            _dt.date.fromisoformat(_vc_load_date) + _dt.timedelta(days=1)
                            if _vc_load_date else _ddo_sim_start + _dt.timedelta(days=1)
                        )
                    except Exception:
                        _default_disc_date = _ddo_sim_start + _dt.timedelta(days=1)
                    _ddo_max_date = _ddo_sim_start + _dt.timedelta(days=max(_ddo_sim_days - 1, 0))
                    _ddo_disc_date = st.date_input(
                        "Discharge date",
                        value=_default_disc_date,
                        min_value=_ddo_sim_start,
                        max_value=_ddo_max_date,
                        key="ddo_date",
                        help="The calendar date this vessel should berth and discharge. "
                             "If she arrives at BIA before this date she will wait. "
                             "On this date she takes priority and displaces any incumbent at the target mother.",
                    )

                with _ddo_c3:
                    _ddo_mother = st.selectbox(
                        "Force discharge to",
                        options=_ddo_mother_opts,
                        key="ddo_mother",
                        help="Mother vessel this daughter must discharge to on the discharge date.",
                    )

                with _ddo_c4:
                    st.markdown("<br>", unsafe_allow_html=True)
                    _add_disabled = not bool(_vc_vessel)   # disabled if voyage code not resolved
                    if st.button("➕ Add", key="ddo_add_btn",
                                 use_container_width=True, disabled=_add_disabled):
                        _upd_ddo = dict(st.session_state.get("daughter_discharge_overrides", {}))
                        _disc_iso = _ddo_disc_date.isoformat() if hasattr(_ddo_disc_date, "isoformat") else str(_ddo_disc_date)
                        # Store voyage-code keyed
                        _upd_ddo[_vc_input] = {
                            "vessel":         _vc_vessel,
                            "mother":         str(_ddo_mother),
                            "discharge_date": _disc_iso,
                        }
                        st.session_state["daughter_discharge_overrides"] = _upd_ddo
                        st.session_state.pop("_jmp_full_key", None)
                        st.rerun()

                if not _vc_vessel and _vc_input:
                    st.caption("⚠️ Enter a valid voyage code before adding a rule.")
                elif not _vc_input:
                    st.caption("Enter a voyage code above to enable the Add button.")

                # ── Active rules table ────────────────────────────────────────────────
                if _ddo_state:
                    st.markdown("**Active discharge point rules** (click ✖ to remove):")
                    for _rule_key, _rule_val in sorted(_ddo_state.items()):
                        # Voyage-code keyed (new format)
                        if isinstance(_rule_val, dict) and "mother" in _rule_val and "vessel" in _rule_val:
                            _r_vc    = str(_rule_key)
                            _r_vn    = _rule_val.get("vessel", "?")
                            _r_mn    = _rule_val.get("mother", "?")
                            _r_date  = _rule_val.get("discharge_date", "")
                            _vc_bg   = VESSEL_COLORS.get(_r_vn, "#064e3b")
                            _mc_bg   = MOTHER_COLORS.get(_r_mn, "#3b82f6")
                            _dc1, _dc2 = st.columns([7, 1])
                            with _dc1:
                                st.markdown(
                                    f'<span style="background:#0f172a;color:#7dd3fc;border-radius:3px;'
                                    f'padding:2px 8px;font-size:11px;font-weight:700;font-family:monospace">'
                                    f'{_r_vc}</span>'
                                    f'<span style="background:{_vc_bg};color:#fff;border-radius:4px;'
                                    f'padding:2px 8px;font-size:11px;font-weight:700;margin-left:6px">'
                                    f'{_r_vn}</span>'
                                    f'<span style="font-size:12px;color:#374151;margin-left:6px">'
                                    f'→ discharge to </span>'
                                    f'<span style="background:{_mc_bg}22;color:{_mc_bg};border:1px solid {_mc_bg}66;'
                                    f'border-radius:4px;padding:2px 8px;font-size:11px;font-weight:700">'
                                    f'{_r_mn}</span>'
                                    + (f'<span style="font-size:11px;color:#64748b;margin-left:6px">'
                                       f'on <b>{_r_date}</b></span>' if _r_date else "")
                                    + '<span style="font-size:10px;color:#0369a1;margin-left:6px;'
                                      'background:#e0f2fe;border-radius:3px;padding:1px 5px">'
                                      '⏸ waits if early · displaces incumbent</span>',
                                    unsafe_allow_html=True,
                                )
                            with _dc2:
                                if st.button("✖", key=f"ddo_del_{_rule_key}",
                                             use_container_width=True):
                                    _rm_ddo = dict(st.session_state.get("daughter_discharge_overrides", {}))
                                    _rm_ddo.pop(_rule_key, None)
                                    st.session_state["daughter_discharge_overrides"] = _rm_ddo
                                    st.session_state.pop("_jmp_full_key", None)
                                    st.rerun()
                        # Legacy vessel/day keyed format (backward compat display)
                        elif isinstance(_rule_val, dict):
                            for _dk, _de in sorted(_rule_val.items(), key=lambda x: int(x[0])):
                                _r_mn = _de.get("mother", str(_de)) if isinstance(_de, dict) else str(_de)
                                _vc_bg = VESSEL_COLORS.get(_rule_key, "#064e3b")
                                _dc1, _dc2 = st.columns([7, 1])
                                with _dc1:
                                    st.markdown(
                                        f'<span style="background:{_vc_bg};color:#fff;border-radius:4px;'
                                        f'padding:2px 8px;font-size:11px;font-weight:700">{_rule_key}</span>'
                                        f'<span style="font-size:12px;color:#374151;margin-left:6px">'
                                        f'Day {int(_dk)+1} → {_r_mn} (legacy)</span>',
                                        unsafe_allow_html=True,
                                    )
                                with _dc2:
                                    if st.button("✖", key=f"ddo_del_{_rule_key}_{_dk}",
                                                 use_container_width=True):
                                        _rm_ddo = dict(st.session_state.get("daughter_discharge_overrides", {}))
                                        if _rule_key in _rm_ddo and isinstance(_rm_ddo[_rule_key], dict):
                                            _rm_ddo[_rule_key].pop(str(_dk), None)
                                            _rm_ddo[_rule_key].pop(int(_dk), None)
                                            if not _rm_ddo[_rule_key]:
                                                del _rm_ddo[_rule_key]
                                        st.session_state["daughter_discharge_overrides"] = _rm_ddo
                                        st.session_state.pop("_jmp_full_key", None)
                                        st.rerun()
                    if st.button("🗑️ Clear all discharge rules", key="ddo_clear_all"):
                        st.session_state["daughter_discharge_overrides"] = {}
                        st.session_state.pop("_jmp_full_key", None)
                        st.rerun()
                else:
                    st.caption("No rules active. Add one above to force a daughter vessel to a specific mother.")

            # ── Discharge Override Panel (ZeeZee — third-party vessel) ───────────────
            _zz_schedule = st.session_state.get("zeezee_schedule", [])

            with st.expander(
                "🚢 Discharge Override Panel — ZeeZee" +
                (f" · {len(_zz_schedule)} recurring visit(s)" if _zz_schedule else ""),
                expanded=bool(_zz_schedule),
            ):
                st.markdown(
                    '<div style="font-size:12px;color:#64748b;margin-bottom:10px">'
                    'Nominate <b>ZeeZee</b>, a third-party vessel, to discharge to any available '
                    'primary mother vessel at Point B on a recurring monthly date. '
                    'ZeeZee is delayed by daughter vessel queues for a maximum of '
                    '<b>2 days</b>, after which she takes priority regardless. '
                    'If no daughter queue exists, she discharges as soon as possible.</div>',
                    unsafe_allow_html=True,
                )

                # ── Add a new recurring visit ─────────────────────────────────────────
                _zz_c1, _zz_c2, _zz_c3, _zz_c4 = st.columns([1, 2, 2, 1])
                with _zz_c1:
                    _zz_dom = st.number_input(
                        "Day of month", min_value=1, max_value=28, value=15, step=1,
                        key="zz_dom",
                        help="Calendar day-of-month ZeeZee arrives each month (1–28)."
                    )
                with _zz_c2:
                    _zz_vol = st.number_input(
                        "Volume (bbl)", min_value=10_000, max_value=1_000_000,
                        value=200_000, step=5_000,
                        key="zz_vol",
                        help="Cargo volume ZeeZee brings on each visit (bbl)."
                    )
                with _zz_c3:
                    _zz_api = st.number_input(
                        "API gravity (°)", min_value=15.0, max_value=55.0,
                        value=32.0, step=0.5,
                        key="zz_api",
                        help="API gravity of ZeeZee's cargo."
                    )
                with _zz_c4:
                    st.markdown("<br>", unsafe_allow_html=True)
                    if st.button("➕ Add", key="zz_add_btn", use_container_width=True):
                        _new_entry = {
                            "day_of_month": int(_zz_dom),
                            "volume_bbl":   float(_zz_vol),
                            "api":          float(_zz_api),
                        }
                        _upd_zz = list(st.session_state.get("zeezee_schedule", []))
                        # Prevent duplicate day_of_month entries
                        _upd_zz = [e for e in _upd_zz if e.get("day_of_month") != int(_zz_dom)]
                        _upd_zz.append(_new_entry)
                        _upd_zz.sort(key=lambda e: e["day_of_month"])
                        st.session_state["zeezee_schedule"] = _upd_zz
                        st.session_state.pop("_jmp_full_key", None)
                        st.rerun()

                # ── Active schedule table with remove buttons ─────────────────────────
                if _zz_schedule:
                    st.markdown("**Active recurring visits** (click ✖ to remove):")
                    for _zi, _ze in enumerate(_zz_schedule):
                        _zc1, _zc2 = st.columns([6, 1])
                        with _zc1:
                            _zdom = _ze.get("day_of_month", "?")
                            _zvol = _ze.get("volume_bbl", 0)
                            _zapi = _ze.get("api", 0)
                            st.markdown(
                                f'<span style="background:#1e3a5f;color:#fff;border-radius:4px;'
                                f'padding:2px 10px;font-size:11px;font-weight:700">ZeeZee</span> '
                                f'<span style="font-size:12px;color:#374151">'
                                f' Day <b>{_zdom}</b> of every month · '
                                f'<b>{int(_zvol):,} bbl</b> @ <b>{_zapi:.1f}°</b> API'
                                f' · 2-day daughter queue tolerance</span>',
                                unsafe_allow_html=True,
                            )
                        with _zc2:
                            if st.button("✖", key=f"zz_del_{_zi}", use_container_width=True):
                                _upd_zz2 = [e for e in st.session_state.get("zeezee_schedule", [])
                                            if e.get("day_of_month") != _zdom]
                                st.session_state["zeezee_schedule"] = _upd_zz2
                                st.session_state.pop("_jmp_full_key", None)
                                st.rerun()
                    if st.button("🗑️ Clear all ZeeZee visits", key="zz_clear_all"):
                        st.session_state["zeezee_schedule"] = []
                        st.session_state.pop("_jmp_full_key", None)
                        st.rerun()
                else:
                    st.caption(
                        "No ZeeZee visits scheduled. Add one above to activate recurring "
                        "third-party discharge at Point B."
                    )

            # ── Force Export Departure ────────────────────────────────────────────────
            sec("🚢 Force Export Departure")
            st.caption(
                "Schedule a primary mother vessel to sail for export discharge on a specific date, "
                "regardless of her current stock level. She will complete the full export cycle "
                "(documentation → sail → discharge → return empty). "
                "Once DOC starts, the berth is locked — no new daughter can berth until she returns."
            )

            if "forced_export_departures" not in st.session_state:
                st.session_state["forced_export_departures"] = []
            _forced_deps = st.session_state["forced_export_departures"]

            _fexp_primary_mothers = [
                m for m in list(getattr(mod, "MOTHER_NAMES",
                                        ["Bryanston", "GreenEagle", "Alkebulan"]))
            ]

            _fexp_c1, _fexp_c2, _fexp_c3 = st.columns([3, 3, 1])
            with _fexp_c1:
                _fexp_mother = st.selectbox(
                    "Mother vessel", _fexp_primary_mothers,
                    key="fexp_mother_sel", label_visibility="collapsed",
                    help="Mother vessel to force-depart on the nominated date.",
                )
            with _fexp_c2:
                _fexp_date = st.date_input(
                    "Departure date", value=sim_start_date,
                    key="fexp_date_sel", label_visibility="collapsed",
                    format="DD/MM/YYYY",
                    help="Calendar date on which this mother will begin export documentation. "
                         "She sails during the first available export sail window that day. "
                         "The berth is locked from DOC start until she returns.",
                )
            with _fexp_c3:
                if st.button("➕ Add", key="fexp_add_btn", use_container_width=True):
                    _fexp_entry = {"mother": _fexp_mother, "date": _fexp_date.isoformat()}
                    if _fexp_entry not in _forced_deps:
                        _forced_deps.append(_fexp_entry)
                        st.session_state["forced_export_departures"] = _forced_deps
                    st.rerun()

            if _forced_deps:
                for _fi, _fe in enumerate(_forced_deps):
                    _fec1, _fec2, _fec3 = st.columns([3, 3, 1])
                    _fmc = MOTHER_COLORS.get(_fe["mother"], "#3b82f6")
                    with _fec1:
                        st.markdown(
                            f'<span style="background:{_fmc};color:#fff;border-radius:4px;'
                            f'padding:2px 10px;font-size:11px;font-weight:700">{_fe["mother"]}</span>',
                            unsafe_allow_html=True,
                        )
                    with _fec2:
                        try:
                            _fdate_disp = _dt.date.fromisoformat(_fe["date"]).strftime("%-d %b %Y")
                        except Exception:
                            _fdate_disp = _fe["date"]
                        st.markdown(
                            f'<span style="font-size:11px;color:#1e40af">'
                            f'🚢 Force sail {_fdate_disp} · berth locked until return</span>',
                            unsafe_allow_html=True,
                        )
                    with _fec3:
                        if st.button("✕", key=f"fexp_rm_{_fi}", use_container_width=True):
                            _forced_deps.pop(_fi)
                            st.session_state["forced_export_departures"] = _forced_deps
                            st.rerun()
                if st.button("🗑️ Clear all forced departures", key="fexp_clear_all"):
                    st.session_state["forced_export_departures"] = []
                    st.rerun()
            else:
                st.caption("No forced departures scheduled.")

            # ── Export Unavailability Windows ─────────────────────────────────────────
            sec("🚫 Export Unavailability")
            st.caption(
                "Block export sailings for a date range. Mother vessels will continue to receive "
                "cargo as operationally possible but will be held at BIA until the unavailability "
                "window ends. Vessels that are already mid-export cycle (DOC/SAILING/IN_PORT) when "
                "the window begins will complete that cycle normally."
            )

            if "export_unavailability_windows" not in st.session_state:
                st.session_state["export_unavailability_windows"] = []
            _exp_unavail_list = st.session_state["export_unavailability_windows"]

            _eu_c1, _eu_c2, _eu_c3, _eu_c4 = st.columns([3, 3, 3, 1])
            with _eu_c1:
                st.caption("Reason / label")
                _eu_label = st.text_input(
                    "Label", value="Export unavailable",
                    key="eu_label_inp", label_visibility="collapsed",
                    placeholder="e.g. Terminal maintenance",
                )
            with _eu_c2:
                st.caption("Unavailable from")
                _eu_start = st.date_input(
                    "Unavailable from", value=sim_start_date,
                    key="eu_start_sel", label_visibility="collapsed",
                    format="DD/MM/YYYY",
                )
            with _eu_c3:
                st.caption("Available again from")
                _eu_end = st.date_input(
                    "Available again from",
                    value=sim_start_date + _dt.timedelta(days=7),
                    key="eu_end_sel", label_visibility="collapsed",
                    format="DD/MM/YYYY",
                )
            with _eu_c4:
                st.caption("")
                if st.button("➕ Add", key="eu_add_btn", use_container_width=True):
                    _eu_err = None
                    if _eu_end <= _eu_start:
                        _eu_err = "End date must be after start date."
                    if _eu_err:
                        st.error(_eu_err, icon="❌")
                    else:
                        _exp_unavail_list.append({
                            "label":      _eu_label or "Export unavailable",
                            "start_date": _eu_start.isoformat(),
                            "end_date":   _eu_end.isoformat(),
                        })
                        st.session_state["export_unavailability_windows"] = _exp_unavail_list
                        st.rerun()

            if _exp_unavail_list:
                _euh_c1, _euh_c2, _euh_c3, _euh_c4 = st.columns([3, 3, 3, 1])
                with _euh_c1: st.caption("Reason")
                with _euh_c2: st.caption("Unavailable from")
                with _euh_c3: st.caption("Available again from")
                with _euh_c4: st.caption("")

                for _eui, _eur in enumerate(_exp_unavail_list):
                    _eu_r1, _eu_r2, _eu_r3, _eu_r4 = st.columns([3, 3, 3, 1])
                    _esd_fmt = _dt.date.fromisoformat(_eur["start_date"]).strftime("%d %b %Y")
                    _eed_fmt = _dt.date.fromisoformat(_eur["end_date"]).strftime("%d %b %Y")
                    _edur = (_dt.date.fromisoformat(_eur["end_date"]) -
                             _dt.date.fromisoformat(_eur["start_date"])).days
                    with _eu_r1:
                        st.markdown(
                            f'<span style="display:inline-block;background:#dc2626;color:#fff;'
                            f'font-weight:700;font-size:12px;padding:2px 10px;border-radius:4px">'
                            f'🚫 {_eur["label"]}</span>',
                            unsafe_allow_html=True,
                        )
                    with _eu_r2:
                        st.markdown(f"**{_esd_fmt}**")
                    with _eu_r3:
                        st.markdown(f"**{_eed_fmt}** · {_edur}d window")
                    with _eu_r4:
                        if st.button("✕", key=f"eu_rm_{_eui}", use_container_width=True):
                            _exp_unavail_list.pop(_eui)
                            st.session_state["export_unavailability_windows"] = _exp_unavail_list
                            st.rerun()

                if st.button("🗑️ Clear all export unavailability windows", key="eu_clear_all"):
                    st.session_state["export_unavailability_windows"] = []
                    st.rerun()
            else:
                st.caption("No export unavailability windows configured.")

            # ── Helper: derive plan start date ────────────────────────────────────────
            try:
                _jmp_start = _dt.date.fromisoformat(_start_iso_str)
            except Exception:
                _jmp_start = _dt.date.today()

            _total_sim_days = params["sim_days"]
            _JMP_PAGE_SIZE  = 60   # max days per JMP page (keeps table responsive)

            if _total_sim_days > _JMP_PAGE_SIZE:
                _n_pages = (_total_sim_days + _JMP_PAGE_SIZE - 1) // _JMP_PAGE_SIZE
                _page_labels = [
                    f"Days {1 + p*_JMP_PAGE_SIZE}–{min(_total_sim_days, (p+1)*_JMP_PAGE_SIZE)}"
                    for p in range(_n_pages)
                ]
                _jmp_page   = st.selectbox("📅 JMP page", _page_labels, index=0, key="jmp_page_sel")
                _page_idx   = _page_labels.index(_jmp_page)
                _jmp_d0     = 1  + _page_idx * _JMP_PAGE_SIZE         # first day on page
                _jmp_d1     = min(_total_sim_days, (_page_idx+1) * _JMP_PAGE_SIZE)  # last day
            else:
                _jmp_d0, _jmp_d1 = 1, _total_sim_days
            _jmp_days = _jmp_d1 - _jmp_d0 + 1   # days on this page


            # ── Build per-day data from log_df and tl_df ──────────────────────────────
            _storage_cols = ["SanBarth_bbl","JasmineS_bbl","Westmore_bbl","Duke_bbl","Starturn_bbl","PGM_bbl"]
            # Pair each mother with its timeline column, then keep only those whose
            # column is actually present in tl_df.  This keeps the page working even
            # if the deployed simulation file is an older build that does not emit a
            # given mother's *_bbl column (e.g. Alkebulan on a stale sim deployment).
            _mother_pairs = [("Bryanston","Bryanston_bbl"), ("GreenEagle","GreenEagle_bbl"),
                             ("Alkebulan","Alkebulan_bbl")]
            _tl_cols = set(tl_df.columns) if (tl_df is not None and hasattr(tl_df, "columns")) else set()
            _mother_pairs = [(n, c) for (n, c) in _mother_pairs if c in _tl_cols]
            _mother_names = [n for (n, c) in _mother_pairs]
            _mother_cols  = [c for (n, c) in _mother_pairs]
            _storage_names = ["SanBarth","JasmineS","Westmore","Duke","Starturn","PGM"]

            def _parse_cargo(detail):
                return _extract_cargo_bbl(detail)

            def _parse_storage(detail):
                m = re.search(r"\| (\w+):", detail)
                return m.group(1) if m else ""

            def _parse_mother(detail):
                m = re.search(r"\| (\w+):", detail)
                return m.group(1) if m else ""

            def _kkk(bbl):
                """Format bbl to abbreviated thousands."""
                if bbl >= 1_000_000: return f"{bbl/1_000_000:.1f}M"
                if bbl >= 1_000:     return f"{bbl//1000}k"
                return str(bbl)

            # Pre-index events by day.
            # t=0 is 08:00 Day 1 — there are no Day-0 events. Kept as empty list
            # for forward-compatibility in case a very early event slips through.
            _day0_loadings = log_df[
                (log_df["Day"] < 1) & (log_df["Event"] == "LOADING_START")
            ].to_dict("records")

            # Build a day→mothers_at_export lookup from EXPORT_SAIL_START → EXPORT_FENDERING_COMPLETE.
            # The sim logs both with vessel_name = mother_name (e.g. "Bryanston").
            # Key fix: pair each start with the FIRST fendering-complete that comes AFTER it,
            # not by list index — because EXPORT_RETURN_ARRIVE also fires between the two events
            # and would corrupt index-based pairing across multiple export voyages.
            _mother_export_days = {}
            _export_sail_df = log_df[log_df["Event"] == "EXPORT_SAIL_START"]
            _export_end_df  = log_df[log_df["Event"] == "EXPORT_FENDERING_COMPLETE"]
            for _emn in _export_sail_df["Vessel"].unique():
                _sail_days = sorted(
                    _export_sail_df[_export_sail_df["Vessel"] == _emn]["Day"].tolist()
                )
                _fend_days = sorted(
                    _export_end_df[_export_end_df["Vessel"] == _emn]["Day"].tolist()
                )
                for _sd in _sail_days:
                    # Find the first fendering-complete strictly after this sail-start
                    _after = [d for d in _fend_days if d >= _sd]
                    _ed = _after[0] if _after else _total_sim_days
                    for _xd in range(int(_sd), int(_ed) + 1):
                        _mother_export_days.setdefault(_emn, set()).add(_xd)

            _ev = {}
            for _day in range(1, _total_sim_days + 1):
                _d = log_df[log_df["Day"] == _day]
                _day_loadings = _d[_d["Event"]=="LOADING_START"].to_dict("records")
                # Merge Day-0 loadings into Day 1, deduplicating by vessel
                if _day == 1:
                    _d1_vessel_set = {r["Vessel"] for r in _day_loadings}
                    _day_loadings  = _day_loadings + [
                        r for r in _day0_loadings if r["Vessel"] not in _d1_vessel_set
                    ]
                # Mothers at export today — read directly from the Mother column
                _mothers_away = {mn for mn, days in _mother_export_days.items() if _day in days}
                _ev[_day] = {
                    "loadings":          _day_loadings,
                    "returning":         _d[_d["Event"]=="ARRIVED_LOADING_POINT"].to_dict("records"),
                    "fairway":           _d[_d["Event"]=="ARRIVED_FAIRWAY"].to_dict("records"),
                    "berthing_b":        _d[_d["Event"]=="BERTHING_START_B"].to_dict("records"),
                    "discharge":         _d[_d["Event"]=="DISCHARGE_START"].to_dict("records"),
                    "aborts":            _d[_d["Event"]=="GREENEAGLE_CAPACITY_ABORT"].to_dict("records"),
                    "mothers_at_export": _mothers_away,
                }
                # Opening stock: 08:00 row for this day — t=0 is 08:00, so index 0 is already 08:00
                _t = tl_df[tl_df["Day"] == _day]
                _api_cols  = ["SanBarth_api","JasmineS_api","Westmore_api","Duke_api","Starturn_api","PGM_api"]
                # Derived from the (possibly filtered) _mother_names so the name↔column
                # pairing in the zip below can never drift out of alignment.
                _mapi_cols = [f"{_n}_api" for _n in _mother_names]
                _ovf_cols  = ["SanBarth_Overflow_Accum_bbl","JasmineS_Overflow_Accum_bbl",
                              "Westmore_Overflow_Accum_bbl","Duke_Overflow_Accum_bbl",
                              "Starturn_Overflow_Accum_bbl","PGM_Overflow_Accum_bbl"]
                if not _t.empty:
                    _f = _t.iloc[0]
                    _ev[_day]["stocks"] = {
                        n: int(_f[c]) for n, c in zip(_storage_names, _storage_cols)
                    }
                    _ev[_day]["stocks"]["Ibom"] = int(_f["PointF_Active_Loading_bbl"]) if "PointF_Active_Loading_bbl" in _f.index else 0
                    _ev[_day]["m_stocks"] = {
                        n: int(_f[c]) for n, c in zip(_mother_names, _mother_cols)
                    }
                    _ev[_day]["stock_apis"] = {
                        n: round(float(_f[c]), 2) if c in _f.index else 0.0
                        for n, c in zip(_storage_names, _api_cols)
                    }
                    _ev[_day]["stock_apis"]["Ibom"] = 32.0
                    _ev[_day]["m_stock_apis"] = {
                        n: round(float(_f[c]), 2) if c in _f.index else 0.0
                        for n, c in zip(_mother_names, _mapi_cols)
                    }
                    # Per-storage overflow volumes at 08:00 for this day
                    _ev[_day]["overflows"] = {
                        n: int(_f[c]) if c in _f.index else 0
                        for n, c in zip(_storage_names, _ovf_cols)
                    }
                else:
                    _ev[_day]["stocks"]      = {n: 0   for n in _storage_names}
                    _ev[_day]["stocks"]["Ibom"] = 0
                    _ev[_day]["m_stocks"]    = {n: 0   for n in _mother_names}
                    _ev[_day]["stock_apis"]  = {n: 0.0 for n in _storage_names}
                    _ev[_day]["stock_apis"]["Ibom"] = 32.0
                    _ev[_day]["m_stock_apis"]= {n: 0.0 for n in _mother_names}
                    _ev[_day]["overflows"]   = {n: 0   for n in _storage_names}

            # ── CSS for the plan table ─────────────────────────────────────────────────
            st.markdown("""
        <style>
          .jmp-wrap{overflow-x:auto;padding:4px 0}
          .jmp-table{border-collapse:collapse;min-width:100%;font-size:11px;
                     font-family:'Segoe UI',system-ui,sans-serif}
          .jmp-table th{background:#1a2744;color:#ffffff;padding:5px 8px;
                        text-align:center;font-size:10px;font-weight:700;
                        letter-spacing:.04em;border:1px solid #344d80;white-space:nowrap}
          .jmp-table th.sec-hdr-cell{background:#0f1a35;font-size:10px;
                                      letter-spacing:.06em;text-transform:uppercase}
          .jmp-table td{padding:5px 7px;border:1px solid #e2e8f0;vertical-align:top;
                        white-space:nowrap;min-width:70px}
          .jmp-table tr:nth-child(even) td{background:#f8f9fb}
          .jmp-table tr:nth-child(odd)  td{background:#ffffff}
          .jmp-date{font-weight:700;color:#1a2744;font-size:11px}
          .jmp-stock{font-size:10px;font-weight:600;color:#374151}
          .jmp-entry{display:inline-block;border-radius:4px;padding:2px 6px;
                     margin:1px 0;font-size:10px;font-weight:600;color:#fff;
                     white-space:nowrap;line-height:1.5}
          .jmp-idle{color:#94a3b8;font-size:10px;font-style:italic}
          .jmp-bia-entry{display:inline-block;border-radius:4px;padding:2px 6px;
                         margin:1px 0;font-size:10px;font-weight:600;
                         white-space:nowrap;line-height:1.5}
        </style>""", unsafe_allow_html=True)

            # ── Column structure (mirrors the image) ──────────────────────────────────
            # We render as HTML table for full visual control + PNG export
            _vc = VESSEL_COLORS
            _mc = MOTHER_COLORS

            def _chip(vessel, text, bg=None):
                c = bg or _vc.get(vessel, "#94a3b8")
                return f'<span class="jmp-entry" style="background:{c}">{text}</span>'

            def _mchip(mother, text):
                c = _mc.get(mother, "#94a3b8")
                return f'<span class="jmp-bia-entry" style="background:{c}22;color:{c};border:1px solid {c}66">{text}</span>'

            def _idle():
                return '<span class="jmp-idle">—</span>'

            def _vcode_badge(r, mto_transient=False):
                """Voyage-code badge rendered below a vessel chip.
                mto_transient=True renders red background with white text (transient offload).
                """
                vc = str(r.get("VoyageCode", "")).strip()
                if not vc:
                    return ""
                if mto_transient:
                    bg, fg = "#dc2626", "#ffffff"
                else:
                    bg, fg = "rgba(0,0,0,0.35)", "#ffffff"
                return (
                    f'<div style="margin-top:2px">'
                    f'<span style="background:{bg};border-radius:2px;'
                    f'padding:0 5px;font-size:9px;font-family:monospace;'
                    f'letter-spacing:0.04em;color:{fg}">{vc}</span></div>'
                )


            # ── Mother-vessel idle reason helper ──────────────────────────────────────
            # Called whenever a mother cell would otherwise show "—".  Queries
            # log_df to find the most informative reason and, when an ongoing
            # operation is detected, shows its ETC from the log.
            def _mother_idle_reason(mother_name, day_num, _ldf=log_df):
                """Return an HTML snippet explaining why a mother cell is blank today.

                Priority for primary mothers (Bryanston / GreenEagle):
                  1. Ongoing pump from a previous day — show ETC of cast-off.
                     Day-0 startup-seed events are excluded to avoid false
                     "Hose conn. since 07:00" messages on Day 1.
                  2. Export intake buffer (post-discharge settling window)
                  3. Export documentation started
                  4. Mother away (WAITING_MOTHER_RETURN events)
                  5. Active berth occupant berthing / hose-connecting today
                  6. Vessels waiting for this mother (capacity / berth queue)
                  7. No qualifying vessels at BIA (fallback)
                """
                # ══ Primary mothers: Bryanston / GreenEagle ══════════════════════

                # 1. Ongoing pump from a prior CALENDAR day
                # Day-0 events (startup seeds, t ≤ 0) are excluded — they would
                # otherwise show "Hose conn. since 07:00" on Day 1 for seeded vessels.
                # We also compare by ISO date so that early-morning events (e.g. 06:30)
                # that share Day=N with the operational day N are not treated as prior-day.
                _this_date_iso = (_jmp_start + _dt.timedelta(days=day_num - 1)).isoformat()
                # FIX: extend to Day <= day_num so operations that START on the
                # current sim day are visible as "ongoing" when rendering that day.
                # The existing _completed check already filters out anything that
                # finished before day_num, so widening here is safe.
                _next_date_iso = (_jmp_start + _dt.timedelta(days=day_num)).isoformat()
                _prior = _ldf[
                    (_ldf["Mother"] == mother_name) &
                    (_ldf["Day"] >= 1) &
                    (_ldf["Day"] <= day_num) &
                    (~_ldf["Time"].str.startswith(_next_date_iso)) &   # exclude next-calendar-day events
                    (_ldf["Event"].isin([
                        "DISCHARGE_START", "HOSE_CONNECTION_START_B",
                        "BERTHING_START_B",
                    ]))
                ].sort_values("Time")
                _completed = _ldf[
                    (_ldf["Mother"] == mother_name) &
                    (_ldf["Day"] >= 1) &
                    (_ldf["Day"] <= day_num) &
                    (_ldf["Event"].isin(["CAST_OFF_COMPLETE_B"]))
                ].sort_values("Time")
                if not _prior.empty:
                    _last_start = _prior.iloc[-1]
                    _after_comp = _completed[_completed["Time"] > _last_start["Time"]]
                    if _after_comp.empty:
                        # ── Issue-3 fix: a stale "Pumping" must not mask export
                        # readiness.  When the pumping discharge has already
                        # COMPLETED and the mother has reached its export trigger
                        # (stock at/above the trigger by this day), the mother is no
                        # longer actively pumping — she is full and held at BIA
                        # awaiting export.  Previously the status stayed "🔄 Pumping:
                        # <vessel> ETC cast-off <far-future>" because the cast-off was
                        # scheduled days ahead, which misrepresented an export-ready
                        # mother as still loading.  Detect this and show the correct
                        # "at capacity — ready for export" status instead.
                        _disch_done = _ldf[
                            (_ldf["Mother"] == mother_name) &
                            (_ldf["Event"] == "DISCHARGE_COMPLETE") &
                            (_ldf["Time"] >= _last_start["Time"]) &
                            (_ldf["Day"] <= day_num)
                        ]
                        _export_started = _ldf[
                            ((_ldf["Mother"] == mother_name) | (_ldf["Vessel"] == mother_name)) &
                            (_ldf["Event"].isin(["EXPORT_DOC_START", "EXPORT_SAIL_START"])) &
                            (_ldf["Time"] >= _last_start["Time"]) &
                            (_ldf["Day"] <= day_num)
                        ]
                        # Mother stock at this day from the timeline (08:00 snapshot).
                        _mstock = None
                        try:
                            _scol = f"{mother_name}_bbl"
                            _trow = tl_df[tl_df["Day"] == day_num]
                            if not _trow.empty and _scol in _trow.columns:
                                _mstock = float(_trow.iloc[-1][_scol])
                        except Exception:
                            _mstock = None
                        # Export trigger for this mother, read from the engine module
                        # (MOTHER_EXPORT_TRIGGER_BY_NAME) so it always matches the
                        # value the simulation actually used.
                        _trig = None
                        try:
                            _trig_map = getattr(mod, "MOTHER_EXPORT_TRIGGER_BY_NAME", {})
                            _trig = float(_trig_map.get(
                                mother_name,
                                getattr(mod, "MOTHER_EXPORT_TRIGGER", 1e18)))
                        except Exception:
                            _trig = None
                        # If the pump finished and the mother is at/above her export
                        # trigger (and export hasn't visibly sailed yet this day),
                        # report export readiness rather than stale pumping.
                        if (not _disch_done.empty and _export_started.empty
                                and _mstock is not None and _trig is not None
                                and _mstock >= _trig - 1.0):
                            return (
                                '<span style="font-size:9px;color:#b45309;font-weight:600">'
                                '🛢 At capacity —<br>ready for export</span>'
                            )
                        _co_row = _ldf[
                            (_ldf["Mother"] == mother_name) &
                            (_ldf["Day"] >= _last_start["Day"]) &
                            (_ldf["Day"] <= day_num + 4) &   # multi-day operations
                            (_ldf["Event"] == "CAST_OFF_START_B")
                        ].sort_values("Time")
                        _vessel = _last_start.get("Vessel", "")
                        if not _co_row.empty:
                            _co = _co_row.iloc[-1]
                            _etc_time = _co["Time"]
                            _etc_disp = (
                                _etc_time[11:16]
                                if _etc_time[:10] == _last_start["Time"][:10]
                                else f"{_etc_time[8:10]}/{_etc_time[5:7]} {_etc_time[11:16]}"
                            )
                            return (
                                f'<span style="font-size:9px;color:#92400e;font-weight:600">'
                                f'🔄 Pumping: {_vessel}<br>'
                                f'ETC cast-off {_etc_disp}</span>'
                            )
                        return (
                            f'<span style="font-size:9px;color:#92400e;font-weight:600">'
                            f'🔄 Pumping: {_vessel}</span>'
                        )

                # 2–7. Same-day events
                _day_rows = _ldf[
                    (_ldf["Day"] == day_num) &
                    ((_ldf["Mother"] == mother_name) |
                     (_ldf["Vessel"] == mother_name))
                ]

                # 2. Export intake buffer
                if not _day_rows[_day_rows["Event"] == "EXPORT_INTAKE_BUFFER"].empty:
                    return ('<span style="font-size:9px;color:#6b21a8;font-weight:600">'
                            '⏳ Post-discharge<br>settling buffer</span>')

                # 3. Export doc started
                if not _day_rows[_day_rows["Event"] == "EXPORT_DOC_START"].empty:
                    return ('<span style="font-size:9px;color:#1d4ed8;font-weight:600">'
                            '📋 Export docs<br>in progress</span>')

                # 4. Waiting for mother return
                _wmr = _ldf[
                    (_ldf["Day"] == day_num) &
                    (_ldf["Event"] == "WAITING_MOTHER_RETURN") &
                    (_ldf["Detail"].str.contains(mother_name, na=False))
                ]
                if not _wmr.empty:
                    return ('<span style="font-size:9px;color:#0369a1;font-weight:600">'
                            '⚓ Awaiting mother<br>return from export</span>')

                # 5. Active berthing or hose event that belongs to THIS calendar day
                # (not early-morning events of the NEXT day that happen to share
                # the same Day number due to the 24h boundary).
                # Filter: Time column date prefix must match this row's calendar date.
                _date_iso = (_jmp_start + _dt.timedelta(days=day_num - 1)).isoformat()
                _berth_events = _day_rows[
                    _day_rows["Event"].isin(["BERTHING_START_B", "HOSE_CONNECTION_START_B"]) &
                    _day_rows["Time"].str.startswith(_date_iso)
                ]
                if not _berth_events.empty:
                    _be  = _berth_events.iloc[-1]
                    _v   = _be.get("Vessel", "")
                    _etc = _be["Time"][11:16]
                    _lbl = "Berthing" if _be["Event"] == "BERTHING_START_B" else "Hose conn."
                    return (
                        f'<span style="font-size:9px;color:#065f46;font-weight:600">'
                        f'🔗 {_lbl}: {_v}<br>'
                        f'since {_etc}</span>'
                    )

                # 6. Vessels waiting for berth / capacity
                _wt = _ldf[
                    (_ldf["Day"] == day_num) &
                    (_ldf["Mother"] == mother_name) &
                    (_ldf["Event"].isin([
                        "WAITING_BERTH_B", "WAITING_MOTHER_CAPACITY",
                        "WAITING_MOTHER_RETURN",
                    ]))
                ]
                if not _wt.empty:
                    _wev = _wt.iloc[-1]["Event"]
                    if _wev == "WAITING_MOTHER_CAPACITY":
                        return ('<span style="font-size:9px;color:#b45309;font-weight:600">'
                                '⏳ At capacity —<br>awaiting export</span>')
                    return ('<span style="font-size:9px;color:#475569;font-weight:600">'
                            '⏳ Berth queue —<br>no eligible vessel</span>')

                # 7. No qualifying vessels (fallback)
                return ('<span style="font-size:9px;color:#94a3b8;font-style:italic">'
                        'No vessel available</span>')


            # ── Ongoing-load ETC helper (shared by BOTH loading-cell renderers) ───
            # When NO load STARTS at a storage on a given day, detect a load that
            # began earlier and is still in progress (its LOADING_COMPLETE is on a
            # later calendar day) and return an HTML 'ongoing — ETC' marker; '' when
            # the berth is genuinely free.  Defined here (not inside a day loop) so
            # the main JMP renderer (_lcell) and the full-run renderer (_fload) both
            # use the identical logic — a multi-day load (e.g. Laphroaig ~38h at
            # Westmore over 3 calendar days) shows the ETC on the in-between days in
            # every loading table instead of a blank cell.
            def _storage_ongoing_load(storage, day_num):
                _day_date = _jmp_start + _dt.timedelta(days=day_num - 1)
                _starts = log_df[
                    (log_df["Event"] == "LOADING_START")
                    & (log_df["Day"] < day_num)
                    & (log_df["Detail"].apply(lambda d: _parse_storage(d) == storage))
                ]
                if _starts.empty:
                    return ""
                _starts = _starts.sort_values("Time")
                _last = _starts.iloc[-1]
                _ves = _last["Vessel"]
                _start_time = str(_last["Time"])
                _comp = log_df[
                    (log_df["Vessel"] == _ves)
                    & (log_df["Event"] == "LOADING_COMPLETE")
                    & (log_df["Time"] >= _start_time)
                ].sort_values("Time")
                if _comp.empty:
                    # never completed within the run → still loading through today
                    _etc_disp = "—"
                else:
                    _ct = str(_comp.iloc[0]["Time"])
                    try:
                        _cdate = _dt.date.fromisoformat(_ct[:10])
                    except Exception:
                        _cdate = None
                    # Ongoing only if completion is on a LATER calendar day; a same-day
                    # or earlier completion means the berth is free (no false marker).
                    if _cdate is None or _cdate <= _day_date:
                        return ""
                    _days_ahead = (_cdate - _day_date).days
                    _etc_disp = (_ct[11:16] if _days_ahead == 0
                                 else f"{_ct[11:16]} +{_days_ahead}d")
                _c = _vc.get(_ves, "#94a3b8")
                return (
                    f'<span style="display:inline-block;border-radius:4px;'
                    f'padding:2px 6px;margin:1px 0;font-size:9px;font-weight:600;'
                    f'background:{_c}22;color:{_c};border:1px dashed {_c}88;'
                    f'white-space:nowrap;line-height:1.4">'
                    f'🛢 Loading: {_ves}<br>ETC {_etc_disp}</span>'
                )

            _html = ['<div class="jmp-wrap"><table class="jmp-table">']

            # Canonical mother-column list for the Discharging Plan section.
            # Derived from the sim's MOTHER_NAMES so any primary (e.g. Alkebulan)
            # is included automatically and the header always matches the data
            # cells built below (which iterate the same list).
            _JMP_MOTHER_DISP = {}
            _jmp_moms = list(getattr(mod, "MOTHER_NAMES",
                                     ["Bryanston", "GreenEagle", "Alkebulan"]))
            _moms_hdr_html = "".join(f"<th>{_JMP_MOTHER_DISP.get(_m, _m)}</th>" for _m in _jmp_moms)
            _disch_colspan = len(_jmp_moms) + 1   # mothers + ZeeZee column

            # Header row 1 — section labels
            _html.append(
                '<tr>'
                '<th rowspan="2" class="sec-hdr-cell">Date</th>'
                '<th colspan="7" class="sec-hdr-cell">Opening Stock (bbl)</th>'
                '<th colspan="6" class="sec-hdr-cell">Loading Plan</th>'
                '<th colspan="2" class="sec-hdr-cell">Returning to Load</th>'
                '<th colspan="1" class="sec-hdr-cell">Arriving BIA</th>'
                f'<th colspan="{_disch_colspan}" class="sec-hdr-cell">Discharging Plan</th>'
                '<th style="background:#1a3a2a" class="sec-hdr-cell">MTO</th>'
                '</tr>'
            )
            # Header row 2 — column names
            _html.append(
                '<tr>'
                '<th>SanBarth</th><th>JasmineS</th><th>Westmore</th><th>Duke</th><th>Starturn</th><th>PGM</th><th>Ibom</th>'
                '<th>SanBarth</th><th>JasmineS</th><th>Westmore</th><th>Duke</th><th>Starturn</th><th>PGM</th>'
                '<th>Vessel &rarr; Storage</th><th>ETA</th>'
                '<th>Vessel (ETA)</th>'
                f'{_moms_hdr_html}'
                '<th style="background:#1e3a5f">ZeeZee</th>'
                '<th style="background:#1a3a2a">MTO<br><span style="font-size:9px;font-weight:400">Transient ↔ Discharger</span></th>'
                '</tr>'
            )

            for _day in range(_jmp_d0, _jmp_d1 + 1):
                _date = _jmp_start + _dt.timedelta(days=_day - 1)
                _de = _ev[_day]
                _stocks  = _de["stocks"]
                _mstocks = _de["m_stocks"]

                # ── Date cell ─────────────────────────────────────────────────────────
                _date_cell = f'<td class="jmp-date">{_date.strftime("%-d %b %Y")}<br><span style="font-size:9px;color:#64748b">{_date.strftime("%a")}</span></td>'

                # ── Stock cells ───────────────────────────────────────────────────────
                # Thresholds from ops colour-code chart:
                #   Safe (green)  < lower_limit
                #   Borderline (amber)  lower_limit – upper_limit
                #   Unsafe (red)  > upper_limit
                _STOCK_THRESHOLDS = {
                    "SanBarth":    (189_000, 228_000),
                    "JasmineS":  (189_000, 228_000),
                    "Westmore":  (130_000, 175_000),   # Unsafe >175k; Borderline 130k-175k; Safe <130k
                    "Ibom":      ( 70_000,  84_400),
                    "Starturn":  ( 45_500,  54_860),
                    "Duke":      ( 63_000,  76_000),   # proportional: ~70% & ~84% of 90k
                    "PGM":       ( 19_600,  23_520),   # 70% / 84% of 28k cap: Safe <19.6k · Borderline 19.6-23.5k · Unsafe >23.5k
                }
                def _scell(name):
                    v    = _stocks.get(name, 0)
                    api  = _de.get("stock_apis", {}).get(name, 0.0)
                    ovf  = _de.get("overflows", {}).get(name, 0)
                    lo, hi = _STOCK_THRESHOLDS.get(name, (189_000, 228_000))
                    if v < lo:
                        bg, col, label = "#166534", "#bbf7d0", "Safe"
                    elif v <= hi:
                        bg, col, label = "#854d0e", "#fef08a", "Borderline"
                    else:
                        bg, col, label = "#991b1b", "#fecaca", "Unsafe"
                    _api_str = f'<br><span style="color:{col};font-size:8px;opacity:0.8">API {api:.2f}°</span>' if v > 0 else ""
                    _ovf_str = (f'<span style="color:#fca5a5;font-size:8px;font-weight:700"> (+{_kkk(ovf)})</span>'
                                if ovf > 0 else "")
                    return (
                        f'<td style="background:{bg};text-align:center">' +
                        f'<span style="color:{col};font-weight:700;font-size:10px">{_kkk(v)}</span>' +
                        _ovf_str +
                        f'<br><span style="color:{col};font-size:8px;opacity:0.85">{label}</span>' +
                        _api_str +
                        '</td>'
                    )

                _stock_cells = "".join(_scell(n) for n in _storage_names) + _scell("Ibom")

                # ── Loading plan cells — one column per storage ───────────────────────
                _active_overrides = st.session_state.get("jmp_storage_overrides", {})

                def _lcell(storage):
                    entries = [r for r in _de["loadings"] if _parse_storage(r["Detail"])==storage]
                    if not entries:
                        # No load STARTS today — but a multi-day load may still be in
                        # progress at this berth; show its ETC rather than "—".
                        _ongoing = _storage_ongoing_load(storage, _day)
                        return f'<td>{_ongoing or _idle()}</td>'
                    chips = []
                    for r in entries:
                        _ov_entry = _active_overrides.get(r["Vessel"], {}).get(str(_day))
                        _is_forced = (
                            _ov_entry == storage
                            or (isinstance(_ov_entry, dict)
                                and _ov_entry.get("storage") == storage)
                        )
                        # Voyage code badge — rendered on a new line below vessel name so
                        # the loading-plan column stays slim (not stretched by inline badges)
                        _ev_vcode = str(r.get("VoyageCode", "")).strip()
                        _vcode_sub = (
                            f'<div style="margin-top:2px;margin-bottom:1px">'
                            f'<span style="background:rgba(0,0,0,0.40);border-radius:2px;'
                            f'padding:0 5px;font-size:9px;font-family:monospace;'
                            f'letter-spacing:0.03em">{_ev_vcode}</span></div>'
                            if _ev_vcode else ""
                        )
                        _label = (
                            f"{r['Vessel']} | {r['Time'][11:16]} | {_kkk(_parse_cargo(r['Detail']))}"
                            + (" 🔒" if _is_forced else "")
                        )
                        chips.append(_chip(r["Vessel"], _label) + _vcode_sub)
                    inner = "<br>".join(chips)
                    return f"<td>{inner}</td>"

                _load_cells = "".join(_lcell(n) for n in _storage_names)

                # ── Returning to load ─────────────────────────────────────────────────
                _rets = _de["returning"]
                if _rets:
                    # Resolve "Point X storage area" → actual storage name
                    _PT_STOR = {
                        "A": "SanBarth / JasmineS",
                        "C": "Westmore",
                        "D": "Duke (Awoba)",
                        "E": "Starturn",
                        "G": "PGM",
                        "F": "Ibom",
                    }
                    def _ret_label(detail):
                        _pm = re.search(r"Point ([A-G])", detail)
                        if _pm:
                            return _PT_STOR.get(_pm.group(1), f"Point {_pm.group(1)}")
                        return detail.split("Arrived ")[-1].split(" —")[0]
                    _ret_vessel = "<br>".join(
                        _chip(r["Vessel"], f"{r['Vessel']} → {_ret_label(r['Detail'])}")
                        for r in _rets
                    )
                    _ret_eta = "<br>".join(r["Time"][11:16] for r in _rets)
                else:
                    _ret_vessel = _idle()
                    _ret_eta    = _idle()

                # ── Arriving BIA ──────────────────────────────────────────────────────
                def _eta_label(r):
                    """Build vessel ETA label with cargo volume and +N day suffix when ETA crosses midnight."""
                    _eta_time = r["Time"][11:16]
                    _eta_date_str = r["Time"][:10]
                    _cargo = _parse_cargo(r.get("Detail", ""))
                    _cargo_str = f" | {_kkk(_cargo)}" if _cargo > 0 else ""
                    try:
                        import datetime as _dtimp
                        _eta_date = _dtimp.date.fromisoformat(_eta_date_str)
                        _days_ahead = (_eta_date - _date).days
                        if _days_ahead == 1:
                            return f"{r['Vessel']} ({_eta_time} +1d){_cargo_str}"
                        elif _days_ahead >= 2:
                            return f"{r['Vessel']} ({_eta_time} +{_days_ahead}d){_cargo_str}"
                    except Exception:
                        pass
                    return f"{r['Vessel']} ({_eta_time}){_cargo_str}"

                _fwy = _de["fairway"]
                if _fwy:
                    _bia_arr = "<br>".join(
                        _chip(r["Vessel"], _eta_label(r)) + _vcode_badge(r)
                        for r in _fwy
                    )
                else:
                    _bia_arr = _idle()

                # ── Discharge plan cells — one column per mother ──────────────────────
                def _dcell(mother):
                    entries  = [r for r in _de["discharge"] if _parse_mother(r["Detail"])==mother]
                    aborts   = [r for r in _de.get("aborts", []) if r.get("Mother") == mother]
                    ms       = _mstocks.get(mother, 0)
                    mapi     = _de.get("m_stock_apis", {}).get(mother, 0.0)
                    _api_bit = f' · API {mapi:.2f}°' if ms > 0 else ""
                    _at_exp  = mother in _de.get("mothers_at_export", set())

                    def _entry_chip(r):
                        vc = str(r.get("VoyageCode", "")).strip()
                        _is_mto = vc.endswith("A") and len(vc) > 1
                        return (
                            _chip(r["Vessel"],
                                  f"{r['Vessel']} | {r['Time'][11:16]} | {_kkk(_parse_cargo(r['Detail']))}")
                            + _vcode_badge(r, mto_transient=_is_mto)
                        )

                    def _abort_chip(r):
                        return _chip(r["Vessel"],
                                     f"⚠️ {r['Vessel']} | {r['Time'][11:16]} | Cap. abort",
                                     bg="#d97706")

                    if _at_exp:
                        stk = f'<span style="font-size:9px;color:#bfdbfe">Stock: {_kkk(ms)}{_api_bit}</span>'
                        _exp_label = '<div style="font-size:9px;font-weight:700;color:#fff;letter-spacing:0.06em;margin-bottom:2px">⚓ EXPORT OPS</div>'
                        if not entries:
                            return (f'<td style="background:#1e3a5f;text-align:center;vertical-align:middle">'
                                    f'{_exp_label}{stk}</td>')
                        stk2 = f'<span style="font-size:9px;color:#bfdbfe;display:block;margin-bottom:2px">Stock: {_kkk(ms)}{_api_bit}</span>'
                        inner = "<br>".join(_entry_chip(r) for r in entries)
                        return f'<td style="background:#1e3a5f">{_exp_label}{stk2}{inner}</td>'
                    stk = f'<span style="font-size:9px;color:#94a3b8">Stock: {_kkk(ms)}{_api_bit}</span>'
                    abort_html = ("<br>".join(_abort_chip(r) for r in aborts) + "<br>") if aborts else ""
                    if not entries:
                        if aborts:
                            stk2 = f'<span style="font-size:9px;color:#64748b;display:block;margin-bottom:2px">Stock: {_kkk(ms)}{_api_bit}</span>'
                            return f'<td style="background:#fffbeb">{stk2}{abort_html}</td>'
                        # FIX: show "no ullage" when mother is at or above capacity.
                        # Catches the Day 3 case where GreenEagle fills to 750k/750k
                        # from an overnight operation — no daughter can berth
                        # until the export voyage sails and ullage reopens.
                        _mcap_map = getattr(mod, "MOTHER_CAPACITY_BY_NAME", {})
                        _this_cap = _mcap_map.get(mother, 0)
                        if _this_cap > 0 and ms >= _this_cap:
                            return (
                                f'<td style="text-align:center;vertical-align:middle">'
                                f'{stk}<br>'
                                f'<span style="font-size:9px;color:#b91c1c;font-weight:600">'
                                f'⛔ At capacity —<br>no ullage available</span></td>'
                            )
                        _reason = _mother_idle_reason(mother, _day)
                        return (
                            f'<td style="text-align:center;vertical-align:middle">'
                            f'{stk}<br>{_reason}</td>'
                        )
                    stk2 = f'<span style="font-size:9px;color:#64748b;display:block;margin-bottom:2px">Stock: {_kkk(ms)}{_api_bit}</span>'
                    inner = "<br>".join(_entry_chip(r) for r in entries)
                    return f"<td>{stk2}{abort_html}{inner}</td>"

                _disch_cells = "".join(_dcell(n) for n in _jmp_moms)

                # ── ZeeZee column: show DISCHARGE_START events for this day ──────────
                def _zz_dcell():
                    # ── FIX: slice log_df for THIS day (not the stale build-loop _d) ─
                    _day_rows = log_df[log_df["Day"] == _day]
                    _zz_entries = [
                        r for r in _de["discharge"]
                        if r.get("Vessel") == "ZeeZee"
                    ]
                    _zz_waiting = [
                        r for r in _day_rows[_day_rows["Event"].isin([
                            "WAITING_BERTH_B", "WAITING_MOTHER_CAPACITY",
                            "BERTHING_START_B", "HOSE_CONNECTION_START_B",
                            "ZEEZEE_DEADLINE_OVERRIDE", "GREENEAGLE_CAPACITY_ABORT",
                        ])].to_dict("records")
                        if r.get("Vessel") == "ZeeZee"
                    ]
                    if not _zz_entries and not _zz_waiting:
                        # Check if ZeeZee is visiting but not yet discharging
                        _zz_present = [
                            r for r in _day_rows[_day_rows["Vessel"] == "ZeeZee"].to_dict("records")
                        ]
                        if _zz_present:
                            _status = _zz_present[-1].get("Event", "")
                            _lbl = {
                                "VESSEL_JOINED":              "🚢 Arrived",
                                "WAITING_BERTH_B":            "⏳ Waiting berth",
                                "WAITING_MOTHER_CAPACITY":    "⏳ No cap.",
                                "ZEEZEE_DEADLINE_OVERRIDE":   "⚡ Priority",
                                "BERTHING_START_B":           "⚓ Berthing",
                                "HOSE_CONNECTION_START_B":    "🔗 Hose conn.",
                                "GREENEAGLE_CAPACITY_ABORT":  "⚠️ Cap. abort",
                            }.get(_status, _status)
                            return (f'<td style="background:#f0f4ff;text-align:center">'
                                    f'<span style="font-size:10px;color:#1e3a5f;font-weight:600">'
                                    f'{_lbl}</span></td>')
                        return f'<td style="text-align:center">{_idle()}</td>'
                    if _zz_entries:
                        inner = "<br>".join(
                            _chip("ZeeZee",
                                  f"ZeeZee | {r['Time'][11:16]} | {_kkk(_parse_cargo(r['Detail']))}",
                                  bg="#1e3a5f")
                            for r in _zz_entries
                        )
                        return f'<td style="background:#eef2ff">{inner}</td>'
                    # Waiting/berthing state — show status chip
                    _zws = _zz_waiting[-1]
                    _ev_name = _zws.get("Event", "")
                    _lbl2 = {
                        "WAITING_BERTH_B":            "⏳ Waiting berth",
                        "WAITING_MOTHER_CAPACITY":    "⏳ Awaiting cap.",
                        "ZEEZEE_DEADLINE_OVERRIDE":   "⚡ Priority",
                        "BERTHING_START_B":           "⚓ Berthing",
                        "HOSE_CONNECTION_START_B":    "🔗 Hose conn.",
                        "GREENEAGLE_CAPACITY_ABORT":  "⚠️ Cap. abort",
                    }.get(_ev_name, _ev_name)
                    return (f'<td style="background:#f0f4ff;text-align:center">'
                            f'<span style="font-size:10px;color:#1e3a5f;font-weight:600">'
                            f'{_lbl2}</span></td>')

                _disch_cells = "".join(_dcell(n) for n in _jmp_moms)

                # ── MTO column: show all transient/discharger pairs for this day ───────
                def _mto_dcell():
                    # Slice log_df for THIS day
                    _day_rows = log_df[log_df["Day"] == _day]
                    import re as _re
                    _mto_nom = _day_rows[
                        _day_rows["Event"] == "MTO_TRANSIENT_NOMINATED"
                    ].to_dict("records")
                    _mto_dis = _day_rows[
                        _day_rows["Event"] == "MTO_DISCHARGE_TO_TRANSIENT"
                    ].to_dict("records")
                    if not _mto_nom and not _mto_dis:
                        return f'<td style="text-align:center;background:#f0fdf4">{_idle()}</td>'

                    # Group nominations by transient vessel
                    _seen_transients = {}  # transient_name -> list of nomination records
                    for _nr in _mto_nom:
                        _tn = _nr["Vessel"]
                        _seen_transients.setdefault(_tn, []).append(_nr)

                    # Map discharger events back to transient by parsing Detail text
                    # Detail format: "... → {transient_name}: ..." or "... to {transient_name}: ..."
                    _dis_by_transient = {}
                    for _dr in _mto_dis:
                        _det = _dr.get("Detail", "")
                        _dt_match = (_re.search(r"→\s*(\w+)\s*:", _det)
                                     or _re.search(r"to\s+(\w+)\s*:", _det))
                        _trn_key = _dt_match.group(1) if _dt_match else None

                        if _trn_key:
                            # Add to seen_transients even if there's no NOM event (startup seeds)
                            if _trn_key not in _seen_transients:
                                _seen_transients[_trn_key] = []  # empty nom list — dis-only block
                            _dis_by_transient.setdefault(_trn_key, []).append(_dr)
                        else:
                            _fb = next(iter(_seen_transients), None)
                            if _fb:
                                _dis_by_transient.setdefault(_fb, []).append(_dr)

                    _blocks = []
                    for _tidx, (_tname, _tnoms) in enumerate(_seen_transients.items()):
                        _tc  = VESSEL_COLORS.get(_tname, "#334155")
                        _time = _tnoms[0]["Time"][11:16] if _tnoms else ""
                        _trn_vcode = str(_tnoms[0].get("VoyageCode", "")).strip() if _tnoms else ""
                        _trn_detail = str(_tnoms[-1].get("Detail", "")) if _tnoms else ""
                        _trn_onboard = ""
                        try:
                            _ob = _re.search(r"on-board:\s*([\d,]+)\s*bbl", _trn_detail)
                            if _ob:
                                _trn_onboard = f"{int(_ob.group(1).replace(',','')) // 1000}k bbl"
                        except Exception:
                            pass
                        _vcode_badge = (
                            f'<div style="background:rgba(0,0,0,0.3);border-radius:2px;'
                            f'padding:0 5px;font-size:9px;font-family:monospace;color:#fff;'
                            f'display:inline-block;margin:1px 0">{_trn_vcode}</div>'
                        ) if _trn_vcode else ""
                        _onboard_badge = (
                            f'<div style="font-size:8px;color:#166534;margin:1px 0">📦 {_trn_onboard}</div>'
                        ) if _trn_onboard else ""
                        _dis_rows = _dis_by_transient.get(_tname, [])
                        _dis_chips = "".join(
                            f'<div style="margin:1px 0">'
                            f'<span style="background:{VESSEL_COLORS.get(r["Vessel"],"#334155")};color:#fff;'
                            f'border-radius:3px;padding:1px 5px;font-size:9px;font-weight:700;'
                            f'display:inline-block">{r["Vessel"]}</span>'
                            + (f'<span style="display:block;background:rgba(0,0,0,0.3);border-radius:2px;'
                               f'padding:0 4px;font-size:8px;font-family:monospace;color:#fff;margin-top:1px">'
                               f'{str(r.get("VoyageCode","")).strip()}</span>'
                               if str(r.get("VoyageCode","")).strip() else "")
                            + f'</div>'
                            for r in _dis_rows
                        ) if _dis_rows else ""
                        # Separator between multiple MTO blocks
                        _sep = '<hr style="border:none;border-top:1px solid #86efac;margin:4px 0">' if _tidx > 0 else ""
                        _blocks.append(
                            f'{_sep}'
                            f'<div style="font-size:9px;color:#166534;font-weight:700;margin-bottom:2px">🔄 MTO {_time}</div>'
                            f'<span style="background:{_tc};color:#fff;border-radius:3px;'
                            f'padding:1px 6px;font-size:10px;font-weight:700">{_tname}</span>'
                            f'{_vcode_badge}{_onboard_badge}'
                            + (f'<div style="font-size:9px;color:#64748b;margin:2px 0">receives from</div>'
                               f'{_dis_chips}' if _dis_chips else "")
                        )

                    return (
                        f'<td style="background:#ecfdf5;text-align:center">'
                        + "".join(_blocks)
                        + f'</td>'
                    )

                _html.append(
                    f"<tr>{_date_cell}{_stock_cells}{_load_cells}"
                    f"<td>{_ret_vessel}</td><td style='text-align:center'>{_ret_eta}</td>"
                    f"<td>{_bia_arr}</td>"
                    f"{_disch_cells}{_zz_dcell()}{_mto_dcell()}</tr>"
                )

            _html.append("</table></div>")
            _table_html = "\n".join(_html)

            # ── Render table in an iframe so <script> executes.
            # The top scrollbar is position:sticky so it pins to the top of
            # the iframe viewport as you scroll down the page — always visible.
            # The bottom scrollbar is the native overflow of .jmp-wrap and is
            # always visible at the foot of the iframe.
            # scrolling=False + exact height means the iframe never gets its own
            # vertical scrollbar — the page scrolls instead, which keeps both
            # the sticky top bar and the native bottom bar permanently on screen.
            _iframe_css = """
              *{box-sizing:border-box}
              html,body{margin:0;padding:0;background:#fff}
              /* Top mirror scrollbar — always visible at top of iframe */
              #top-scroll{
                overflow-x:scroll;
                overflow-y:hidden;
                height:14px;
                background:#f1f5f9;
                border-bottom:1px solid #cbd5e1;
                border-top:1px solid #cbd5e1;
                /* sticky keeps it pinned as iframe scrolls vertically */
                position:sticky;
                top:0;
                z-index:10;
              }
              #top-scroll-inner{height:1px;display:block}
              .jmp-wrap{overflow-x:auto;overflow-y:visible;padding:0}
              .jmp-table{border-collapse:collapse;min-width:100%;font-size:11px;
                         font-family:'Segoe UI',system-ui,sans-serif}
              .jmp-table th{background:#1a2744;color:#ffffff;padding:5px 8px;
                            text-align:center;font-size:10px;font-weight:700;
                            letter-spacing:.04em;border:1px solid #344d80;white-space:nowrap}
              .jmp-table th.sec-hdr-cell{background:#0f1a35;font-size:10px;
                                          letter-spacing:.06em;text-transform:uppercase}
              .jmp-table td{padding:5px 7px;border:1px solid #e2e8f0;vertical-align:top;
                            white-space:nowrap;min-width:70px}
              .jmp-table tr:nth-child(even) td{background:#f8f9fb}
              .jmp-table tr:nth-child(odd)  td{background:#ffffff}
              .jmp-date{font-weight:700;color:#1a2744;font-size:11px}
              .jmp-stock{font-size:10px;font-weight:600;color:#374151}
              .jmp-entry{display:inline-block;border-radius:4px;padding:2px 6px;
                         margin:1px 0;font-size:10px;font-weight:600;color:#fff;
                         white-space:nowrap;line-height:1.5}
              .jmp-idle{color:#94a3b8;font-size:10px;font-style:italic}
              .jmp-bia-entry{display:inline-block;border-radius:4px;padding:2px 6px;
                             margin:1px 0;font-size:10px;font-weight:600;
                             white-space:nowrap;line-height:1.5}
              /* Custom scrollbar styling for better visibility */
              .jmp-wrap::-webkit-scrollbar{width:14px;height:14px}
              .jmp-wrap::-webkit-scrollbar-track{background:#f1f5f9}
              .jmp-wrap::-webkit-scrollbar-thumb{background:#cbd5e1;border-radius:7px}
              .jmp-wrap::-webkit-scrollbar-thumb:hover{background:#94a3b8}
              .jmp-wrap{scrollbar-width:thin;scrollbar-color:#cbd5e1 #f1f5f9}
            """

            _iframe_doc = f"""<!DOCTYPE html>
<html><head><meta charset="utf-8"><style>{_iframe_css}</style></head>
<body>
<div id="top-scroll"><div id="top-scroll-inner"></div></div>
{_table_html}
<script>
(function(){{
  var top   = document.getElementById('top-scroll');
  var inner = document.getElementById('top-scroll-inner');
  var wrap  = document.querySelector('.jmp-wrap');
  if (!top || !wrap) return;

  function setWidth(){{
    var tbl = wrap.querySelector('table');
    if (tbl) {{
      inner.style.width = tbl.scrollWidth + 'px';
      // Tell Streamlit the exact content height so iframe fits perfectly
      var h = document.body.scrollHeight;
      window.parent.postMessage({{type:'jmp_height', h: h}}, '*');
    }}
  }}

  // Run after fonts/layout settle
  setWidth();
  setTimeout(setWidth, 200);
  setTimeout(setWidth, 600);

  var busy = false;
  top.addEventListener('scroll', function(){{
    if (busy) return; busy = true;
    wrap.scrollLeft = top.scrollLeft;
    busy = false;
  }});
  wrap.addEventListener('scroll', function(){{
    if (busy) return; busy = true;
    top.scrollLeft = wrap.scrollLeft;
    busy = false;
  }});
  window.addEventListener('resize', setWidth);
}})();
</script>
</body></html>"""

            # Height: subtract 2 header rows, estimate 26px per data row + top-bar overhead.
            # height:fit-content on body means no blank whitespace below the table.
            _data_trs = max(_table_html.count('<tr') - 2, 1)
            # 600px capped at 4200 — scrolling=True handles overflow vertically
            _iframe_h = min(max(_data_trs * 26 + 46, 400), 4200)
            _stc.html(_iframe_doc, height=_iframe_h, scrolling=True)

            # ── Legend ─────────────────────────────────────────────────────────────────
            _leg_html = '<div style="margin:10px 0 4px;display:flex;flex-wrap:wrap;gap:6px;align-items:center">'
            _leg_html += '<span style="font-size:11px;font-weight:700;color:#1a2744;margin-right:4px">Vessel colours:</span>'
            for _vn, _vc2 in VESSEL_COLORS.items():
                _leg_html += f'<span style="background:{_vc2};color:#fff;border-radius:4px;padding:2px 8px;font-size:10px;font-weight:700">{_vn}</span>'
            _leg_html += '</div>'
            st.markdown(_leg_html, unsafe_allow_html=True)

            # ── PDF/HTML Download ─────────────────────────────────────────────────────
            st.markdown("---")
            _jmp_pg_start = _jmp_start + _dt.timedelta(days=_jmp_d0 - 1)
            _jmp_pg_end   = _jmp_start + _dt.timedelta(days=_jmp_d1 - 1)
            if _total_sim_days > _JMP_PAGE_SIZE:
                _jmp_title_str = (
                    f"Journey Management Plan — {_jmp_pg_start.strftime('%d %b %Y')} to "
                    f"{_jmp_pg_end.strftime('%d %b %Y')} "
                    f"(Days {_jmp_d0}–{_jmp_d1} of {_total_sim_days})"
                )
            else:
                _jmp_title_str = f"Journey Management Plan — {_jmp_start.strftime('%d %b %Y')} ({_total_sim_days} days)"

            # ── Build FULL table HTML (all days, regardless of current page view) ──────
            # Guard: only rebuild when the simulation changes, not on every widget
            # interaction. A 16-char hash of (start_date, sim_days, export_day_sets)
            # is stored in session_state alongside the finished HTML.
            _jmp_rebuild_key = hashlib.md5(
                f"{_jmp_start.isoformat()}:{_total_sim_days}:"
                f"{json.dumps({k: sorted(v) for k, v in _mother_export_days.items()}, sort_keys=True)}"
                .encode()
            ).hexdigest()[:16]
            _cached_full_html   = st.session_state.get("_jmp_full_html")
            _cached_full_key    = st.session_state.get("_jmp_full_key")
            _cached_full_csv    = st.session_state.get("_jmp_full_csv")
            _full_run_html_ready = (_cached_full_html is not None
                                    and _cached_full_key == _jmp_rebuild_key)

            _full_title_str = (
                f"Journey Management Plan — {_jmp_start.strftime('%d %b %Y')} to "
                f"{(_jmp_start + _dt.timedelta(days=_total_sim_days - 1)).strftime('%d %b %Y')} "
                f"({_total_sim_days} days)"
            )
            # Canonical mother-column list for the exported JMP (mirrors the
            # in-app builder) — derived from MOTHER_NAMES so Alkebulan and any
            # other primary appear automatically and header/cells stay aligned.
            _JMP_MOTHER_DISP = {}
            _jmp_moms = list(getattr(mod, "MOTHER_NAMES",
                                     ["Bryanston", "GreenEagle", "Alkebulan"]))
            _moms_hdr_html = "".join(f"<th>{_JMP_MOTHER_DISP.get(_m, _m)}</th>" for _m in _jmp_moms)
            _disch_colspan = len(_jmp_moms) + 1   # mothers + ZeeZee column
            _full_rows_html = []
            for _fd in range(1, _total_sim_days + 1):
                _fdate = _jmp_start + _dt.timedelta(days=_fd - 1)
                _fde   = _ev[_fd]
                _fstocks  = _fde["stocks"]
                _fmstocks = _fde["m_stocks"]

                _fdate_cell = (
                    f'<td class="jmp-date">{_fdate.strftime("%-d %b %Y")}<br>'
                    f'<span style="font-size:9px;color:#64748b">{_fdate.strftime("%a")}</span></td>'
                )

                # Stock cells — exact same logic as main renderer (_scell)
                _FSTOCK_THRESHOLDS = {
                    "SanBarth":    (189_000, 228_000),
                    "JasmineS":  (189_000, 228_000),
                    "Westmore":  (130_000, 175_000),   # Unsafe >175k; Borderline 130k-175k; Safe <130k
                    "Ibom":      ( 70_000,  84_400),
                    "Starturn":  ( 45_500,  54_860),
                    "Duke":      ( 63_000,  76_000),
                    "PGM":       ( 19_600,  23_520),   # 70% / 84% of 28k cap: Safe <19.6k · Borderline 19.6-23.5k · Unsafe >23.5k
                }
                def _fscell(name):
                    v    = _fstocks.get(name, 0)
                    api  = _fde.get("stock_apis", {}).get(name, 0.0)
                    ovf  = _fde.get("overflows", {}).get(name, 0)
                    lo, hi = _FSTOCK_THRESHOLDS.get(name, (189_000, 228_000))
                    if v < lo:
                        bg, col, label = "#166534", "#bbf7d0", "Safe"
                    elif v <= hi:
                        bg, col, label = "#854d0e", "#fef08a", "Borderline"
                    else:
                        bg, col, label = "#991b1b", "#fecaca", "Unsafe"
                    _api_str = f'<br><span style="color:{col};font-size:8px;opacity:0.8">API {api:.2f}°</span>' if v > 0 else ""
                    _ovf_str = (f'<span style="color:#fca5a5;font-size:8px;font-weight:700"> (+{_kkk(ovf)})</span>'
                                if ovf > 0 else "")
                    return (
                        f'<td style="background:{bg};text-align:center">' +
                        f'<span style="color:{col};font-weight:700;font-size:10px">{_kkk(v)}</span>' +
                        _ovf_str +
                        f'<br><span style="color:{col};font-size:8px;opacity:0.85">{label}</span>' +
                        _api_str + '</td>'
                    )
                _fstock_cells = "".join(_fscell(n) for n in _storage_names) + _fscell("Ibom")

                # Loading cells — exact same logic as main renderer (_lcell)
                _factive_ov = st.session_state.get("jmp_storage_overrides", {})
                def _fload(name):
                    entries = [r for r in _fde["loadings"] if _parse_storage(r["Detail"]) == name]
                    if not entries:
                        # No load STARTS today — show ongoing multi-day load ETC if any
                        # (identical behaviour to the main renderer's _lcell).
                        _f_ongoing = _storage_ongoing_load(name, _fd)
                        return f'<td>{_f_ongoing or _idle()}</td>'
                    chips = []
                    for r in entries:
                        _ov_fentry = _factive_ov.get(r["Vessel"], {}).get(str(_fd))
                        _f_forced = (
                            _ov_fentry == name
                            or (isinstance(_ov_fentry, dict)
                                and _ov_fentry.get("storage") == name)
                        )
                        _f_label = (
                            f"{r['Vessel']} | {r['Time'][11:16]} | {_kkk(_parse_cargo(r['Detail']))}"
                            + (" 🔒" if _f_forced else "")
                        )
                        _f_vcode = str(r.get("VoyageCode", "")).strip()
                        _f_vcode_sub = (
                            f'<div style="margin-top:2px;margin-bottom:1px">'
                            f'<span style="background:rgba(0,0,0,0.40);border-radius:2px;'
                            f'padding:0 5px;font-size:9px;font-family:monospace;'
                            f'letter-spacing:0.03em">{_f_vcode}</span></div>'
                            if _f_vcode else ""
                        )
                        chips.append(_chip(r["Vessel"], _f_label) + _f_vcode_sub)
                    inner = "<br>".join(chips)
                    return f"<td>{inner}</td>"
                _fload_cells = "".join(_fload(n) for n in _storage_names)

                # Returning vessel
                _fret = _fde["returning"]
                _fret_vessel = "<br>".join(
                    _chip(r["Vessel"], f"{r['Vessel']} → {_ret_label(r['Detail'])}")
                    for r in _fret
                ) if _fret else _idle()
                _fret_eta = "<br>".join(r["Time"][11:16] for r in _fret) if _fret else _idle()

                def _feta_label(r):
                    """Full-run ETA label with cargo volume and +N day suffix when ETA crosses midnight."""
                    _eta_time = r["Time"][11:16]
                    _eta_date_str = r["Time"][:10]
                    _cargo = _parse_cargo(r.get("Detail", ""))
                    _cargo_str = f" | {_kkk(_cargo)}" if _cargo > 0 else ""
                    try:
                        import datetime as _dtimp2
                        _feta_date = _dtimp2.date.fromisoformat(_eta_date_str)
                        _fdate = _jmp_start + _dt.timedelta(days=_fd - 1)
                        _fdays_ahead = (_feta_date - _fdate).days
                        if _fdays_ahead == 1:
                            return f"{r['Vessel']} ({_eta_time} +1d){_cargo_str}"
                        elif _fdays_ahead >= 2:
                            return f"{r['Vessel']} ({_eta_time} +{_fdays_ahead}d){_cargo_str}"
                    except Exception:
                        pass
                    return f"{r['Vessel']} ({_eta_time}){_cargo_str}"

                _ffwy = _fde["fairway"]
                _fbia_arr = "<br>".join(
                    _chip(r["Vessel"], _feta_label(r)) + _vcode_badge(r)
                    for r in _ffwy
                ) if _ffwy else _idle()

                # Discharge cells
                def _fdcell(mother):
                    entries  = [r for r in _fde["discharge"] if _parse_mother(r["Detail"]) == mother]
                    aborts   = [r for r in _fde.get("aborts", []) if r.get("Mother") == mother]
                    ms       = _fmstocks.get(mother, 0)
                    mapi     = _fde.get("m_stock_apis", {}).get(mother, 0.0)
                    _api_bit = f' · API {mapi:.2f}°' if ms > 0 else ""
                    _at_exp  = mother in _fde.get("mothers_at_export", set())

                    def _fentry_chip(r):
                        vc = str(r.get("VoyageCode", "")).strip()
                        _is_mto = vc.endswith("A") and len(vc) > 1
                        return (
                            _chip(r["Vessel"],
                                  f"{r['Vessel']} | {r['Time'][11:16]} | {_kkk(_parse_cargo(r['Detail']))}")
                            + _vcode_badge(r, mto_transient=_is_mto)
                        )

                    def _fabort_chip(r):
                        return _chip(r["Vessel"],
                                     f"⚠️ {r['Vessel']} | {r['Time'][11:16]} | Cap. abort",
                                     bg="#d97706")

                    if _at_exp:
                        stk = f'<span style="font-size:9px;color:#bfdbfe">Stock: {_kkk(ms)}{_api_bit}</span>'
                        _exp_label = '<div style="font-size:9px;font-weight:700;color:#fff;letter-spacing:0.06em;margin-bottom:2px">⚓ EXPORT OPS</div>'
                        if not entries:
                            return (f'<td style="background:#1e3a5f;text-align:center;vertical-align:middle">'
                                    f'{_exp_label}{stk}</td>')
                        stk2 = f'<span style="font-size:9px;color:#bfdbfe;display:block;margin-bottom:2px">Stock: {_kkk(ms)}{_api_bit}</span>'
                        inner = "<br>".join(_fentry_chip(r) for r in entries)
                        return f'<td style="background:#1e3a5f">{_exp_label}{stk2}{inner}</td>'
                    stk = f'<span style="font-size:9px;color:#94a3b8">Stock: {_kkk(ms)}{_api_bit}</span>'
                    abort_html = ("<br>".join(_fabort_chip(r) for r in aborts) + "<br>") if aborts else ""
                    if not entries:
                        if aborts:
                            stk2 = f'<span style="font-size:9px;color:#64748b;display:block;margin-bottom:2px">Stock: {_kkk(ms)}{_api_bit}</span>'
                            return f'<td style="background:#fffbeb">{stk2}{abort_html}</td>'
                        _freason = _mother_idle_reason(mother, _fd)
                        return (
                            f'<td style="text-align:center;vertical-align:middle">'
                            f'{stk}<br>{_freason}</td>'
                        )
                    stk2 = f'<span style="font-size:9px;color:#64748b;display:block;margin-bottom:2px">Stock: {_kkk(ms)}{_api_bit}</span>'
                    inner = "<br>".join(_fentry_chip(r) for r in entries)
                    return f"<td>{stk2}{abort_html}{inner}</td>"
                _fdisch_cells = "".join(_fdcell(n) for n in _jmp_moms)

                # ── ZeeZee column for full-run / PDF render ───────────────────────────
                def _zz_fdcell():
                    _fzz_entries = [
                        r for r in _fde["discharge"]
                        if r.get("Vessel") == "ZeeZee"
                    ]
                    _fd_day_rows = log_df[log_df["Day"] == _fd]
                    _fzz_waiting = [
                        r for r in _fd_day_rows[
                            _fd_day_rows["Event"].isin([
                                "WAITING_BERTH_B", "WAITING_MOTHER_CAPACITY",
                                "BERTHING_START_B", "HOSE_CONNECTION_START_B",
                                "ZEEZEE_DEADLINE_OVERRIDE", "GREENEAGLE_CAPACITY_ABORT",
                            ])
                        ].to_dict("records")
                        if r.get("Vessel") == "ZeeZee"
                    ]
                    if not _fzz_entries and not _fzz_waiting:
                        _fzz_present = [
                            r for r in _fd_day_rows[
                                _fd_day_rows["Vessel"] == "ZeeZee"
                            ].to_dict("records")
                        ]
                        if _fzz_present:
                            _fstatus = _fzz_present[-1].get("Event", "")
                            _flbl = {
                                "VESSEL_JOINED":              "🚢 Arrived",
                                "WAITING_BERTH_B":            "⏳ Waiting",
                                "WAITING_MOTHER_CAPACITY":    "⏳ No cap.",
                                "ZEEZEE_DEADLINE_OVERRIDE":   "⚡ Priority",
                                "BERTHING_START_B":           "⚓ Berthing",
                                "HOSE_CONNECTION_START_B":    "🔗 Hose conn.",
                                "GREENEAGLE_CAPACITY_ABORT":  "⚠️ Cap. abort",
                            }.get(_fstatus, _fstatus)
                            return (f'<td style="background:#f0f4ff;text-align:center">'
                                    f'<span style="font-size:10px;color:#1e3a5f;font-weight:600">'
                                    f'{_flbl}</span></td>')
                        return f'<td style="text-align:center">{_idle()}</td>'
                    if _fzz_entries:
                        inner = "<br>".join(
                            _chip("ZeeZee",
                                  f"ZeeZee | {r['Time'][11:16]} | {_kkk(_parse_cargo(r['Detail']))}",
                                  bg="#1e3a5f")
                            for r in _fzz_entries
                        )
                        return f'<td style="background:#eef2ff">{inner}</td>'
                    _fzws = _fzz_waiting[-1]
                    _flbl2 = {
                        "WAITING_BERTH_B":            "⏳ Waiting",
                        "WAITING_MOTHER_CAPACITY":    "⏳ Awaiting cap.",
                        "ZEEZEE_DEADLINE_OVERRIDE":   "⚡ Priority",
                        "BERTHING_START_B":           "⚓ Berthing",
                        "HOSE_CONNECTION_START_B":    "🔗 Hose conn.",
                        "GREENEAGLE_CAPACITY_ABORT":  "⚠️ Cap. abort",
                    }.get(_fzws.get("Event", ""), _fzws.get("Event", ""))
                    return (f'<td style="background:#f0f4ff;text-align:center">'
                            f'<span style="font-size:10px;color:#1e3a5f;font-weight:600">'
                            f'{_flbl2}</span></td>')

                _fdisch_cells = "".join(_fdcell(n) for n in _jmp_moms)

                # ── MTO column for full-run / PDF render ──────────────────────────────
                def _mto_fdcell():
                    import re as _re_fd
                    _fd_day_rows = log_df[log_df["Day"] == _fd]
                    _fmto_nom = _fd_day_rows[
                        _fd_day_rows["Event"] == "MTO_TRANSIENT_NOMINATED"
                    ].to_dict("records")
                    _fmto_dis = _fd_day_rows[
                        _fd_day_rows["Event"] == "MTO_DISCHARGE_TO_TRANSIENT"
                    ].to_dict("records")
                    if not _fmto_nom and not _fmto_dis:
                        return f'<td style="text-align:center;background:#f0fdf4">{_idle()}</td>'

                    # Group nominations by transient vessel
                    _fseen_transients = {}
                    for _fnr in _fmto_nom:
                        _ftn = _fnr["Vessel"]
                        _fseen_transients.setdefault(_ftn, []).append(_fnr)

                    # Map discharger events back to transient by parsing Detail text
                    _fdis_by_transient = {}
                    for _fdr in _fmto_dis:
                        _fdt_match = _re_fd.search(r"to\s+(\w+):", _fdr.get("Detail", ""))
                        _ftrn_key = _fdt_match.group(1) if _fdt_match else None
                        if _ftrn_key and _ftrn_key in _fseen_transients:
                            _fdis_by_transient.setdefault(_ftrn_key, []).append(_fdr)
                        else:
                            _ffb = next(iter(_fseen_transients), None)
                            if _ffb:
                                _fdis_by_transient.setdefault(_ffb, []).append(_fdr)

                    _fblocks = []
                    for _ftidx, (_ftname, _ftnoms) in enumerate(_fseen_transients.items()):
                        _ftc  = VESSEL_COLORS.get(_ftname, "#334155")
                        _ftime = _ftnoms[0]["Time"][11:16] if _ftnoms else ""
                        _ftrn_vcode = str(_ftnoms[0].get("VoyageCode", "")).strip() if _ftnoms else ""
                        _ftrn_detail = str(_ftnoms[-1].get("Detail", "")) if _ftnoms else ""
                        _ftrn_onboard = ""
                        try:
                            _fob = _re_fd.search(r"on-board:\s*([\d,]+)\s*bbl", _ftrn_detail)
                            if _fob:
                                _ftrn_onboard = f"{int(_fob.group(1).replace(',','')) // 1000}k bbl"
                        except Exception:
                            pass
                        _fvcode_badge = (
                            f'<div style="background:rgba(0,0,0,0.3);border-radius:2px;'
                            f'padding:0 5px;font-size:9px;font-family:monospace;color:#fff;'
                            f'display:inline-block;margin:1px 0">{_ftrn_vcode}</div>'
                        ) if _ftrn_vcode else ""
                        _fonboard_badge = (
                            f'<div style="font-size:8px;color:#166534;margin:1px 0">📦 {_ftrn_onboard}</div>'
                        ) if _ftrn_onboard else ""
                        _fdis_rows = _fdis_by_transient.get(_ftname, [])
                        _fdis_chips = "".join(
                            f'<div style="margin:1px 0">'
                            f'<span style="background:{VESSEL_COLORS.get(r["Vessel"],"#334155")};color:#fff;'
                            f'border-radius:3px;padding:1px 5px;font-size:9px;font-weight:700;'
                            f'display:inline-block">{r["Vessel"]}</span>'
                            + (f'<span style="display:block;background:rgba(0,0,0,0.3);border-radius:2px;'
                               f'padding:0 4px;font-size:8px;font-family:monospace;color:#fff;margin-top:1px">'
                               f'{str(r.get("VoyageCode","")).strip()}</span>'
                               if str(r.get("VoyageCode","")).strip() else "")
                            + f'</div>'
                            for r in _fdis_rows
                        ) if _fdis_rows else ""
                        _fsep = '<hr style="border:none;border-top:1px solid #86efac;margin:4px 0">' if _ftidx > 0 else ""
                        _fblocks.append(
                            f'{_fsep}'
                            f'<div style="font-size:9px;color:#166534;font-weight:700;margin-bottom:2px">🔄 MTO {_ftime}</div>'
                            f'<span style="background:{_ftc};color:#fff;border-radius:3px;'
                            f'padding:1px 6px;font-size:10px;font-weight:700">{_ftname}</span>'
                            f'{_fvcode_badge}{_fonboard_badge}'
                            + (f'<div style="font-size:9px;color:#64748b;margin:2px 0">receives from</div>'
                               f'{_fdis_chips}' if _fdis_chips else "")
                        )

                    return (
                        f'<td style="background:#ecfdf5;text-align:center">'
                        + "".join(_fblocks)
                        + f'</td>'
                    )

                _full_rows_html.append(
                    f"<tr>{_fdate_cell}{_fstock_cells}{_fload_cells}"
                    f"<td>{_fret_vessel}</td><td style='text-align:center'>{_fret_eta}</td>"
                    f"<td>{_fbia_arr}</td>"
                    f"{_fdisch_cells}{_zz_fdcell()}{_mto_fdcell()}</tr>"
                )

            # Re-use same header as main table
            _full_table_html = (
                '<div class="jmp-wrap"><table class="jmp-table">'
                '<tr>'
                '<th rowspan="2" class="sec-hdr-cell">Date</th>'
                '<th colspan="7" class="sec-hdr-cell">Opening Stock (bbl)</th>'
                '<th colspan="6" class="sec-hdr-cell">Loading Plan</th>'
                '<th colspan="2" class="sec-hdr-cell">Returning to Load</th>'
                '<th colspan="1" class="sec-hdr-cell">Arriving BIA</th>'
                f'<th colspan="{_disch_colspan}" class="sec-hdr-cell">Discharging Plan</th>'
                '<th style="background:#1a3a2a" class="sec-hdr-cell">MTO</th>'
                '</tr>'
                '<tr>'
                '<th>SanBarth</th><th>JasmineS</th><th>Westmore</th><th>Duke</th><th>Starturn</th><th>PGM</th><th>Ibom</th>'
                '<th>SanBarth</th><th>JasmineS</th><th>Westmore</th><th>Duke</th><th>Starturn</th><th>PGM</th>'
                '<th>Vessel &rarr; Storage</th><th>ETA</th>'
                '<th>Vessel (ETA)</th>'
                f'{_moms_hdr_html}'
                '<th style="background:#1e3a5f">ZeeZee</th>'
                '<th style="background:#1a3a2a">MTO<br><span style="font-size:9px;font-weight:400">Transient ↔ Discharger</span></th>'
                '</tr>'
                + "".join(_full_rows_html)
                + '</table></div>'
            )

            _css_rules = chr(10).join([
                ".jmp-wrap{overflow-x:auto;padding:4px 0}",
                ".jmp-table{border-collapse:collapse;min-width:100%;font-size:11px}",
                ".jmp-table th{background:#1a2744;color:#fff;padding:5px 8px;text-align:center;font-size:10px;font-weight:700;letter-spacing:.04em;border:1px solid #344d80;white-space:nowrap}",
                ".jmp-table th.sec-hdr-cell{background:#0f1a35}",
                ".jmp-table td{padding:5px 7px;border:1px solid #e2e8f0;vertical-align:top;white-space:nowrap}",
                ".jmp-table tr:nth-child(even) td{background:#f8f9fb}",
                ".jmp-table tr:nth-child(odd) td{background:#ffffff}",
                ".jmp-date{font-weight:700;color:#1a2744;font-size:11px}",
                ".jmp-entry{display:inline-block;border-radius:4px;padding:2px 6px;margin:1px 0;font-size:10px;font-weight:600;color:#fff;white-space:nowrap;line-height:1.5}",
                ".jmp-idle{color:#94a3b8;font-size:10px;font-style:italic}",
                ".jmp-bia-entry{display:inline-block;border-radius:4px;padding:2px 6px;margin:1px 0;font-size:10px;font-weight:600;white-space:nowrap;line-height:1.5}",
                "@media print{body{margin:8px}@page{size:landscape;margin:10mm}}",
            ])
            _vessel_legend = "".join(
                f'<span style="background:{c};color:#fff;border-radius:4px;padding:2px 8px;font-size:10px;font-weight:700">{v}</span>'
                for v, c in VESSEL_COLORS.items()
            )

            # Page-scoped HTML (current page — for on-screen reference)
            _full_html = f"""<!DOCTYPE html>
        <html>
        <head>
        <meta charset="utf-8">
        <style>
          body{{margin:16px;background:#fff;font-family:'Segoe UI',system-ui,Arial,sans-serif}}
          h2{{color:#1a2744;font-size:15px;margin:0 0 10px;font-weight:800;letter-spacing:.03em}}
          {_css_rules}
        </style>
        </head>
        <body>
        <h2>🗺️ {_jmp_title_str}</h2>
        {_table_html}
        <div style="margin:8px 0 4px;display:flex;flex-wrap:wrap;gap:6px">
        {_vessel_legend}
        </div>
        <div style="font-size:9px;color:#94a3b8;margin-top:8px">Generated: {_dt.datetime.now().strftime("%Y-%m-%d %H:%M")} | Tanker Operations v5</div>
        </body></html>"""

            # Full-run HTML (all days — for PDF download)
            # Only build if cache miss; otherwise reuse stored value.
            if not _full_run_html_ready:
                _build_full_now = True
            else:
                _build_full_now = False
                _full_run_html = _cached_full_html
            if _build_full_now:
                _full_run_html_inner = f"""<!DOCTYPE html>
        <html>
        <head>
        <meta charset="utf-8">
        <style>
          body{{margin:16px;background:#fff;font-family:'Segoe UI',system-ui,Arial,sans-serif}}
          h2{{color:#1a2744;font-size:15px;margin:0 0 10px;font-weight:800;letter-spacing:.03em}}
          {_css_rules}
        </style>
        </head>
        <body>
        <h2>🗺️ {_full_title_str}</h2>
        {_full_table_html}
        <div style="margin:8px 0 4px;display:flex;flex-wrap:wrap;gap:6px">
        {_vessel_legend}
        </div>
        <div style="font-size:9px;color:#94a3b8;margin-top:8px">Generated: {_dt.datetime.now().strftime("%Y-%m-%d %H:%M")} | Tanker Operations v5</div>
        </body></html>"""
                _full_run_html = _full_run_html_inner
                # Store in session_state so subsequent widget re-renders skip the build
                st.session_state["_jmp_full_html"] = _full_run_html
                st.session_state["_jmp_full_key"]  = _jmp_rebuild_key

            _dc1, _dc2, _dc3 = st.columns([2,2,2])
            with _dc1:
                # Full-run HTML download — all days, landscape print-ready
                st.download_button(
                    f"📥 Download Full JMP as HTML ({_total_sim_days} days → Print → Save as PDF)",
                    data=_full_run_html.encode("utf-8"),
                    file_name=f"journey_plan_full_{_jmp_start.isoformat()}.html",
                    mime="text/html",
                    help=(
                        f"Downloads the complete {_total_sim_days}-day Journey Management Plan as a "
                        "self-contained HTML file. Open in Chrome/Edge → Ctrl+P (or Cmd+P) → "
                        "Change destination to 'Save as PDF' → Layout: Landscape → Save."
                    )
                )
            with _dc2:
                # CSV export of raw plan data
                _jmp_rows = []
                for _day in range(1, _total_sim_days + 1):
                    _date = _jmp_start + _dt.timedelta(days=_day - 1)
                    _de2 = _ev[_day]
                    for r in _de2["loadings"]:
                        _jmp_rows.append({"Date": _date, "Section": "Loading", "Vessel": r["Vessel"],
                            "Location": _parse_storage(r["Detail"]), "Time": r["Time"][11:16],
                            "Cargo_bbl": _parse_cargo(r["Detail"]), "Mother": "",
                            "VoyageCode": r.get("VoyageCode",""), "Detail": r.get("Detail","")})
                    for r in _de2["discharge"]:
                        _jmp_rows.append({"Date": _date, "Section": "Discharge", "Vessel": r["Vessel"],
                            "Location": "", "Time": r["Time"][11:16],
                            "Cargo_bbl": _parse_cargo(r["Detail"]), "Mother": _parse_mother(r["Detail"]),
                            "VoyageCode": r.get("VoyageCode",""), "Detail": r.get("Detail","")})
                    for r in _de2["returning"]:
                        _jmp_rows.append({"Date": _date, "Section": "Returning", "Vessel": r["Vessel"],
                            "Location": r["Detail"].split("Arrived ")[-1].split(" —")[0],
                            "Time": r["Time"][11:16], "Cargo_bbl": 0, "Mother": "",
                            "VoyageCode": r.get("VoyageCode",""), "Detail": r.get("Detail","")})
                    for r in _de2["fairway"]:
                        _jmp_rows.append({"Date": _date, "Section": "BIA Arrival", "Vessel": r["Vessel"],
                            "Location": "Fairway", "Time": r["Time"][11:16], "Cargo_bbl": 0, "Mother": "",
                            "VoyageCode": r.get("VoyageCode",""), "Detail": r.get("Detail","")})
                    # Add MTO events for completeness
                    _day_log = log_df[log_df["Day"] == _day] if not log_df.empty else pd.DataFrame()
                    if not _day_log.empty:
                        _mto_evts = _day_log[_day_log["Event"].str.contains("MTO|EXPORT|WAITING_BERTH_B|CAST_OFF", na=False)]
                        for _, r in _mto_evts.iterrows():
                            _jmp_rows.append({"Date": _date, "Section": r["Event"], "Vessel": r["Vessel"],
                                "Location": "", "Time": str(r["Time"])[11:16],
                                "Cargo_bbl": "", "Mother": r.get("Mother",""),
                                "VoyageCode": r.get("VoyageCode",""), "Detail": str(r.get("Detail",""))[:120]})
                _jmp_csv = pd.DataFrame(_jmp_rows).to_csv(index=False).encode()
                st.download_button(
                    f"📥 Download Complete Activity Log CSV ({_total_sim_days} days)",
                    data=_jmp_csv,
                    file_name=f"journey_plan_full_{_jmp_start.isoformat()}.csv",
                    mime="text/csv"
                )
            with _dc3:
                # ── Daily stock snapshot: download & re-use as input ──────────
                # Build a tidy CSV of per-day opening stocks, APIs and overflow
                # for all storages and mother vessels.  The same file can be
                # re-uploaded below to seed the next run's startup volumes.
                _snap_rows = []
                for _sd in range(1, _total_sim_days + 1):
                    _sdate = _jmp_start + _dt.timedelta(days=_sd - 1)
                    _sde   = _ev[_sd]
                    _srow  = {"Date": _sdate.isoformat(), "Day": _sd}
                    for _sn in _storage_names:
                        _srow[f"{_sn}_bbl"]     = _sde["stocks"].get(_sn, 0)
                        _srow[f"{_sn}_api"]     = _sde["stock_apis"].get(_sn, 0.0)
                        _srow[f"{_sn}_overflow"] = _sde["overflows"].get(_sn, 0)
                    for _mn in _mother_names:
                        _srow[f"{_mn}_bbl"]     = _sde["m_stocks"].get(_mn, 0)
                        _srow[f"{_mn}_api"]     = _sde["m_stock_apis"].get(_mn, 0.0)
                    _snap_rows.append(_srow)
                _snap_csv_bytes = pd.DataFrame(_snap_rows).to_csv(index=False).encode()

                st.download_button(
                    f"📷 Download Daily Stock Snapshot (CSV · {_total_sim_days} days)",
                    data=_snap_csv_bytes,
                    file_name=f"daily_stock_snapshot_{_jmp_start.isoformat()}.csv",
                    mime="text/csv",
                    help=(
                        "Downloads per-day opening stock (bbl), API gravity and overflow "
                        "for every storage and mother vessel. "
                        "Re-upload this file below to seed a future run with these volumes — "
                        "pick the target date row and the startup volumes will be applied automatically."
                    ),
                    use_container_width=True,
                )

                # ── Re-upload snapshot to seed next run ───────────────────────
                _snap_upload = st.file_uploader(
                    "📂 Re-use snapshot as startup data",
                    type=["csv"],
                    key="jmp_snap_upload",
                    help=(
                        "Upload a previously downloaded Daily Stock Snapshot CSV. "
                        "Select which day's row to use as the startup volumes for the next simulation run."
                    ),
                )
                if _snap_upload is not None:
                    try:
                        _snap_df = pd.read_csv(_snap_upload)
                        _snap_dates = list(_snap_df["Date"].astype(str))
                        _snap_sel   = st.selectbox(
                            "Startup date to use",
                            options=range(len(_snap_dates)),
                            format_func=lambda i: f"Day {_snap_df.iloc[i]['Day']} — {_snap_dates[i]}",
                            key="jmp_snap_sel",
                        )
                        if st.button("✅ Apply snapshot volumes", key="jmp_snap_apply",
                                     use_container_width=True):
                            _sr = _snap_df.iloc[_snap_sel]
                            # Apply storage volumes to session state
                            for _sn in _storage_names:
                                _bbl_col = f"{_sn}_bbl"
                                if _bbl_col in _sr.index:
                                    st.session_state[f"sv_{_sn}"] = int(_sr[_bbl_col])
                            # Apply mother vessel volumes
                            for _mn in _mother_names:
                                _bbl_col = f"{_mn}_bbl"
                                _api_col = f"{_mn}_api"
                                if _bbl_col in _sr.index:
                                    st.session_state[f"mv_{_mn}"] = int(_sr[_bbl_col])
                                if _api_col in _sr.index:
                                    st.session_state[f"mapi_{_mn}"] = float(_sr[_api_col])
                            st.success(
                                f"✅ Startup volumes applied from Day {int(_sr['Day'])} "
                                f"({_snap_dates[int(_snap_sel)]}). "
                                "Re-run the simulation to use these values.",
                                icon="✅",
                            )
                    except Exception as _snap_err:
                        st.error(f"❌ Could not parse snapshot: {_snap_err}")




        with _tide_tab:
            sec("🌊 Tidal Prediction — Declared Daylight Tides")



            if _tide_bytes is None:
                st.info("ℹ️ No tidal file uploaded — upload a tidal CSV in the sidebar to see declared daylight tides and activate the breakwater constraint.")
            else:
                # Parse tide bytes directly — no sim module dependency
                try:
                    _tide_min_m = 1.6
                    _DSTART     = 6
                    _DEND       = 18
                    _SIM_HOUR_OFFSET = 8.0
                    _sim_epoch  = _dt.datetime(_today_date.year, _today_date.month, _today_date.day, 8, 0)  # t=0 = 08:00
                    _sim_days_t = params.get("sim_days", 14)

                    def _parse_tide_bytes(raw_bytes, epoch_dt):
                        """Parse raw tide CSV bytes → {abs_hour: height} dict."""
                        text   = raw_bytes.decode("utf-8-sig", errors="replace")
                        sample = text[:2048]
                        delim  = "," if sample.count(",") >= sample.count(";") else ";"
                        reader = csv.DictReader(io.StringIO(text), delimiter=delim)
                        rows   = [{k.strip().lower().replace(" ","_"): v.strip()
                                   for k, v in r.items()} for r in reader]
                        if not rows:
                            return {}
                        date_col   = next((k for k in rows[0] if "date" in k), None)
                        time_col   = next((k for k in rows[0] if "time" in k), None)
                        height_col = next((k for k in rows[0]
                                           if any(x in k for x in ("height","tide","level","_m"))), None)
                        if not (date_col and time_col and height_col):
                            return {}
                        raw_pts = {}
                        for row in rows:
                            try:
                                ds = row[date_col]; ts = row[time_col]; hs = row[height_col]
                                if not hs: continue
                                if "/" in ds:
                                    p = ds.split("/")
                                    d = (_dt.datetime(int(p[2]),int(p[1]),int(p[0]))
                                         if len(p[2])==4
                                         else _dt.datetime(int(p[0]),int(p[1]),int(p[2])))
                                else:
                                    d = _dt.datetime.fromisoformat(ds.split("T")[0])
                                t_parts = ts.split(":")
                                hh, mm = int(t_parts[0]), int(t_parts[1][:2])
                                dt  = d.replace(hour=hh, minute=mm)
                                ht  = float(re.sub(r"[^0-9.\-]", "", hs))
                                diff = (dt - epoch_dt).total_seconds() / 3600.0
                                raw_pts[round(diff * 2) / 2] = ht
                            except Exception:
                                continue
                        if not raw_pts:
                            return {}
                        # Linear interpolation onto 0.5 h grid
                        sk = sorted(raw_pts)
                        full = {}
                        _slot_start = int(sk[0] * 2)
                        _slot_end = int(sk[-1] * 2) + 2
                        for slot in [x * 0.5 for x in range(_slot_start, _slot_end)]:
                            if slot in raw_pts:
                                full[slot] = raw_pts[slot]
                            else:
                                lo = max((k for k in sk if k <= slot), default=None)
                                hi = min((k for k in sk if k >= slot), default=None)
                                if lo is not None and hi is not None and hi != lo:
                                    f = (slot - lo) / (hi - lo)
                                    full[slot] = raw_pts[lo] + f * (raw_pts[hi] - raw_pts[lo])
                                elif lo is not None:
                                    full[slot] = raw_pts[lo]
                                elif hi is not None:
                                    full[slot] = raw_pts[hi]
                        return full

                    _tide_tbl = _parse_tide_bytes(_tide_bytes, _sim_epoch)
                    _tide_ok  = bool(_tide_tbl)

                    if not _tide_ok:
                        st.warning("⚠️ Tidal file uploaded but could not be parsed. Check column names: Date (DD/MM/YYYY) · Time (HH:MM) · Tide_Height_m")
                    else:
                        _sim_days_t = params.get("sim_days", 14)

                        # ── Build daily summary ──────────────────────────────────────
                        _daily_rows = []
                        for _day in range(_sim_days_t):
                            _date_d    = _today_date + _dt.timedelta(days=_day)
                            _day_start = _day * 24.0 - _SIM_HOUR_OFFSET
                            _day_end   = _day_start + 24.0

                            # All half-hour slots for this calendar day
                            _slots = {h: v for h, v in _tide_tbl.items()
                                      if _day_start <= h < _day_end}

                            if not _slots:
                                _daily_rows.append({
                                    "Date": _date_d.strftime("%a %d %b"),
                                    "High Tide": "—", "High Time": "—",
                                    "Low Tide": "—", "Low Time": "—",
                                    "Declared Daylight Tides (>1.6m)": "No tidal data for this day",
                                    "Declared Tides": 0,
                                })
                                continue

                            # High and low tide
                            _peak_h    = max(_slots, key=_slots.get)
                            _trough_h  = min(_slots, key=_slots.get)
                            _peak_dt   = (_sim_epoch + _dt.timedelta(hours=_peak_h)).strftime("%H:%M")
                            _trough_dt = (_sim_epoch + _dt.timedelta(hours=_trough_h)).strftime("%H:%M")

                            _declared = []
                            for _h in sorted(_slots):
                                _hod = (_h + _SIM_HOUR_OFFSET) % 24
                                _hgt = _slots[_h]
                                if _DSTART <= _hod < _DEND and _hgt > _tide_min_m:
                                    _tm = (_sim_epoch + _dt.timedelta(hours=_h)).strftime("%H:%M")
                                    _declared.append(f"{_tm} ({_hgt:.2f} m)")

                            _declared_str = "  ·  ".join(_declared) if _declared else "❌ No declared daylight tide >1.6m"

                            _daily_rows.append({
                                "Date":   _date_d.strftime("%a %d %b"),
                                "High Tide": f"{_slots[_peak_h]:.2f} m",
                                "High Time": _peak_dt,
                                "Low Tide":  f"{_slots[_trough_h]:.2f} m",
                                "Low Time":  _trough_dt,
                                "Declared Daylight Tides (>1.6m)": _declared_str,
                                "Declared Tides": len(_declared),
                            })

                        _tide_df = pd.DataFrame(_daily_rows)

                        # ── Metric strip ────────────────────────────────────────────
                        _no_cross_days = (_tide_df["Declared Tides"] == 0).sum()
                        _avg_declared  = _tide_df["Declared Tides"].mean()
                        _total_declared = _tide_df["Declared Tides"].sum()
                        _tc1, _tc2, _tc3, _tc4 = st.columns(4)
                        with _tc1: kpi("Sim Days Covered", f'{len(_daily_rows)}')
                        with _tc2: kpi("Avg Declared Tides/Day",  f'{_avg_declared:.1f}')
                        with _tc3: kpi("Total Declared Tides",    f'{int(_total_declared)}')
                        with _tc4: kpi("Restricted Days",
                                       f'{_no_cross_days}',
                                       sub="days with no declared daylight tide" if _no_cross_days else "✅ all days have declared tides")

                        # ── Threshold reminder ───────────────────────────────────────
                        st.markdown(
                            f'<div style="background:#f0f9ff;border:1px solid #3b82f6;border-radius:8px;'
                            f'padding:10px 16px;margin:8px 0 12px;font-size:12px;color:#1e40af">'
                            f'🌊 <b>Breakwater crossing rule:</b> vessels may only depart when tide is '
                            f'<b>>{_tide_min_m:.1f} m</b> '
                            f'AND within daylight (<b>{_DSTART:02d}:00–{_DEND:02d}:00</b>). '
                            f'Only daylight tide points above threshold are declared. '
                            f'The simulation enforces this for all outbound departures from SanBarth '
                            f'and return sailings from BIA.</div>',
                            unsafe_allow_html=True)

                        # ── Daily table ──────────────────────────────────────────────
                        def _tide_row_color(row):
                            # Use the declared tide count stored in a parallel list by index
                            idx = row.name
                            w = _daily_rows[idx]["Declared Tides"] if idx < len(_daily_rows) else 1
                            if w == 0:
                                return ['background-color:#fef2f2;color:#991b1b'] * len(row)
                            elif w == 1:
                                return ['background-color:#fef9c3;color:#713f12'] * len(row)
                            else:
                                return ['background-color:#f0fdf4;color:#14532d'] * len(row)

                        _tide_display_df = _tide_df.drop(columns=["Declared Tides"]).reset_index(drop=True)
                        _tide_display = _tide_display_df.style.apply(_tide_row_color, axis=1)
                        st.dataframe(_tide_display, hide_index=True, width="stretch")

                        # ── Intraday chart for selected day ──────────────────────────
                        st.markdown("**📈 Intraday tidal profile — select a day to inspect:**")
                        _day_opts   = [r["Date"] for r in _daily_rows]
                        _sel_day_lbl = st.selectbox("Day", _day_opts, key="tide_day_sel",
                                                    label_visibility="collapsed")
                        _sel_day_idx = _day_opts.index(_sel_day_lbl)
                        _sd_start    = _sel_day_idx * 24.0 - _SIM_HOUR_OFFSET
                        _sd_end      = _sd_start + 24.0
                        _sd_slots    = {h: v for h, v in _tide_tbl.items()
                                        if _sd_start <= h < _sd_end}

                        if _sd_slots:
                            import plotly.graph_objects as _pgo
                            _sd_hours  = sorted(_sd_slots)
                            _sd_hods   = [((h + _SIM_HOUR_OFFSET) % 24) for h in _sd_hours]
                            _sd_heights = [_sd_slots[h] for h in _sd_hours]
                            _sd_labels  = [f"{int(h):02d}:{int((h%1)*60):02d}" for h in _sd_hods]

                            _fig_t = _pgo.Figure()
                            # Tide curve
                            _fig_t.add_trace(_pgo.Scatter(
                                x=_sd_hods, y=_sd_heights,
                                mode="lines", name="Tide height (m)",
                                line=dict(color="#3b82f6", width=2.5),
                                hovertemplate="%{text}: %{y:.2f} m<extra></extra>",
                                text=_sd_labels,
                            ))
                            # Threshold line
                            _fig_t.add_hline(
                                y=_tide_min_m, line_dash="dash",
                                line_color="#ef4444", line_width=1.5,
                                annotation_text=f"Min crossing {_tide_min_m:.1f} m",
                                annotation_position="bottom right",
                                annotation_font_color="#ef4444",
                            )
                            # Daylight shading
                            _fig_t.add_vrect(x0=_DSTART, x1=_DEND,
                                fillcolor="rgba(253,224,71,0.12)", line_width=0,
                                annotation_text="Daylight window", annotation_position="top left",
                                annotation_font_color="#b45309", annotation_font_size=10)
                            # Valid crossing zones (tide > threshold AND daylight) — green fill
                            _in_zone = False
                            _zone_x0 = None
                            for _i, (_hod, _hgt) in enumerate(zip(_sd_hods, _sd_heights)):
                                _ok = _hgt > _tide_min_m and _DSTART <= _hod < _DEND
                                if _ok and not _in_zone:
                                    _zone_x0 = _hod; _in_zone = True
                                elif not _ok and _in_zone:
                                    _fig_t.add_vrect(x0=_zone_x0, x1=_hod,
                                        fillcolor="rgba(34,197,94,0.18)", line_width=0)
                                    _in_zone = False
                            if _in_zone:
                                _fig_t.add_vrect(x0=_zone_x0, x1=_DEND,
                                    fillcolor="rgba(34,197,94,0.18)", line_width=0)

                            _fig_t.update_layout(
                                height=280, margin=dict(l=40, r=20, t=30, b=40),
                                paper_bgcolor="#0f1a35", plot_bgcolor="#0f1a35",
                                font=dict(color="#cbd5e1", size=11),
                                xaxis=dict(title="Hour of day", tickmode="linear",
                                           tick0=0, dtick=2, gridcolor="#1e2d4a",
                                           range=[0, 24]),
                                yaxis=dict(title="Height (m)", gridcolor="#1e2d4a"),
                                legend=dict(bgcolor="rgba(0,0,0,0)", font_size=10),
                                showlegend=True,
                            )
                            st.plotly_chart(_fig_t, width="stretch", config={"displayModeBar": False})
                            # Summary for selected day
                            _sel_row = _daily_rows[_sel_day_idx]
                            _cwin_txt = _sel_row["Declared Daylight Tides (>1.6m)"]
                            st.caption(
                                f"**{_sel_day_lbl}** — High: {_sel_row['High Tide']} at {_sel_row['High Time']} · "
                                f"Low: {_sel_row['Low Tide']} at {_sel_row['Low Time']} · "
                                f"Declared daylight tides: {_cwin_txt}"
                            )
                except Exception as _e_tide:
                    st.warning(f"⚠️ Could not render tidal prediction: {_e_tide}")

            # ==========================================================================
            # ── SECTION 0: OPTIMIZATION ENGINE ───────────────────────────────────────
            # ==========================================================================




    sec("🧠 Optimization Engine — Heuristic Parameter Search")

    if not run_opt:
        # Clear any previously persisted best params so stale optimizer results
        # don't influence the simulation when the optimizer is switched off.
        st.session_state.pop("_best_opt_params", None)
        st.session_state.pop("selected_opt_scenario", None)
        st.markdown(
            '<div class="alert-info">ℹ️ Optimizer is off. '
            'Enable <b>Run Optimizer</b> in the sidebar to sweep 240 parameter '
            'combinations and auto-select the best configuration.</div>',
            unsafe_allow_html=True)
    else:

        base_params_for_opt = {k: int(v) if isinstance(v, float) and v == int(v) else v
                               for k, v in params.items()}
        if _tide_bytes is not None:
            base_params_for_opt["_tide_csv_bytes_hex"] = binascii.hexlify(_tide_bytes).decode()
        base_params_for_opt["_sim_start_date"] = _start_iso_str if _start_iso_str else ""
        opt_cache_key = json.dumps(base_params_for_opt, sort_keys=True)

        # ── Override-state advisory ──────────────────────────────────────────
        _ddo_active = bool(st.session_state.get("daughter_discharge_overrides", {}))
        _sdo_active = bool(st.session_state.get("jmp_storage_overrides", {}))
        _exp_active = bool(st.session_state.get("export_operations", {}))
        if _ddo_active or _sdo_active or _exp_active:
            _ovr_items = []
            if _ddo_active: _ovr_items.append("Daughter Discharge Overrides")
            if _sdo_active: _ovr_items.append("JMP Storage Overrides")
            if _exp_active: _ovr_items.append("Export Operations (mother away)")
            st.markdown(
                '<div class="alert-info" style="margin-bottom:10px">'
                '🔍 <b>Optimizer runs on a clean baseline</b> — your active operator '
                f'overrides (<b>{", ".join(_ovr_items)}</b>) are intentionally excluded from '
                'the parameter sweep. The optimizer finds the best <em>system parameters</em> '
                'independent of day-specific assignments. Apply the recommended parameters '
                'then re-apply your overrides in the normal panels above.'
                '</div>',
                unsafe_allow_html=True)

        with st.spinner("🔍 Running optimization sweep (up to 90 s)…"):
            best_json, tbl_json = run_optimizer(opt_cache_key)

        best_r  = json.loads(best_json)
        opt_tbl = pd.read_json(io.StringIO(tbl_json), orient="records")
        best_sc = best_r["score"]
        best_pr = best_r["params"]
        _scen_eval   = best_r.get("scenarios_evaluated", len(opt_tbl))
        _grid_total  = best_r.get("grid_total", 216)
        _budget_hit  = best_r.get("budget_exhausted", False)
        _wall_s      = best_r.get("wall_seconds", 0)
        # Persist best params so the run_sim() call (earlier in this rerun) can use
        # them on the NEXT Streamlit rerun without needing best_pr to be defined first.
        st.session_state["_best_opt_params"] = best_pr

        # ── Non-negotiables banner ───────────────────────────────────────────
        st.markdown(
            '<div style=\"background:#fff8f8;border:1px solid #ef4444;border-radius:8px;padding:12px 16px;margin-bottom:12px;\"><span style=\"color:#dc2626;font-weight:600;font-size:13px;\">🔒 Non-Negotiables</span><span style=\"color:#64748b;font-size:12px;margin-left:10px\">— locked constraints never varied by the optimizer</span><div style=\"margin-top:8px;display:flex;flex-wrap:wrap;gap:8px;\"><span style=\"background:#fee2e2;border:1px solid #ef4444;border-radius:5px;padding:4px 10px;color:#dc2626;font-size:12px;\">🌅 Daylight operations: 06:00 – 18:00</span><span style=\"background:#fee2e2;border:1px solid #ef4444;border-radius:5px;padding:4px 10px;color:#dc2626;font-size:12px;\">⚓ Berthing window: 06:00 – 18:00</span><span style=\"background:#fee2e2;border:1px solid #ef4444;border-radius:5px;padding:4px 10px;color:#dc2626;font-size:12px;\">🔗 Cast-off window: 06:00 – 18:00</span><span style=\"background:#fee2e2;border:1px solid #ef4444;border-radius:5px;padding:4px 10px;color:#dc2626;font-size:12px;\">🚢 Export departure: no earlier than 06:00</span><span style=\"background:#fee2e2;border:1px solid #ef4444;border-radius:5px;padding:4px 10px;color:#dc2626;font-size:12px;\">🕔 Day 2+ daily storage reassessment: 05:00</span></div></div>',
            unsafe_allow_html=True)

        # ── Tidal status widget ──────────────────────────────────────────
        _tide_html_color = "#3b82f6" if _tide_bytes else "#94a3b8"
        _tide_html_text  = (
            "🌊 Tidal Constraint Active — breakwater tide >1.6 m (daylight 06:00-18:00) · 2h from SanBarth · 4h from BIA"
            if _tide_bytes else
            "⚠️ No tidal file uploaded — daylight-only rules applied in this sweep"
        )
        st.markdown(
            f'<div style="background:#f8faff;border:1px solid {_tide_html_color};'
            f'border-radius:8px;padding:10px 16px;margin-bottom:12px;">'
            f'<span style="color:#79c0ff;font-size:13px;">{_tide_html_text}</span></div>',
            unsafe_allow_html=True)

        # ── Best configuration banner ─────────────────────────────────────────
        _budget_note = (
            f"⏱ Budget reached after {_wall_s:.0f}s — {_scen_eval}/{_grid_total} scenarios"
            if _budget_hit else
            f"{_scen_eval}/{_grid_total} scenarios in {_wall_s:.0f}s"
        )
        st.markdown(
            f'<div class="opt-best">'
            f'<div style="display:flex;align-items:center;margin-bottom:12px">'
            f'  <div class="opt-score">{best_sc["composite"]:.1f}</div>'
            f'  <div style="margin-left:14px">'
            f'    <span class="opt-badge">OPTIMAL CONFIGURATION</span><br>'
            f'    <span style="color:#8b949e;font-size:12px">composite score / 100 · '
            f'    ranked #1 · {_budget_note}</span>'
            f'  </div>'
            f'</div>'
            f'<div style="margin-bottom:10px;color:#e6edf3;font-size:13px">'
            f'  <b>Selected parameters:</b>'
            f'</div>'
            f'<span class="opt-param">dead-stock factor: '
            f'  <b>×{best_pr["dead_stock_factor"]:.2f}</b></span>'
            f'<span class="opt-param">Ibom trigger: '
            f'  <b>{best_pr["ibom_trigger_bbl"]:,} bbl</b></span>'
            f'<span class="opt-param">export window start: '
            f'  <b>{best_pr["export_sail_window_start"]:02d}:00</b></span>'
            f'<span class="opt-param">berthing window: '
            f'  <b>{best_pr["berthing_start"]:02d}:00 – {best_pr["berthing_end"]:02d}:00</b></span>'
            f'</div>',
            unsafe_allow_html=True)

        # ── Sub-score breakdown ───────────────────────────────────────────────
        sub_cols = st.columns(5)
        SCORE_DIMS = [
            ("Stock Drawdown", best_sc["throughput_score"], "#56d364", "28% weight"),
            ("Fleet Utilisation", best_sc["idle_score"],    "#f1c40f", "15% weight"),
            ("Storage Safety", best_sc["overflow_score"],   "#79c0ff", "42% weight"),
            ("Fair Allocation", best_sc["fairness_score"],  "#c084fc", "12% weight"),
            ("Turnaround", best_sc["turnaround_score"],     "#fb923c", "1% weight"),
        ]
        for col, (label, val, color, weight) in zip(sub_cols, SCORE_DIMS):
            with col:
                bar_pct = max(2, int(val))
                st.markdown(
                    f'<div class="kpi-card">'
                    f'  <div class="kpi-label">{label}<br>'
                    f'    <span style="color:#484f58">{weight}</span></div>'
                    f'  <div class="kpi-value" style="color:{color}">{val:.1f}</div>'
                    f'  <div class="score-bar-wrap">'
                    f'    <div class="score-bar" '
                    f'         style="width:{bar_pct}%;background:{color}"></div>'
                    f'  </div>'
                    f'</div>',
                    unsafe_allow_html=True)

        st.markdown("<br>", unsafe_allow_html=True)

        # ── Raw metrics from best run ─────────────────────────────────────────
        rm_cols = st.columns(4)
        with rm_cols[0]: kpi("Loaded (best config)",    f'{best_sc["total_loaded_bbl"]:,} bbl')
        with rm_cols[1]: kpi("Exported (best config)",  f'{best_sc["total_exported_bbl"]:,.0f} bbl')
        with rm_cols[2]: kpi("Spilled (best config)",   f'{best_sc["total_spilled_bbl"]:,.0f} bbl',
                              sub="✅ none" if best_sc["total_spilled_bbl"] == 0 else "⚠️ overflow")
        avg_cyc = best_sc.get("avg_cycle_hours")
        with rm_cols[3]: kpi("Avg Cycle (best config)",
                              f'{avg_cyc:.1f}h' if avg_cyc else "—")
        st.caption(
            f"Stock drawdown: {best_sc.get('stock_drawdown_bbl', 0):,.0f} bbl "
            f"({best_sc.get('stock_drawdown_pct', 0):.1f}%) · "
            f"Early (24h) drawdown: {best_sc.get('early_drawdown_pct', 0):.1f}%"
        )

        # ── Bottlenecks ───────────────────────────────────────────────────────
        bns = best_sc.get("bottlenecks", [])
        if bns:
            st.markdown(
                '<div class="alert-warn">⚠️ <b>Bottlenecks detected in best config:</b> '
                + " · ".join(bns) + "</div>",
                unsafe_allow_html=True)
        else:
            st.markdown(
                '<div class="alert-ok">✅ No significant bottlenecks in optimal configuration</div>',
                unsafe_allow_html=True)

        # ── Vessel utilisation heatmap ────────────────────────────────────────
        vu = best_sc.get("vessel_utilisation", {})
        if vu:
            st.markdown("**Vessel utilisation — optimal configuration:**")
            vu_cols = st.columns(len(vu))
            for ci, (vn, util_pct) in enumerate(vu.items()):
                with vu_cols[ci]:
                    color = "#56d364" if util_pct >= 70 else ("#f1c40f" if util_pct >= 45 else "#f85149")
                    st.markdown(
                        f'<div class="kpi-card">'
                        f'  <div class="kpi-label">{vn}</div>'
                        f'  <div class="kpi-value" style="color:{color}">{util_pct:.0f}%</div>'
                        f'  <div class="score-bar-wrap">'
                        f'    <div class="score-bar" '
                        f'         style="width:{max(2,int(util_pct))}%;background:{color}"></div>'
                        f'  </div>'
                        f'</div>',
                        unsafe_allow_html=True)

        # ── Storage performance table ─────────────────────────────────────────
        su = best_sc.get("storage_utilisation", {})
        if su:
            st.markdown("**Storage performance — optimal configuration:**")
            su_df = pd.DataFrame([
                {"Storage": sn,
                 "Avg Util %": d["avg_pct"],
                 "Peak Util %": d["peak_pct"],
                 "Overflow (bbl)": f'{d["overflow_bbl"]:,}',
                 "Status": "⚠️ overflow" if d["overflow_bbl"] > 0 else "✅ clean"}
                for sn, d in su.items()
            ])
            st.dataframe(su_df, width='stretch', hide_index=True)

        # ── Scenario comparison table ─────────────────────────────────────────
        with st.expander("📊 All scenarios ranked — click to compare"):
            # ── Scenario selector UI ───────────────────────────────────────────
            # Show which scenario is currently active (if any was selected)
            _active_scen = st.session_state.get("selected_opt_scenario")
            if _active_scen:
                _asc = _active_scen
                st.markdown(
                    f'<div style="background:#f0fdf4;border:1px solid #22c55e;border-radius:8px;'
                    f'padding:10px 14px;margin-bottom:10px;font-size:13px;">'
                    f'▶ <b>Running Scenario #{_asc["rank"]}</b> — '
                    f'Score {_asc["score"]:.1f} | '
                    f'Dead-stock ×{_asc["dead_stock_factor"]:.2f} | '
                    f'Ibom trigger {_asc["ibom_trigger_bbl"]:,} bbl | '
                    f'Export window {_asc["export_sail_window_start"]:02d}:00 | '
                    f'Berthing {_asc["berthing_start"]:02d}:00–{_asc["berthing_end"]:02d}:00'
                    f'&nbsp;&nbsp;<span style="color:#64748b;font-size:11px">'
                    f'(not the optimal — manually selected)</span></div>',
                    unsafe_allow_html=True,
                )
                if st.button("✖ Clear — revert to optimal", key="clear_opt_scenario"):
                    st.session_state.pop("selected_opt_scenario", None)
                    st.rerun()
            else:
                st.markdown(
                    '<div style="background:#f8faff;border:1px solid #94a3b8;border-radius:8px;'
                    'padding:8px 14px;margin-bottom:10px;font-size:12px;color:#475569;">'
                    '💡 Click <b>▶ Run</b> on any row to initialise the simulation with that scenario\'s parameters.</div>',
                    unsafe_allow_html=True,
                )

            display_cols = [
                "Rank", "Score", "Stock Drawdown", "Fleet Util", "Storage Safety",
                "Fair Allocation", "Export", "Turnaround", "Loaded (bbl)", "Spilled (bbl)",
                "Avg Cycle (h)", "dead_stock_x", "pf_trigger_k",
                "exp_window_h", "berth_start_h", "berth_end_h",
            ]
            tbl_show = opt_tbl[display_cols].copy()
            tbl_show["Score"]        = tbl_show["Score"].round(1)
            tbl_show["Stock Drawdown"] = tbl_show["Stock Drawdown"].round(1)
            tbl_show["Fleet Util"]     = tbl_show["Fleet Util"].round(1)
            tbl_show["Storage Safety"] = tbl_show["Storage Safety"].round(1)
            tbl_show["Fair Allocation"] = tbl_show["Fair Allocation"].round(1)
            tbl_show["Export"]       = tbl_show["Export"].round(1)
            tbl_show["Turnaround"]   = tbl_show["Turnaround"].round(1)
            st.dataframe(
                tbl_show.head(50), width='stretch',
                hide_index=True,
                column_config={
                    "Score":          st.column_config.ProgressColumn("Score", min_value=0, max_value=100, format="%.1f"),
                    "Stock Drawdown": st.column_config.NumberColumn("Stock Drawdown", format="%.1f"),
                    "Storage Safety": st.column_config.NumberColumn("Storage Safety", format="%.1f"),
                    "Loaded (bbl)":st.column_config.NumberColumn("Loaded", format="%,d"),
                    "Spilled (bbl)":st.column_config.NumberColumn("Spilled", format="%,d"),
                },
            )

            # ── Per-scenario run buttons ────────────────────────────────────────
            st.markdown("**▶ Select a scenario to run:**")
            _top_n = min(50, len(opt_tbl))
            _btn_cols = st.columns(min(_top_n, 10))   # up to 10 buttons per row
            for _si in range(_top_n):
                _row   = opt_tbl.iloc[_si]
                _rank  = int(_row["Rank"])
                _score = round(float(_row["Score"]), 1)
                _col   = _btn_cols[_si % 10]
                _is_active = (_active_scen is not None and _active_scen["rank"] == _rank)
                _lbl   = f"#{_rank} ({_score})" if not _is_active else f"✓ #{_rank}"
                with _col:
                    if st.button(_lbl, key=f"run_scen_{_rank}",
                                 type="primary" if _is_active else "secondary",
                                 help=f"Rank {_rank} | Score {_score} | "
                                      f"DSF ×{_row['dead_stock_x']:.2f} | "
                                      f"Ibom {int(_row['pf_trigger_k'])}k bbl | "
                                      f"Export {int(_row['exp_window_h']):02d}:00 | "
                                      f"Berth {int(_row['berth_start_h']):02d}:00–{int(_row['berth_end_h']):02d}:00"):
                        st.session_state["selected_opt_scenario"] = {
                            "rank":                    _rank,
                            "score":                   _score,
                            "dead_stock_factor":       float(_row["dead_stock_x"]),
                            "ibom_trigger_bbl":        int(_row["pf_trigger_k"]) * 1000,
                            "export_sail_window_start":int(_row["exp_window_h"]),
                            "berthing_start":          int(_row["berth_start_h"]),
                            "berthing_end":            int(_row["berth_end_h"]),
                        }
                        st.rerun()

            st.caption(
                f"Showing top 50 of {len(opt_tbl)} scenarios. "
                f"ibom_trigger_k = Ibom trigger ÷ 1000. "
                f"dead_stock_x = loading threshold multiplier."
            )

        # ── Download best config ──────────────────────────────────────────────
        best_export = {
            "optimal_params": best_pr,
            "scores":         best_sc,
            "all_scenarios":  json.loads(tbl_json),
        }
        st.download_button(
            "📥 Download Optimization Report (JSON)",
            data=json.dumps(best_export, indent=2),
            file_name="tanker_optimization_report.json",
            mime="application/json",
        )


    # ==========================================================================
    # ── EXCEPTION / WARNING BANNER ───────────────────────────────────────────
    # ==========================================================================
    def _render_warning_banner(log_df, tl_df, S, params, mod):
        """Persistent warning panel for operationally significant conditions.
        Fires before the KPI section so operators see issues immediately.
        """
        warnings_list = []
        _sim_days_w = params.get("sim_days", 30)
        _scap = getattr(mod, "STORAGE_CAPACITY_BY_NAME", {})
        _crit = getattr(mod, "STORAGE_CRITICAL_THRESHOLD_BY_NAME", {})
        _prod = getattr(mod, "STORAGE_PRODUCTION_RATE_BY_NAME", {})
        _stor_names = ["SanBarth", "JasmineS", "Westmore", "Duke", "Starturn", "PGM"]
        _vnames_w = list(getattr(mod, "VESSEL_NAMES", []))

        # 1. Overflow risk within 24 h at end of sim
        if not tl_df.empty:
            _last = tl_df.iloc[-1]
            for _sn in _stor_names:
                _col = f"{_sn}_bbl"
                if _col not in tl_df.columns:
                    continue
                _st = float(_last.get(_col, 0))
                _cap = float(_scap.get(_sn, 270_000))
                _rate = float(_prod.get(_sn, 0))
                if _rate > 0:
                    _h2o = (_cap - _st) / _rate
                    if _h2o < 24.0 and _st > 0:
                        warnings_list.append(
                            f"🔴 **{_sn}**: overflow risk at end of simulation — "
                            f"{_st:,.0f}/{_cap:,.0f} bbl, ~{_h2o:.1f}h to capacity"
                        )

        # 2. BIA congestion
        if not tl_df.empty:
            _bia_wait_sts = {"WAITING_BERTH_B", "WAITING_MOTHER_CAPACITY", "WAITING_MOTHER_RETURN"}
            _bia_count_series = sum(
                tl_df[vn].isin(_bia_wait_sts).astype(int)
                for vn in _vnames_w if vn in tl_df.columns
            )
            if hasattr(_bia_count_series, "max") and int(_bia_count_series.max()) > 3:
                _cong_hours = float((_bia_count_series > 3).sum()) * 0.5
                if _cong_hours > 4:
                    warnings_list.append(
                        f"🟠 **BIA congestion**: {_cong_hours:.1f}h of >3 vessels "
                        f"simultaneously queued at Point B — consider enabling MTO or "
                        f"adjusting berthing windows"
                    )

        # 3. Mother starvation
        if not tl_df.empty:
            _exp_trigs = getattr(
                mod, "MOTHER_EXPORT_TRIGGER_BY_NAME",
                {"Bryanston": getattr(mod, "MOTHER_EXPORT_TRIGGER", 450_000),
                 "GreenEagle": getattr(mod, "GREENEAGLE_EXPORT_TRIGGER_BBL", 680_000)}
            )
            for _mn in ["Bryanston", "GreenEagle"]:
                _mcol = f"{_mn}_bbl"
                if _mcol not in tl_df.columns:
                    continue
                _trig = float(_exp_trigs.get(_mn, 450_000))
                _starved_frac = float((tl_df[_mcol].astype(float) < 0.40 * _trig).mean())
                if _starved_frac > 2 / max(1, _sim_days_w):
                    warnings_list.append(
                        f"🟡 **{_mn} mother starvation**: below 40% fill "
                        f"for {_starved_frac * _sim_days_w:.1f} of {_sim_days_w} simulated days"
                    )

        # 4. Storage overflow occurred
        if S.get("spilled", 0) > 0:
            warnings_list.append(
                f"🔴 **Storage overflow**: {S['spilled']:,.0f} bbl spilled "
                f"across {S.get('ovf_events', 0)} overflow events"
            )

        # 5. Fleet idle with stock available
        if not tl_df.empty and _vnames_w:
            _idle_sts_w = {"IDLE_A", "WAITING_BERTH_A", "WAITING_STOCK", "WAITING_DEAD_STOCK"}
            _min_vcap = min(getattr(mod, "VESSEL_CAPACITIES", {"_": 85000}).values())
            _evac_thresh = float(getattr(mod, "DEAD_STOCK_FACTOR", 1.75)) * float(_min_vcap)
            _any_idle_s = sum(
                tl_df[vn].isin(_idle_sts_w).astype(int)
                for vn in _vnames_w if vn in tl_df.columns
            )
            _scols = [f"{sn}_bbl" for sn in _stor_names if f"{sn}_bbl" in tl_df.columns]
            if _scols:
                _max_stor_s = tl_df[_scols].max(axis=1)
                _simultaneous = float(((_any_idle_s > 2) & (_max_stor_s >= _evac_thresh)).mean())
                if _simultaneous > 0.10:
                    warnings_list.append(
                        f"🟡 **Fleet idle with stock**: >2 vessels idle while storage "
                        f"above dead-stock threshold in {_simultaneous * 100:.0f}% of timesteps"
                    )

        if warnings_list:
            with st.expander(
                f"⚠️  {len(warnings_list)} Operational Warning"
                + ("s" if len(warnings_list) != 1 else "")
                + " Detected — click to review",
                expanded=True,
            ):
                for _w in warnings_list:
                    st.markdown(
                        f'<div style="background:#fffbeb;border-left:4px solid #f59e0b;'
                        f'padding:8px 12px;border-radius:4px;margin-bottom:6px;'
                        f'font-size:13px;">{_w}</div>',
                        unsafe_allow_html=True,
                    )

    _render_warning_banner(log_df, tl_df, S, params, mod)

    # ==========================================================================
    # ── SECTION 2: SIMULATION KPIs ───────────────────────────────────────────
    # ==========================================================================
    _start_lbl = _dt.date.fromisoformat(_start_iso_str).strftime('%-d %b %Y') if _start_iso_str else 'Today'
    sec(f"📈 {params['sim_days']}-Day Simulation Forecast — from {_start_lbl}")

    # ── Active scenario banner ─────────────────────────────────────────────────
    _active_kpi_scen = st.session_state.get("selected_opt_scenario")
    if run_opt and _active_kpi_scen:
        _aksc = _active_kpi_scen
        _kpi_cols_b = st.columns([8, 2])
        with _kpi_cols_b[0]:
            st.markdown(
                f'<div style="background:#fefce8;border:1px solid #f59e0b;border-radius:8px;'
                f'padding:9px 14px;margin-bottom:8px;font-size:13px;">'
                f'⚡ <b>Running Scenario #{_aksc["rank"]}</b> '
                f'(Score {_aksc["score"]:.1f}) — '
                f'Dead-stock ×{_aksc["dead_stock_factor"]:.2f} · '
                f'Ibom trigger {_aksc["ibom_trigger_bbl"]:,} bbl · '
                f'Export {_aksc["export_sail_window_start"]:02d}:00 · '
                f'Berthing {_aksc["berthing_start"]:02d}:00–{_aksc["berthing_end"]:02d}:00'
                f'</div>',
                unsafe_allow_html=True,
            )
        with _kpi_cols_b[1]:
            if st.button("✖ Revert to optimal", key="clear_opt_kpi"):
                st.session_state.pop("selected_opt_scenario", None)
                st.rerun()
    elif run_opt:
        st.markdown(
            f'<div style="background:#f0fdf4;border:1px solid #22c55e;border-radius:8px;'
            f'padding:9px 14px;margin-bottom:8px;font-size:13px;">'
            f'✅ <b>Running optimal scenario</b> — '
            f'Score {best_sc["composite"]:.1f} | '
            f'Dead-stock ×{best_pr["dead_stock_factor"]:.2f} · '
            f'Ibom trigger {best_pr["ibom_trigger_bbl"]:,} bbl · '
            f'Export {best_pr["export_sail_window_start"]:02d}:00 · '
            f'Berthing {best_pr["berthing_start"]:02d}:00–{best_pr["berthing_end"]:02d}:00'
            f'</div>',
            unsafe_allow_html=True,
        )

    k1 = st.columns(5)
    with k1[0]: kpi("Total Loadings",   str(S["loadings"]))
    with k1[1]: kpi("Total Discharges", str(S["discharges"]))
    with k1[2]: kpi("Volume Loaded",    f"{S['loaded']:,} bbl")
    with k1[3]: kpi("Volume Exported",  f"{S['exported']:,.0f} bbl")
    with k1[4]: kpi("Export Voyages",   str(S["exports"]))

    st.markdown("<br>", unsafe_allow_html=True)
    k2 = st.columns(5)
    with k2[0]: kpi("Total Produced",  f"{S['produced']:,.0f} bbl")
    with k2[1]: kpi("Total Spilled",   f"{S['spilled']:,.0f} bbl",
                     sub="⚠️ overflow detected" if S["spilled"]>0 else "✅ no spill")
    with k2[2]: kpi("Overflow Events", str(S["ovf_events"]))
    all_stor = sum(S.get(f"final_{n}",0) for n in ["SanBarth","JasmineS","Westmore","Duke","Starturn"])
    all_moth = sum(S.get(f"final_{n}",0) for n in ["Bryanston","GreenEagle","Alkebulan"])
    with k2[3]: kpi("Final All Storage", f"{all_stor:,.0f} bbl")
    with k2[4]: kpi("Final All Mothers", f"{all_moth:,.0f} bbl")

    st.markdown("<br>", unsafe_allow_html=True)
    k3 = st.columns(5)
    _sapi = S.get("storage_api", {})
    _mapi = S.get("mother_api",  {})
    _xapi = S.get("avg_exported_api", 0.0)
    with k3[0]: kpi("SanBarth API",    f"{_sapi.get('SanBarth',   0.0):.2f}°", sub="end of period")
    with k3[1]: kpi("JasmineS API",  f"{_sapi.get('JasmineS', 0.0):.2f}°", sub="end of period")
    with k3[2]: kpi("Westmore API",  f"{_sapi.get('Westmore',  0.0):.2f}°", sub="end of period")
    _all_moth_vol = sum(S.get(f"final_{n}", 0) for n in ["Bryanston","GreenEagle","Alkebulan"])
    _blended_moth = (
        sum(S.get(f"final_{n}", 0) * _mapi.get(n, 0.0)
            for n in ["Bryanston","GreenEagle","Alkebulan"]) / _all_moth_vol
        if _all_moth_vol > 0 else 0.0
    )
    with k3[3]: kpi("Mother Blended API", f"{_blended_moth:.2f}°", sub="all mothers combined")
    with k3[4]: kpi("Exported API",  f"{_xapi:.2f}°" if _xapi else "—", sub="weighted avg of exports")

    # ── Stochastic variability run report ─────────────────────────────────────
    _vsum = S.get("variability_summary", {})
    if _vsum.get("enabled"):
        st.markdown("<br>", unsafe_allow_html=True)
        st.markdown("#### 🎲 Variability Run Report")
        _weather_h = _vsum.get("weather_hold_h_total", 0.0)
        _cal       = _vsum.get("calibration", {})
        _vk1, _vk2, _vk3 = st.columns(3)
        with _vk1:
            kpi("Weather Holds Total", f"{_weather_h:.1f} h",
                sub=f"Across all transit legs")
        with _vk2:
            _n_ops = sum(v.get("n", 0) for v in _cal.values())
            kpi("Operations Sampled", str(_n_ops),
                sub="Loading · discharge · transit · berthing")
        with _vk3:
            _bias_vals = [v.get("pct_bias", 0.0) for v in _cal.values() if v.get("n", 0) > 0]
            _mean_bias = sum(_bias_vals) / len(_bias_vals) if _bias_vals else 0.0
            kpi("Mean Duration Bias", f"{_mean_bias:+.1f}%",
                sub="Actual vs planned (+ = slower)")
        if _cal:
            with st.expander("📊 Calibration Detail — Planned vs Actual Durations"):
                st.caption(
                    "Comparison of nominal (planned) vs sampled (actual) operation durations. "
                    "Use this table to calibrate CV values: if mean bias is large, "
                    "adjust the CV slider and re-run."
                )
                _cal_rows = []
                for op, m in sorted(_cal.items()):
                    if m.get("n", 0) == 0:
                        continue
                    _cal_rows.append({
                        "Operation":        op.replace("_", " ").title(),
                        "N":                m["n"],
                        "Mean Planned (h)": f"{m['mean_planned_h']:.2f}",
                        "Mean Actual (h)":  f"{m['mean_actual_h']:.2f}",
                        "Bias (h)":         f"{m['mean_bias_h']:+.3f}",
                        "RMSE (h)":         f"{m['rmse_h']:.3f}",
                        "Bias %":           f"{m['pct_bias']:+.1f}%",
                    })
                if _cal_rows:
                    st.dataframe(pd.DataFrame(_cal_rows), hide_index=True,
                                 use_container_width=True)
                # Download calibration CSV
                _cal_csv = pd.DataFrame(_cal_rows).to_csv(index=False).encode()
                st.download_button(
                    "📥 Download Calibration Report (CSV)",
                    data=_cal_csv,
                    file_name=f"calibration_report_{params.get('sim_start_date','')}.csv",
                    mime="text/csv",
                    use_container_width=True,
                )

    # ==========================================================================
    # ── SECTION 3: CAPACITY RECOMMENDATIONS ──────────────────────────────────
    # ==========================================================================
    sec("💡 Capacity & Fleet Recommendations")
    recs = capacity_recommendations(S, params, tl_df, mod, log_df=log_df)
    render_recommendations(recs)

    if S["spilled"] > 0:
        spill_by = S.get("spill_by_storage",{})
        rows_sp = [{"Storage":k,"Overflow (bbl)":f"{v:,.0f}",
                    "% of Total":f"{v/S['spilled']*100:.1f}%"}
                   for k,v in sorted(spill_by.items(), key=lambda x:-x[1]) if v>0]
        if rows_sp:
            st.markdown("**Overflow breakdown by storage point:**")
            st.dataframe(pd.DataFrame(rows_sp), width='content', hide_index=True)

    # ==========================================================================
    # ── SECTION 3b: COMBINED OPERATIONS SUMMARY ───────────────────────────────
    # ==========================================================================
    sec("📝 Combined Operations Summary")

    # ── Derive narrative values ───────────────────────────────────────────────
    _sim_d   = params["sim_days"]
    _loaded  = S.get("loaded", 0)
    _exported= S.get("exported", 0)
    _spilled = S.get("spilled", 0)
    _loadings= S.get("loadings", 0)
    _discs   = S.get("discharges", 0)
    _produced= S.get("produced", 0)
    _eff     = (_loaded / max(_produced, 1)) * 100
    _lifts_pd= _loadings / max(_sim_d, 1)
    _ovf_ev  = S.get("ovf_events", 0)
    _spill_pct = (_spilled / max(_produced, 1)) * 100
    _final_storage = sum(S.get(f"final_{n}", 0) for n in ["SanBarth","JasmineS","Westmore","Duke","Starturn"])
    _final_mothers = sum(S.get(f"final_{n}", 0) for n in ["Bryanston","GreenEagle","Alkebulan"])

    # Storage by field
    _stor_finals = {n: S.get(f"final_{n}", 0) for n in ["SanBarth","JasmineS","Westmore","Duke","Starturn","PGM"]}
    _capacities  = {"SanBarth":400_000,"JasmineS":290_000,"Westmore":270_000,"Duke":90_000,"Starturn":70_000,"PGM":28_000}
    _tightest    = min(_stor_finals, key=lambda k: _capacities[k] - _stor_finals[k] if _capacities[k]>0 else 1e9)
    _most_empty  = min(_stor_finals, key=lambda k: _stor_finals[k])

    # Challenge flags
    _has_spill   = _spilled > 0
    _has_overflow= _ovf_ev  > 0
    _low_eff     = _eff < 75
    _high_eff    = _eff >= 92
    _tight_cap   = any((_stor_finals[n] / _capacities[n]) > 0.88 for n in _stor_finals)
    _low_storage = any(_stor_finals[n] < _capacities[n] * 0.12 for n in _stor_finals)

    # Vessel days active (rough proxy: loadings × avg cycle)
    _vessels_active = len(params.get("vessel_states", {})) if params.get("vessel_states") else 8

    sc1, sc2 = st.columns([3, 2])

    with sc1:
        # ── Operations narrative card ─────────────────────────────────────────
        _eff_tag  = "tag-green" if _high_eff else ("tag-amber" if _eff >= 75 else "tag-red")
        _spill_tag= "tag-green" if not _has_spill else "tag-red"
        st.markdown(f"""
<div class="summary-card">
  <h4>🛢️ Operational Overview — {_sim_d}-Day Period</h4>
  <p>Over the simulated <strong>{_sim_d}-day period</strong>, the daughter vessel fleet completed
  <strong>{_loadings} loading lifts</strong> across all storage fields and discharged
  <strong>{_discs} times</strong> to the mother vessel tankers at BIA.
  A total of <strong>{_loaded:,.0f} bbl</strong> was lifted from storage and
  <strong>{_exported:,.0f} bbl</strong> exported to the export terminal.</p>
  <p>Field production injected <strong>{_produced:,.0f} bbl</strong> into storage across the period,
  at a combined average of <strong>{_produced/_sim_d:,.0f} bbl/day</strong>.
  Fleet lifting efficiency — the proportion of produced volumes successfully lifted —
  was <strong>{_eff:.1f}%</strong>
  <span class="summary-tag {_eff_tag}">{'✅ Strong' if _high_eff else ('⚠️ Moderate' if _eff>=75 else '❌ Low')}</span>.
  Overflow events recorded: <strong>{_ovf_ev}</strong>
  <span class="summary-tag {_spill_tag}">{'✅ No spill' if not _has_spill else f'⚠️ {_spilled:,.0f} bbl spilled'}</span>.</p>
  <p>At period end, combined storage held <strong>{_final_storage:,.0f} bbl</strong>
  across all five fields, with mother vessels retaining <strong>{_final_mothers:,.0f} bbl</strong>.
  Average lift rate: <strong>{_lifts_pd:.2f} lifts/day</strong>.</p>
</div>""", unsafe_allow_html=True)

        # ── Challenges card ───────────────────────────────────────────────────
        _challenges = []
        if _has_spill:
            _challenges.append(f"<li><strong>Storage overflow</strong> — {_spilled:,.0f} bbl lost to overflow "
                f"({_spill_pct:.1f}% of total production). The <strong>{_tightest}</strong> tank was nearest "
                f"capacity at period end. Consider earlier vessel scheduling or increased lift frequency.</li>")
        if _low_eff:
            _challenges.append("<li><strong>Low lifting efficiency</strong> — The fleet was unable to keep "
                "pace with production inflow. This is typically caused by berthing congestion, tidal "
                "constraints, or insufficient vessel count during peak production windows.</li>")
        if _tight_cap:
            tight_names = [n for n in _stor_finals if (_stor_finals[n]/_capacities[n])>0.88]
            _challenges.append(f"<li><strong>High tank utilisation</strong> at "
                f"{', '.join(tight_names)} — tanks above 88% capacity increase spill risk. "
                f"Prioritise these fields in dispatch sequencing.</li>")
        if _low_storage:
            low_names = [n for n in _stor_finals if _stor_finals[n] < _capacities[n]*0.12]
            _challenges.append(f"<li><strong>Low closing stock</strong> at "
                f"{', '.join(low_names)} — below 12% capacity at period end. Verify production "
                f"continuity and ensure no unplanned field shutdowns are pending.</li>")
        if not _challenges:
            _challenges.append("<li>No significant operational challenges identified in this simulation period. "
                "All storage levels, lifting efficiency, and overflow metrics are within acceptable bounds.</li>")

        st.markdown(f"""
<div class="summary-card">
  <h4>⚠️ Challenges & Risks</h4>
  <ul>{''.join(_challenges)}</ul>
</div>""", unsafe_allow_html=True)

    with sc2:
        # ── Vessel requirement card ───────────────────────────────────────────
        _req_vessels = max(4, round(_lifts_pd * 4.5))   # rough estimate: avg 4.5d cycle
        _req_tag = "tag-green" if _req_vessels <= 6 else ("tag-amber" if _req_vessels <= 8 else "tag-red")
        st.markdown(f"""
<div class="summary-card">
  <h4>🚢 Vessel Requirements</h4>
  <p>Based on <strong>{_lifts_pd:.2f} lifts/day</strong> and an average voyage cycle of
  ~4–5 days (load + sail + discharge + return), the operation requires an estimated
  <strong>{_req_vessels} active daughter vessels</strong>
  <span class="summary-tag {_req_tag}">Fleet size estimate</span>.</p>
  <ul>
    <li><strong>SanBarth/Sego/Awoba/Dawes</strong> — standard routes via BIA and/or
    Cawthorne passage; cycle ~4–5 days per vessel</li>
    <li><strong>Ibom</strong> — offshore buoy; Bedford &amp; Balham on rotation with swap trigger at {S.get("ibom_trigger",65000):,.0f} bbl. When no swap is active, Bedford/Balham support Point A (SanBarth/JasmineS) loading.</li>
    <li><strong>Watson</strong> — restricted to Point A (SanBarth/JasmineS) and Sego (Westmore)</li>
    <li><strong>Mother tankers</strong> — 3 vessels (Bryanston, GreenEagle, MT SanBarth)
    required to be available at BIA to maintain discharge throughput</li>
  </ul>
  <p>Vessel availability below {max(4, _req_vessels-1)} active daughters will likely
  result in storage accumulation and increased overflow risk.</p>
</div>""", unsafe_allow_html=True)

        # ── Stability maintenance card ────────────────────────────────────────
        st.markdown(f"""
<div class="summary-card">
  <h4>🔒 Stability Factors to Maintain</h4>
  <ul>
    <li><strong>Tidal schedule adherence</strong> — all crossings of the main
                SanBarth→BIA breakwater require tide &gt;1.6 m during daylight. Departures must
    be planned against the tidal window; delays compound across the fleet.</li>
    <li><strong>Cawthorne passage coordination</strong> — Awoba-bound vessels
    use a 3-leg tidal passage; any slot missed adds ~6–12h to the cycle.</li>
    <li><strong>Mother vessel turnaround</strong> — export voyages must complete
    before the next daughter batch arrives. A delayed export creates a queue at BIA
    that backs up all storage fields.</li>
    <li><strong>Stock threshold discipline</strong> — vessels should not berth at
    storage below the 175% minimum-stock threshold; premature berthing locks a
    berth slot without completing a load.</li>
    <li><strong>Production continuity</strong> — any unplanned shutdown at SanBarth,
    JasmineS, or Westmore (highest volume fields) materially reduces available
    lifting volume and stresses downstream scheduling.</li>
    <li><strong>Ibom swap protocol</strong> — the active/standby rotation must
    execute cleanly; a missed swap leaves one vessel idle and reduces Ibom
    throughput by ~50%.</li>
  </ul>
</div>""", unsafe_allow_html=True)

    # ── Potential issues banner ───────────────────────────────────────────────
    _issues = []
    if _has_spill:
        _issues.append(f'<span class="summary-tag tag-red">🔴 Overflow risk — {_spilled:,.0f} bbl</span>')
    if _tight_cap:
        _issues.append('<span class="summary-tag tag-amber">🟡 High tank utilisation</span>')
    if _low_eff:
        _issues.append('<span class="summary-tag tag-amber">🟡 Low lifting efficiency</span>')
    if _low_storage:
        _issues.append('<span class="summary-tag tag-amber">🟡 Low closing stock</span>')
    if not _issues:
        _issues.append('<span class="summary-tag tag-green">🟢 All metrics within normal bounds</span>')

    st.markdown(
        f'<div style="margin:10px 0 4px;font-size:12px;font-weight:700;color:#1a2744;">'
        f'Potential Issues Flagged:</div>'
        + " ".join(_issues),
        unsafe_allow_html=True
    )

    
        # ==========================================================================
    # ── SECTION 4: STORAGE FORECAST CHARTS ───────────────────────────────────
    # ==========================================================================
    sec("📦 Storage Volume Forecast")
    st.plotly_chart(chart_storage(tl_df), width='stretch')

    oc1, oc2 = st.columns(2)
    with oc1:
        of = chart_overflow(tl_df)
        if of: st.plotly_chart(of, width='stretch')
    with oc2:
        st.plotly_chart(chart_util(tl_df), width='stretch')

    sec("📦 Forecast End-of-Period Storage Levels")
    sc = st.columns(6)
    storage_items = [("SanBarth","A",400_000),("JasmineS","A",290_000),
                     ("Westmore","C",270_000),("Duke","D",90_000),("Starturn","E",70_000),
                     ("PGM","G",28_000)]
    _s_api = S.get("storage_api", {})
    for i,(name,pt,cv) in enumerate(storage_items):
        fv    = S.get(f"final_{name}", 0)
        pct   = fv / cv * 100
        _api  = _s_api.get(name, 0.0)
        with sc[i]:
            kpi(f"{name} (Pt {pt})", f"{fv:,.0f} bbl",
                sub=f"{pct:.0f}% full · <b>API {_api:.2f}°</b>")

    # ==========================================================================
    # ── SECTION 5: MOTHER VESSEL FORECAST ────────────────────────────────────
    # ==========================================================================
    sec("🛢️ Mother Vessel Forecast — BIA")
    st.plotly_chart(chart_mothers(tl_df, EXPORT_TRIG, MOTHER_CAP_BY_NAME,
                                   export_trigger_by_name=getattr(mod, "MOTHER_EXPORT_TRIGGER_BY_NAME", {})),
                    width='stretch')
    _m_api = S.get("mother_api", {})

    mc = st.columns(3)
    for i,(mn,mk) in enumerate([("Bryanston","bryanston"),
                                  ("GreenEagle","greeneagle"),
                                  ("Alkebulan","alkebulan")]):
        with mc[i]:
            fv    = S.get(f"final_{mn}", 0)
            start = params.get(mk, 0)
            d     = fv - start
            col_s = "#56d364" if d >= 0 else "#f85149"
            _mapi = _m_api.get(mn, 0.0)
            _api_txt = f" · <b>API {_mapi:.2f}°</b>" if fv > 0 else ""

            kpi(mn, f"{fv:,.0f} bbl",
                sub=f'<span style="color:{col_s}">{"▲" if d>=0 else "▼"} '
                    f'{d:+,.0f} bbl vs 08:00</span>{_api_txt}')

    # ==========================================================================
    # ── SECTION 6: GANTT ─────────────────────────────────────────────────────
    # ==========================================================================
    sec("⛴️ Vessel Activity Timeline (Gantt)")
    st.plotly_chart(chart_gantt(tl_df, vnames, log_df=log_df), width='stretch')

    with st.expander("🎨 Colour key"):
        ck = st.columns(4)
        for i,vn in enumerate(vnames):
            with ck[i%4]:
                base = VESSEL_COLORS.get(vn,"#aaa")
                st.markdown(
                    f'<span class="pill" style="background:{base};color:#fff">{vn}</span>',
                    unsafe_allow_html=True)
                for sc_code, lbl in [("IDLE_A","Idle"),("LOADING","Loading"),
                    ("PF_LOADING","Ibom"),("SAILING_AB","Sailing → mother"),
                    ("DISCHARGING","Discharging"),("SAILING_BA","Returning"),
                    ("WAITING_DEAD_STOCK","Waiting dead-stock")]:
                    st.markdown(
                        f'<span style="background:{vcolor(vn,sc_code)};'
                        f'padding:1px 8px;border-radius:3px;font-size:11px">'
                        f'&nbsp;</span> {lbl}', unsafe_allow_html=True)

    # ==========================================================================
    # ── SECTION 7: VOYAGE COUNTS ──────────────────────────────────────────────
    # ==========================================================================
    sec("📊 Voyage Counts per Vessel")
    v1, v2 = st.columns([3,2])
    with v1:
        st.plotly_chart(chart_voyage_bars(log_df, vnames), width='stretch')
    with v2:
        rows_v = []
        for vn in vnames:
            vl = log_df[log_df.Vessel==vn]
            ld = len(vl[vl.Event=="LOADING_START"])
            dc = len(vl[vl.Event=="DISCHARGE_START"])
            vc = mod.VESSEL_CAPACITIES.get(vn,mod.DAUGHTER_CARGO_BBL)
            _loaded_bbl = _safe_sum_cargo(vl.loc[vl.Event=="LOADING_START", "Detail"])
            _discharged_bbl = _safe_sum_cargo(vl.loc[vl.Event=="DISCHARGE_START", "Detail"])
            rows_v.append({"Vessel":vn,"Loads":ld,"Discharges":dc,
                            "Vol Loaded":f"{_loaded_bbl:,} bbl","Vol Discharged":f"{_discharged_bbl:,} bbl","Base Cap":f"{vc:,}"})
        st.dataframe(pd.DataFrame(rows_v), width='stretch', hide_index=True)

    # ==========================================================================
    # ── SECTION 8: PER-VESSEL TABS ────────────────────────────────────────────
    # ==========================================================================
    sec("🚢 Per-Vessel Event Log")
    vtabs = st.tabs(vnames)
    for vtab, vn in zip(vtabs, vnames):
        with vtab:
            vlog  = log_df[log_df.Vessel==vn].copy()
            loads = vlog[vlog.Event=="LOADING_START"]
            discs = vlog[vlog.Event=="DISCHARGE_START"]
            vcap  = mod.VESSEL_CAPACITIES.get(vn,mod.DAUGHTER_CARGO_BBL)
            _vol_loaded = _safe_sum_cargo(loads["Detail"]) if not loads.empty else 0
            _vol_discharged = _safe_sum_cargo(discs["Detail"]) if not discs.empty else 0
            base  = VESSEL_COLORS.get(vn,"#aaa")
            ml,mr = st.columns([1,3])
            with ml:
                st.markdown(
                    f'<span class="pill" style="background:{base};color:#fff;'
                    f'font-size:15px;padding:5px 16px">{vn}</span><br><br>',
                    unsafe_allow_html=True)
                kpi("Voyages", str(len(loads)))
                st.markdown("<br>",unsafe_allow_html=True)
                kpi("Base Capacity", f"{vcap:,} bbl", "JasmineS +8% · Westmore -18%")
                st.markdown("<br>",unsafe_allow_html=True)
                kpi("Vol Loaded", f"{_vol_loaded:,} bbl")
                st.markdown("<br>",unsafe_allow_html=True)
                kpi("Vol Discharged", f"{_vol_discharged:,} bbl")
                st.markdown("<br>**Storages used:**", unsafe_allow_html=True)
                used = (vlog[vlog.Event=="LOADING_START"]["Detail"]
                        .str.extract(r"Loading \d[,\d]+ bbl \| (\w+):")
                        .dropna()[0].value_counts())
                for sn,cnt in used.items():
                    st.markdown(
                        f'<span class="pill" style="background:{STORAGE_COLORS.get(sn,"#aaa")};'
                        f'color:#fff">{sn} ×{cnt}</span>', unsafe_allow_html=True)
            with mr:
                show  = ["Time","Day","Voyage","Event","Detail"]
                extra = [c for c in ["SanBarth_bbl","JasmineS_bbl","Duke_bbl",
                                     "Starturn_bbl","Mother_bbl","Vessel_api"] if c in vlog.columns]
                # Rename Vessel_api for clarity in the table
                _vlog_show = vlog[show+extra].copy()
                if "Vessel_api" in _vlog_show.columns:
                    _vlog_show = _vlog_show.rename(columns={"Vessel_api": "Cargo API°"})
                    # Only show API when vessel is carrying cargo (non-zero rows)
                    _vlog_show["Cargo API°"] = _vlog_show["Cargo API°"].replace(0.0, pd.NA)
                st.dataframe(_vlog_show, width='stretch', height=380)
            # API summary for this vessel
            with ml:
                _load_api_rows = vlog[vlog.Event=="LOADING_START"]
                if "Vessel_api" in _load_api_rows.columns and not _load_api_rows.empty:
                    _avg_vapi = _load_api_rows["Vessel_api"].replace(0, pd.NA).mean()
                    if pd.notna(_avg_vapi):
                        st.markdown("<br>", unsafe_allow_html=True)
                        kpi("Avg Cargo API", f"{_avg_vapi:.2f}°")

    # ==========================================================================
    # ── SECTION 9: STORAGE POINT TABS ────────────────────────────────────────
    # ==========================================================================
    sec("📍 Storage Breakdown")
    stabs = st.tabs(["SanBarth (A)","JasmineS (A)","Westmore (C)","Duke (D)","Starturn (E)","PGM (G)"])
    st_info = [
        ("SanBarth","SanBarth_bbl","SanBarth_Overflow_Accum_bbl",400_000,"A",sorted(mod.VESSEL_NAMES)),
        ("JasmineS","JasmineS_bbl","JasmineS_Overflow_Accum_bbl",290_000,"A",sorted(mod.VESSEL_NAMES)),
        ("Westmore","Westmore_bbl","Westmore_Overflow_Accum_bbl",270_000,"C",sorted(mod.WESTMORE_PERMITTED_VESSELS)),
        ("Duke","Duke_bbl","Duke_Overflow_Accum_bbl",90_000,"D",sorted(mod.DUKE_PERMITTED_VESSELS)),
        ("Starturn","Starturn_bbl","Starturn_Overflow_Accum_bbl",70_000,"E",sorted(mod.STARTURN_PERMITTED_VESSELS)),
        ("PGM","PGM_bbl","PGM_Overflow_Accum_bbl",28_000,"G",sorted(getattr(mod,"PGM_PERMITTED_VESSELS",{"SantaMonica"}))),
    ]
    for stab,(sname,vc,ovfc,cv,pt,perm) in zip(stabs,st_info):
        with stab:
            sf = go.Figure()
            if vc in tl_df.columns:
                sf.add_trace(go.Scatter(x=tl_df.Time, y=tl_df[vc], name=f"{sname} Volume",
                    fill="tozeroy", fillcolor=_hex_to_rgba(STORAGE_COLORS[sname]),
                    line=dict(color=STORAGE_COLORS[sname], width=2)))
            if ovfc in tl_df.columns:
                sf.add_trace(go.Scatter(x=tl_df.Time, y=tl_df[ovfc], name="Overflow (accum)",
                    line=dict(color="#ef4444", dash="dot", width=1.5)))
            sf.add_hline(y=cv, line=dict(color="#ef4444",dash="dash"),
                         annotation_text=f"Capacity {cv:,} bbl")
            sf.update_layout(height=230, margin=dict(l=50,r=20,t=20,b=30), **_DARK,
                             yaxis=dict(tickformat=",",**_GRID),
                             xaxis=_GRID, legend=dict(bgcolor="#ffffff"))
            st.plotly_chart(sf, width='stretch')
            sloads = log_df[(log_df.Event=="LOADING_START") &
                            (log_df.Detail.str.contains(sname, na=False))]
            c1,c2 = st.columns(2)
            with c1:
                kpi(f"Loadings from {sname}", str(len(sloads)),
                    sub=f"Permitted: {', '.join(perm)}")
            with c2:
                if not sloads.empty:
                    st.dataframe(sloads.groupby("Vessel").size()
                                 .reset_index(name="Loads"), width='stretch', hide_index=True)

    # ==========================================================================
    # ── SECTION 10: SEQUENCE & IBOM LOGS ──────────────────────────────────
    # ==========================================================================
    sec("🔀 Mother Vessel Discharge Sequence Log")
    seq = log_df[log_df.Event.isin(["BERTHING_START_B","MOTHER_SEQUENCE_ASSIGNMENT",
                                     "MOTHER_PRIORITY_ASSIGNMENT"])]
    st.dataframe(seq[["Time","Day","Vessel","Voyage","Event","Detail"]]
                 if not seq.empty else pd.DataFrame(columns=["Time","Day","Vessel","Event","Detail"]), width='stretch', height=300)

    sec("🔁 Ibom Bedford / Balham Swap Log")
    pf = log_df[log_df.Event.isin(
        ["IBOM_SWAP_TRIGGER","IBOM_SWAP_START","IBOM_SWAP_COMPLETE"])]
    if pf.empty:
        st.caption("No Ibom swaps in this simulation period.")
    else:
        st.dataframe(pf[["Time","Day","Vessel","Voyage","Event","Detail"]], width='stretch', height=240)

    # ==========================================================================
    # ── SECTION 11: FULL EVENT LOG ────────────────────────────────────────────
    # ==========================================================================
    sec("📋 Full Event Log")
    f1,f2,f3,f4 = st.columns(4)
    all_ents = vnames + ["SanBarth","JasmineS","Westmore","Duke","Starturn",
                         "Bryanston","GreenEagle","Alkebulan"]
    with f1: vf   = st.multiselect("Vessel / Entity", all_ents, [], key="vf")
    with f2: ef   = st.multiselect("Event type", sorted(log_df.Event.dropna().unique()), [], key="ef")
    _slider_max = max(2, params["sim_days"])
    with f3: dr = st.slider("Day range", 1, _slider_max, (1, min(params["sim_days"], _slider_max)))
    with f4: srch = st.text_input("Search Detail", placeholder="e.g. SanBarth, Bryanston…")

    filt = log_df[log_df.Day.between(dr[0],dr[1])].copy()
    if vf:   filt = filt[filt.Vessel.isin(vf)]
    if ef:   filt = filt[filt.Event.isin(ef)]
    if srch: filt = filt[filt.Detail.str.contains(srch, case=False, na=False)]

    show_c = ["Time","Day","Vessel","Voyage","Event","Detail"]
    extra  = [c for c in ["SanBarth_bbl","JasmineS_bbl","Westmore_bbl",
                           "Duke_bbl","Starturn_bbl","Mother_bbl",
                           "Vessel_api","SanBarth_api","JasmineS_api",
                           "Westmore_api","Duke_api","Starturn_api",
                           "Bryanston_api","GreenEagle_api"] if c in filt.columns]
    _filt_show = filt[show_c+extra].rename(columns={
        "Vessel_api": "Cargo API°", "SanBarth_api": "SanBarth API°",
        "JasmineS_api": "JasmineS API°", "Westmore_api": "Westmore API°",
        "Duke_api": "Duke API°", "Starturn_api": "Starturn API°",
        "Bryanston_api": "Bryanston API°",
        "GreenEagle_api": "GreenEagle API°",
    })
    # Zero API values not meaningful — blank them
    for _ac in [c for c in _filt_show.columns if c.endswith("API°")]:
        _filt_show[_ac] = _filt_show[_ac].replace(0.0, pd.NA)
    st.dataframe(_filt_show, width='stretch', height=440)
    st.caption(f"Showing {len(filt):,} of {len(log_df):,} events")



    # ==========================================================================
    # ── SCENARIO COMPARISON ───────────────────────────────────────────────────
    # ==========================================================================
    sec("📊 Scenario Comparison")

    if "saved_scenarios" not in st.session_state:
        st.session_state.saved_scenarios = {}

    _sc_c1, _sc_c2 = st.columns([3, 1])
    with _sc_c1:
        _sc_name = st.text_input(
            "Scenario name",
            value=f"Run {len(st.session_state.saved_scenarios) + 1}",
            key="scenario_name_input",
            placeholder="e.g. 'Base case', 'Reduced Westmore', 'Watson offline'",
        )
    with _sc_c2:
        st.markdown("<br>", unsafe_allow_html=True)
        if st.button("💾 Save current scenario", use_container_width=True, key="save_scenario_btn"):
            _sc_kpis = {
                "Exports":        S.get("exports", 0),
                "Loaded (bbl)":   S.get("loaded", 0),
                "Exported (bbl)": S.get("exported", 0),
                "Spilled (bbl)":  S.get("spilled", 0),
                "Loadings":       S.get("loadings", 0),
                "Discharges":     S.get("discharges", 0),
            }
            st.session_state.saved_scenarios[_sc_name] = {
                "kpis":   _sc_kpis,
                "params": {k: v for k, v in params.items()
                           if isinstance(v, (int, float, str, bool))},
            }
            st.success(f"Scenario '{_sc_name}' saved.")

    _saved = st.session_state.saved_scenarios
    if _saved:
        _sc_names = list(_saved.keys())
        _all_kpi_keys = sorted({k for sc in _saved.values() for k in sc["kpis"]})
        _compare_rows = []
        for _kk in _all_kpi_keys:
            _row = {"KPI": _kk}
            _vals = [_saved[n]["kpis"].get(_kk, 0) for n in _sc_names]
            _best_idx = _vals.index(max(_vals)) if max(_vals) != min(_vals) else -1
            for i, _n in enumerate(_sc_names):
                _v = _saved[_n]["kpis"].get(_kk, 0)
                _row[_n] = f"{_v:,.0f}" + (" ✅" if i == _best_idx else "")
            _compare_rows.append(_row)
        st.dataframe(pd.DataFrame(_compare_rows).set_index("KPI"),
                     use_container_width=True, height=min(320, 42 + 35 * len(_compare_rows)))

        if st.button("🗑️ Clear all saved scenarios", key="clear_scenarios_btn"):
            st.session_state.saved_scenarios = {}
            st.rerun()
    else:
        st.caption("No scenarios saved yet. Run the simulation and click 'Save current scenario'.")

    sec("⬇️ Download Results")

    # ── Excel export (all sheets in one workbook) ──────────────────────────
    def _build_excel_export(log_df, tl_df, S, vnames, params, recs):
        """Build a multi-sheet Excel workbook as bytes."""
        import io as _io
        try:
            import openpyxl as _openpyxl
        except ImportError:
            return None
        buf = _io.BytesIO()
        with pd.ExcelWriter(buf, engine="openpyxl") as xw:
            # Sheet 1: Event log
            log_df.to_excel(xw, sheet_name="Event Log", index=False)
            # Sheet 2: Timeline snapshots
            tl_df.to_excel(xw, sheet_name="Timeline", index=False)
            # Sheet 3: KPI summary
            _kpi_rows = [
                ("Simulation Start Date",  params.get("sim_start_date", "")),
                ("Simulation Days",        params.get("sim_days", 0)),
                ("Total Loadings",         S.get("loadings", 0)),
                ("Total Discharges",       S.get("discharges", 0)),
                ("Volume Loaded (bbl)",    S.get("loaded", 0)),
                ("Volume Exported (bbl)",  S.get("exported", 0)),
                ("Volume Produced (bbl)",  S.get("produced", 0)),
                ("Volume Spilled (bbl)",   S.get("spilled", 0)),
                ("Overflow Events",        S.get("ovf_events", 0)),
                ("Export Voyages",         S.get("exports", 0)),
            ]
            for _sn in ["SanBarth","JasmineS","Westmore","Duke","Starturn","PGM"]:
                _kpi_rows.append((f"Final {_sn} (bbl)", S.get(f"final_{_sn}", 0)))
            for _mn in ["Bryanston","GreenEagle","Alkebulan"]:
                _kpi_rows.append((f"Final {_mn} (bbl)", S.get(f"final_{_mn}", 0)))
            pd.DataFrame(_kpi_rows, columns=["KPI", "Value"]).to_excel(
                xw, sheet_name="KPI Summary", index=False)
            # Sheet 4: Per-vessel voyage table
            if not log_df.empty and "Event" in log_df.columns:
                _voy = log_df[log_df["Event"] == "LOADING_START"][[
                    "Time","Day","Vessel","VoyageCode","Event","Detail"
                ]].copy() if "VoyageCode" in log_df.columns else \
                log_df[log_df["Event"] == "LOADING_START"][[
                    "Time","Day","Vessel","Event","Detail"
                ]].copy()
                _voy.to_excel(xw, sheet_name="Voyages", index=False)
            # Sheet 5: Recommendations
            if recs:
                _rec_rows = [{"Type": r["type"], "Title": r["title"],
                              "Detail": r.get("detail","")} for r in recs]
                pd.DataFrame(_rec_rows).to_excel(
                    xw, sheet_name="Recommendations", index=False)
        return buf.getvalue()

    _xl_bytes = _build_excel_export(log_df, tl_df, S, vnames, params, recs)
    if _xl_bytes:
        _start_lbl_xl = params.get("sim_start_date", "").replace("-","")
        st.download_button(
            "📊 Download Full Report (Excel — 5 sheets)",
            data=_xl_bytes,
            file_name=f"tanker_report_{_start_lbl_xl}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True,
        )
        st.caption("Excel contains: Event Log · Timeline · KPI Summary · Voyages · Recommendations")
    else:
        st.caption("Install openpyxl for Excel export: pip install openpyxl")
    st.markdown("<br>", unsafe_allow_html=True)

    d1,d2,d3 = st.columns(3)
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
        ["Simulation Start Date", _dt.date.fromisoformat(_start_iso_str).strftime('%d/%m/%Y')],
        ["Simulation Days",      params["sim_days"]],
            ["Total Loadings",       S["loadings"]],
            ["Total Discharges",     S["discharges"]],
            ["Volume Loaded (bbl)",  S["loaded"]],
            ["Volume Exported (bbl)",S["exported"]],
            ["Volume Produced (bbl)",S["produced"]],
            ["Volume Spilled (bbl)", S["spilled"]],
            ["Overflow Events",      S["ovf_events"]],
        ]
        for name,pt,cv in storage_items:
            rows.append([f"Final {name} (bbl)", S.get(f"final_{name}",0)])
        for mn in ["Bryanston","GreenEagle","Alkebulan"]:
            rows.append([f"Final {mn} (bbl)", S.get(f"final_{mn}",0)])
        for rec in recs:
            rows.append([f"Rec [{rec['type']}]", rec["title"]])
        st.download_button(
            "📥 Summary + Recommendations (CSV)",
            pd.DataFrame(rows,columns=["Metric","Value"]).to_csv(index=False).encode(),
            "tanker_summary_v5.csv","text/csv")

    # ==========================================================================
    # ── AUTO-REFRESH ──────────────────────────────────────────────────────────
    # ==========================================================================
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
        f"Baseline: 08:00 position report · "
        f"Last run: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} · "
        f"Vessels: {', '.join(vnames)}"
    )


if __name__ == "__main__":
    try:
        main()
    except Exception as _top_exc:
        import traceback as _tb
        st.error(
            f"**Unexpected application error** — please refresh the page.\n\n"
            f"```\n{_tb.format_exc()}\n```",
            icon="🚨",
        )
