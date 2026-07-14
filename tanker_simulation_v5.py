"""
=============================================================
OIL TANKER DAUGHTER VESSEL OPERATION SIMULATION  (v5)
=============================================================
Simulates the continuous loading/offloading cycle between:
    - Storage Vessel (SanBarth / Point A)  - capacity 800,000 bbls
    - Daughter Vessels: Sherlock, Laphroaig, Rathbone, Bedford, Balham, Woodstock, Bagshot
    - Mother Vessel (Bryanston / Point B) - capacity 550,000 bbls

v5 changes — Multi-point independent storage loading at Point A/C/D/E:
    Point A has two active storage load points (SanBarth and JasmineS).
    Point C has one active storage load point (Westmore).
    Point D has one active storage load point (Duke).
    Point E has one active storage load point (Starturn).
    Each load point has its own berth timeline and stock level.
    Daughter vessels may berth/load from either load point based on
    available stock and berth timing, allowing parallel operations.
=============================================================
"""

import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from datetime import datetime, timedelta, date as _date
from dataclasses import dataclass, field as _dc_field
import random

# Version identifier — read by tanker_app.py to auto-clear Streamlit cache
# on deployment. Bump this string whenever the sim logic changes in a way
# that would invalidate cached run_sim() results.
SIM_VERSION = "5.50-export-loading-lockout-and-status-fixes"

# -----------------------------------------------------------------
# VOYAGE CODE SYSTEM
# -----------------------------------------------------------------
# Each daughter vessel loading is assigned a short, unique reference code
# so operators can unambiguously identify and reassign a specific voyage's
# discharge target (e.g. "lock BGT-007 to Alkebulan").
#
# Format:  <3-LETTER-PREFIX>-<ZERO-PADDED-3-DIGIT-VOYAGE>
# Examples: SHK-001  LAP-003  BGT-007  WDK-002  STA-004
#
# The prefix is derived from the vessel name (first 3 chars, uppercased,
# with short aliases for longer names).
_VESSEL_CODE_PREFIX = {
    "Sherlock":    "SHK",
    "Laphroaig":  "LAP",
    "Rathbone":   "RTH",
    "SantaMonica":"STM",
    "Bedford":    "BDF",
    "Balham":     "BLH",
    "Woodstock":  "WDK",
    "Bagshot":    "BGT",
    "Watson":     "WTS",
    "Amyla":    "AMY",
    "FatimaZarah":"FTZ",
    # ZeeZee (third-party)
    "ZeeZee":     "ZZE",
}

def make_voyage_code(vessel_name: str, voyage_num: int) -> str:
    """Return a short, unique voyage reference code for a loading event.

    Format: <PREFIX>-<NNN>  e.g. ``SHK-001``, ``BGT-007``.
    Custom vessels not in _VESSEL_CODE_PREFIX use the first three
    characters of their name, uppercased.
    """
    prefix = _VESSEL_CODE_PREFIX.get(
        vessel_name,
        vessel_name[:3].upper() if vessel_name else "UNK",
    )
    return f"{prefix}-{int(voyage_num):03d}"

# -----------------------------------------------------------------
# PRODUCTION API GRAVITY (degrees API per source)
# -----------------------------------------------------------------
STORAGE_API = {
    "SanBarth"  : 29.00,
    "JasmineS": 43.36,
    "Westmore" : 31.10,
    "Duke"    : 41.20,
    "Starturn" : 39.54,
    "PGM"     : 36.00,   # Point G
}
IBOM_API = 32.00   # Point F (Bedford / Balham)

# -----------------------------------------------------------------
# SIMULATION EPOCH  (set via set_sim_epoch before instantiating)
# -----------------------------------------------------------------
_SIM_EPOCH = datetime(2025, 1, 1, 8, 0)   # default; overridden by set_sim_epoch() — t=0 = 08:00
SIM_HOUR_OFFSET = 8  # t=0 is 08:00 wall-clock; add this to sim-hours before window comparisons

def set_sim_epoch(d):
    """Set the calendar start date for the simulation (accepts date or datetime).
    t=0 is anchored to 08:00 on the given date so all displayed times start at 08:00.
    """
    global _SIM_EPOCH
    if isinstance(d, _date) and not isinstance(d, datetime):
        d = datetime(d.year, d.month, d.day, 8, 0)  # anchor t=0 to 08:00
    elif isinstance(d, datetime) and d.hour == 0 and d.minute == 0:
        d = d.replace(hour=8)  # upgrade midnight datetime to 08:00
    _SIM_EPOCH = d

# -----------------------------------------------------------------
# TIDAL TABLE  (loaded via load_tide_table before instantiating)
# -----------------------------------------------------------------
# _TIDE_TABLE maps absolute_hour (float) -> tide_height_m (float).
# If None, tidal gating is disabled and only daylight applies.
_TIDE_TABLE = None          # {float: float}  hour -> height
TIDE_MIN_CROSSING_M = 1.6

def load_tide_table(csv_path):
    """
    Parse a tidal prediction CSV into _TIDE_TABLE.
    Expected columns (case-insensitive, flexible separators):
        Date       — DD/MM/YYYY  or  YYYY-MM-DD
        Time       — HH:MM
        Tide_Height_m (or Height, or Level)
    Rows are interpolated onto every 0.5 h slot covering the sim period.
    """
    global _TIDE_TABLE
    import csv as _csv, re as _re

    raw = []
    with open(csv_path, newline="", encoding="utf-8-sig") as f:
        # Detect delimiter
        sample = f.read(2048); f.seek(0)
        delim = "," if sample.count(",") >= sample.count(";") else ";"
        reader = _csv.DictReader(f, delimiter=delim)
        # Normalise column names
        for row in reader:
            norm = {k.strip().lower().replace(" ","_"): v.strip() for k,v in row.items()}
            raw.append(norm)

    if not raw:
        _TIDE_TABLE = None
        return

    # Find column names
    date_col   = next((k for k in raw[0] if "date" in k), None)
    time_col   = next((k for k in raw[0] if "time" in k), None)
    height_col = next((k for k in raw[0]
                       if any(x in k for x in ("height","tide","level","m_"))), None)
    if not (date_col and time_col and height_col):
        _TIDE_TABLE = None
        return

    parsed = {}   # datetime -> float
    for row in raw:
        try:
            ds = row[date_col]; ts = row[time_col]; hs = row[height_col]
            if not hs: continue
            # Parse date
            if "/" in ds:
                parts = ds.split("/")
                if len(parts[2]) == 4:   # DD/MM/YYYY
                    dt_date = datetime(int(parts[2]), int(parts[1]), int(parts[0]))
                else:                    # YYYY/MM/DD
                    dt_date = datetime(int(parts[0]), int(parts[1]), int(parts[2]))
            else:
                dt_date = datetime.fromisoformat(ds.split("T")[0])
            # Parse time — handle both 'H:MM' and 'HH:MM' formats
            t_parts = ts.split(":")
            hh, mm = int(t_parts[0]), int(t_parts[1][:2])
            dt = dt_date.replace(hour=hh, minute=mm)
            height = float(_re.sub(r"[^0-9.\-]","", hs))
            parsed[dt] = height
        except Exception:
            continue

    if not parsed:
        _TIDE_TABLE = None
        return

    # Build absolute-hour lookup keyed on hours-since-_SIM_EPOCH
    table = {}
    for dt, h in parsed.items():
        diff = (dt - _SIM_EPOCH).total_seconds() / 3600.0
        table[round(diff * 2) / 2] = h   # snap to nearest 0.5 h

    # Interpolate to fill every 0.5 h slot in the sim window (0 .. 365*24)
    if table:
        sorted_keys = sorted(table)
        full = {}
        for slot in [x * 0.5 for x in range(int(sorted_keys[-1] * 2) + 2)]:
            if slot in table:
                full[slot] = table[slot]
            else:
                # linear interpolation between nearest neighbours
                lo = max((k for k in sorted_keys if k <= slot), default=None)
                hi = min((k for k in sorted_keys if k >= slot), default=None)
                if lo is not None and hi is not None and hi != lo:
                    t_frac = (slot - lo) / (hi - lo)
                    full[slot] = table[lo] + t_frac * (table[hi] - table[lo])
                elif lo is not None:
                    full[slot] = table[lo]
                elif hi is not None:
                    full[slot] = table[hi]
        _TIDE_TABLE = full
    else:
        _TIDE_TABLE = None

# =============================================================================
# ╔═══════════════════════════════════════════════════════════════════════════╗
# ║                    OPERATIONS CONFIG TABLE                               ║
# ║  Edit values in this section only. The simulation below reads these      ║
# ║  constants by name — nothing else needs changing when you update them.   ║
# ║                                                                          ║
# ║  HOW TO ADD A NEW DAUGHTER VESSEL:                                       ║
# ║    1. Add her name to VESSEL_NAMES (sets dispatch order)                 ║
# ║    2. Add her cargo capacity to VESSEL_CAPACITIES (bbl)                  ║
# ║    3. Add her name to the relevant *_PERMITTED_VESSELS sets below        ║
# ║    4. If Point A only: add to POINT_A_ONLY_VESSELS                       ║
# ║    5. If SanBarth slow-loader: add to SANBARTH_SLOW_LOADERS                  ║
# ║    6. If Point A load cap applies: add to POINT_A_LOAD_CAP_VESSELS       ║
# ╚═══════════════════════════════════════════════════════════════════════════╝
# =============================================================================

SIMULATION_DAYS = 30   # How many days to simulate

# ── SECTION A: DAUGHTER VESSELS ──────────────────────────────────────────────
#  Dispatch order = top to bottom. Add a new vessel by inserting a new row.
#  Column 1: Name (str)   Column 2: Cargo capacity (bbl)
# ─────────────────────────────────────────────────────────────────────────────
#  Name            Capacity (bbl)   Notes
# ─────────────────────────────────────────────────────────────────────────────
_DAUGHTER_ROWS = [
    # name           capacity
    ( "Sherlock",    85_000 ),   # Point A — loads 1st in cycle
    ( "Laphroaig",   85_000 ),   # Point A — loads 2nd
    ( "Rathbone",    44_000 ),   # Point A/D/E — loads 3rd
    ( "SantaMonica", 28_000 ),   # Point A/D/E — loads 4th
    ( "Bedford",     85_000 ),   # Point A/B — loads 4th (Ibom)
    ( "Balham",      85_000 ),   # Point A/B — loads 5th (Ibom)
    ( "Woodstock",   42_000 ),   # Point A/E — loads 6th
    ( "Bagshot",     43_000 ),   # Point A/C/D — loads 7th
    ( "Watson",      85_000 ),   # Point A/C — loads 8th
    ( "Amyla",     63_000 ),   # Point A/C/F — loads 9th
    ( "Rahama",    30_000 ),   # Point A (SanBarth/JasmineS) + Westmore (C) + Duke (D) — loads 10th
    ( "FatimaZarah", 50_000 ),   # Point A (SanBarth/JasmineS) — loads 11th
]
# ─────────────────────────────────────────────────────────────────────────────
# Derived — do not edit these two lines
VESSEL_NAMES      = [row[0] for row in _DAUGHTER_ROWS]
VESSEL_CAPACITIES = {row[0]: row[1] for row in _DAUGHTER_ROWS}

DAUGHTER_CARGO_BBL = 85_000   # Default cargo for vessels not in VESSEL_CAPACITIES

# ── SECTION B: VESSEL LOADING PERMISSIONS ────────────────────────────────────
#  Add / remove vessel names from each set to control where they can load.
#  A vessel absent from a set is BLOCKED at that storage.
# ─────────────────────────────────────────────────────────────────────────────
WESTMORE_PERMITTED_VESSELS  = {"Sherlock", "Bagshot", "Rathbone", "Watson", "Laphroaig", "Amyla", "Rahama"}
DUKE_PERMITTED_VESSELS      = {"Woodstock", "Bagshot", "Rathbone", "SantaMonica", "Rahama"}
STARTURN_PERMITTED_VESSELS  = {"Woodstock", "Rathbone", "SantaMonica", "Bagshot"}
POINT_A_ONLY_VESSELS        = set()   # Amyla now permitted at Westmore (C) and Ibom (F)
SANBARTH_SLOW_LOADERS         = {"Woodstock", "Bagshot", "Rathbone", "SantaMonica", "Rahama"}  # reduced SanBarth rate

# Vessels that may never be nominated as the MTO transient (receiver).
# They may still act as MTO dischargers (pumping cargo into another vessel).
MTO_NEVER_RECEIVER          = {"Rahama", "SantaMonica"}
# Vessels that must only discharge directly to a mother vessel.
# They are excluded from MTO discharger pairings (no ship-to-ship transfer).
PRIMARY_MOTHERS_ONLY_VESSELS = {"SantaMonica"}
POINT_A_LOAD_CAP_VESSELS    = {"Bedford", "Balham"}      # capped at POINT_A_LOAD_CAP_BBL at Point A

# Special per-vessel storage allowlists (overrides all set-based checks above)
# SantaMonica: Starturn (E) and PGM (G) only
STORAGE_PRIMARY_NAME   = "SanBarth"
STORAGE_SECONDARY_NAME = "JasmineS"
STORAGE_TERTIARY_NAME  = "Westmore"
STORAGE_QUATERNARY_NAME = "Duke"
STORAGE_QUINARY_NAME   = "Starturn"
STORAGE_SENARY_NAME    = "PGM"          # Point G — SantaMonica only
PGM_PERMITTED_VESSELS  = {"SantaMonica"}  # only vessel allowed at Point G
SANTAMONICA_PERMITTED_STORAGES = (
    STORAGE_QUINARY_NAME,       # Starturn — Point E
    STORAGE_SENARY_NAME,        # PGM      — Point G
)
# Watson: SanBarth (A), JasmineS (A), Westmore (C)
WATSON_PERMITTED_STORAGES = (
    STORAGE_PRIMARY_NAME,       # SanBarth   — Point A
    STORAGE_SECONDARY_NAME,     # JasmineS — Point A
    STORAGE_TERTIARY_NAME,      # Westmore — Point C
)
# Laphroaig: JasmineS (A), Westmore (C)
LAPHROAIG_PERMITTED_STORAGES = (
    STORAGE_SECONDARY_NAME,     # JasmineS — Point A
    STORAGE_TERTIARY_NAME,      # Westmore — Point C
)
# Amyla: SanBarth (A), JasmineS (A), Westmore (C), Ibom (F offshore buoy)
AMYLA_PERMITTED_STORAGES = (
    STORAGE_PRIMARY_NAME,       # SanBarth   — Point A
    STORAGE_SECONDARY_NAME,     # JasmineS — Point A
    STORAGE_TERTIARY_NAME,      # Westmore — Point C
    "Ibom",                     # Point F  — offshore buoy
)

# ── SECTION C: PRODUCTION RATES (bbl/hr) ─────────────────────────────────────
#  Storage          Rate (bph)   Notes
# ─────────────────────────────────────────────────────────────────────────────
PRODUCTION_RATE_BPH          = 1_600   # SanBarth & JasmineS (Point A)
WESTMORE_PRODUCTION_RATE_BPH =   960   # Westmore (Point C)
DUKE_PRODUCTION_RATE_BPH     =   250   # Duke (Point D)
STARTURN_PRODUCTION_RATE_BPH =    125   # Starturn (Point E)
PGM_PRODUCTION_RATE_BPH      =     80   # PGM (Point G) — 80 bbl/hour; SantaMonica only
POINT_F_LOAD_RATE_BPH        =   165   # Ibom offshore buoy (Point F) — also production rate

# ── SECTION D: STORAGE CAPACITIES (bbl) ──────────────────────────────────────
#  Storage          Capacity (bbl)   Notes
# ─────────────────────────────────────────────────────────────────────────────
STORAGE_CAPACITY_BBL          = 270_000   # generic default for any unlisted tank
SANBARTH_STORAGE_CAPACITY_BBL = 400_000   # SanBarth — Point A floating storage
DUKE_STORAGE_CAPACITY_BBL     =  90_000   # Duke
STARTURN_STORAGE_CAPACITY_BBL =  70_000   # Starturn
PGM_STORAGE_CAPACITY_BBL      =  28_000   # PGM (Point G) — SantaMonica only
# Westmore max stock is set to 220,000 in STORAGE_CAPACITY_BY_NAME (see below).
# WESTMORE_STORAGE_CAPACITY_BBL = 220_000

# ── SECTION E: MOTHER VESSEL CAPACITIES (bbl) ────────────────────────────────
#  Mother           Capacity (bbl)   Notes
# ─────────────────────────────────────────────────────────────────────────────
MOTHER_CAPACITY_BBL  = 550_000   # Bryanston (default for primary mothers)
#                                  # Alkebulan and GreenEagle capacities set below

# ── SECTION F: LOADING RATES (bbl/hr at each storage) ────────────────────────
#  Storage          Rate (bph)   Notes
# ─────────────────────────────────────────────────────────────────────────────
SANBARTH_LOAD_RATE_BPH      = 6_538   # Standard SanBarth rate (85,000 bbl / 13 h)
SANBARTH_LOAD_RATE_SLOW_BPH = 5_000   # Reduced rate for SANBARTH_SLOW_LOADERS
JASMINES_LOAD_RATE_BPH    = 4_000   # JasmineS (85,000 bbl / 21.25 h)
WESTMORE_LOAD_RATE_BPH    = 2_000   # Westmore
DUKE_LOAD_RATE_BPH        = 3_500   # Duke
STARTURN_LOAD_RATE_BPH    = 2_500   # Starturn
PGM_LOAD_RATE_BPH         = 270   # PGM (Point G) — pump rate; SantaMonica loads 9,500 bbl

# ── SECTION G: DISCHARGE / EXPORT RATES (bbl/hr) ─────────────────────────────
#  Operation         Rate (bph)   Notes
# ─────────────────────────────────────────────────────────────────────────────
EXPORT_RATE_BPH            = 20_000   # Mother vessel export pump rate

# Per-vessel discharge rates (bbl/hr) at Point B.
# Vessels absent from this dict use DISCHARGE_HOURS (fixed 12-hour default).
# When present, discharge duration = cargo_bbl / rate (dynamic).
VESSEL_DISCHARGE_RATE_BPH: dict = {
    "SantaMonica": 2_500,   # 2,500 bph → 28,000 bbl discharges in ~11.2 h
    "ZeeZee":      7_000,   # 7,000 bph — operator-specified discharge rate
    # Standard 85 k-class rate: 85,000 bbl / 12 h ≈ 7,083 bph.
    # Amyla (63 k) runs the same pump rate as Bedford — smaller cargo means shorter discharge.
    "Bedford":     7_083,   # 85,000 bbl class — 12 h full-load discharge
    "Balham":      7_083,   # 85,000 bbl class — 12 h full-load discharge
    "Sherlock":    7_083,   # 85,000 bbl class — 12 h full-load discharge
    "Laphroaig":   7_083,   # 85,000 bbl class — 12 h full-load discharge
    "Watson":      7_083,   # 85,000 bbl class — 12 h full-load discharge
    "Amyla":       7_083,   # same pump rate as Bedford (63 k cargo → ~8.9 h discharge)
    "Woodstock":   3_500,   # 42,000 bbl class — 12 h full-load discharge
    "Bagshot":     3_583,   # 43,000 bbl class — 12 h full-load discharge
    "Rathbone":    3_667,   # 44,000 bbl class — 12 h full-load discharge
    "Rahama":      4_000,   # 30,000 bbl class — operator-specified 4,000 bph
    "FatimaZarah": 4_167,   # 50,000 bbl class — 12 h full-load discharge
}
# ── SECTION H: POINT A LOAD CAP ──────────────────────────────────────────────
POINT_A_LOAD_CAP_BBL = 63_000   # Max load at Point A for POINT_A_LOAD_CAP_VESSELS

# =============================================================================
# ║  END OF CONFIG TABLE — do not edit below this line unless you know what  ║
# ║  you are doing. The simulation reads the constants above directly.        ║
# =============================================================================

# ── Internal: validation scenario flags (leave False unless testing) ──────────
POINT_B_DISTRIBUTION_TEST_MODE = False
POINT_B_DISTRIBUTION_TEST_DAYS = 3

# ── MULTIPLE TRANSIENT OPERATION (MTO) ───────────────────────────────────────
# When True: if >=2 shuttle vessels are stuck at Point B waiting (WAITING_BERTH_B
# or WAITING_MOTHER_CAPACITY) and cannot berth today (hard blockage or all berths
# occupied past daylight end), a mid-day (12:00) nomination fires once per day.
#
# The vessel with the most headroom in the MTO capacity table is nominated as
# transient storage. The smallest waiting shuttle transfers its full cargo into
# the transient (clamped to available headroom so the cap is never exceeded).
# The discharger is then freed to return and reload immediately.
#
# The transient vessel carries the accumulated cargo and discharges to a primary
# mother opportunistically — it checks for an available berth every hourly tick
# and takes the first window that opens, regardless of what day it is.
# It does NOT wait to reach its capacity limit before offloading.
#
# MTO_MAX_PARCELS_BEFORE_OFFLOAD controls only how many additional shuttle
# top-ups the transient may accept on subsequent congested days WHILE it is
# still waiting for a berth.  The capacity ceiling prevents overfilling.
#
# Set at runtime by run_sim() from the app toggle; default True so MTO is
# active from the first tick unless explicitly disabled by the operator.
MULTIPLE_TRANSIENT_OPERATION = True

# ── MTO TRANSIENT STORAGE CAPACITIES (bbl) ───────────────────────────────────
# Maximum volume a vessel may hold when acting as temporary storage at BIA.
# A discharger's transfer is clamped so the transient never exceeds this cap.
# Vessels absent from this dict use their normal cargo_capacity as the cap.
MTO_TRANSIENT_CAPACITY_BBL: dict = {
    "Balham":     125_000,
    "Bedford":    125_000,
    "Amyla":    125_000,
    "Bagshot":    125_000,
    "Laphroaig":  230_000,
    "Sherlock":   230_000,
    "Watson":     230_000,
    "Rathbone":    78_000,
    "SantaMonica": 35_000,
    "Woodstock":   95_000,
}

# ── MTO MULTI-PARCEL ACCUMULATION ────────────────────────────────────────────
# Controls how many additional shuttle cargoes the transient vessel may accept
# on subsequent congested days while it is still waiting for a mother berth.
# This is NOT a "fill before offload" target — the transient discharges
# opportunistically as soon as any mother berth opens, whatever its volume.
# Setting this higher lets the transient absorb more stranded cargoes on
# prolonged congested periods (e.g. mother away at export for 2+ days).
# The optimizer sweeps this parameter when MTO is enabled.
# How long an MTO transient that has reached its parcel limit may sit unable to
# claim ANY primary berth before it is allowed to *queue* for the soonest-freeing
# occupied primary (waiting its turn, never displacing an active incumbent).
# Below this threshold the transient just rechecks normally, so ordinary berth
# waits are not perturbed; only a genuinely stuck transient (whose only space-
# having primary is continuously busy) escalates so its cargo is not stranded.
MTO_OFFLOAD_STUCK_ESCALATION_HOURS: float = 24.0   # 1 day

MTO_MAX_PARCELS_BEFORE_OFFLOAD: int = 1       # base value when primaries available
MTO_MAX_PARCELS_ESCALATED:     int = 3       # raised when BOTH primaries are down

# ── Internal: derived values (auto-computed from config table above) ──────────
NUM_DAUGHTERS             = len(VESSEL_NAMES)
MAX_DAUGHTER_CARGO        = max(VESSEL_CAPACITIES.values(), default=DAUGHTER_CARGO_BBL)
MIN_INCOMING_TRANSFER_BBL = min(VESSEL_CAPACITIES.values(), default=DAUGHTER_CARGO_BBL)

# ── Internal: initialisation defaults (overridden by app at runtime) ─────────
# Storage defaults: 80% of standard 270k SanBarth/JasmineS capacity.
# Per-tank 80% values are applied in run_sim() using each tank's actual capacity.
STORAGE_INIT_BBL = 216_000   # generic standalone default; the app applies
                             # per-tank 80%-of-capacity values at run time.
# Mother startup stock defaults — mirrors the app's UI defaults so that running
# the simulation standalone produces the same initial conditions as the app.
# run_sim() overwrites these after construction with the operator-supplied values,
# so this dict only affects direct Simulation() instantiation (e.g. tests, scripts).
MOTHER_INIT_BBL_BY_NAME = {
    "Bryanston":  450_000,   # app default: ~82% of 550k capacity
    "GreenEagle": 300_000,   # app default: ~40% of 750k capacity
    "Alkebulan":  300_000,   # app default: ~40% of 750k capacity (clone of GreenEagle)
}
MOTHER_INIT_BBL  = 0   # legacy scalar; used as fallback for any unlisted mother

# ── Internal: dead-stock and dispatch tuning ──────────────────────────────────
DEAD_STOCK_FACTOR         = 1.75   # vessel waits until 175% of cargo is available
DEAD_STOCK_MAX_WAIT_HOURS = 12.0   # abort dead-stock wait after this many hours

# Per-storage dead-stock factor overrides.
# High-production tanks (SanBarth/JasmineS) can sustain the 1.75 default.
# Small, slow-filling tanks must dispatch sooner so vessels never wait
# indefinitely at a nearly-full small tank.
# PGM uses 1.0 — SantaMonica loads every cycle regardless of dead-stock.
DEAD_STOCK_FACTOR_BY_STORAGE: dict = {
    # "SanBarth"    : 1.75,   # default — omit to use DEAD_STOCK_FACTOR
    # "JasmineS"  : 1.75,   # default — omit to use DEAD_STOCK_FACTOR
    "Westmore"   : 1.50,
    "Duke"       : 1.25,
    "Starturn"   : 1.25,
    "PGM"        : 1.00,
}
DUKE_STARTURN_DEAD_STOCK_BBL = 5_000
DUKE_MIN_REMAINING_BBL    = 5_000
STARTURN_MIN_REMAINING_BBL = 5_000
PGM_MIN_REMAINING_BBL      = 2_000   # PGM dead-stock reserve (small tank)

# ── Internal: load-cap multipliers (JasmineS oversizes, Westmore undersizes) ─
JASMINES_LOAD_CAP_MULTIPLIER  = 1.08
WESTMORE_LOAD_CAP_OFFSET_BBL  = 1_000    # Point C: vessels load exactly 1,000 bbl below normal capacity

# ── Internal: Point F (Ibom) tuning ──────────────────────────────────────────
POINT_F_SWAP_HOURS          = 2
POINT_F_MIN_TRIGGER_BBL     = 65_000
STARTURN_PRE_TANK_TOP_TRIGGER_RATIO = 0.90
DUKE_PRE_TANK_TOP_TRIGGER_RATIO     = 0.90
PRE_TANK_TOP_TRIGGER_RATIO_DEFAULT  = 0.90

# ── Internal: operational timing (hours) ─────────────────────────────────────
HOSE_CONNECTION_HOURS  = 2.0
LOAD_HOURS             = 12
DISCHARGE_HOURS        = 12
CAST_OFF_HOURS         = 0.2
BERTHING_DELAY_HOURS   = 0.5

# =============================================================================
# ── STOCHASTIC VARIABILITY & REALISM ENGINE ───────────────────────────────────
#
# Converts deterministic fixed-duration operations to realistic probabilistic
# ones, reflecting the variability observed in real offshore logistics.
#
# ENABLE_VARIABILITY = False  → original deterministic behaviour (default).
# ENABLE_VARIABILITY = True   → durations sampled from calibrated distributions;
#                               weather disruptions and equipment delays applied.
#
# All distributions are parameterised as (nominal_hours, cv) where cv is the
# coefficient of variation (std_dev / mean).  A triangular distribution is used
# throughout because it is fully specified by three intuitive field parameters
# (minimum, mode, maximum) and never produces negative durations.
#
# CALIBRATION GUIDE — replace the default cv values with values derived from
# your historical port records once available:
#   cv = 0.05  → low variability  (well-controlled operation, modern port)
#   cv = 0.15  → medium           (typical offshore field operation)
#   cv = 0.25  → high             (ageing equipment, adverse conditions)
#   cv = 0.40  → severe           (congested or weather-exposed location)
# =============================================================================

ENABLE_VARIABILITY           = False   # master switch — set True to enable

# ── Per-operation coefficient of variation (cv = σ/μ) ─────────────────────────
VARIABILITY_CV_LOADING         = 0.15   # loading duration: pump-rate uncertainty
VARIABILITY_CV_DISCHARGE       = 0.12   # discharge at BIA: hose/valve variability
VARIABILITY_CV_TRANSIT         = 0.10   # passage time: current/weather/speed
VARIABILITY_CV_BERTHING        = 0.20   # manoeuvring delay: pilot availability
VARIABILITY_CV_HOSE_CONNECT    = 0.18   # hose connection: crew readiness
VARIABILITY_CV_CAST_OFF        = 0.15   # cast-off: weather, line handling
VARIABILITY_CV_EXPORT_DOC      = 0.20   # documentation: port office workload
VARIABILITY_CV_FENDER_PREP     = 0.15   # fender preparation

# ── Weather disruption model ───────────────────────────────────────────────────
# Each half-hour timestep has a WEATHER_PROB_PER_HOUR * TIME_STEP_HOURS chance
# of triggering a weather hold.  When triggered, operations are suspended for
# a duration drawn from an exponential distribution (mean = WEATHER_HOLD_MEAN_H).
# Only affects crossing/transit states; berthed operations are not interrupted.
WEATHER_PROB_PER_HOUR        = 0.02    # 2 %/h base probability (offshore typical)
WEATHER_HOLD_MEAN_H          = 3.0     # mean hold duration (hours)
WEATHER_HOLD_MAX_H           = 12.0    # cap: no single hold exceeds this

# ── Equipment breakdown / inspection delay model ──────────────────────────────
# Applied at loading point: random chance of an additional delay before the
# pump starts (equipment check, line flush, cargo measurement dispute).
EQUIP_DELAY_PROB_PER_LOAD    = 0.08    # 8 % of loads experience a delay event
EQUIP_DELAY_MEAN_H           = 1.5     # mean delay duration (hours)
EQUIP_DELAY_MAX_H            = 6.0     # cap: inspection/repair ceiling

# ── Human decision lag ────────────────────────────────────────────────────────
# Represents the lag between a vessel becoming operationally ready and the
# actual command being issued (shift handover, communications gap, paperwork).
HUMAN_LAG_PROB               = 0.12    # 12 % of berthing events experience lag
HUMAN_LAG_MEAN_H             = 0.5     # mean lag (hours)
HUMAN_LAG_MAX_H              = 2.0     # cap

# ── Congestion multiplier ─────────────────────────────────────────────────────
# When multiple vessels are waiting at BIA (≥ CONGESTION_THRESHOLD vessels in
# WAITING_BERTH_B), all berthing and hose-connection durations are multiplied
# by CONGESTION_FACTOR to model the real-world slowdown from crowded anchorage,
# increased marine traffic, and stretched port resources.
CONGESTION_THRESHOLD         = 3       # vessels at BIA before congestion applies
CONGESTION_FACTOR            = 1.20    # 20 % duration penalty under congestion

# ── Production rate variability ───────────────────────────────────────────────
# Field production is not perfectly constant. Each daily pre-ops reassessment
# applies a small perturbation to the effective production rate.
PRODUCTION_VARIABILITY_CV    = 0.05    # 5 % day-to-day production fluctuation

# ── Random seed for reproducibility ──────────────────────────────────────────
# Set to an integer for reproducible runs (useful for calibration).
# Set to None for a fresh random sequence each run.
VARIABILITY_RANDOM_SEED      = None

def _variability_sample(nominal_h: float, cv: float) -> float:
    """Return a sampled duration from a triangular distribution.

    The triangular is parameterised so that:
        mode = nominal_h
        min  = nominal_h * max(0.01, 1.0 - 2*cv)  (floor at 1% of nominal)
        max  = nominal_h * (1.0 + 2*cv)

    This gives a plausible spread: for cv=0.15 a 2h nominal operation samples
    roughly between 1.4 h and 2.6 h with mode 2 h.

    Returns nominal_h unchanged when ENABLE_VARIABILITY is False.
    """
    if not ENABLE_VARIABILITY or nominal_h <= 0:
        return nominal_h
    lo  = nominal_h * max(0.01, 1.0 - 2.0 * cv)
    hi  = nominal_h * (1.0 + 2.0 * cv)
    return random.triangular(lo, hi, nominal_h)


def _weather_hold_hours() -> float:
    """Return a weather hold duration (0 if no event) at the current timestep.

    Probability is scaled to TIME_STEP_HOURS so results are independent of
    timestep size.  Returns 0 when ENABLE_VARIABILITY is False.
    """
    if not ENABLE_VARIABILITY:
        return 0.0
    if random.random() < WEATHER_PROB_PER_HOUR * TIME_STEP_HOURS:
        hold = random.expovariate(1.0 / WEATHER_HOLD_MEAN_H)
        return min(hold, WEATHER_HOLD_MAX_H)
    return 0.0


def _equipment_delay_hours() -> float:
    """Return an equipment/inspection delay at load start (0 = no event)."""
    if not ENABLE_VARIABILITY:
        return 0.0
    if random.random() < EQUIP_DELAY_PROB_PER_LOAD:
        delay = random.expovariate(1.0 / EQUIP_DELAY_MEAN_H)
        return min(delay, EQUIP_DELAY_MAX_H)
    return 0.0


def _human_lag_hours() -> float:
    """Return a human decision lag at berthing (0 = no event)."""
    if not ENABLE_VARIABILITY:
        return 0.0
    if random.random() < HUMAN_LAG_PROB:
        lag = random.expovariate(1.0 / HUMAN_LAG_MEAN_H)
        return min(lag, HUMAN_LAG_MAX_H)
    return 0.0


def _congestion_factor(n_waiting: int) -> float:
    """Return the duration multiplier under port congestion."""
    if not ENABLE_VARIABILITY or n_waiting < CONGESTION_THRESHOLD:
        return 1.0
    return CONGESTION_FACTOR


# =============================================================================
# ── CALIBRATION & VALIDATION TRACKING ────────────────────────────────────────
#
# SimulationStats collects planned-vs-actual durations for each operation
# type.  At the end of a run, .calibration_report() returns a dict suitable
# for display in the dashboard or export to CSV.
# =============================================================================

class SimulationStats:
    """Accumulates planned vs actual operation durations for calibration."""

    def __init__(self):
        self._records: list = []   # (operation, planned_h, actual_h)

    def record(self, operation: str, planned_h: float, actual_h: float) -> None:
        self._records.append((operation, planned_h, actual_h))

    def calibration_report(self) -> dict:
        """Return per-operation mean bias and RMSE between planned and actual."""
        from collections import defaultdict
        import math
        buckets = defaultdict(list)
        for op, planned, actual in self._records:
            buckets[op].append((planned, actual))
        report = {}
        for op, pairs in buckets.items():
            n         = len(pairs)
            bias      = sum(a - p for p, a in pairs) / n
            rmse      = math.sqrt(sum((a - p)**2 for p, a in pairs) / n)
            mean_plan = sum(p for p, _ in pairs) / n
            mean_act  = sum(a for _, a in pairs) / n
            report[op] = {
                "n":         n,
                "mean_planned_h": round(mean_plan, 3),
                "mean_actual_h":  round(mean_act,  3),
                "mean_bias_h":    round(bias,  3),
                "rmse_h":         round(rmse,  3),
                "pct_bias":       round(100 * bias / mean_plan, 1) if mean_plan else 0,
            }
        return report

def _berth_free_at(pump_end_sim_hour: float) -> float:
    """Return the sim-hour at which the mother berth is truly free after a discharge.

    A vessel is physically alongside the mother until cast-off completes.
    Cast-off is constrained to the window [CAST_OFF_START, CAST_OFF_END) wall-clock.
    When pumping finishes after CAST_OFF_END (e.g. 23:56) the vessel cannot cast
    off until the next morning at CAST_OFF_START (06:00), so the berth remains
    occupied overnight — locking out any other vessel until then.

    Using a flat ``+ CAST_OFF_HOURS`` incorrectly frees the berth at pump_end + 0.2h
    regardless of whether the nighttime restriction delays cast-off by 6-12 hours.
    """
    wall_at_pump_end = (pump_end_sim_hour + SIM_HOUR_OFFSET) % 24
    if CAST_OFF_START <= wall_at_pump_end < CAST_OFF_END:
        # Pump ends inside cast-off window — cast off immediately
        cast_off_t = pump_end_sim_hour
    else:
        # Pump ends outside cast-off window — roll forward to next window open
        days_elapsed = int(pump_end_sim_hour // 24)
        sim_co_today = days_elapsed * 24 + (CAST_OFF_START - SIM_HOUR_OFFSET)
        if pump_end_sim_hour <= sim_co_today:
            cast_off_t = sim_co_today
        else:
            cast_off_t = sim_co_today + 24  # next calendar day
    return cast_off_t + CAST_OFF_HOURS
POST_BERTHING_START_GAP_HOURS         = 0.5
POST_MOTHER_BERTHING_START_GAP_HOURS  = 1.0

# ── Internal: daylight / berthing windows ────────────────────────────────────
CAST_OFF_START   = 6
CAST_OFF_END     = 17.5
BERTHING_START   = 6
BERTHING_END     = 18
DAYLIGHT_START   = 6
DAYLIGHT_END     = 18

# ── Internal: export operation timing ────────────────────────────────────────
EXPORT_DOC_HOURS            = 2
EXPORT_SAIL_HOURS           = 6
# Export phases during which a mother is NOT physically available at BIA and must
# never receive cargo (daughter discharge / MTO offload).  RETURNING
# covers the post-export return sail + fendering window — including it is what
# prevents loading a mother that finished exporting but is still hours away at the
# terminal or fendering on arrival.
EXPORT_BUSY_STATES = frozenset({"DOC", "SAILING", "HOSE", "IN_PORT", "RETURNING"})
EXPORT_SAIL_WINDOW_START    = 6
EXPORT_SAIL_WINDOW_END      = 15
EXPORT_HOSE_HOURS           = 4
EXPORT_SERIES_BUFFER_HOURS  = 48
# Minimum idle time (hours) required after the last daughter cast-off before the
# export DOC may fire on a primary mother.  This window gives operators and the
# sim time to confirm the mother's final intake volume before documentation starts.
# Applies to both natural (export_ready) and forced export departures.
EXPORT_INTAKE_BUFFER_HOURS  = 2.0
MOTHER_EXPORT_VOLUME        = 400_000

# Export departure look-ahead: if ≥ this many daughters are inbound/waiting
# at BIA in the next EXPORT_LOOKFORWARD_HOURS, defer departure unless the
# mother is physically full (cannot accept another cargo).
EXPORT_DEFER_INBOUND_THRESHOLD  = 3       # defer if ≥3 daughters inbound
EXPORT_LOOKFORWARD_HOURS        = 36      # look 36 h ahead for inbound daughters

# ── Internal: route leg durations (hours) ────────────────────────────────────
# Point A/C ↔ BIA
SAIL_HOURS_A_TO_BW      = 1.5   # Point A/C → Breakwater
SAIL_HOURS_CROSS_BW_AC  = 0.5   # Cross Breakwater (daylight/tidal)
SAIL_HOURS_BW_TO_FWY    = 2.0   # Breakwater → Fairway Buoy
SAIL_HOURS_FWY_TO_B     = 2.0   # Fairway Buoy → BIA
SAIL_HOURS_B_TO_FWY     = 2.0   # BIA → Fairway Buoy
SAIL_HOURS_FWY_TO_BW    = 2.0   # Fairway Buoy → Breakwater
SAIL_HOURS_BW_TO_A      = 1.5   # Breakwater → Point A/C
SAIL_HOURS_A_TO_B = SAIL_HOURS_A_TO_BW + SAIL_HOURS_CROSS_BW_AC + SAIL_HOURS_BW_TO_FWY + SAIL_HOURS_FWY_TO_B
SAIL_HOURS_B_TO_A = SAIL_HOURS_B_TO_FWY + SAIL_HOURS_FWY_TO_BW + SAIL_HOURS_CROSS_BW_AC + SAIL_HOURS_BW_TO_A
SAIL_HOURS_B_TO_F       = 3     # BIA → Ibom
# Point D ↔ BIA
SAIL_HOURS_D_TO_CH      = 3.0   # Point D → Cawthorne Channel
SAIL_HOURS_CH_TO_BW_OUT = 1.0   # Cawthorne Channel → Breakwater
SAIL_HOURS_CROSS_BW     = 0.5   # Cross Breakwater
SAIL_HOURS_BW_TO_B      = 1.5   # Breakwater → BIA
SAIL_HOURS_B_TO_BW      = 1.5   # BIA → Breakwater
SAIL_HOURS_BW_TO_CH_IN  = 1.0   # Breakwater → Cawthorne Channel
SAIL_HOURS_CH_TO_D      = 3.0   # Cawthorne Channel → Point D
SAIL_HOURS_D_TO_CHANNEL = SAIL_HOURS_D_TO_CH
SAIL_HOURS_CHANNEL_TO_B = SAIL_HOURS_CH_TO_BW_OUT + SAIL_HOURS_CROSS_BW + SAIL_HOURS_BW_TO_B


def _sail_leg(nominal_h: float, sim=None) -> float:
    """Return a (possibly stochastic) duration for a sailing leg.

    When ENABLE_VARIABILITY is True, transit duration is sampled from a
    triangular distribution (cv = VARIABILITY_CV_TRANSIT) and a weather
    hold event may be added.  The weather hold total is accumulated on
    sim._weather_hold_hours_total when a Simulation instance is passed.

    Used throughout the sailing state handlers as a drop-in replacement
    for bare SAIL_HOURS_* constants.  When variability is disabled this
    returns nominal_h unchanged, making the function zero-overhead in
    deterministic mode.
    """
    hold   = _weather_hold_hours()
    actual = _variability_sample(nominal_h, VARIABILITY_CV_TRANSIT) + hold
    if hold > 0 and sim is not None and hasattr(sim, "_weather_hold_hours_total"):
        sim._weather_hold_hours_total += hold
        if hasattr(sim, "_sim_stats"):
            sim._sim_stats.record("weather_hold", 0.0, hold)
    if sim is not None and hasattr(sim, "_sim_stats") and ENABLE_VARIABILITY:
        sim._sim_stats.record("transit", nominal_h, actual)
    return actual

# ── SECTION I: MOTHER VESSELS ────────────────────────────────────────────────
#  Each row: (Name, Capacity bbl).
#  Add a new mother by adding a row and updating MOTHER_CAPACITY_BY_NAME below.
# ─────────────────────────────────────────────────────────────────────────────
#  Name             Capacity (bbl)   Notes
# ─────────────────────────────────────────────────────────────────────────────
MOTHER_PRIMARY_NAME    = "Bryanston"
MOTHER_SECONDARY_NAME  = "GreenEagle"
MOTHER_TERTIARY_NAME   = "GreenEagle"   # kept for legacy references — same vessel
MOTHER_QUINARY_NAME    = "Alkebulan"    # primary exporting mother at Point B (clone of GreenEagle)

GREENEAGLE_CAPACITY_BBL      = 750_000
GREENEAGLE_EXPORT_TRIGGER_BBL = 680_000
# Alkebulan is a primary mother at Point B with identical specification to
# GreenEagle (same capacity and export trigger).  Defined as clones so the two
# vessels always stay in sync if GreenEagle's figures are ever retuned.
ALKEBULAN_CAPACITY_BBL        = GREENEAGLE_CAPACITY_BBL
ALKEBULAN_EXPORT_TRIGGER_BBL  = GREENEAGLE_EXPORT_TRIGGER_BBL
# Bryanston uses MOTHER_CAPACITY_BBL (550,000) defined in Section E above.

# ── Internal: simulation time step ───────────────────────────────────────────
TIME_STEP_HOURS = 0.5


# ── SECTION J: THIRD-PARTY VESSEL — ZEEZEE ───────────────────────────────────
#  ZeeZee is an external tanker that discharges to a primary mother vessel at
#  Point B.  Her schedule is set by the Discharge Override Panel in the app.
#  The sim reads ZEEZEE_SCHEDULE at runtime (populated by run_sim each call).
#  Leave defaults here; the panel controls live values.
# ─────────────────────────────────────────────────────────────────────────────
#  ZEEZEE_SCHEDULE is a list of dicts, each representing one recurring visit:
#    {"day_of_month": int,   # 1-28 calendar day-of-month for arrival
#     "volume_bbl":   float, # cargo volume for that visit
#     "api":          float} # API gravity of cargo
#  Multiple entries allow different months to have different volumes/days.
#  Populated by the app via run_sim; never edited here directly.
ZEEZEE_SCHEDULE: list = []          # [{day_of_month, volume_bbl, api}, ...]
ZEEZEE_MAX_DAUGHTER_WAIT_HOURS = 48.0   # max delay caused by daughter queue

# Forced export departure schedule.
# Structure: {mother_name: [sim_hour_of_departure, ...]}
# Populated by run_sim from the operator's Force Export panel.
# When the run loop reaches a scheduled hour the named mother is forced
# into DOC state immediately (bypassing export_ready and eligibility tests).
# The mother sails, exports, and returns empty — exactly like a normal export.
EXPORT_FORCE_SCHEDULE: dict = {}   # {mother_name: [sim_hour, ...]}

def storage_adjusted_load_cap(base_cap, storage_name, vessel_name=None):
    """Return effective cargo loaded from a storage for a vessel.

    Storage-specific adjustments apply before any explicit operational caps.
    JasmineS loads 8% above the vessel's normal capacity.
    Westmore (Point C) loads exactly 1,000 bbl below the vessel's normal capacity.
    PGM (Point G) loads ~75% below the vessel's normal capacity (SantaMonica only, ~7k bbl).
    Explicit Point A caps for Bedford/Balham still take precedence.
    """
    cap = int(round(base_cap))
    if storage_name == STORAGE_SECONDARY_NAME:
        cap = int(round(base_cap * JASMINES_LOAD_CAP_MULTIPLIER))
    elif storage_name == STORAGE_TERTIARY_NAME:
        cap = max(0, int(round(base_cap)) - WESTMORE_LOAD_CAP_OFFSET_BBL)
    elif storage_name == STORAGE_SENARY_NAME:
        cap = min(int(round(base_cap)), 9_500)   # SantaMonica @ PGM — fixed 9,500 bbl loading limit
    if (vessel_name in POINT_A_LOAD_CAP_VESSELS
            and storage_name in {STORAGE_PRIMARY_NAME, STORAGE_SECONDARY_NAME}):
        cap = min(cap, POINT_A_LOAD_CAP_BBL)
    return max(0, cap)


def _default_vessel_base_capacity(vessel_name):
    return VESSEL_CAPACITIES.get(vessel_name, DAUGHTER_CARGO_BBL)


def _allowed_default_storages(vessel_name):
    if vessel_name in {"Bedford", "Balham"}:
        return [STORAGE_PRIMARY_NAME, STORAGE_SECONDARY_NAME, "Ibom"]
    if vessel_name == "Amyla":
        return list(AMYLA_PERMITTED_STORAGES)
    if vessel_name == "SantaMonica":
        return list(SANTAMONICA_PERMITTED_STORAGES)
    if vessel_name == "Watson":
        return list(WATSON_PERMITTED_STORAGES)
    if vessel_name == "Laphroaig":
        return list(LAPHROAIG_PERMITTED_STORAGES)
    if vessel_name in POINT_A_ONLY_VESSELS:
        return [STORAGE_PRIMARY_NAME, STORAGE_SECONDARY_NAME]
    return [
        STORAGE_PRIMARY_NAME,
        STORAGE_SECONDARY_NAME,
        *([STORAGE_TERTIARY_NAME] if vessel_name in WESTMORE_PERMITTED_VESSELS else []),
        *([STORAGE_QUATERNARY_NAME] if vessel_name in DUKE_PERMITTED_VESSELS else []),
        *([STORAGE_QUINARY_NAME] if vessel_name in STARTURN_PERMITTED_VESSELS else []),
        *([STORAGE_SENARY_NAME] if vessel_name in PGM_PERMITTED_VESSELS else []),
    ]


_DEFAULT_EFFECTIVE_LOAD_CAPS = []
for _vname in VESSEL_NAMES:
    _base_cap = _default_vessel_base_capacity(_vname)
    for _stor in _allowed_default_storages(_vname):
        if _stor == "Ibom":
            _DEFAULT_EFFECTIVE_LOAD_CAPS.append(_base_cap)
        else:
            _DEFAULT_EFFECTIVE_LOAD_CAPS.append(
                storage_adjusted_load_cap(_base_cap, _stor, _vname)
            )

if _DEFAULT_EFFECTIVE_LOAD_CAPS:
    MAX_DAUGHTER_CARGO = max(_DEFAULT_EFFECTIVE_LOAD_CAPS)
    MIN_INCOMING_TRANSFER_BBL = min(_DEFAULT_EFFECTIVE_LOAD_CAPS)
    MOTHER_EXPORT_TRIGGER = MOTHER_CAPACITY_BBL - MAX_DAUGHTER_CARGO

# -----------------------------------------------------------------
# CUSTOM VESSEL INJECTION
# -----------------------------------------------------------------
# Register mid-sim daughter vessels via add_custom_vessel() before
# instantiating Simulation().  Each vessel joins the fleet on the
# specified calendar date and participates fully in the dispatch cycle.
#
# Quick-start example:
#   add_custom_vessel(
#       name="Aldgate",
#       join_date="2025-02-10",        # or datetime.date(2025, 2, 10)
#       cargo_capacity=60_000,
#       permitted_storages=["SanBarth", "JasmineS", "Duke"],
#   )

@dataclass
class CustomVesselSpec:
    """Specification for a daughter vessel that joins mid-simulation."""
    name:               str
    join_date:          object          # date | datetime | "YYYY-MM-DD"
    cargo_capacity:     int             # barrels per voyage
    permitted_storages: list = _dc_field(default_factory=list)
    # Resolved at Simulation.__init__ time — not set by caller
    _join_hour: float  = _dc_field(default=None, init=False, repr=False)


# Module-level registry — populated by add_custom_vessel(); cleared by
# run_sim() before restoring to let Streamlit reruns stay isolated.
_CUSTOM_VESSELS: list = []


def add_custom_vessel(name, join_date, cargo_capacity, permitted_storages=None):
    """Register a new daughter vessel to enter the fleet on *join_date*.

    Parameters
    ----------
    name : str
        Unique vessel name.  Must not clash with any name in VESSEL_NAMES.
    join_date : datetime.date | datetime.datetime | str "YYYY-MM-DD"
        Calendar date the vessel becomes active.  Activates at 08:00 on
        that day, matching the simulation's daily start anchor.
    cargo_capacity : int
        Maximum cargo per voyage in barrels (e.g. 60_000).
    permitted_storages : list[str] | None
        Storage names the vessel may load from.  Valid values:
            "SanBarth"    (Point A)
            "JasmineS"  (Point A)
            "Westmore"  (Point C)
            "Duke"      (Point D)
            "Starturn"  (Point E)
        Pass None or [] to allow SanBarth and JasmineS only (safe default).
    """
    if permitted_storages is None:
        permitted_storages = []
    if isinstance(join_date, str):
        join_date = _date.fromisoformat(join_date)
    if name in VESSEL_NAMES:
        raise ValueError(
            f"add_custom_vessel: '{name}' already exists in VESSEL_NAMES."
        )
    _valid = {STORAGE_PRIMARY_NAME, STORAGE_SECONDARY_NAME,
              STORAGE_TERTIARY_NAME, STORAGE_QUATERNARY_NAME,
              STORAGE_QUINARY_NAME}
    _bad = [s for s in permitted_storages if s not in _valid]
    if _bad:
        raise ValueError(
            f"add_custom_vessel: unknown storage name(s) {_bad}. "
            f"Valid: {sorted(_valid)}"
        )
    _CUSTOM_VESSELS.append(CustomVesselSpec(
        name=name,
        join_date=join_date,
        cargo_capacity=int(cargo_capacity),
        permitted_storages=list(permitted_storages),
    ))

# ── Vessel resumption dates ────────────────────────────────────────────────
# Maps existing daughter vessel name → {date, storage}.  Set via
# set_vessel_resumption() before creating a Simulation().
# The vessel sleeps (held in IDLE_A) until 08:00 on the resumption date,
# then wakes with absolute loading priority at the designated storage,
# bypassing the serial berthing gap — but still respecting storage_berth_free_at
# to avoid physical conflicts.  All resumption state is cleared after the
# priority berth completes so the vessel returns to normal operation.
_VESSEL_RESUMPTION_DATES: dict = {}


def set_vessel_resumption(name: str, date_val, storage: str) -> None:
    """Register a resumption hold for an existing daughter vessel.

    Args:
        name:     Vessel name (must exist in VESSEL_NAMES).
        date_val: Resumption date — a date/datetime object or "YYYY-MM-DD" string.
                  The vessel will be held idle until 08:00 on this date.
        storage:  Storage to lock to on wake (one of the five storage names).
    """
    _valid_storages = {
        "SanBarth", "JasmineS", "Westmore", "Duke", "Starturn",
    }
    if name not in VESSEL_NAMES:
        raise ValueError(
            f"set_vessel_resumption: '{name}' is not a known vessel. "
            f"Known vessels: {VESSEL_NAMES}"
        )
    if storage not in _valid_storages:
        raise ValueError(
            f"set_vessel_resumption: unknown storage '{storage}'. "
            f"Valid storages: {sorted(_valid_storages)}"
        )
    if isinstance(date_val, str):
        from datetime import datetime as _dt
        date_val = _dt.fromisoformat(date_val).date()
    _VESSEL_RESUMPTION_DATES[name] = {"date": date_val, "storage": storage}


STORAGE_NAMES = [
    STORAGE_PRIMARY_NAME,
    STORAGE_SECONDARY_NAME,
    STORAGE_TERTIARY_NAME,
    STORAGE_QUATERNARY_NAME,
    STORAGE_QUINARY_NAME,
    STORAGE_SENARY_NAME,
]
STORAGE_POINT = {
    STORAGE_PRIMARY_NAME: "A",
    STORAGE_SECONDARY_NAME: "A",
    STORAGE_TERTIARY_NAME: "C",
    STORAGE_QUATERNARY_NAME: "D",
    STORAGE_QUINARY_NAME: "E",
    STORAGE_SENARY_NAME: "G",
}
# Per-vessel-day loading point overrides set by the JMP Override Panel in the app.
# Structure: {vessel_name: {day_number (int): storage_name (str)}}
# Applied before normal dispatch scoring so the forced assignment wins.
# Cleared and repopulated by run_sim on every call; never persists across runs.
STORAGE_DISPATCH_OVERRIDES: dict = {}

# Per-voyage discharge point overrides for daughter vessels.
#
# Structure (voyage-code keyed — preferred):
#   {voyage_code (str): {"vessel": str,
#                        "mother": str,
#                        "discharge_date": "YYYY-MM-DD"}}
#
# Legacy structure (vessel/day keyed — still supported):
#   {vessel_name (str): {day_key_0based (int): mother_name (str)}}
#
# Behaviour for voyage-code keyed entries:
#  - The override is matched against a vessel's current voyage_code.
#  - If the vessel arrives at BIA BEFORE discharge_date, she waits
#    (WAITING_BERTH_B) until that exact calendar date before berthing.
#  - When the date arrives the override takes priority and the mother
#    berth lock is reset, displacing any incumbent vessel to WAITING_BERTH_B.
#  - If no discharge_date is set the vessel berths at the earliest
#    opportunity on the first day she arrives at BIA.
#
# ZeeZee is unaffected -- controlled via ZEEZEE_SCHEDULE separately.
# Cleared and repopulated by run_sim on every call; never persists.
DAUGHTER_DISCHARGE_OVERRIDES: dict = {}

STORAGE_CAPACITY_BY_NAME = {name: STORAGE_CAPACITY_BBL for name in STORAGE_NAMES}
STORAGE_CAPACITY_BY_NAME[STORAGE_PRIMARY_NAME] = SANBARTH_STORAGE_CAPACITY_BBL
STORAGE_CAPACITY_BY_NAME[STORAGE_SECONDARY_NAME] = 290_000
STORAGE_CAPACITY_BY_NAME[STORAGE_TERTIARY_NAME] = 220_000   # Westmore max stock (reduced from 270k)
STORAGE_CAPACITY_BY_NAME[STORAGE_QUATERNARY_NAME] = DUKE_STORAGE_CAPACITY_BBL
STORAGE_CAPACITY_BY_NAME[STORAGE_QUINARY_NAME] = STARTURN_STORAGE_CAPACITY_BBL
STORAGE_CAPACITY_BY_NAME[STORAGE_SENARY_NAME] = PGM_STORAGE_CAPACITY_BBL
STORAGE_PRODUCTION_RATE_BY_NAME = {name: PRODUCTION_RATE_BPH for name in STORAGE_NAMES}
STORAGE_PRODUCTION_RATE_BY_NAME[STORAGE_TERTIARY_NAME] = WESTMORE_PRODUCTION_RATE_BPH
STORAGE_PRODUCTION_RATE_BY_NAME[STORAGE_QUATERNARY_NAME] = DUKE_PRODUCTION_RATE_BPH
STORAGE_PRODUCTION_RATE_BY_NAME[STORAGE_QUINARY_NAME] = STARTURN_PRODUCTION_RATE_BPH
STORAGE_PRODUCTION_RATE_BY_NAME[STORAGE_SENARY_NAME] = PGM_PRODUCTION_RATE_BPH

# -----------------------------------------------------------------
# DISPATCH BIAS — production-rate preference & position-aware spread
# -----------------------------------------------------------------
# High-production storages (SanBarth/JasmineS/Westmore) get a small apparent
# gap tightening so they are sorted as more urgent than low-production peers
# (Duke/Starturn) at similar real risk levels.  Only active within
# DISPATCH_BIAS_FORECAST_BBL of critical so it never overrides a genuine
# emergency at Duke/Starturn.
DISPATCH_BIAS_FORECAST_BBL  = 150_000  # window inside which bias activates
DISPATCH_BIAS_MAX_FACTOR    = 0.22     # max apparent-gap compression (22 %)

# Route-area travel-time matrix used by position-aware spread forecasting.
# Keys are (from_area, to_area) as single-char strings matching STORAGE_POINT.
# Values are minimum travel hours (conservative lower bound, no waiting).
# A vessel *already at* the same area has 0h travel.
_ROUTE_TRAVEL_HOURS = {
    ("A", "A"): 0.0,
    ("A", "C"): 0.0,   # Points A and C share the same breakwater approach
    ("C", "A"): 0.0,
    ("C", "C"): 0.0,
    ("A", "B"): 7.0,   # A/C → BIA  (1.5 + 0.5 + 2 + 2, tidal delays ignored)
    ("C", "B"): 7.0,
    ("B", "A"): 8.0,   # BIA → A/C  (2 + 2 + 0.5 + 1.5)
    ("B", "C"): 8.0,
    ("D", "B"): 6.0,   # D → BIA    (3 + 1 + 0.5 + 1.5)
    ("B", "D"): 6.0,   # BIA → D    (1.5 + 0.5 + 1 + 3)
    ("E", "B"): 3.0,   # Starturn direct
    ("B", "E"): 3.0,
    ("G", "B"): 3.0,   # PGM direct (same corridor as Starturn)
    ("B", "G"): 3.0,
    ("D", "A"): 14.0,  # D → BIA + BIA → A  (conservative, through BIA)
    ("D", "C"): 14.0,
    ("A", "D"): 14.0,
    ("C", "D"): 14.0,
    ("D", "E"): 9.0,   # D → BIA → E  (conservative)
    ("E", "D"): 9.0,
    ("E", "A"): 11.0,
    ("E", "C"): 11.0,
    ("A", "E"): 11.0,
    ("C", "E"): 11.0,
    ("G", "A"): 11.0,  # PGM → BIA → A (conservative)
    ("A", "G"): 11.0,
    ("G", "C"): 11.0,
    ("C", "G"): 11.0,
    ("G", "D"): 9.0,
    ("D", "G"): 9.0,
    ("G", "E"): 6.0,
    ("E", "G"): 6.0,
    ("G", "G"): 0.0,
}

# Spread to D/E is suppressed unless the projected stock at D/E will be below
# critical within SPREAD_DE_URGENCY_HORIZON hours of the vessel's ETA there.
SPREAD_DE_URGENCY_HORIZON = 12.0   # hours ahead to check — tighter gate suppresses more D/E spreads

# Optional: if A/C high-production storage is itself approaching critical
# within this window, hold A/C vessels back even if D/E is also in need.
SPREAD_AC_HOLD_HORIZON = 36.0

# Optional temporary production overrides by date window.
# Format:
# [
#   {
#     "start_date": "YYYY-MM-DD",
#     "end_date": "YYYY-MM-DD",
#     "rates": {"SanBarth": 0, "JasmineS": 0, ...}
#   }
# ]
PRODUCTION_RATE_OVERRIDES = []
STORAGE_CRITICAL_THRESHOLD_BY_NAME = {
    STORAGE_PRIMARY_NAME: SANBARTH_STORAGE_CAPACITY_BBL,
    STORAGE_SECONDARY_NAME: 290_000,
    STORAGE_TERTIARY_NAME: 175_000,   # Unsafe when >175k (reduced from 225k)
    STORAGE_QUATERNARY_NAME: 90_000,
    STORAGE_QUINARY_NAME: 70_000,
    STORAGE_SENARY_NAME: 28_000,      # PGM — full capacity is the trigger (small tank)
}
MOTHER_NAMES = [
    MOTHER_PRIMARY_NAME,      # Bryanston  (primary, exports)
    MOTHER_SECONDARY_NAME,    # GreenEagle (primary, exports)
    MOTHER_QUINARY_NAME,      # Alkebulan  (primary, exports — clone of GreenEagle)
]
MOTHER_CAPACITY_BY_NAME = {
    MOTHER_PRIMARY_NAME:    MOTHER_CAPACITY_BBL,       # Bryanston  — Section E
    MOTHER_SECONDARY_NAME:  GREENEAGLE_CAPACITY_BBL,   # GreenEagle — Section I
    MOTHER_QUINARY_NAME:    ALKEBULAN_CAPACITY_BBL,    # Alkebulan  — Section I (== GreenEagle)
}
MOTHER_EXPORT_TRIGGER_BY_NAME = {
    MOTHER_PRIMARY_NAME:   MOTHER_EXPORT_TRIGGER,
    MOTHER_SECONDARY_NAME: GREENEAGLE_EXPORT_TRIGGER_BBL,
    MOTHER_QUINARY_NAME:   ALKEBULAN_EXPORT_TRIGGER_BBL,
}

# Startup-day (Day 1) Point B nomination override.
# When enabled, Point B auto-prioritization is disabled only on Day 1 and
# assignment must come from this manual vessel->mother mapping.
# Day 2+ always uses the standard strict Point B prioritization rules.
STARTUP_DAY_DISABLE_POINT_B_PRIORITY = False
STARTUP_DAY_POINT_B_MANUAL_NOMINATIONS = {
    # Safety-net for Balham startup: it is seeded in BERTHING_B at GreenEagle
    # from Day 1.  If it is ever displaced back to WAITING_BERTH_B on Day 1
    # (e.g. by a concurrent-berth edge case), this nomination ensures it returns
    # to GreenEagle rather than waiting indefinitely for auto-prioritisation.
    "Balham": MOTHER_SECONDARY_NAME,   # GreenEagle — mirrors app Day-1 position
    "Woodstock": MOTHER_PRIMARY_NAME,  # Bryanston — default Day-1 discharge pairing
}

# Test seed: force selected vessels to Point B at full load on startup.
POINT_B_TEST_STARTUP_FULL_LOAD_NOMINATIONS = {
    # Example:
    # "Sherlock": MOTHER_PRIMARY_NAME,
}

# -----------------------------------------------------------------
# STATE TRACKING
# -----------------------------------------------------------------
STATUS_CODES = {
    "IDLE_A"            : "Idle at assigned loading point (A/C/D/E)",
    "WAITING_BERTH_A"   : "Waiting for berthing window at assigned loading point",
    "BERTHING_A"        : "Berthing at assigned loading point",
    "HOSE_CONNECT_A"    : "Hose connection at assigned loading point",
    "LOADING"           : "Loading at assigned loading point",
    "DOCUMENTING"       : "Documentation after loading",
    "CAST_OFF"          : "Cast-off from storage vessel",
    "CAST_OFF_B"        : "Cast-off from mother vessel",
    "WAITING_CAST_OFF"  : "Waiting for cast-off window",
    "EXPORT_DOC"        : "Mother export documentation",
    "EXPORT_INTAKE_BUFFER" : "Export ready but waiting for post-discharge intake buffer (2h settling)",
    "CONCURRENT_BERTH_ABORT" : "Concurrent berth guard: vessel aborted — another actor already occupies this berth",
    "EXPORT_SAIL"       : "Sailing to export terminal",
    "EXPORT_HOSE"       : "Hose connection at export terminal",
    "SAILING_AB"          : "Sailing A/C -> Breakwater (1.5h)",
    "SAILING_CROSS_BW_AC" : "Crossing Breakwater A/C outbound (0.5h, tidal)",
    "SAILING_BW_TO_FWY"   : "After crossing -> Fairway Buoy (2h)",
    "SAILING_AB_LEG2"     : "Sailing Fairway Buoy -> BIA (2h)",
    "SAILING_B_TO_FWY"    : "Returning BIA -> Fairway Buoy (2h)",
    "SAILING_FWY_TO_BW"   : "Fairway Buoy -> Breakwater (2h)",
    "SAILING_CROSS_BW_IN_AC": "Crossing Breakwater A/C inbound (0.5h, tidal)",
    "SAILING_BW_TO_A"     : "After crossing -> Point A/C (1.5h)",
    "SAILING_D_CHANNEL"          : "Sailing D -> Cawthorne Channel (3h)",
    "SAILING_CH_TO_BW_OUT"       : "Sailing Cawthorne Ch -> Breakwater (1h)",
    "SAILING_CROSS_BW_OUT"       : "Crossing Breakwater outbound (0.5h)",
    "SAILING_BW_TO_B"            : "Sailing clear breakwater -> BIA (1.5h)",
    "SAILING_B_TO_BW_IN"         : "Sailing BIA -> clear breakwater (1.5h)",
    "SAILING_CROSS_BW_IN"        : "Crossing Breakwater inbound (0.5h)",
    "SAILING_BW_TO_CH_IN"        : "Sailing Breakwater -> Cawthorne Ch (1h)",
    "SAILING_CH_TO_D"            : "Sailing Cawthorne Ch -> Point D (3h)",
    "WAITING_BERTH_B"   : "Waiting for berthing window at Point B mother",
    "BERTHING_B"        : "Berthing at Point B mother",
    "HOSE_CONNECT_B"    : "Hose connection at Point B mother",
    "IDLE_B"            : "Idle at Point B mother",
    "DISCHARGING"       : "Discharging to Point B mother",
    "SAILING_BA"        : "Returning B -> selected loading point (A/C/D/E/G)",
    "WAITING_DAYLIGHT"  : "Waiting for Daylight Window",
    "WAITING_FAIRWAY"   : "Waiting at Fairway Buoy",
    "WAITING_MOTHER_CAPACITY" : "Waiting for space on mother vessel",
    "WAITING_MOTHER_RETURN" : "Waiting for mother to return from export",
    "WAITING_DEAD_STOCK"    : "Berthed — waiting for dead-stock threshold",
    "WAITING_RETURN_STOCK"  : "Waiting at Point B for return destination assignment",
    "SAILING_B_TO_F"        : "Sailing BIA -> Ibom (swap takeover)",
    "PF_LOADING"            : "Loading at Point F",
    "PF_SWAP"               : "Point F swap/takeover in progress",
    # Mid-simulation mother unavailability window
    "MOTHER_UNAVAILABLE_START" : "Mother vessel entered scheduled unavailability window",
    "MOTHER_UNAVAILABLE_END"   : "Mother vessel exited scheduled unavailability window — resuming operations",
    # Multiple Transient Operation events
    "MTO_TRANSIENT_NOMINATED"      : "MTO: vessel nominated as temporary storage at Point B",
    "MTO_DISCHARGE_TO_TRANSIENT"   : "MTO: vessel discharging cargo to transient storage vessel",
    "MTO_TRANSFER_COMPLETE"        : "MTO: vessel-to-vessel cargo transfer complete",
    "MTO_TRANSIENT_PRIORITY_BERTH" : "MTO: transient storage vessel claiming priority berth at mother",
    "MTO_PARCEL_LIMIT_REACHED"     : "MTO: transient vessel reached max parcel count — forcing offload",
    "MTO_TRANSIENT_CAP_REACHED"    : "MTO: transient vessel at storage capacity — forcing offload",
    "MTO_ABORT_INSUFFICIENT_SPACE" : "MTO: regulatory abort — mother lacks space for full cargo; re-anchoring",
    "MTO_REANCHOR"                 : "MTO: transient vessel re-anchoring at BIA — awaiting qualifying mother",
    # Universal hard-capacity abort (replaces the old GreenEagle-only GREENEAGLE_CAPACITY_ABORT).
    # Fires at HOSE_CONNECT_B → DISCHARGING when vessel cargo exceeds live mother headroom.
    "MOTHER_CAPACITY_ABORT"        : "Hard-cap abort: vessel cargo exceeds mother headroom — cast off and reassign",
    "GREENEAGLE_CAPACITY_ABORT"    : "Hard-cap abort (GreenEagle): vessel cargo exceeds capacity — cast off and reassign",
    "DORMANCY_ACTIVATED"           : "Mid-sim dormancy window started — vessel idle until resumption date",
    "DORMANCY_DEFERRED"            : "Dormancy deferred — vessel has cargo on board; will activate after BIA discharge",
}



class ThirdPartyVessel:
    """ZeeZee — third-party tanker arriving at Point B fully loaded once a month.

    Arrives at BIA on ZEEZEE_SCHEDULE day_of_month each calendar month.
    Discharges to the earliest available mother vessel.
    Priority rules:
      - If daughter-vessel queue is blocking all primary berths, ZeeZee waits
        up to ZEEZEE_MAX_DAUGHTER_WAIT_HOURS (48 h = 2 days) then forces a
        berth regardless.
      - If mothers are operationally absent (on export / offline / at capacity)
        ZeeZee waits without consuming her daughter-congestion clock.

    Status lifecycle:
      WAITING_B → BERTHING_B → HOSE_CONNECT_B → DISCHARGING → CAST_OFF_B → None
    """

    DISCHARGE_RATE_BPH = 20_000   # fallback only — overridden by VESSEL_DISCHARGE_RATE_BPH["ZeeZee"]

    def __init__(self, volume_bbl: float, api: float, arrival_t: float):
        self.name            = "ZeeZee"
        self.cargo_bbl       = float(volume_bbl)
        self.cargo_capacity  = float(volume_bbl)
        self.api             = float(api)
        self.status          = "WAITING_B"
        self.arrival_t       = arrival_t
        self.next_event_time = arrival_t
        self.assigned_mother = None
        self.current_voyage  = 1
        # Daughter-congestion clock — started when a mother exists but all
        # berths are held by daughters; reset when a genuine operational
        # constraint (no mother available) is responsible for the delay.
        self.daughter_block_since: float | None = None

    def __repr__(self):
        return f"ZeeZee[{self.status}|{self.cargo_bbl:,.0f}bbl]"



class DaughterVessel:
    def __init__(self, name, start_offset_hours=0, cargo_capacity=None):
        self.name = name
        self.cargo_capacity = cargo_capacity if cargo_capacity is not None else DAUGHTER_CARGO_BBL
        self.cargo_bbl = 0
        self.status = "IDLE_A"
        self.operation_start = None
        self.operation_end   = None
        self.next_event_time = start_offset_hours
        self.current_voyage = 0
        # Per-vessel trip counter — incremented only when this vessel starts a
        # genuinely new loading voyage.  Replaces the old global voyage_counter
        # so that each vessel's codes are sequential (STM-001, STM-002, …)
        # regardless of what other vessels are doing simultaneously.
        self._vessel_voyage_counter: int = 0
        self.queue_position = None
        self.assigned_storage = None
        self.assigned_load_hours = None
        self.assigned_mother = None
        self.target_point = "A"
        # Short voyage reference code stamped at LOADING_START (e.g. "SHK-001")
        self.voyage_code: str = ""
        # FIX 1: track exact arrival hour at Point B for FIFO queue ordering
        self.arrival_at_b = None
        # Track when dead-stock waiting began so we can escape if too long
        self.dead_stock_wait_start = None
        # ── Resumption hold fields ────────────────────────────────────────
        # Set by Simulation.__init__ when _VESSEL_RESUMPTION_DATES is populated.
        self.resumption_hour        = None   # sim-hour of 08:00 on resumption date
        self.resumption_storage     = None   # storage locked to on wake
        self.resumption_priority    = False  # True while priority berth is pending
        self.resumption_hold_logged = False  # suppresses repeated RESUMPTION_HOLD spam
        # ── JMP override lock ─────────────────────────────────────────────
        # Set True when a JMP loading-point override takes effect.
        # While True: daily preops and hourly reassessment are suppressed
        # so the override cannot be silently undone by the scoring engine.
        # Cleared when loading completes (LOADING_COMPLETE event).
        self._jmp_override_locked   = False
        # Date-shift: sim-hour on or after which loading may begin.
        # None = no date restriction; vessel loads as soon as berth is free.
        self._jmp_load_after_hour   = None
        # ── Multiple Transient Operation state ────────────────────────────
        # Set to the day_key (int) on which this vessel was nominated as
        # transient storage.  None when not an active MTO transient.
        # Cleared in the WAITING_BERTH_B handler after the priority berth
        # is claimed.
        self._mto_transient_since_day = None
        self._pf_load_ceiling  = None  # optional cap for startup-day Ibom loading
        # Number of parcels (discharger transfers) received so far while
        # acting as transient storage.  Used with MTO_MAX_PARCELS_BEFORE_OFFLOAD
        # to decide when to stop accumulating and seek an offloading window.
        self._mto_parcels_received: int = 0
        # Flag set when this vessel transitions from transient storage to
        # actively offloading to a primary mother. Causes DISCHARGE_START
        # to stamp VoyageCode with an "A" suffix (e.g. AMY-000A) so the
        # MTO discharge is distinguishable from a normal cargo delivery.
        self._is_mto_offload: bool = False
        # Tracks the last WAITING_BERTH_B log state to suppress duplicate entries
        # when half-step scanning produces no change in assignment or slot time.
        self._wb_last_logged_start:  object = None
        self._wb_last_logged_mother: object = None
        # Mid-sim dormancy: vessel operates normally until dormancy_start_hour,
        # then becomes dormant (IDLE_A) until _dormancy_end_hour (=resumption_hour).
        self.dormancy_start_hour: object = None
        self._dormancy_end_hour:  object = None   # resumption sim-hour for mid-sim window
        self._dormancy_pending:   bool   = False  # deferred dormancy — activate after next BIA discharge
        # Tracks when the MTO transient berth is free for the next discharger.
        # Set to transfer_end_t each time a discharger starts its approach.
        self._mto_berth_free_at: float = 0.0

    def __repr__(self):
        return f"{self.name}[{self.status}|cargo={self.cargo_bbl:,}bbl]"


class Simulation:
    def __init__(self):
        self.storage_bbl = {
            name: min(STORAGE_INIT_BBL, STORAGE_CAPACITY_BY_NAME[name])
            for name in STORAGE_NAMES
        }
        self.mother_bbl = {
            name: MOTHER_INIT_BBL_BY_NAME.get(name, MOTHER_INIT_BBL)
            for name in MOTHER_NAMES
        }
        self.total_exported = 0
        self.total_produced = 0
        self.total_spilled = 0
        self.storage_overflow_bbl = {name: 0.0 for name in STORAGE_NAMES}
        self.point_f_overflow_accum_bbl = 0.0
        self.storage_overflow_events = 0

        # ── API gravity tracking ──────────────────────────────────────────
        # storage_api: weighted-average API of current inventory in each storage
        self.storage_api = {name: STORAGE_API.get(name, 0.0) for name in STORAGE_NAMES}
        # vessel_api: weighted-average API of cargo on board each vessel (by name)
        self.vessel_api  = {}   # populated when vessels are created
        # mother_api: weighted-average API of inventory in each mother vessel
        self.mother_api  = {name: 0.0 for name in MOTHER_NAMES}
        self.total_exported_api_bbl = 0.0   # sum(vol * api) for export tracking
        self.log = []
        self.timeline = []
        self.voyage_counter = 0

        self.storage_berth_free_at = {name: 0.0 for name in STORAGE_NAMES}
        self.next_storage_berthing_start_at = {
            point: 0.0 for point in sorted(set(STORAGE_POINT.values()))
        }
        self.mother_berth_free_at = {name: 0.0 for name in MOTHER_NAMES}
        self.next_mother_berthing_start_at = 0.0
        self.mother_available_at = {name: 0.0 for name in MOTHER_NAMES}

        self.export_ready = {name: False for name in MOTHER_NAMES}
        self.export_ready_since = {name: None for name in MOTHER_NAMES}
        self.export_state = {name: None for name in MOTHER_NAMES}
        self.export_start_time = {name: None for name in MOTHER_NAMES}
        self.export_end_time = {name: None for name in MOTHER_NAMES}
        self.next_export_allowed_at = 0.0
        self.last_export_mother = None
        # Tracks the sim-hour of the most recent daughter cast-off from each mother.
        # The export DOC may not fire until EXPORT_INTAKE_BUFFER_HOURS after this
        # timestamp, giving a clean settling window before documentation begins.
        self.export_intake_last_cast_off = {name: 0.0 for name in MOTHER_NAMES}
        # Startup seed: mothers away at export at t=0.  Keyed by mother name,
        # value is the sim-hour they are available again.  Consulted by
        # mother_is_at_point_b so the full export state machine is never
        # touched by the seed — export_state stays None and is set only by
        # the sim's own DOC→SAILING→HOSE→IN_PORT machinery.
        self.mother_seeded_away_until = {name: 0.0 for name in MOTHER_NAMES}
        # Mid-simulation unavailability windows — keyed by mother name, value is
        # a list of (start_h, end_h) sim-hour tuples.  mother_is_at_point_b()
        # returns False for any t inside a window.  Daughters
        # daughters are automatically rerouted to available mothers.
        self.mother_unavailability_windows: dict = {name: [] for name in MOTHER_NAMES}
        self.export_unavailability_windows: list = []   # list of (start_h, end_h) tuples
        # Serial-discharge lock registry — {day_key: set(mother_names_currently_locked)}.
        # A mother's name is in the set while a pumping operation is active.
        # The lock is set at pump-start and cleared at cast-off completion.
        # Using a set() means the same mother can be locked → released → locked
        # multiple times per day, enabling two or more serial discharges on the
        # same calendar day when the first vessel completes early enough.
        # Covers Bryanston and GreenEagle equally.
        self.point_b_day_assigned_mothers = {}
        # Maps mother_name → the day_key on which pumping started (used by
        # _point_b_deregister_mother so an overnight cast-off deregisters from
        # the correct pump-start day, not from the (different) cast-off day).
        self._point_b_registered_day: dict = {}

        # ── Calibration & variability infrastructure ──────────────────────────
        # SimulationStats records planned-vs-actual durations for post-run
        # calibration reporting.  Always initialised; only populated when
        # ENABLE_VARIABILITY is True (but the object is safe to query either way).
        self._sim_stats = SimulationStats()
        # Seed the RNG for this run.  A fixed seed → reproducible stochastic runs.
        if ENABLE_VARIABILITY and VARIABILITY_RANDOM_SEED is not None:
            random.seed(VARIABILITY_RANDOM_SEED)
        # ── Weather disruption tracking ───────────────────────────────────────
        # Running total of weather hold hours injected into transit operations.
        self._weather_hold_hours_total = 0.0
        # Per-day stochastic production rate overrides — populated by
        # run_daily_preops_storage_reassessment when ENABLE_VARIABILITY is True.
        # Empty in deterministic mode (production_rate_bph_at skips it).
        self.production_rate_override_by_name: dict = {}
        self.storage_critical_active = {name: False for name in STORAGE_NAMES}
        self.point_f_vessels = ["Bedford", "Balham"]
        self.point_f_active_loader = "Balham"
        self.point_f_swap_pending_for = None
        self.point_f_swap_triggered_by = None
        self.production_rate_overrides = self._build_production_override_rules()
        # Post-breakwater A/C reassessment gate: remains inactive until a
        # daughter crosses inbound breakwater heading to Point A/C.
        self.ac_post_bw_reassess_active = False
        self.ac_post_bw_next_reassess_at = None
        self.daily_preops_last_day_key = -1
        # ── Multiple Transient Operation tracking ────────────────────────────
        # Set of calendar day-keys (int(t//24)) on which MTO has already fired.
        # Ensures exactly one transient nomination per calendar day.
        self._mto_days_fired: dict = {}   # {day_key: fire_count} — max 2 per day

        # ── Custom vessel injection ───────────────────────────────────────────
        # Maps vessel_name → frozenset of permitted storage names.
        # An empty set means "SanBarth + JasmineS only" (safe default).
        # Populated at join time; consulted by storage_allowed_for_vessel().
        self._custom_vessel_storage_permissions: dict = {}
        # Resolve each registered spec's join hour relative to _SIM_EPOCH.
        # Vessels whose join date precedes t=0 are clamped to t=0 so they
        # still enter the fleet rather than being silently skipped.
        self._pending_custom_vessels: list = []
        for _spec in _CUSTOM_VESSELS:
            _jd = _spec.join_date
            if isinstance(_jd, datetime):
                _join_dt = _jd.replace(hour=8, minute=0, second=0, microsecond=0)
            else:
                _join_dt = datetime(_jd.year, _jd.month, _jd.day, 8, 0)
            _spec._join_hour = max(
                0.0,
                (_join_dt - _SIM_EPOCH).total_seconds() / 3600.0,
            )
            self._pending_custom_vessels.append(_spec)

        # ── ZeeZee — third-party monthly visitor ─────────────────────────────
        # self.zeezee: None when absent; ThirdPartyVessel instance while active.
        # self.zeezee_months_visited: set of (year, month) already triggered so
        #   the monthly check fires exactly once per calendar month.
        self.zeezee: "ThirdPartyVessel | None" = None
        self.zeezee_months_visited: set = set()

        offsets = [0] * NUM_DAUGHTERS      # all vessels wake up simultaneously
        self.vessels = []
        for i in range(NUM_DAUGHTERS):
            name = VESSEL_NAMES[i]
            cap = VESSEL_CAPACITIES.get(name, DAUGHTER_CARGO_BBL)
            self.vessels.append(DaughterVessel(name, offsets[i], cargo_capacity=cap))
        self.total_loaded = 0
        # Initialise vessel API to zero (no cargo on board at start).
        # PF_LOADING vessels are initialised to IBOM_API since Ibom
        # production has a constant API — no blending needed.
        for vv in self.vessels:
            self.vessel_api[vv.name] = 0.0
        # ── Vessel startup positions ──────────────────────────────────────────
        # Default startup scenario:
        #   Sherlock, Laphroaig, Rathbone, SantaMonica, Bagshot, Watson, Berners
        #     → cargo_bbl = 0, status = SAILING_BA (Leg 1 — returning to SanBarth)
        #   Bedford  → PF_LOADING at Ibom (active loader)
        #   Balham   → BERTHING_B at GreenEagle (just arrived from Ibom)
        # Overridden at runtime when vessel_states_json or POINT_B_DISTRIBUTION_TEST_MODE
        # supplies explicit positions.

        _RETURNING_LEG1 = {
            "Sherlock", "Laphroaig", "Rathbone", "SantaMonica",
            "Bagshot",  "Watson",    "Amyla",
        }
        _seeded_startups = set()
        if POINT_B_DISTRIBUTION_TEST_MODE:
            _seeded_startups = set(POINT_B_TEST_STARTUP_FULL_LOAD_NOMINATIONS.keys())

        for vv in self.vessels:
            if vv.name == "Bedford":
                vv.status          = "PF_LOADING"
                vv.target_point    = "F"
                vv.cargo_bbl       = 30_000
                vv.next_event_time = 0.0
                vv._voyage_assigned = True
                vv.current_voyage   = 1
                vv._vessel_voyage_counter = 1
                vv.voyage_code      = make_voyage_code(vv.name, 1)
                self.vessel_api[vv.name] = IBOM_API

            elif vv.name == "Balham":
                vv.status           = "BERTHING_B"
                vv.target_point     = "B"
                vv.cargo_bbl        = 85_000
                vv.assigned_mother  = MOTHER_SECONDARY_NAME   # GreenEagle
                vv.next_event_time  = BERTHING_DELAY_HOURS
                vv._voyage_assigned = True
                vv.current_voyage   = 1
                vv._vessel_voyage_counter = 1
                vv.voyage_code      = make_voyage_code(vv.name, 1)
                self.vessel_api[vv.name] = IBOM_API

            elif vv.name in _RETURNING_LEG1 and vv.name not in _seeded_startups:
                # Returning to SanBarth — empty, on Leg 1 of the return voyage.
                # next_event_time set so the vessel arrives at Point A at
                # t = SAIL_HOURS_B_TO_A (6h), spread slightly to avoid a
                # simultaneous thundering-herd at the storage berths.
                _spread = list(sorted(_RETURNING_LEG1)).index(vv.name) * 0.5
                vv.status           = "SAILING_BA"
                vv.target_point     = "A"
                vv.cargo_bbl        = 0
                vv.next_event_time  = _sail_leg(SAIL_HOURS_B_TO_A, self) + _spread
                vv._voyage_assigned = False   # fresh voyage assigned on IDLE_A
                vv.current_voyage   = 0
                vv._vessel_voyage_counter = 0
                vv.voyage_code      = ""
                self.vessel_api[vv.name] = 0.0

        if POINT_B_DISTRIBUTION_TEST_MODE:
            for vv in self.vessels:
                nominated_mother = POINT_B_TEST_STARTUP_FULL_LOAD_NOMINATIONS.get(vv.name)
                if not nominated_mother:
                    continue
                _cap = vv.cargo_capacity
                vv.status = "HOSE_CONNECT_B"
                vv.target_point = "B"
                vv.cargo_bbl = _cap
                vv.assigned_mother = nominated_mother
                vv.next_event_time = 0.0
                vv._voyage_assigned = True
                vv.current_voyage = max(vv.current_voyage, 1)
                vv._vessel_voyage_counter = vv.current_voyage
                vv.voyage_code    = make_voyage_code(vv.name, vv.current_voyage)
                self.vessel_api[vv.name] = STORAGE_API.get(STORAGE_PRIMARY_NAME, 0.0)
                self.log_event(
                    0,
                    vv.name,
                    "HOSE_CONNECTION_START_B",
                    f"Point-B test seed active: hose connected at {nominated_mother} — ready to commence discharge ({_cap:,} bbl, started at t=0)",
                    voyage_num=vv.current_voyage,
                    mother=nominated_mother,
                )

        # Reserve Point B berth timelines for any t=0 vessels already active on a mother.
        initial_gate_end = 0.0
        for vv in self.vessels:
            mother_name = vv.assigned_mother
            if mother_name not in MOTHER_NAMES:
                continue
            if vv.status == "BERTHING_B":
                _disch_rate_init = VESSEL_DISCHARGE_RATE_BPH.get(vv.name)
                _disch_hrs_init = (vv.cargo_bbl / _disch_rate_init) if _disch_rate_init else DISCHARGE_HOURS
                _pump_end_init   = BERTHING_DELAY_HOURS + HOSE_CONNECTION_HOURS + _disch_hrs_init
                _end = _berth_free_at(_pump_end_init)   # accounts for nighttime cast-off delay
            elif vv.status == "HOSE_CONNECT_B":
                _disch_rate_init = VESSEL_DISCHARGE_RATE_BPH.get(vv.name)
                _disch_hrs_init = (vv.cargo_bbl / _disch_rate_init) if _disch_rate_init else DISCHARGE_HOURS
                _end = _berth_free_at(HOSE_CONNECTION_HOURS + _disch_hrs_init)
            elif vv.status == "DISCHARGING":
                _disch_rate_init = VESSEL_DISCHARGE_RATE_BPH.get(vv.name)
                _disch_hrs_init  = (vv.cargo_bbl / _disch_rate_init) if _disch_rate_init else DISCHARGE_HOURS
                _end = _berth_free_at(_disch_hrs_init)
            else:
                continue
            self.mother_berth_free_at[mother_name] = max(self.mother_berth_free_at[mother_name], _end)
            initial_gate_end = max(initial_gate_end, _end)
        self.next_mother_berthing_start_at = initial_gate_end
        # Sim-level Ibom tracking: Bedford active, no swap pending
        self.point_f_active_loader     = "Bedford"
        self.point_f_swap_pending_for  = None
        self.point_f_swap_triggered_by = None

        # ── Vessel resumption dates ───────────────────────────────────────
        # Resolve each entry in _VESSEL_RESUMPTION_DATES to a sim-hour and
        # stamp the corresponding DaughterVessel object.
        # MUST run AFTER self.vessels is fully populated so lookups succeed.
        # Dates before t=0 are clamped to 0.0 — vessel wakes immediately but
        # still holds the priority storage lock for its first load.
        for _vname, _rentry in _VESSEL_RESUMPTION_DATES.items():
            _rv = next((vv for vv in self.vessels if vv.name == _vname), None)
            if _rv is None:
                continue
            if _SIM_EPOCH is None:
                # Epoch not set — skip resumption seeding to avoid crash;
                # set_sim_epoch() must be called before Simulation() is instantiated.
                continue
            _rdate = _rentry["date"]
            if isinstance(_rdate, datetime):
                _rdt = _rdate.replace(hour=8, minute=0, second=0, microsecond=0)
            else:
                _rdt = datetime(_rdate.year, _rdate.month, _rdate.day, 8, 0)
            _rhour = max(0.0, (_rdt - _SIM_EPOCH).total_seconds() / 3600.0)
            _rv.resumption_hour    = _rhour
            _rv.resumption_storage = _rentry["storage"]
            # Push the vessel's first event time forward so it sleeps silently
            # until the resumption tick fires.
            if _rhour > 0.0:
                _rv.next_event_time = _rhour

    def _resolve_discharge_override(self, vessel_name: str, voyage_code: str, t: float):
        """Return (mother, discharge_date_iso) for the active override, or (None, None).

        Lookup order:
        1. Voyage-code keyed entries in DAUGHTER_DISCHARGE_OVERRIDES
           (e.g. {"SHK-001": {"vessel": "Sherlock", "mother": "Bryanston",
                               "discharge_date": "2025-04-16"}})
        2. Legacy vessel/day entries (plain string or {"mother": ..., "date": ...})

        discharge_date_iso is the calendar date (YYYY-MM-DD) the vessel should
        berth at the mother.  None means "berth as soon as feasible".
        """
        ddo = DAUGHTER_DISCHARGE_OVERRIDES
        if not ddo:
            return None, None

        # 1. Voyage-code keyed (preferred)
        if voyage_code and voyage_code in ddo:
            entry = ddo[voyage_code]
            if isinstance(entry, dict):
                return entry.get("mother"), entry.get("discharge_date")

        # 2. Legacy vessel/day keyed
        vessel_overrides = ddo.get(vessel_name, {})
        if vessel_overrides:
            day_key = int(t // 24)
            current_cal_date = self.hours_to_dt(t).date().isoformat()
            for _dk, _entry in vessel_overrides.items():
                if isinstance(_entry, dict):
                    _date   = _entry.get("date")
                    _mother = _entry.get("mother")
                    if _date and _mother and _date == current_cal_date:
                        return _mother, _date
                else:
                    if int(_dk) == day_key:
                        return str(_entry), None

        return None, None

    def _discharge_override_date_reached(self, discharge_date_iso: str, t: float) -> bool:
        """True when the simulation clock is on or past the target discharge date."""
        if not discharge_date_iso:
            return True
        current_date = self.hours_to_dt(t).date().isoformat()
        return current_date >= discharge_date_iso

    def _displace_incumbent_at_mother(self, mother_name: str, t: float):
        """Force any vessel currently berthing/discharging at mother_name to
        WAITING_BERTH_B so the override vessel can take the slot immediately."""
        for vv in self.vessels:
            if vv.assigned_mother != mother_name:
                continue
            if vv.status in {"BERTHING_B", "HOSE_CONNECT_B"}:
                # Vessel has not yet started pumping — safe to displace
                vv.status = "WAITING_BERTH_B"
                vv.next_event_time = self.next_daylight_hourly_berth_check(t, point="B")
                self.log_event(
                    t, vv.name, "WAITING_BERTH_B",
                    f"Displaced from {mother_name} berth by override priority vessel; "
                    f"reassessing at {self.hours_to_dt(vv.next_event_time).strftime('%Y-%m-%d %H:%M')}",
                    voyage_num=vv.current_voyage, mother=mother_name,
                )
        # Reset the berth lock so the override vessel can claim it now
        self.mother_berth_free_at[mother_name] = t

    @staticmethod
    def blend_api(vol_a, api_a, vol_b, api_b):
        """Return volume-weighted blended API. Returns api_b if vol_a is zero."""
        total = vol_a + vol_b
        if total <= 0:
            return api_b if api_b else api_a
        return (vol_a * api_a + vol_b * api_b) / total

    def point_f_other_vessel(self, vessel_name):
        return next((name for name in self.point_f_vessels if name != vessel_name), None)

    def point_f_active_loading_bbl(self):
        for vv in self.vessels:
            if vv.name == self.point_f_active_loader and vv.status in {"PF_LOADING", "IDLE_A"}:
                return vv.cargo_bbl
        return 0.0

    def mother_capacity_bbl(self, mother_name):
        return MOTHER_CAPACITY_BY_NAME.get(mother_name, MOTHER_CAPACITY_BBL)

    def mother_export_trigger_bbl(self, mother_name):
        return MOTHER_EXPORT_TRIGGER_BY_NAME.get(mother_name, MOTHER_EXPORT_TRIGGER)

    def total_storage_bbl(self):
        return sum(self.storage_bbl.values())

    def total_mother_bbl(self):
        return sum(self.mother_bbl.values())

    def _build_production_override_rules(self):
        rules = []
        for raw_rule in PRODUCTION_RATE_OVERRIDES:
            if not isinstance(raw_rule, dict):
                continue
            start_s = raw_rule.get("start_date")
            end_s = raw_rule.get("end_date")
            raw_rates = raw_rule.get("rates") or {}
            if not (start_s and end_s and isinstance(raw_rates, dict)):
                continue
            try:
                start_d = datetime.fromisoformat(str(start_s)).date()
                end_d = datetime.fromisoformat(str(end_s)).date()
            except Exception:
                continue
            if end_d < start_d:
                start_d, end_d = end_d, start_d
            rates = {}
            for storage_name, rate_val in raw_rates.items():
                if storage_name not in STORAGE_NAMES:
                    continue
                try:
                    rates[storage_name] = max(0.0, float(rate_val))
                except Exception:
                    continue
            if rates:
                rules.append({
                    "start": start_d,
                    "end": end_d,
                    "rates": rates,
                })
        return rules

    def production_rate_bph_at(self, storage_name, t):
        base_rate = STORAGE_PRODUCTION_RATE_BY_NAME.get(storage_name, 0.0)
        # Per-day stochastic variability override (set by run_daily_preops when
        # ENABLE_VARIABILITY is True; empty dict in deterministic mode).
        if hasattr(self, "production_rate_override_by_name"):
            var_rate = self.production_rate_override_by_name.get(storage_name)
            if var_rate is not None:
                base_rate = var_rate
        if not self.production_rate_overrides:
            return base_rate
        current_date = self.hours_to_dt(t).date()
        for rule in self.production_rate_overrides:
            if rule["start"] <= current_date <= rule["end"]:
                if storage_name in rule["rates"]:
                    return rule["rates"][storage_name]
        return base_rate

    def storage_load_hours(self, storage_name, cargo_bbl, vessel_name=None,
                           record_stats: bool = True):
        """Return loading duration in hours for cargo_bbl loaded at storage_name.

        When ENABLE_VARIABILITY is True the nominal duration is perturbed by a
        triangular sample (cv = VARIABILITY_CV_LOADING) and an equipment/
        inspection delay may be added.  Planned vs actual is recorded in
        self._sim_stats for post-run calibration.

        Woodstock, Bagshot and Rathbone load at SANBARTH_LOAD_RATE_SLOW_BPH (5,000 bph)
        when loading from SanBarth; all other vessel/storage combinations use the
        standard rate map.
        """
        _RATE_MAP = {
            STORAGE_PRIMARY_NAME:   SANBARTH_LOAD_RATE_BPH,
            STORAGE_SECONDARY_NAME: JASMINES_LOAD_RATE_BPH,
            STORAGE_TERTIARY_NAME:  WESTMORE_LOAD_RATE_BPH,
            STORAGE_QUATERNARY_NAME: DUKE_LOAD_RATE_BPH,
            STORAGE_QUINARY_NAME:   STARTURN_LOAD_RATE_BPH,
            STORAGE_SENARY_NAME:    PGM_LOAD_RATE_BPH,
        }
        rate = _RATE_MAP.get(storage_name)
        if rate:
            if storage_name == STORAGE_PRIMARY_NAME and vessel_name in SANBARTH_SLOW_LOADERS:
                rate = SANBARTH_LOAD_RATE_SLOW_BPH
            nominal = cargo_bbl / rate
        else:
            nominal = LOAD_HOURS  # fallback for unknown storages

        # Apply variability
        actual = (_variability_sample(nominal, VARIABILITY_CV_LOADING)
                  + _equipment_delay_hours())
        if record_stats and hasattr(self, "_sim_stats"):
            self._sim_stats.record("loading", nominal, actual)
        return actual

    def effective_load_cap(self, vessel_name, storage_name):
        """Return the loading volume cap for vessel at storage_name.
        JasmineS loads 8% above normal capacity and Westmore loads 18%
        below normal capacity. Explicit Point A caps for Bedford/Balham
        still override those adjusted capacities.
        Pass storage_name="__any__" to get full capacity (non-Point-A probe).
        """
        vessel = next((v for v in self.vessels if v.name == vessel_name), None)
        full_cap = vessel.cargo_capacity if vessel else DAUGHTER_CARGO_BBL
        if storage_name == "__any__":
            return full_cap
        return storage_adjusted_load_cap(full_cap, storage_name, vessel_name)

    def loading_start_threshold(self, storage_name, cargo_bbl):
        if storage_name in (STORAGE_QUATERNARY_NAME, STORAGE_QUINARY_NAME, STORAGE_SENARY_NAME):
            # Duke/Starturn/PGM rule: per-storage dead-stock factor applies.
            # Vessels must not drain a small tank below a safe reserve.
            _dsf = DEAD_STOCK_FACTOR_BY_STORAGE.get(storage_name, DEAD_STOCK_FACTOR)
            required = max(_dsf * cargo_bbl, cargo_bbl + DUKE_STARTURN_DEAD_STOCK_BBL)
            return min(required, STORAGE_CAPACITY_BY_NAME[storage_name])
        # SanBarth / JasmineS / Westmore: per-storage factor (default 1.75)
        _dsf = DEAD_STOCK_FACTOR_BY_STORAGE.get(storage_name, DEAD_STOCK_FACTOR)
        required = _dsf * cargo_bbl
        return min(required, STORAGE_CAPACITY_BY_NAME[storage_name])

    def storage_allowed_for_vessel(self, storage_name, vessel_name):
        # Custom vessels carry an explicit permitted-storage list.  When that
        # entry exists, use it exclusively — skip all standard permission sets.
        if vessel_name in self._custom_vessel_storage_permissions:
            allowed = self._custom_vessel_storage_permissions[vessel_name]
            # Empty set means "SanBarth + JasmineS only" (safe Point-A default)
            if not allowed:
                return storage_name in (STORAGE_PRIMARY_NAME,
                                        STORAGE_SECONDARY_NAME)
            return storage_name in allowed
        if vessel_name == "SantaMonica":
            return storage_name in SANTAMONICA_PERMITTED_STORAGES
        if vessel_name == "Watson":
            return storage_name in WATSON_PERMITTED_STORAGES
        if vessel_name == "Laphroaig":
            return storage_name in LAPHROAIG_PERMITTED_STORAGES
        if vessel_name == "Amyla":
            return storage_name in AMYLA_PERMITTED_STORAGES
        if vessel_name in POINT_A_ONLY_VESSELS and STORAGE_POINT.get(storage_name) != "A":
            return False
        if storage_name == STORAGE_TERTIARY_NAME and vessel_name not in WESTMORE_PERMITTED_VESSELS:
            return False
        if storage_name == STORAGE_QUATERNARY_NAME and vessel_name not in DUKE_PERMITTED_VESSELS:
            return False
        if storage_name == STORAGE_QUINARY_NAME and vessel_name not in STARTURN_PERMITTED_VESSELS:
            return False
        if storage_name == STORAGE_SENARY_NAME and vessel_name not in PGM_PERMITTED_VESSELS:
            return False
        return True

    def storage_min_remaining_after_load(self, storage_name):
        if storage_name == STORAGE_QUATERNARY_NAME:
            return DUKE_MIN_REMAINING_BBL
        if storage_name == STORAGE_QUINARY_NAME:
            return STARTURN_MIN_REMAINING_BBL
        if storage_name == STORAGE_SENARY_NAME:
            return PGM_MIN_REMAINING_BBL
        return 0.0

    def return_allocation_candidate(self, cargo_bbl, vessel_name, point_restrict=None):
        # Consider every storage this vessel is permitted to load from.
        allowed_storages = [
            name for name in STORAGE_NAMES
            if self.storage_allowed_for_vessel(name, vessel_name)
            and (point_restrict is None or STORAGE_POINT.get(name) == point_restrict)
        ]
        # Per-storage effective cap: Bedford/Balham are capped at 63k at Point A
        cap_by_storage = {
            name: self.effective_load_cap(vessel_name, name)
            for name in allowed_storages
        }
        threshold_by_storage = {
            name: self.loading_start_threshold(name, cap_by_storage[name])
            for name in allowed_storages
        }

        pre_tank_top_candidates = []

        trigger_ratio_by_storage = {
            STORAGE_QUATERNARY_NAME: DUKE_PRE_TANK_TOP_TRIGGER_RATIO,
            STORAGE_QUINARY_NAME: STARTURN_PRE_TANK_TOP_TRIGGER_RATIO,
            STORAGE_SENARY_NAME: PRE_TANK_TOP_TRIGGER_RATIO_DEFAULT,
        }
        for storage_name in allowed_storages:
            stock = self.storage_bbl[storage_name]
            _stor_cap = STORAGE_CAPACITY_BY_NAME[storage_name]
            _load_cap = cap_by_storage[storage_name]  # effective load volume
            trigger_ratio = trigger_ratio_by_storage.get(storage_name, PRE_TANK_TOP_TRIGGER_RATIO_DEFAULT)
            pre_tank_top_trigger = _stor_cap * trigger_ratio
            reserve_required = self.storage_min_remaining_after_load(storage_name)
            if (
                stock >= pre_tank_top_trigger
                and stock >= (_load_cap + reserve_required)
            ):
                pre_tank_top_candidates.append(storage_name)

        if pre_tank_top_candidates:
            selected_pre_tank_top = max(
                pre_tank_top_candidates,
                key=lambda name: (
                    self.storage_bbl[name] / STORAGE_CAPACITY_BY_NAME[name],
                    self.storage_bbl[name],
                    name,
                ),
            )
            return selected_pre_tank_top, threshold_by_storage[selected_pre_tank_top], threshold_by_storage

        eligible = [
            name for name in allowed_storages
            if self.storage_bbl[name] >= threshold_by_storage[name]
        ]
        if not eligible:
            # Proactive positioning fallback: still nominate a return storage so
            # the vessel can sail/berth and wait at hose connection for stock build.
            if not allowed_storages:
                return None, None, threshold_by_storage
            def _fallback_key(name):
                stock   = self.storage_bbl[name]
                raw_gap = abs(stock - STORAGE_CRITICAL_THRESHOLD_BY_NAME[name])
                if raw_gap <= DISPATCH_BIAS_FORECAST_BBL:
                    effective_gap = raw_gap * (1.0 - self.production_rate_bias_factor(name))
                else:
                    effective_gap = raw_gap
                return (stock, -effective_gap, name)
            fallback = max(allowed_storages, key=_fallback_key)
            return fallback, threshold_by_storage[fallback], threshold_by_storage

        def rank_key(storage_name):
            # Use the same rank tuple as storage_dispatch_rank so the two
            # allocation paths are always consistent.
            return self.storage_dispatch_rank(storage_name)

        selected = min(eligible, key=rank_key)
        return selected, threshold_by_storage[selected], threshold_by_storage

    def assign_ac_point_post_breakwater(self, v, t):
        """Determine and assign Point A/C target immediately after inbound
        A/C breakwater crossing. This guarantees each crossing vessel receives
        a post-breakwater point allocation, even if berthing must wait."""
        ac_allowed = [
            name for name in STORAGE_NAMES
            if STORAGE_POINT.get(name) in ("A", "C")
            and self.storage_allowed_for_vessel(name, v.name)
        ]
        if not ac_allowed:
            v.target_point = "A"
            self.log_event(
                t,
                v.name,
                "RETURN_POINT_ALLOCATED",
                "Post-breakwater reassessment: no explicit A/C storage permissions found; defaulting to Point A",
                voyage_num=v.current_voyage,
            )
            return

        # Select the most urgent A/C storage for this vessel using the same
        # risk-rank scoring as return_allocation_candidate: storage closest to
        # its overflow critical threshold (production-rate biased) sorts first.
        # Using max(stock) here was wrong — it always sent vessels to whichever
        # storage happened to have more barrels rather than the one most at risk.
        def _ac_risk_rank(name):
            stock    = self.storage_bbl[name]
            crit     = STORAGE_CRITICAL_THRESHOLD_BY_NAME[name]
            unsafe   = 0 if stock >= crit else 1
            raw_gap  = abs(stock - crit)
            if raw_gap <= DISPATCH_BIAS_FORECAST_BBL:
                bias         = self.production_rate_bias_factor(name)
                effective_gap = raw_gap * (1.0 - bias)
            else:
                effective_gap = raw_gap
            return (unsafe, effective_gap, -stock, name)

        ready    = []
        fallback = []
        for name in ac_allowed:
            cap   = self.effective_load_cap(v.name, name)
            thr   = self.loading_start_threshold(name, cap)
            stock = self.storage_bbl[name]
            fallback.append((name, thr, stock))
            if stock >= thr:
                ready.append((name, thr, stock))

        pool = ready if ready else fallback
        selected_storage, selected_thr, selected_stock = min(pool, key=lambda x: _ac_risk_rank(x[0]))
        v.target_point = STORAGE_POINT.get(selected_storage, "A")
        self.log_event(
            t,
            v.name,
            "RETURN_POINT_ALLOCATED",
            f"Post-breakwater reassessment assigned Point {v.target_point} via {selected_storage} "
            f"({selected_stock:,.0f} bbl, loading-start threshold {selected_thr:,.0f} bbl)",
            voyage_num=v.current_voyage,
        )

    # -- Helpers ----------------------------------------------------------
    def hours_to_dt(self, h):
        return _SIM_EPOCH + timedelta(hours=h)

    def tide_height_at(self, hour):
        """Return tidal height at a given sim hour, or None if no table loaded."""
        if _TIDE_TABLE is None:
            return None
        slot = round(hour * 2) / 2
        # Walk forward up to 1h to find nearest populated slot
        for delta in [0, 0.5, -0.5, 1.0, -1.0]:
            h = _TIDE_TABLE.get(slot + delta)
            if h is not None:
                return h
        return None

    def tide_ok_at(self, hour):
        """True if tidal height is above minimum crossing level (or no table loaded)."""
        if _TIDE_TABLE is None:
            return True
        h = self.tide_height_at(hour)
        return h is not None and h > TIDE_MIN_CROSSING_M

    def tide_high_ok_at(self, hour):
        """Backward-compatible alias used by prior logic; no local-peak requirement."""
        return self.tide_ok_at(hour)

    def tidal_period_label(self, hour):
        wall_h = (hour + SIM_HOUR_OFFSET) % 24
        if DAYLIGHT_START <= wall_h < DAYLIGHT_END and self.tide_ok_at(hour):
            return "daylight tide >1.6m"
        return "outside daylight/tidal window"

    def tidal_periods_available_for_day(self, hour):
        """Return daylight tide availability summary for the calendar day of `hour`."""
        if _TIDE_TABLE is None:
            return "daylight operations (no tidal file)"
        # Align to calendar midnight, not sim-epoch boundary.
        # Without this, hours 06:00-07:30 (sim hours -2 to -0.5 on day 0)
        # get floor-divided to day_key=-1 and are never scanned.
        cal_day  = int((hour + SIM_HOUR_OFFSET) // 24)
        day_start = cal_day * 24 - SIM_HOUR_OFFSET   # sim-hour at 00:00 of that calendar day
        valid_slots = 0
        for slot in [day_start + 0.5 * i for i in range(48)]:
            wall_h = (slot + SIM_HOUR_OFFSET) % 24
            if not (DAYLIGHT_START <= wall_h < DAYLIGHT_END):
                continue
            if self.tide_ok_at(slot):
                valid_slots += 1
        return (f"{valid_slots} daylight tide slot(s) >{TIDE_MIN_CROSSING_M:.1f}m"
                if valid_slots else f"no daylight tide >{TIDE_MIN_CROSSING_M:.1f}m")

    def next_tidal_sail(self, current_hour):
        """
        Return the earliest hour >= current_hour that satisfies BOTH:
          - daylight (DAYLIGHT_START <= (h+SIM_HOUR_OFFSET)%24 < DAYLIGHT_END)
                    - tide height > TIDE_MIN_CROSSING_M (skipped if no table)
        Scans forward in 0.5 h steps for up to 7 days.
        """
        # Fast path: no tidal table — fall back to pure daylight check
        if _TIDE_TABLE is None:
            return self.next_daylight_sail(current_hour)
        t = self.next_daylight_sail(current_hour)
        for _ in range(336):   # max 7 days * 48 half-hour steps
            wall_h = (t + SIM_HOUR_OFFSET) % 24
            if DAYLIGHT_START <= wall_h < DAYLIGHT_END and self.tide_ok_at(t):
                return t
            t += 0.5
            # skip to next daylight start if outside window
            wall_h2 = (t + SIM_HOUR_OFFSET) % 24
            if not (DAYLIGHT_START <= wall_h2 < DAYLIGHT_END):
                t = self.next_daylight_sail(t)
        return self.next_daylight_sail(current_hour)

    def next_daylight_sail(self, current_hour):
        """Return earliest sim-hour >= current_hour within wall-clock daylight
        window [DAYLIGHT_START, DAYLIGHT_END)."""
        wall_h = (current_hour + SIM_HOUR_OFFSET) % 24
        if DAYLIGHT_START <= wall_h < DAYLIGHT_END:
            return current_hour
        days_elapsed = int(current_hour // 24)
        sim_dl_today = days_elapsed * 24 + (DAYLIGHT_START - SIM_HOUR_OFFSET)
        if current_hour <= sim_dl_today:
            return sim_dl_today
        else:
            return sim_dl_today + 24

    def next_daylight_hourly_berth_check(self, current_hour, point=None):
        """Return next berthing recheck time during daylight window.

        Vessels waiting for a berth at Point B scan every TIME_STEP_HOURS
        (0.5h) during daylight — the same cadence used by vessels scanning
        for available berths at loading points.  This ensures that the moment
        a mother's berth frees up (after a discharging vessel casts off), the
        waiting daughter is immediately reallocated to it rather than waiting
        up to an hour for the next check.
        """
        wall_h = (current_hour + SIM_HOUR_OFFSET) % 24
        if BERTHING_START <= wall_h < BERTHING_END:
            # Scan every half-step during daylight
            nxt = round(current_hour + TIME_STEP_HOURS, 2)
            wall_next = (nxt + SIM_HOUR_OFFSET) % 24
            if BERTHING_START <= wall_next < BERTHING_END:
                return nxt
        # Outside daylight — jump to next daylight window start
        days_elapsed = int(current_hour // 24)
        sim_bs_today = days_elapsed * 24 + (BERTHING_START - SIM_HOUR_OFFSET)
        if current_hour <= sim_bs_today:
            return sim_bs_today
        return sim_bs_today + 24

    def next_export_sail_start(self, current_hour):
        """Return earliest sim-hour >= current_hour within wall-clock export
        sail window [EXPORT_SAIL_WINDOW_START, EXPORT_SAIL_WINDOW_END)."""
        wall_h = (current_hour + SIM_HOUR_OFFSET) % 24
        if EXPORT_SAIL_WINDOW_START <= wall_h < EXPORT_SAIL_WINDOW_END:
            return current_hour
        days_elapsed = int(current_hour // 24)
        sim_ex_today = days_elapsed * 24 + (EXPORT_SAIL_WINDOW_START - SIM_HOUR_OFFSET)
        if current_hour <= sim_ex_today:
            return sim_ex_today
        else:
            return sim_ex_today + 24

    def next_cast_off_window(self, current_hour):
        """Return earliest sim-hour >= current_hour that falls within the
        wall-clock cast-off window [CAST_OFF_START, CAST_OFF_END).
        Converts sim-hours → wall-clock via SIM_HOUR_OFFSET before comparing."""
        wall_h = (current_hour + SIM_HOUR_OFFSET) % 24
        if CAST_OFF_START <= wall_h < CAST_OFF_END:
            return current_hour
        days_elapsed = int(current_hour // 24)
        # Sim-hour that corresponds to CAST_OFF_START on the same calendar day
        sim_co_today = days_elapsed * 24 + (CAST_OFF_START - SIM_HOUR_OFFSET)
        if current_hour <= sim_co_today:
            return sim_co_today
        else:
            return sim_co_today + 24

    def is_any_vessel_casting_off(self, point=None):
        for v in self.vessels:
            if point is None:
                if v.status in ["WAITING_CAST_OFF", "CAST_OFF", "CAST_OFF_B"]:
                    return True
            elif point == "B":
                if v.status == "CAST_OFF_B":
                    return True
            else:
                if v.status in ["WAITING_CAST_OFF", "CAST_OFF"] and v.target_point == point:
                    return True
        return False

    def is_valid_berthing_time(self, hour, point=None):
        """Return True if hour falls within the berthing window and no cast-off is active."""
        wall_h = (hour + SIM_HOUR_OFFSET) % 24
        return BERTHING_START <= wall_h < BERTHING_END and not self.is_any_vessel_casting_off(point)

    def storage_locked_by_active_berth(self, storage_name, requesting_vessel=None):
        """True when another vessel is physically occupying the berth at storage.
        Blocks a new vessel from berthing until the incumbent has completed loading,
        finished documentation AND physically cast off. A vessel in DOCUMENTING or
        CAST_OFF status is still alongside — the berth is not free until SAILING_AB
        (or SAILING_D_CHANNEL for Point D) begins."""
        lock_statuses = {
            "BERTHING_A",      # arriving and securing
            "HOSE_CONNECT_A",  # connecting cargo hoses
            "LOADING",         # actively loading cargo
            "DOCUMENTING",     # cargo complete, paperwork in progress — still alongside
            "CAST_OFF",        # casting off lines — still physically at berth
        }
        for vv in self.vessels:
            if vv.name == requesting_vessel:
                continue
            if vv.assigned_storage != storage_name:
                continue
            if vv.status in lock_statuses:
                return True
        return False

    def next_berthing_window(self, current_hour, point=None):
        """Return earliest sim-hour >= current_hour within the daylight berthing window.

        No cast-off conflict check.  Cast-offs at one berth do not block berthing
        at a different mother (they are independent physical locations).  Approach
        overlap is handled by BERTHING_DELAY_HOURS.  The previous cast-off loop
        added up to 14 x 24h whenever ANY vessel was in CAST_OFF_B anywhere at
        Point B, stacking all future berth calculations by weeks and freezing the
        simulation completely.  That code is removed.
        """
        wall_h = (current_hour + SIM_HOUR_OFFSET) % 24
        if BERTHING_START <= wall_h < BERTHING_END:
            return current_hour
        days_elapsed = int(current_hour // 24)
        sim_bs_today = days_elapsed * 24 + (BERTHING_START - SIM_HOUR_OFFSET)
        if current_hour <= sim_bs_today:
            return sim_bs_today
        return sim_bs_today + 24

    def point_b_candidate_slots(self, v, at_time):
        """Build feasible Point B mother slots for vessel v at decision time.

        Priority rule:
        """
        berthing_start = self.next_berthing_window(at_time, point="B")
        primary_candidates = []
        # Look-ahead: also include mothers returning from export within 24h so
        # vessels waiting at BIA can reassign to an incoming mother.
        _lookahead_t = at_time + 24.0

        for mother_name in MOTHER_NAMES:
            # Do NOT exclude export_ready primary mothers here.
            # Operational rule: if the mother is physically at BIA and has capacity,
            # she can receive the arriving vessel; this is especially important on
            # startup day where the vessel closest to export volume (e.g. Bryanston)
            # should be topped up rather than left idle. Export DOC/SAILING states
            # are still blocked by mother_is_at_point_b/export_state checks below.
            _at_bia_now  = self.mother_is_at_point_b(mother_name, at_time)
            # Include mothers that will return from export within 24h
            _at_bia_soon = (
                not _at_bia_now
                and self.mother_is_at_point_b(mother_name, _lookahead_t)
            )
            if not _at_bia_now and not _at_bia_soon:
                continue
            _cap = self.mother_capacity_bbl(mother_name)
            # Projected-stock check: include all cargo already committed to this
            # mother from vessels currently at berth (BERTHING_B / HOSE_CONNECT_B)
            # or actively pumping (DISCHARGING).  This prevents over-scheduling
            # (e.g. GreenEagle reaching 986k because 5 vessels were booked when
            # she had headroom but stacked past cap on arrival).
            #
            # Only count vessels whose cargo actually fits within the mother's
            # CURRENT headroom (cap − live stock).  A vessel assigned to this
            # mother but carrying more than the available headroom will be turned
            # away by MOTHER_CAPACITY_ABORT when it arrives — it is a phantom
            # reservation that must not block a genuinely fitting vessel
            # (e.g. SantaMonica 7k blocked by a pre-assigned RTH-001 44k when
            # Bryanston has only 28k headroom: 44k can't fit so it shouldn't count).
            _live_headroom = max(0.0, _cap - self.mother_bbl[mother_name])
            _committed = sum(
                vv.cargo_bbl for vv in self.vessels
                if vv.assigned_mother == mother_name
                and vv.status in {"BERTHING_B", "HOSE_CONNECT_B", "DISCHARGING"}
                and vv is not v
                and vv.cargo_bbl <= _live_headroom   # exclude oversized phantoms
            )
            if self.mother_bbl[mother_name] + _committed + v.cargo_bbl > _cap:
                continue
            earliest = max(berthing_start, self.mother_available_at.get(mother_name, 0.0))
            berth_t = self.next_berthing_window(earliest, point="B")
            # Clamp berth_free_at to at_time — a past timestamp means "free now"
            _effective_free = max(self.mother_berth_free_at[mother_name], 0.0)
            # Anchor to the live occupant's actual departure time
            _occupant = self.mother_berth_current_occupant(mother_name)
            _real_free = max(
                _effective_free,
                _occupant.next_event_time if _occupant is not None else 0.0,
            )
            start = max(
                berth_t,
                _real_free if _real_free > at_time else 0.0,
                self.mother_available_at.get(mother_name, 0.0),
            )
            start = self.next_berthing_window(start, point="B")
            primary_candidates.append((start, berth_t, mother_name))

        candidates = list(primary_candidates)

        # Serial discharge gate — all mother vessels.
        # Blocks a candidate mother only while its discharge lock is active
        # (i.e. a vessel is currently pumping or in hose-connect).  Once the
        # active vessel casts off, _point_b_deregister_mother() clears the lock
        # and this filter passes immediately — allowing a second (or third)
        # discharge to start on the same calendar day when operations complete
        # early enough.  There is NO hard cap on discharges per day.
        def _allowed(entry):
            start, _, mn = entry
            return not self._point_b_mother_assigned_on_day(mn, start)
        candidates = [e for e in candidates if _allowed(e)]

        return berthing_start, candidates

    def mother_is_at_point_b(self, mother_name, t):
        """True when the mother is physically available at Point B.
        Mothers can be marked unavailable via a scheduled maintenance /
        dry-dock window.
        """
        # Startup seed: mother was seeded as away at export until a specific hour
        if t < self.mother_seeded_away_until.get(mother_name, 0.0):
            return False
        # Mid-simulation scheduled unavailability windows (dry-dock / maintenance)
        for _start_h, _end_h in self.mother_unavailability_windows.get(mother_name, []):
            if _start_h <= t < _end_h:
                return False
        # Mother is unavailable whenever mother_available_at is still in the future.
        # This covers every phase of the export round-trip:
        #   DOC      — documenting at BIA, about to depart
        #   SAILING  — outbound transit to export terminal
        #   HOSE     — connecting hoses at export terminal
        #   IN_PORT  — pumping cargo at export terminal
        #   (None)   — export complete, sailing back to BIA (return transit)
        #   (None)   — arrived at BIA, fendering in progress
        # In all of these phases the mother is NOT physically at Point B and must
        # not appear as a valid berth candidate for arriving daughter vessels.
        # The 24-hour lookahead in point_b_candidate_slots (_at_bia_soon) calls
        # this function at t+24h, so daughters can still be pre-assigned when the
        # return is imminent — but actual berthing is gated by mother_berth_free_at
        # (set to return_arrival + FENDERING_HOURS) and the BERTHING_B handler,
        # which reverts to WAITING_MOTHER_RETURN if the mother is still absent.
        _available_at = self.mother_available_at.get(mother_name, 0.0)
        if t < _available_at:
            return False
        # Mother is at Point B and fendering is complete
        return True

    def mother_berth_current_occupant(self, mother_name):
        """Return the vessel currently occupying this mother's berth, or None if free.

        A berth is occupied while a vessel is in any active berth state.
        Returns a (vessel_or_sentinel, volume) tuple internally — use
        mother_berth_active_actors for the full list; this method returns the
        first regular-vessel occupant for backward-compatibility with callers that
        just need to know if the berth is free.
        """
        active_berth_statuses = {"BERTHING_B", "HOSE_CONNECT_B", "DISCHARGING", "CAST_OFF_B"}
        for vv in self.vessels:
            if vv.assigned_mother == mother_name and vv.status in active_berth_statuses:
                return vv
        return None

    def mother_berth_active_actors(self, mother_name):
        """Return a list of (name, cargo_bbl, status) for every actor currently
        berthed at mother_name.

        Used by the concurrent-occupancy guard at BERTHING_B → HOSE_CONNECT_B.
        """
        active_berth_statuses = {"BERTHING_B", "HOSE_CONNECT_B", "DISCHARGING", "CAST_OFF_B"}
        actors = []
        for vv in self.vessels:
            if vv.assigned_mother == mother_name and vv.status in active_berth_statuses:
                actors.append((vv.name, vv.cargo_bbl, vv.status, "vessel", vv))
        return actors

    def _concurrent_berth_guard(self, arriving_vessel_name, arriving_cargo_bbl,
                                 mother_name, t):
        """Check for concurrent occupancy at mother_name just before hose opens.

        Called at the BERTHING_B → HOSE_CONNECT_B transition for every actor
        (regular daughters and MTO transients).

        If more than one actor is berthed simultaneously the actor with the
        smaller cargo is aborted back to WAITING_BERTH_B.  Returns True when the
        arriving actor is the loser and should abort; False when it may proceed.

        Rules:
        • Volume decides: larger cargo stays, smaller cargo aborts.
        • Tie: the actor that arrived first (lower next_event_time) stays.
        • An actor in DISCHARGING or CAST_OFF_B is never aborted — it has
          already started pumping and interrupting would corrupt mother stock.
        """
        actors = self.mother_berth_active_actors(mother_name)
        # ── Physical enforcement: a mother away at export (or sailing back /
        # fendering) is NOT at Point B, and no daughter has
        # permission to sail to the export terminal.  Therefore no actor may berth
        # or open hoses to a mother in any export-busy state — it is physically
        # impossible.  This single chokepoint (hit by every actor at the
        # BERTHING_B → HOSE_CONNECT_B transition) guarantees the rule regardless of
        # which assignment path placed the actor here.  The actor aborts back to
        # WAITING_BERTH_B and re-tries once the mother has fully arrived at BIA and
        # completed fendering (export_state clears to None).
        if self.export_state.get(mother_name) in EXPORT_BUSY_STATES:
            self.log_event(
                t, arriving_vessel_name, "CONCURRENT_BERTH_ABORT",
                f"Cannot berth {mother_name}: mother is away on export duty "
                f"({self.export_state.get(mother_name)}) — no permission to sail to "
                f"the export terminal; {arriving_vessel_name} held until she returns "
                f"to BIA and completes fendering",
                mother=mother_name,
            )
            return True   # arriving actor must abort — mother not physically present
        # Only pre-pump actors can be displaced (BERTHING_B or HOSE_CONNECT_B)
        abortable_statuses = {"BERTHING_B", "HOSE_CONNECT_B"}
        active_discharging = [a for a in actors
                              if a[2] not in abortable_statuses]   # already pumping
        if active_discharging:
            # Someone is already pumping — arriving actor must abort unconditionally
            active_names = [a[0] for a in active_discharging]
            self.log_event(
                t, arriving_vessel_name, "CONCURRENT_BERTH_ABORT",
                f"Concurrent berth guard: {mother_name} occupied by "
                f"{', '.join(active_names)} (DISCHARGING/CAST_OFF_B) — "
                f"{arriving_vessel_name} aborted to WAITING_BERTH_B",
                mother=mother_name,
            )
            return True   # arriving actor loses — abort

        pre_pump = [a for a in actors if a[2] in abortable_statuses]
        if len(pre_pump) <= 1:
            return False  # no conflict — only us (or nobody)

        # Multiple actors in pre-pump state — sort by volume desc, then by
        # next_event_time asc (earlier arrival wins ties)
        def _sort_key(actor):
            vol = actor[1]
            # Use next_event_time from vessel object if available, else 0
            arrival = actor[4].next_event_time if actor[4] is not None else 0.0
            return (-vol, arrival)

        pre_pump_sorted = sorted(pre_pump, key=_sort_key)
        winner_name = pre_pump_sorted[0][0]

        if winner_name != arriving_vessel_name:
            # We are not the winner — abort ourselves
            loser_names  = [a[0] for a in pre_pump_sorted[1:]]
            self.log_event(
                t, arriving_vessel_name, "CONCURRENT_BERTH_ABORT",
                f"Concurrent berth guard: {mother_name} — "
                f"{winner_name} wins ({pre_pump_sorted[0][1]:,.0f} bbl); "
                f"{arriving_vessel_name} ({arriving_cargo_bbl:,.0f} bbl) aborted to WAITING_BERTH_B",
                mother=mother_name,
            )
            return True

        # We are the winner — abort the other pre-pump losers
        for actor in pre_pump_sorted[1:]:
            loser_name, loser_vol, loser_status, loser_type, loser_obj = actor
            if loser_type == "vessel" and loser_obj is not None:
                loser_obj.status          = "WAITING_BERTH_B"
                loser_obj.assigned_mother = None
                loser_obj.next_event_time = self.next_daylight_hourly_berth_check(
                    t, point="B")
                self.log_event(
                    t, loser_name, "CONCURRENT_BERTH_ABORT",
                    f"Concurrent berth guard: {mother_name} — "
                    f"{arriving_vessel_name} wins ({arriving_cargo_bbl:,.0f} bbl); "
                    f"{loser_name} ({loser_vol:,.0f} bbl) aborted to WAITING_BERTH_B",
                    mother=mother_name,
                )
        return False   # arriving actor is the winner — proceed

    def point_b_calendar_day_key(self, t: float) -> int:
        """Return the calendar day key for Point B operations.

        The simulation anchor is 08:00 at t=0, so we align days using
        SIM_HOUR_OFFSET to make day boundaries run 08:00–07:59.
        """
        return int((t + SIM_HOUR_OFFSET) // 24)

    def _point_b_register_mother_start(self, mother_name: str, start_t: float) -> None:
        """Lock this mother's berth at pump-start.

        Implements the SERIAL DISCHARGE rule: only one pumping operation may be
        active per mother at any time.  The lock is released at cast-off
        completion via _point_b_deregister_mother(), at which point a second
        vessel may berth and pump on the same calendar day.

        Covers Bryanston and GreenEagle equally.
        Using a set guarantees exactly one active lock per mother — there is no
        cap on how many times per day the lock can cycle (lock → release → lock
        → release), enabling two or more serial discharges per day when
        operations complete early enough.
        """
        if mother_name not in {MOTHER_PRIMARY_NAME, MOTHER_SECONDARY_NAME}:
            return
        day_key = self.point_b_calendar_day_key(start_t)
        self.point_b_day_assigned_mothers.setdefault(day_key, set()).add(mother_name)
        # Remember which day the slot was locked so deregister can target the
        # correct key even when cast-off is deferred past midnight (overnight
        # daylight restriction).  Without this, a late-evening discharge whose
        # cast-off falls on day N+1 would deregister from day N+1 and leave
        # the day-N slot permanently locked, blocking all subsequent vessels.
        self._point_b_registered_day[mother_name] = day_key

    def _point_b_deregister_mother(self, mother_name: str, t: float) -> None:
        """Release the serial-discharge lock after a vessel has fully cast off.

        Called at CAST_OFF_COMPLETE_B (daughters).  Once released, the berth
        is free for the next eligible
        vessel regardless of calendar day — enabling two or more serial
        discharges per day.

        Uses the stored pump-start day key (not cast-off time) so that discharges
        completing after midnight correctly clear the day-N slot rather than the
        day-(N+1) slot.

        Covers Bryanston and GreenEagle equally.
        """
        if mother_name not in {MOTHER_PRIMARY_NAME, MOTHER_SECONDARY_NAME}:
            return
        # Prefer the recorded pump-start day; fall back to cast-off day only
        # when no registration exists (e.g. seeded vessels at t=0).
        day_key = self._point_b_registered_day.pop(mother_name, None)
        if day_key is None:
            day_key = self.point_b_calendar_day_key(t)
        day_set = self.point_b_day_assigned_mothers.get(day_key)
        if day_set:
            day_set.discard(mother_name)

    def _point_b_mother_assigned_on_day(self, mother_name: str, start_t: float) -> bool:
        """Return True when this mother's serial-discharge lock is currently active.

        The lock is set at pump-start and released at cast-off — so this returns
        False as soon as the current vessel casts off, allowing a second (or
        third) discharge to start on the same calendar day.

        Covers Bryanston and GreenEagle.
        """
        if mother_name not in {MOTHER_PRIMARY_NAME, MOTHER_SECONDARY_NAME}:
            return False
        day_key = self.point_b_calendar_day_key(start_t)
        return mother_name in self.point_b_day_assigned_mothers.get(day_key, set())

    def mother_export_departure_eligible(self, mother_name):
        """Export may depart when the trigger is reached, unless daughter traffic
        at BIA is high enough that staying to absorb more cargo is beneficial.

        Departure rules (in priority order):
          1. MUST sail: mother is physically full (cannot take another daughter cargo)
          2. MUST sail: mother has been export_ready for ≥ EXPORT_SERIES_BUFFER_HOURS
             (prevents indefinite deferral)
          3. DEFER: ≥ EXPORT_DEFER_INBOUND_THRESHOLD daughters are inbound/waiting
             at BIA AND mother still has meaningful capacity remaining — stay and load
             more cargo before sailing (better utilisation of the voyage)
          4. SAIL: trigger reached and daughter pressure is low — depart normally
        """
        stock = self.mother_bbl[mother_name]
        cap   = self.mother_capacity_bbl(mother_name)
        remaining_capacity = max(0.0, cap - stock)

        # Rule 1 — physically full: must sail immediately
        cannot_accommodate_next = remaining_capacity < MIN_INCOMING_TRANSFER_BBL
        if cannot_accommodate_next:
            return True

        # Must have reached the export trigger to be a departure candidate at all
        reached_target = stock >= self.mother_export_trigger_bbl(mother_name)
        if not reached_target:
            return False

        # Rule 2 — safety valve: if export_ready has been set for too long, sail
        # regardless of daughter traffic (prevents indefinite deferral).
        # Use 0.5× buffer (24h) so a full day's daughter traffic never permanently
        # blocks a departure — the mother must export within 24h of reaching trigger.
        ready_since = self.export_ready_since.get(mother_name)
        if ready_since is not None:
            time_ready = getattr(self, '_current_t', 0) - ready_since
            if time_ready >= EXPORT_SERIES_BUFFER_HOURS * 0.5:
                return True

        # Rule 3 — daughter look-ahead deferral
        # If ≥ EXPORT_DEFER_INBOUND_THRESHOLD daughters are inbound/at BIA AND
        # this mother can still absorb at least one more daughter cargo, defer
        # departure so she loads up further before sailing.
        # Exclude established MTO transient receivers: they are stuck at BIA
        # waiting for a primary berth (not delivering to this mother), and
        # counting them inflates the pressure and blocks export indefinitely.
        _raw_inbound = self._daughters_inbound_to_bia()
        _mto_stuck = sum(
            1 for vv in self.vessels
            if getattr(vv, "_mto_transient_since_day", None) is not None
            and vv.status in {"WAITING_BERTH_B", "WAITING_MOTHER_CAPACITY"}
        )
        _n_inbound = max(0, _raw_inbound - _mto_stuck)
        if (_n_inbound >= EXPORT_DEFER_INBOUND_THRESHOLD
                and remaining_capacity >= MIN_INCOMING_TRANSFER_BBL):
            return False

        # Rule 4 — normal departure: trigger reached, daughter pressure is low
        return True

    def _mto_transient_can_discharge_today(self, vessel, t):
        """Return True if the MTO transient vessel can berth a primary mother
        before the end of today's daylight window.

        MTO transients have ABSOLUTE priority at BIA — they displace normal
        daughters via _displace_incumbent_at_mother. Therefore the berth-free
        timestamp is irrelevant: the transient will claim the berth regardless
        of any non-transient reservation. We only check physical availability
        (mother at BIA, not mid-export, has any positive space).

        This prevents the bug where an arriving daughter pre-reserves a berth
        and the transient sees berth_free_at > daylight_end, concludes no berth
        is available, and MTO keeps topping it up indefinitely.
        """
        for _mn in MOTHER_NAMES:
            if not self.mother_is_at_point_b(_mn, t):
                continue
            # Block all export states: a mother in DOC/SAILING is leaving, and
            # one in HOSE/IN_PORT is physically at the export terminal — neither
            # can receive an MTO transient discharge.
            if self.export_state.get(_mn) in EXPORT_BUSY_STATES:
                continue
            if self.mother_capacity_bbl(_mn) - self.mother_bbl.get(_mn, 0) <= 0:
                continue
            # Mother is physically present and has space — MTO transient WILL
            # claim this berth (displacing any normal daughter). Return True.
            return True
        return False

    def _enforce_exclusive_day_at_mother(
        self, mother_name: str, t: float, physical_end: float
    ) -> None:
        """Enforce the exclusive-day berthing rule for an MTO discharge.

        When an MTO transient starts discharging to mother_name at time t:
          1. Lock mother_berth_free_at[mother_name] until the LATER of
             (a) physical_end  — when pumping + cast-off physically completes
             (b) _next_day_berth_start(t) — 08:00 of the NEXT calendar day
             → No other vessel may berth this mother for the rest of today.
          2. Displace any vessel currently in BERTHING_B or HOSE_CONNECT_B
             at this mother back to WAITING_BERTH_B.
             Vessels actively pumping (DISCHARGING) are never interrupted.

        Called from three places:
          • MTO transient HOSE_CONNECT_B → DISCHARGING
        """
        _lock_until = max(physical_end, self._next_day_berth_start(t))
        # (a) Set the exclusive-day berth lock
        self.mother_berth_free_at[mother_name] = max(
            self.mother_berth_free_at.get(mother_name, 0.0),
            _lock_until,
        )
        # (b) Displace any pre-pump incumbents — they can re-berth from tomorrow
        for _vv in self.vessels:
            if _vv.assigned_mother != mother_name:
                continue
            if _vv.status not in {"BERTHING_B", "HOSE_CONNECT_B"}:
                continue
            # Pre-pump state — safe to displace without interrupting any operation
            _vv.status = "WAITING_BERTH_B"
            _vv.assigned_mother = None
            _vv.next_event_time = self.next_daylight_hourly_berth_check(
                t, point="B")
            self.log_event(
                t, _vv.name, "WAITING_BERTH_B",
                f"Displaced from {mother_name}: exclusive-day lock active — "
                f"MTO transient discharging until "
                f"{self.hours_to_dt(_lock_until).strftime('%Y-%m-%d %H:%M')}. "
                f"Re-assessing at "
                f"{self.hours_to_dt(_vv.next_event_time).strftime('%Y-%m-%d %H:%M')}.",
                voyage_num=_vv.current_voyage, mother=mother_name,
            )

    def _next_day_berth_start(self, t: float) -> float:
        """Sim-hour at which the NEXT operating day begins (08:00 wall clock).

        When an MTO transient starts discharging at time t, the target
        mother's berth is exclusive for the rest of that calendar day.
        Lock until max(physical_completion, this value).

        Example: t=9 (17:00 Day1) → returns 16 (08:00 Day2).
        """
        return (int((t + SIM_HOUR_OFFSET) // 24) + 1) * 24 - SIM_HOUR_OFFSET

    def _daughters_inbound_to_bia(self):
        """Count daughter vessels currently arriving at or actively at BIA.

        Only counts vessels physically converging on BIA or discharging there.
        Excludes vessels that have FINISHED at BIA and are departing (CAST_OFF_B,
        WAITING_RETURN_STOCK) — those were inflating the count and falsely
        triggering export deferral Rule 3 indefinitely.

        Used for export departure look-ahead.
        """
        _at_or_inbound = {
            # Inbound — sailing toward BIA
            "SAILING_AB_LEG2", "WAITING_FAIRWAY", "SAILING_BW_TO_FWY",
            "SAILING_CROSS_BW_AC", "SAILING_AB",
            # At BIA — waiting, berthing, or actively discharging
            "WAITING_BERTH_B", "BERTHING_B", "HOSE_CONNECT_B",
            "DISCHARGING", "WAITING_CAST_OFF",
            "WAITING_MOTHER_CAPACITY", "WAITING_MOTHER_RETURN",
            # NOTE: "CAST_OFF_B" and "WAITING_RETURN_STOCK" are EXCLUDED —
            # those vessels completed discharge and are leaving BIA.
            # Counting them inflated pressure and blocked export departures.
        }
        return sum(1 for vv in self.vessels if vv.status in _at_or_inbound)

    def _maybe_run_multiple_transient_op(self, t):
        """Fire at 08:00 AND at the first tick >=12:00 each day.

        Normal mode (export available):
          Single-pair MTO — one transient accumulates parcels until a mother
          berth opens.  Parcel limit = MTO_MAX_PARCELS_BEFORE_OFFLOAD (1) or
          MTO_MAX_PARCELS_ESCALATED (3) when both primaries are down.

        Aggressive mode (export_unavailability window active):
          Multi-pair MTO — every idle large vessel becomes a receiver; every
          idle small vessel discharges to the nearest available receiver.
          Watson can load from Woodstock while Sherlock loads from Bagshot
          simultaneously.  SantaMonica discharges to any receiver.
          No per-day fire limit — the function forms as many pairs as the
          waiters allow on each tick.
          Each receiver fills to its full MTO_TRANSIENT_CAPACITY_BBL, only
          offloading to a mother once a berth genuinely opens.
        """
        if not MULTIPLE_TRANSIENT_OPERATION:
            return

        # ── Is export blocked right now? ─────────────────────────────────────
        _export_unavail_now = any(
            _eu_s <= t < _eu_e
            for (_eu_s, _eu_e) in getattr(self, 'export_unavailability_windows', [])
        )

        # ── Gate 1: time-of-day window ────────────────────────────────────────
        # Scan every half-hour tick during daylight (DAYLIGHT_START–20:00).
        # The per-day fire cap prevents runaway pairing while the hard-waiter
        # check below ensures MTO only fires on confirmed idle BIA vessels.
        wall_hour = (t + SIM_HOUR_OFFSET) % 24
        day_key   = int((t + SIM_HOUR_OFFSET) // 24)
        _in_daylight = DAYLIGHT_START <= wall_hour < 20.0
        if not _in_daylight:
            return

        # ── Gate 2: vessels stranded or converging on BIA ───────────────────
        # Primary condition: ≥2 waiters AND ≥1 hard-waiter (physically stopped).
        # Supplemental condition: ≥2 total waiters AND more vessels converging
        #   than available primary berth slots — fires preemptively so MTO pairs
        #   form before the excess vessels physically stop at BIA.
        # Example: Laphroaig + Rahama + Rathbone all SAILING_AB_LEG2 (no hard
        #   waiter), but only 1 primary mother berth is free today — the 2nd and
        #   3rd vessels will be stranded; MTO fires now so Laphroaig can absorb
        #   Rahama's cargo while still inbound rather than waiting at BIA idle.
        _hard_wait = {"WAITING_BERTH_B", "WAITING_MOTHER_CAPACITY"}
        _soft_wait = {"WAITING_FAIRWAY", "SAILING_AB_LEG2"}
        _all_wait  = _hard_wait | _soft_wait
        waiters = [vv for vv in self.vessels
                   if vv.status in _all_wait and vv.cargo_bbl > 0]
        if len(waiters) < 2:
            return
        _hard_waiters = [vv for vv in waiters if vv.status in _hard_wait]

        if len(_hard_waiters) < 1:
            # No hard-waiter yet — check convergence: count free primary berths.
            # If fewer berths are available than vessels converging, MTO should
            # fire preemptively for the overflow vessels.
            _free_primary_slots = sum(
                1 for mn in MOTHER_NAMES
                if self.mother_is_at_point_b(mn, t)
                and self.mother_capacity_bbl(mn) - self.mother_bbl.get(mn, 0) > 0
                and self.mother_berth_free_at.get(mn, 0.0) <= t + 1e-6
                and self.export_state.get(mn) not in EXPORT_BUSY_STATES
            )
            _converging_vessels = len(waiters)   # all inbound + at-BIA vessels
            if _converging_vessels <= _free_primary_slots:
                # Enough berths for everyone — no MTO needed yet
                return
            # More vessels than berths: fall through to fire MTO
            # Promote soft-waiters to satisfy the rest of the MTO logic
        # (If _hard_waiters >= 1, fall through normally)

        # Per-day fire cap — normal mode 12 pairs/day max (1/hr × 12 h window);
        # unlimited in export-unavailability (aggressive) mode.
        if not _export_unavail_now:
            _bryanston_ok = (self.mother_is_at_point_b(MOTHER_PRIMARY_NAME, t)
                             and self.mother_capacity_bbl(MOTHER_PRIMARY_NAME)
                                 - self.mother_bbl.get(MOTHER_PRIMARY_NAME, 0) > 0)
            _greeneagle_ok = (self.mother_is_at_point_b(MOTHER_SECONDARY_NAME, t)
                              and self.mother_capacity_bbl(MOTHER_SECONDARY_NAME)
                                  - self.mother_bbl.get(MOTHER_SECONDARY_NAME, 0) > 0)
            _primaries_both_down_early = not _bryanston_ok and not _greeneagle_ok
            _max_fires = 24 if _primaries_both_down_early else 12
            _day_fires = self._mto_days_fired.get(day_key, 0)
            if _day_fires >= _max_fires:
                return

        # ── Gate 3: enough mother berths to serve ALL waiting vessels today ────
        # OLD logic: if ANY mother has space + free berth → suppress MTO.
        # NEW logic: count available berth-slots; suppress MTO only when
        #   slots ≥ vessel count (every vessel can discharge directly).
        # When vessels > slots, MTO fires so excess vessels are not stranded.
        # Example: 3 vessels, 1 free berth → 2 will be idle → MTO fires.
        _min_cargo = min(vv.cargo_bbl for vv in waiters)
        _daylight_end_t = (int((t + SIM_HOUR_OFFSET) // 24) * 24
                           + DAYLIGHT_END - SIM_HOUR_OFFSET)

        # Count mothers with (a) space ≥ smallest cargo AND (b) berth free today
        _available_slots = 0
        for mn in MOTHER_NAMES:
            if not self.mother_is_at_point_b(mn, t):
                continue
            if self.mother_capacity_bbl(mn) - self.mother_bbl.get(mn, 0) < _min_cargo:
                continue
            berth_free_at = self.mother_berth_free_at.get(mn, 0.0)
            if berth_free_at <= _daylight_end_t:
                _available_slots += 1

        # Suppress MTO only when every waiting vessel has a direct berth today.
        # If vessels > slots, MTO fires for the overflow — those vessels would
        # otherwise be stranded idle until a berth opens the following day.
        if _available_slots >= len(waiters) and _available_slots > 0:
            return

        # ── Escalation flags ──────────────────────────────────────────────────
        _bryanston_available = (
            self.mother_is_at_point_b(MOTHER_PRIMARY_NAME, t)
            and (self.mother_capacity_bbl(MOTHER_PRIMARY_NAME)
                 - self.mother_bbl[MOTHER_PRIMARY_NAME]) >= _min_cargo
        )
        _greeneagle_available = (
            self.mother_is_at_point_b(MOTHER_SECONDARY_NAME, t)
            and (self.mother_capacity_bbl(MOTHER_SECONDARY_NAME)
                 - self.mother_bbl[MOTHER_SECONDARY_NAME]) >= _min_cargo
        )
        _both_primaries_down = not _bryanston_available and not _greeneagle_available
        _escalate_mto = _both_primaries_down or _export_unavail_now

        # ── Dynamic parcel limit ──────────────────────────────────────────────
        # Instead of a fixed global constant, derive how many top-ups the current
        # MTO receiver should accept from real-time operational demand:
        #
        #  tomorrow_congestion  = # vessels arriving at BIA tomorrow with cargo
        #                         that have no primary berth yet.  When ≥2,
        #                         congestion is expected to persist — the receiver
        #                         should keep absorbing parcels.  When 0 or 1,
        #                         congestion clears — the receiver should offload
        #                         immediately after the first parcel.
        #
        #  headroom_parcels     = how many more full-shuttle cargoes the receiver
        #                         can still hold before hitting its MTO ceiling.
        #                         Used to prevent the limit exceeding physical cap.
        #
        # Result:
        #  • No tomorrow congestion → limit = 1  (discharge as soon as possible)
        #  • Tomorrow congestion, headroom available → limit = min(headroom_parcels,
        #                                               tomorrow_congestion,
        #                                               MTO_MAX_PARCELS_ESCALATED)
        #  • Both primaries down / export window → escalated limit (as before) but
        #                                          also capped by headroom_parcels.
        #
        _tomorrow_t = t + 24.0          # one day ahead
        _tomorrow_day = day_key + 1
        # Vessels that will be waiting/arriving tomorrow: SAILING_AB_LEG2 or
        # WAITING_BERTH_B / WAITING_MOTHER_CAPACITY with cargo, not yet assigned.
        _tomorrow_waiters = [
            vv for vv in self.vessels
            if vv.cargo_bbl > 0
            and vv.status in {
                "SAILING_AB_LEG2", "WAITING_BERTH_B",
                "WAITING_MOTHER_CAPACITY", "WAITING_FAIRWAY",
            }
            # Exclude the current hard-waiters (they're already at BIA today)
            and vv not in _hard_waiters
        ]
        _tomorrow_congestion = len(_tomorrow_waiters)

        # Headroom estimate based on the smallest inbound cargo size.
        # Uses the minimum cargo among all current waiters as the representative
        # shuttle parcel size.
        _representative_parcel = _min_cargo if _min_cargo > 0 else MIN_INCOMING_TRANSFER_BBL

        # Compute headroom for each existing transient (largest headroom wins)
        _transient_vessels = [
            vv for vv in self.vessels
            if (getattr(vv, "_mto_transient_since_day", None) is not None
                or getattr(vv, "_is_mto_offload", False))
        ]
        if _transient_vessels:
            _best_headroom = max(
                max(0.0, MTO_TRANSIENT_CAPACITY_BBL.get(vv.name, vv.cargo_capacity)
                    - vv.cargo_bbl)
                for vv in _transient_vessels
            )
        else:
            # No active transient yet — use largest possible new receiver cap
            _best_headroom = max(
                (MTO_TRANSIENT_CAPACITY_BBL.get(vv.name, vv.cargo_capacity)
                 for vv in waiters
                 if vv.name not in MTO_NEVER_RECEIVER),
                default=float(DAUGHTER_CARGO_BBL),
            )
        _headroom_parcels = max(1, int(_best_headroom / max(_representative_parcel, 1)))

        if _escalate_mto:
            # Both primaries down or export window — aggressive accumulation
            # capped only by physical headroom
            _effective_parcel_limit = min(MTO_MAX_PARCELS_ESCALATED, _headroom_parcels)
        elif _tomorrow_congestion >= 2:
            # Congestion expected tomorrow — stay as receiver, absorb more parcels
            _effective_parcel_limit = min(MTO_MAX_PARCELS_ESCALATED, _headroom_parcels)
        else:
            # Congestion clears tomorrow (or only 1 more vessel) — offload quickly
            _effective_parcel_limit = MTO_MAX_PARCELS_BEFORE_OFFLOAD

        # ─────────────────────────────────────────────────────────────────────
        # AGGRESSIVE MULTI-PAIR mode (export unavailability active)
        # ─────────────────────────────────────────────────────────────────────
        if _export_unavail_now:
            self._mto_run_aggressive_pairs(
                t, day_key, waiters, _hard_wait, _effective_parcel_limit
            )
            return

        # ─────────────────────────────────────────────────────────────────────
        # NORMAL SINGLE-PAIR mode
        # ─────────────────────────────────────────────────────────────────────
        self._mto_days_fired[day_key] = self._mto_days_fired.get(day_key, 0) + 1

        # ── Helper: MTO cap ───────────────────────────────────────────────────
        def _mto_cap(vv):
            return MTO_TRANSIENT_CAPACITY_BBL.get(vv.name, vv.cargo_capacity)

        # ── Check for an existing active transient to top up ──────────────────
        existing_transient = next(
            (vv for vv in waiters
             if (getattr(vv, "_mto_transient_since_day", None) is not None
                 or getattr(vv, "_is_mto_offload", False))),
            None
        )

        if existing_transient is not None:
            # ── PRIORITY OVERRIDE: mother vessel supersedes MTO ───────────────
            # If a primary mother has returned from export and can berth the
            # existing transient before today's daylight ends, do NOT pair the
            # transient with any more dischargers.  The WAITING_BERTH_B handler
            # (which runs immediately after this function in the same tick) will
            # claim the berth and start the discharge.
            #
            # This resolves the race condition where MTO fires first and adds
            # more cargo to Watson while Bryanston just became available —
            # leaving Watson too full to discharge efficiently and blocking the
            # berth from other vessels.
            if self._mto_transient_can_discharge_today(existing_transient, t):
                self.log_event(
                    t, existing_transient.name, "MTO_PARCEL_LIMIT_REACHED",
                    f"[MTO Day {day_key+1}] Mother vessel available — "
                    f"suspending MTO top-ups for {existing_transient.name} "
                    f"({existing_transient.cargo_bbl:,.0f} bbl on board). "
                    f"WAITING_BERTH_B will claim primary berth this tick.",
                    voyage_num=existing_transient.current_voyage,
                )
                return

            _parcels_so_far = getattr(existing_transient, "_mto_parcels_received", 0)
            _trn_cap  = _mto_cap(existing_transient)
            _headroom = max(0.0, _trn_cap - existing_transient.cargo_bbl)
            _has_queued_discharger = any(
                getattr(vv, "_mto_target_vessel", None) == existing_transient.name
                for vv in self.vessels
            )
            if (_parcels_so_far >= _effective_parcel_limit or _headroom <= 0) and not _has_queued_discharger:
                self.log_event(
                    t, existing_transient.name, "MTO_PARCEL_LIMIT_REACHED",
                    f"[MTO Day {day_key+1}] No further top-ups — "
                    f"{'parcel limit reached' if _parcels_so_far >= _effective_parcel_limit else 'at capacity'} "
                    f"({existing_transient.cargo_bbl:,.0f}/{_trn_cap:,.0f} bbl) | "
                    f"{'ESCALATED (both primaries down)' if _both_primaries_down else 'normal limit'} | "
                    f"awaiting opportunistic mother berth",
                    voyage_num=existing_transient.current_voyage,
                )
                return
            elif (_parcels_so_far >= _effective_parcel_limit or _headroom <= 0) and _has_queued_discharger:
                return

            remaining = [vv for vv in waiters
                         if vv is not existing_transient
                         and vv.name not in PRIMARY_MOTHERS_ONLY_VESSELS]
            if not remaining:
                return
            _trn_cap_existing = _mto_cap(existing_transient)
            _fits_topup = [
                vv for vv in remaining
                if vv.cargo_bbl <= _headroom
                and _mto_cap(vv) <= _trn_cap_existing
            ]
            if not _fits_topup:
                return
            discharger_v = min(_fits_topup, key=lambda vv: vv.cargo_bbl)
            transient_v  = existing_transient
            transfer_bbl = min(discharger_v.cargo_bbl, _headroom)
        else:
            # ── Nominate a new transient ──────────────────────────────────────
            def _nom_score(vv):
                cap    = _mto_cap(vv)
                hdroom = max(0.0, cap - vv.cargo_bbl)
                hard_bonus = 1_000_000 if vv.status in _hard_wait else 0
                return cap * 10_000 + hdroom + hard_bonus

            # A vessel that can DIRECTLY discharge to a primary mother today
            # must NEVER be nominated as an MTO transient receiver — it has a
            # viable berth and nominating it as transient wastes that berth slot
            # while artificially inflating the MTO cargo queue.
            #
            # "Can berth directly today" means:
            #   (a) the vessel's cargo fits within a primary mother's live headroom
            #   (b) that mother's berth will be free within today's daylight window
            #   (c) the mother is physically at BIA (not at export)
            #
            # This is the fix for Woodstock (42k) being nominated as MTO transient
            # when Bryanston has 100k headroom and a free berth — Woodstock should
            # discharge to Bryanston directly rather than being consolidated.
            _can_berth_directly = set()
            for _vv in waiters:
                for _mn in MOTHER_NAMES:
                    if not self.mother_is_at_point_b(_mn, t):
                        continue
                    if self.export_ready.get(_mn, False):
                        continue
                    if self.export_state.get(_mn) in EXPORT_BUSY_STATES:
                        continue
                    _mspace = max(
                        0.0,
                        self.mother_capacity_bbl(_mn) - self.mother_bbl.get(_mn, 0)
                    )
                    if _vv.cargo_bbl > _mspace:
                        continue
                    # Berth free within today's daylight?
                    _bfree = self.mother_berth_free_at.get(_mn, 0.0)
                    if _bfree <= _daylight_end_t:
                        _can_berth_directly.add(_vv.name)
                        break

            # Use all waiters (hard + soft) for eligibility — Gate 2 may have
            # fired on soft-waiters (inbound) when converging vessels exceed slots.
            # Exclude vessels that can berth a primary directly — they don't need MTO.
            _eligible_transients = [
                vv for vv in waiters
                if vv.name not in MTO_NEVER_RECEIVER
                and vv.name not in _can_berth_directly
            ]
            if not _eligible_transients:
                return
            waiters_scored = sorted(_eligible_transients, key=_nom_score, reverse=True)
            transient_v   = waiters_scored[0]
            _trn_cap      = _mto_cap(transient_v)
            _headroom     = max(0.0, _trn_cap - transient_v.cargo_bbl)
            if _headroom <= 0:
                return

            remaining = [vv for vv in waiters
                         if vv is not transient_v
                         and vv.name not in PRIMARY_MOTHERS_ONLY_VESSELS
                         and vv.name not in _can_berth_directly]  # never discharge to MTO when direct berth available
            if not remaining:
                return
            _fits_clean = [
                vv for vv in remaining
                if vv.cargo_bbl <= _headroom and _mto_cap(vv) <= _trn_cap
            ]
            _fits_partial = [vv for vv in remaining if vv.cargo_bbl <= _headroom]

            if _fits_clean:
                discharger_v = min(_fits_clean, key=lambda vv: vv.cargo_bbl)
            elif _fits_partial:
                discharger_v = min(_fits_partial, key=lambda vv: vv.cargo_bbl)
            else:
                return

            transfer_bbl = min(discharger_v.cargo_bbl, _headroom)
            transient_v._mto_transient_since_day = day_key
            transient_v._mto_parcels_received    = 0

        if transfer_bbl <= 0:
            return

        self._mto_execute_pair(
            t, day_key, transient_v, discharger_v, transfer_bbl, _escalate_mto
        )

    def _mto_run_aggressive_pairs(self, t, day_key, waiters, hard_wait_set, parcel_limit):
        """Form the globally optimal set of concurrent transient/discharger pairs.

        During export unavailability every eligible vessel with headroom becomes
        a receiver; every other vessel with cargo discharges into the best
        available receiver.  Multiple pairs operate simultaneously:
            Watson  ← Bagshot        (Watson has most headroom)
            Amyla   ← Woodstock      (Amyla becomes receiver when Watson is full)
            SantaMonica → any receiver with headroom

        Algorithm — globally optimal bipartite matching:
          1. Build the set of CANDIDATE RECEIVERS: every non-MTO_NEVER_RECEIVER
             waiter whose berth is free, who has headroom, and who has not
             exceeded the parcel limit.  Rank by (already_receiver, MTO_cap) so
             established receivers are filled first before new ones are opened.
          2. Build the set of CANDIDATE DISCHARGERS: every waiter with cargo > 0
             that is not already designated as a receiver this tick.
          3. Run a greedy optimal matching:
             - For each discharger (smallest cargo first — fastest return to load):
               find the receiver that maximises transferred volume (most headroom,
               berth free, cap >= discharger cargo).
             - If no receiver can take this discharger at full cargo, try partial.
             - A vessel can only hold one role per tick (_assigned set).
          4. Execute all matched pairs.

        Rules:
          - MTO_NEVER_RECEIVER vessels (Rahama, SantaMonica) are dischargers only.
          - Berth serialisation: _mto_berth_free_at prevents double-booking a receiver.
          - Parcel limit prevents endless top-ups before the receiver offloads.
          - A vessel already marked as receiver (_mto_transient_since_day set) is
            preferred as receiver over a fresh vessel of equal cap — fills it first.
        """
        def _mto_cap(vv):
            return MTO_TRANSIENT_CAPACITY_BBL.get(vv.name, vv.cargo_capacity)

        # ── Step 1: identify candidate receivers ──────────────────────────────
        # Sort: already-nominated receivers first (fill before opening new ones),
        # then by MTO cap descending (largest vessel = best receiver).
        def _recv_score(vv):
            already = 1 if getattr(vv, "_mto_transient_since_day", None) is not None else 0
            return (already, _mto_cap(vv))

        _candidate_receivers = sorted(
            [
                vv for vv in waiters
                if vv.name not in MTO_NEVER_RECEIVER
                and max(0.0, _mto_cap(vv) - vv.cargo_bbl) > 0
                and getattr(vv, "_mto_berth_free_at", 0.0) <= t
                and getattr(vv, "_mto_parcels_received", 0) < parcel_limit
                # ── PRIORITY OVERRIDE: exclude vessels that can berth a primary
                # mother today — they must discharge rather than accumulate more
                # cargo.  "Mother vessel daily operation supersedes MTO."
                and not (
                    getattr(vv, "_mto_transient_since_day", None) is not None
                    and self._mto_transient_can_discharge_today(vv, t)
                )
            ],
            key=_recv_score,
            reverse=True,
        )

        if not _candidate_receivers:
            return

        # ── Step 2: identify candidate dischargers ────────────────────────────
        # Any waiter with cargo that is NOT in the receiver pool AND is NOT an
        # established receiver (mto_transient_since_day set).  Established receivers
        # hold consolidated cargo waiting for a mother berth — they must NOT be
        # re-discharged into another vessel (they would lose all accumulated volume).
        _recv_names = {vv.name for vv in _candidate_receivers}
        _established_recv_names = {
            vv.name for vv in waiters
            if getattr(vv, "_mto_transient_since_day", None) is not None
        }
        _candidate_dischargers = sorted(
            [
                vv for vv in waiters
                if vv.cargo_bbl > 0
                and vv.name not in _recv_names
                and vv.name not in _established_recv_names
                and vv.name not in PRIMARY_MOTHERS_ONLY_VESSELS
            ],
            # Smallest cargo first: fastest to discharge and return to load port
            key=lambda vv: vv.cargo_bbl,
        )

        # ── Step 3: optimal greedy matching ───────────────────────────────────
        # For each discharger find the best available receiver.
        # "Best" = most headroom (maximises volume moved per berth slot),
        # ties broken by largest MTO cap (keep large vessels as receivers).
        _assigned_receivers  = set()   # receiver names claimed this tick
        _assigned_dischargers = set()  # discharger names claimed this tick
        _pairs = []                    # list of (recv, discharger, transfer_bbl)

        # Build a mutable headroom map so sequential assignments see updated state
        _headroom_now = {
            vv.name: max(0.0, _mto_cap(vv) - vv.cargo_bbl)
            for vv in _candidate_receivers
        }

        for dis in _candidate_dischargers:
            if dis.name in _assigned_dischargers:
                continue

            # Find the best receiver for this discharger
            best_recv = None
            best_score = -1.0

            for recv in _candidate_receivers:
                if recv.name in _assigned_receivers:
                    continue
                headroom = _headroom_now[recv.name]
                if headroom <= 0:
                    continue
                # Receiver cap must be >= discharger cap (bigger holds smaller rule)
                if _mto_cap(recv) < _mto_cap(dis):
                    continue
                # How much can we actually transfer?
                xfer = min(dis.cargo_bbl, headroom)
                if xfer <= 0:
                    continue
                # Score: full-load preferred; then most headroom; then largest cap
                full_bonus = 1_000_000 if xfer == dis.cargo_bbl else 0
                hard_bonus = 500_000 if dis.status in hard_wait_set else 0
                score = full_bonus + hard_bonus + headroom + _mto_cap(recv)
                if score > best_score:
                    best_score = score
                    best_recv  = recv

            if best_recv is None:
                # Relax the cap constraint — allow same-cap or smaller receiver
                # as last resort (e.g. Amyla receiving Bagshot of equal MTO cap)
                for recv in _candidate_receivers:
                    if recv.name in _assigned_receivers:
                        continue
                    headroom = _headroom_now[recv.name]
                    if headroom <= 0:
                        continue
                    xfer = min(dis.cargo_bbl, headroom)
                    if xfer <= 0:
                        continue
                    full_bonus = 1_000_000 if xfer == dis.cargo_bbl else 0
                    hard_bonus = 500_000 if dis.status in hard_wait_set else 0
                    score = full_bonus + hard_bonus + headroom + _mto_cap(recv)
                    if score > best_score:
                        best_score = score
                        best_recv  = recv

            if best_recv is None:
                continue

            xfer_bbl = min(dis.cargo_bbl, _headroom_now[best_recv.name])
            if xfer_bbl <= 0:
                continue

            _pairs.append((best_recv, dis, xfer_bbl))
            _assigned_receivers.add(best_recv.name)
            _assigned_dischargers.add(dis.name)
            # Reduce headroom so subsequent dischargers see the updated state
            _headroom_now[best_recv.name] -= xfer_bbl

        # ── Step 4: execute all matched pairs ─────────────────────────────────
        _pairs_fired = 0
        for recv, dis, xfer_bbl in _pairs:
            # Initialise receiver tracking on first nomination this stay
            if getattr(recv, "_mto_transient_since_day", None) is None:
                recv._mto_transient_since_day = day_key
                recv._mto_parcels_received    = 0

            self._mto_execute_pair(t, day_key, recv, dis, xfer_bbl, escalated=True)
            _pairs_fired += 1

        if _pairs_fired:
            self._mto_days_fired[day_key] = (
                self._mto_days_fired.get(day_key, 0) + _pairs_fired
            )

    def _mto_execute_pair(self, t, day_key, transient_v, discharger_v,
                          transfer_bbl, escalated=False):
        """Execute one vessel-to-vessel MTO transfer with full BIA timing.

        Separated from _maybe_run_multiple_transient_op so both the single-pair
        normal mode and the multi-pair aggressive mode share the same physics
        and logging.
        """
        def _mto_cap(vv):
            return MTO_TRANSIENT_CAPACITY_BBL.get(vv.name, vv.cargo_capacity)

        # Serialisation: berth locked until previous discharger casts off
        _transient_berth_free = getattr(transient_v, "_mto_berth_free_at", 0.0)
        if _transient_berth_free > t:
            return

        # Full BIA timing: berthing → hose → pump → cast-off
        _berth_start   = t + BERTHING_DELAY_HOURS
        _hose_start    = _berth_start + POST_BERTHING_START_GAP_HOURS
        _pump_start    = _hose_start + HOSE_CONNECTION_HOURS
        _disch_rate    = VESSEL_DISCHARGE_RATE_BPH.get(discharger_v.name)
        transfer_hours = (transfer_bbl / _disch_rate) if _disch_rate else DISCHARGE_HOURS
        _pump_end      = _pump_start + transfer_hours
        cast_off_t     = self.next_cast_off_window(_pump_end)
        transfer_end_t = cast_off_t + CAST_OFF_HOURS

        # Lock this receiver's berth until cast-off completes
        transient_v._mto_berth_free_at = transfer_end_t

        # Blend API
        _dis_api = self.vessel_api.get(discharger_v.name, 0.0)
        _trn_api = self.vessel_api.get(transient_v.name, 0.0)
        _trn_vol = transient_v.cargo_bbl
        _new_trn = _trn_vol + transfer_bbl
        if _new_trn > 0:
            self.vessel_api[transient_v.name] = (
                (_trn_vol * _trn_api + transfer_bbl * _dis_api) / _new_trn
            )
        transient_v.cargo_bbl = _new_trn
        transient_v._mto_parcels_received = getattr(
            transient_v, "_mto_parcels_received", 0) + 1

        # Discharger: empty cargo, return to load
        discharger_v.cargo_bbl = 0
        self.vessel_api[discharger_v.name] = 0.0
        discharger_v.status          = "CAST_OFF_B"
        discharger_v.next_event_time = transfer_end_t

        # Receiver: stays WAITING_BERTH_B, but cannot seek a primary mother
        # berth until the transfer from the discharger is physically complete.
        # transfer_end_t already accounts for berthing + hose + pump + cast-off.
        # Setting next_event_time to t + 30min was the bug causing Woodstock to
        # berth Bryanston 3 hours after Rahama's transfer started — physically
        # impossible given Rahama's 30k / 4000 bph = 7.5h pump time.
        transient_v.status          = "WAITING_BERTH_B"
        transient_v.next_event_time = transfer_end_t

        # Logging
        _parcel_num  = transient_v._mto_parcels_received
        _cap_label   = _mto_cap(transient_v)
        _hdroom_left = max(0.0, _cap_label - transient_v.cargo_bbl)
        _mode_tag    = "AGGRESSIVE-MULTI" if escalated else "NORMAL"
        self.log_event(
            t, transient_v.name, "MTO_TRANSIENT_NOMINATED",
            f"[MTO {_mode_tag} Day {day_key+1} — Parcel {_parcel_num}] "
            f"Received {transfer_bbl:,.0f} bbl from {discharger_v.name} "
            f"@ {_dis_api:.2f}° API | on-board: {transient_v.cargo_bbl:,.0f} bbl "
            f"(cap {_cap_label:,.0f} bbl, {_hdroom_left:,.0f} bbl headroom remaining) | "
            f"berth locked until {self.hours_to_dt(transfer_end_t).strftime('%Y-%m-%d %H:%M')}",
            voyage_num=transient_v.current_voyage,
        )
        self.log_event(
            t, discharger_v.name, "MTO_DISCHARGE_TO_TRANSIENT",
            f"[MTO {_mode_tag} Day {day_key+1}] Berthing+hose+pump to {transient_v.name}: "
            f"{transfer_bbl:,.0f} bbl ({BERTHING_DELAY_HOURS:.1f}h berth + "
            f"{HOSE_CONNECTION_HOURS:.1f}h hose + {transfer_hours:.1f}h pump = "
            f"{BERTHING_DELAY_HOURS + HOSE_CONNECTION_HOURS + transfer_hours:.1f}h total) | "
            f"freed — returning to reload | cast-off "
            f"{self.hours_to_dt(transfer_end_t).strftime('%Y-%m-%d %H:%M')}",
            voyage_num=discharger_v.current_voyage,
        )
        if transfer_hours > 0:
            self.log_event(
                _pump_end, discharger_v.name, "MTO_TRANSFER_COMPLETE",
                f"Transfer complete | {transient_v.name}: {transient_v.cargo_bbl:,.0f} bbl on board | "
                f"next discharger may berth after {self.hours_to_dt(transfer_end_t).strftime('%Y-%m-%d %H:%M')}",
                voyage_num=discharger_v.current_voyage,
            )

    def _run_zeezee(self, t):
        """Monthly arrival trigger + full discharge state machine for ZeeZee.

        Called every timestep from run() BEFORE the daughter vessel loop so
        ZeeZee gets priority in the same tick she is processed.

        Two-clock priority model
        ────────────────────────
        Operational constraint  — mothers away / at capacity / offline.
          ZeeZee waits indefinitely; daughter_block_since is NOT advanced.

        Daughter congestion  — a feasible mother exists but her berth is held
          by a queued daughter.  daughter_block_since starts (or continues).
          After ZEEZEE_MAX_DAUGHTER_WAIT_HOURS the berth is forcibly cleared
          and ZeeZee proceeds immediately.
        """
        # ── Step A: monthly arrival trigger ───────────────────────────────────
        if ZEEZEE_SCHEDULE and self.zeezee is None:
            _zz_wall = (t + SIM_HOUR_OFFSET) % 24
            # Only trigger at the 08:00 wall-clock tick.
            # SIM_HOUR_OFFSET=8 means t=0 is 08:00, so _zz_wall==8.0 at every 08:00.
            if abs(_zz_wall - SIM_HOUR_OFFSET) < TIME_STEP_HOURS * 0.5:
                _cal = self.hours_to_dt(t)
                _ym  = (_cal.year, _cal.month)
                if _ym not in self.zeezee_months_visited:
                    # Find the schedule entry whose day_of_month matches today
                    for _entry in ZEEZEE_SCHEDULE:
                        if _cal.day == _entry.get("day_of_month", 0):
                            self.zeezee_months_visited.add(_ym)
                            _vol = float(_entry.get("volume_bbl", 200_000))
                            _api = float(_entry.get("api", 32.0))
                            self.zeezee = ThirdPartyVessel(
                                volume_bbl=_vol, api=_api, arrival_t=t)
                            self.log_event(
                                t, "ZeeZee", "VESSEL_JOINED",
                                f"ZeeZee arrived at Point B — {_vol:,.0f} bbl "
                                f"@ {_api}° API — awaiting discharge berth",
                            )
                            break

        # ── Step B: state machine ─────────────────────────────────────────────
        _zz = self.zeezee
        if _zz is None or t < _zz.next_event_time:
            return

        if _zz.status == "WAITING_B":
            # Find earliest-available mother
            _best_start  = None
            _best_mother = None
            _bwin        = self.next_berthing_window(t, point="B")
            for _mn in MOTHER_NAMES:
                if not self.mother_is_at_point_b(_mn, t):
                    continue                       # operationally absent
                _mcap = self.mother_capacity_bbl(_mn)
                if self.mother_bbl[_mn] + _zz.cargo_bbl > _mcap:
                    continue                       # no space
                _earliest = max(_bwin,
                                self.mother_berth_free_at[_mn],
                                self.mother_available_at[_mn])
                _slot = self.next_berthing_window(_earliest, point="B")
                if _best_start is None or _slot < _best_start:
                    _best_start  = _slot
                    _best_mother = _mn

            if _best_mother is None:
                # ── No primary operationally available ────────────────────────
                # True constraint (not daughters).  Reset congestion clock.
                _zz.daughter_block_since = None
                _next = self.next_daylight_hourly_berth_check(t, point="B")
                _zz.next_event_time = _next
                self.log_event(t, "ZeeZee", "WAITING_MOTHER_CAPACITY",
                               "No primary mother available (operational constraint); "
                               f"reassessing at "
                               f"{self.hours_to_dt(_next).strftime('%Y-%m-%d %H:%M')}")
                return

            # A feasible mother exists — check if daughters are blocking her
            _berth_blocked_by_daughter = any(
                v.assigned_mother == _best_mother
                and v.status in {"BERTHING_B", "HOSE_CONNECT_B", "DISCHARGING"}
                for v in self.vessels
            )

            if _best_start > t + TIME_STEP_HOURS * 0.5 and _berth_blocked_by_daughter:
                # ── Daughter-congestion wait ──────────────────────────────────
                if _zz.daughter_block_since is None:
                    _zz.daughter_block_since = t
                    self.log_event(
                        t, "ZeeZee", "WAITING_BERTH_B",
                        f"Berth at {_best_mother} held by daughter vessel; "
                        f"2-day deadline starts — "
                        f"force-berth at "
                        f"{self.hours_to_dt(t + ZEEZEE_MAX_DAUGHTER_WAIT_HOURS).strftime('%Y-%m-%d %H:%M')}",
                    )
                _waited = t - _zz.daughter_block_since
                if _waited >= ZEEZEE_MAX_DAUGHTER_WAIT_HOURS:
                    # ── 2-day deadline exceeded: force berth ──────────────────
                    self.mother_berth_free_at[_best_mother] = t
                    _best_start = self.next_berthing_window(t, point="B")
                    self.log_event(
                        t, "ZeeZee", "ZEEZEE_DEADLINE_OVERRIDE",
                        f"2-day daughter queue exceeded ({_waited:.1f} h); "
                        f"forcing berth at {_best_mother}",
                    )
                    _zz.daughter_block_since = None
                    # Fall through to BERTHING_B below
                else:
                    _next = self.next_daylight_hourly_berth_check(t, point="B")
                    _zz.next_event_time = _next
                    return

            # ── Berth secured: proceed to BERTHING_B ─────────────────────────
            _zz.daughter_block_since = None
            _zz.assigned_mother = _best_mother
            _zz_rate = VESSEL_DISCHARGE_RATE_BPH.get("ZeeZee", ThirdPartyVessel.DISCHARGE_RATE_BPH)
            _discharge_hrs = _zz.cargo_bbl / _zz_rate
            _pump_end_zz   = _best_start + BERTHING_DELAY_HOURS + HOSE_CONNECTION_HOURS + _discharge_hrs
            _discharge_end = _berth_free_at(_pump_end_zz)
            self.mother_berth_free_at[_best_mother] = max(
                self.mother_berth_free_at[_best_mother], _discharge_end)
            _zz.status = "BERTHING_B"
            _zz.next_event_time = _best_start + BERTHING_DELAY_HOURS
            self.log_event(
                _best_start, "ZeeZee", "BERTHING_START_B",
                f"ZeeZee berthing at {_best_mother} "
                f"(priority discharge — {BERTHING_DELAY_HOURS*60:.0f} min procedure)",
                mother=_best_mother,
            )

        elif _zz.status == "BERTHING_B":
            _mn = _zz.assigned_mother
            if not self.mother_is_at_point_b(_mn, t):
                # Mother departed — requeue
                _zz.status = "WAITING_B"
                _zz.assigned_mother = None
                _zz.next_event_time = self.next_daylight_hourly_berth_check(t, point="B")
                self.log_event(t, "ZeeZee", "WAITING_MOTHER_RETURN",
                               f"{_mn} departed during ZeeZee berthing; requeueing")
                return
            _zz.status = "HOSE_CONNECT_B"
            _zz.next_event_time = t + HOSE_CONNECTION_HOURS
            self.log_event(t, "ZeeZee", "HOSE_CONNECTION_START_B",
                           f"Hose connection at {_mn} ({HOSE_CONNECTION_HOURS:.0f} h)",
                           mother=_mn)

        elif _zz.status == "HOSE_CONNECT_B":
            _mn = _zz.assigned_mother
            _mcap = self.mother_capacity_bbl(_mn)
            if self.mother_bbl[_mn] + _zz.cargo_bbl > _mcap:
                # Capacity issue — wait 6 h and retry
                _zz.next_event_time = t + 6
                self.log_event(t, "ZeeZee", "WAITING_MOTHER_CAPACITY",
                               f"{_mn} lacks space; rechecking in 6 h")
                return
            # Blend API and credit mother
            self.mother_api[_mn] = self.blend_api(
                self.mother_bbl[_mn], self.mother_api.get(_mn, 0.0),
                _zz.cargo_bbl, _zz.api,
            )
            self.mother_bbl[_mn] += _zz.cargo_bbl
            self.total_loaded    += _zz.cargo_bbl
            _zz_rate = VESSEL_DISCHARGE_RATE_BPH.get("ZeeZee", ThirdPartyVessel.DISCHARGE_RATE_BPH)
            _discharge_hrs = _zz.cargo_bbl / _zz_rate
            _zz.status = "DISCHARGING"
            self.mother_berth_free_at[_mn] = max(
                self.mother_berth_free_at[_mn], _berth_free_at(t + _discharge_hrs))
            _zz.next_event_time = t + _discharge_hrs
            self.log_event(
                t, "ZeeZee", "DISCHARGE_START",
                f"Discharging {_zz.cargo_bbl:,.0f} bbl "
                f"@ {_zz.api:.2f}° API | "
                f"{_mn}: {self.mother_bbl[_mn]:,.0f} bbl "
                f"(blended {self.mother_api.get(_mn, 0.0):.2f}° API)",
                mother=_mn,
            )

        elif _zz.status == "DISCHARGING":
            _mn = _zz.assigned_mother
            _zz.status = "CAST_OFF_B"
            _zz.next_event_time = t + CAST_OFF_HOURS
            self.log_event(t, "ZeeZee", "DISCHARGE_COMPLETE",
                           f"{_mn}: {self.mother_bbl[_mn]:,.0f} bbl | "
                           f"ZeeZee departing in {CAST_OFF_HOURS*60:.0f} min",
                           mother=_mn)

        elif _zz.status == "CAST_OFF_B":
            self.log_event(t, "ZeeZee", "VESSEL_DEPARTED",
                           "ZeeZee cast off and departed — next visit next month")
            self.zeezee = None   # visit complete; reset for next month trigger

            # NOTE: the physical BERTHING_START_B event is intentionally NOT logged
            # here.  berth_start is a *plan*; another actor can still claim the berth
            # first.  The berth is logged only once the vessel actually proceeds past
            # the concurrent-occupancy guard at the BERTHING_B → HOSE_CONNECT_B
            # transition, so the journey plan never shows a physical berth that was
            # immediately aborted without pumping.

    # ── Variability-aware duration helpers ───────────────────────────────────

    def _berthing_delay(self) -> float:
        """Return a sampled berthing delay (manoeuver + human lag).

        Accounts for pilot availability, traffic separation, crew readiness and
        human decision lag.  Returns BERTHING_DELAY_HOURS in deterministic mode.
        """
        nominal = BERTHING_DELAY_HOURS
        if not ENABLE_VARIABILITY:
            return nominal
        sampled = _variability_sample(nominal, VARIABILITY_CV_BERTHING) + _human_lag_hours()
        if hasattr(self, "_sim_stats"):
            self._sim_stats.record("berthing_delay", nominal, sampled)
        return sampled

    def _hose_connect_hours(self) -> float:
        """Return a sampled hose-connection duration.

        Reflects variability in crew readiness, equipment condition, and
        number of vessels competing for port resources.  Returns
        HOSE_CONNECTION_HOURS in deterministic mode.
        """
        nominal = HOSE_CONNECTION_HOURS
        if not ENABLE_VARIABILITY:
            return nominal
        n_waiting = sum(
            1 for vv in self.vessels
            if vv.status in {"WAITING_BERTH_B", "BERTHING_B", "HOSE_CONNECT_B"}
        )
        sampled = (
            _variability_sample(nominal, VARIABILITY_CV_HOSE_CONNECT)
            * _congestion_factor(n_waiting)
        )
        if hasattr(self, "_sim_stats"):
            self._sim_stats.record("hose_connect", nominal, sampled)
        return sampled

    def _export_doc_hours(self) -> float:
        """Return a sampled export documentation duration.

        Port office workload, pre-departure inspections and cargo measurement
        disputes all contribute to documentation variability.
        """
        nominal = EXPORT_DOC_HOURS
        if not ENABLE_VARIABILITY:
            return nominal
        sampled = _variability_sample(nominal, VARIABILITY_CV_EXPORT_DOC)
        if hasattr(self, "_sim_stats"):
            self._sim_stats.record("export_doc", nominal, sampled)
        return sampled

    def calibration_report(self) -> dict:
        """Return the planned-vs-actual calibration metrics collected this run.

        Keys: operation names.  Values: dicts with n, mean_planned_h,
        mean_actual_h, mean_bias_h, rmse_h, pct_bias.

        Useful for comparing the simulation against historical port data
        and adjusting the CV constants to match observed variability.
        """
        if hasattr(self, "_sim_stats"):
            return self._sim_stats.calibration_report()
        return {}

    def next_wall_clock_hour(self, current_hour, wall_clock_hour):
        """Return next sim-hour aligned to a wall-clock hour (0-23)."""
        day_key = int(current_hour // 24)
        sim_target_today = day_key * 24 + (wall_clock_hour - SIM_HOUR_OFFSET)
        if current_hour <= sim_target_today:
            return sim_target_today
        return sim_target_today + 24

    def projected_mother_stock(self, mother_name, horizon, exclude_vessel=None):
        """Projected mother stock by horizon based on currently committed BIA work.

        Oversized phantom reservations (vessels whose cargo exceeds the mother's
        current live headroom) are excluded so they don't artificially inflate
        the projected stock and bias mother selection away from Bryanston/GreenEagle
        when genuine headroom exists for smaller vessels like SantaMonica.
        """
        projected = float(self.mother_bbl[mother_name])
        _live_headroom = max(0.0, self.mother_capacity_bbl(mother_name) - projected)
        for vv in self.vessels:
            if vv.name == exclude_vessel:
                continue
            if vv.assigned_mother != mother_name or vv.cargo_bbl <= 0:
                continue
            # Skip phantom reservations: oversized vessels will be turned away
            # by MOTHER_CAPACITY_ABORT and should not inflate the projected stock.
            if vv.cargo_bbl > _live_headroom:
                continue
            add_at = None
            if vv.status == "HOSE_CONNECT_B":
                add_at = vv.next_event_time
            elif vv.status == "BERTHING_B":
                add_at = vv.next_event_time + HOSE_CONNECTION_HOURS
            elif vv.status == "WAITING_BERTH_B":
                add_at = vv.next_event_time + BERTHING_DELAY_HOURS + HOSE_CONNECTION_HOURS
            if add_at is not None and add_at <= horizon + 1e-6:
                projected += vv.cargo_bbl
        return projected

    def select_point_b_mother(self, v, decision_time, day_key, candidates):
        """Pick the best Point B mother for faster turnaround and export readiness.

        Grouping:
          group 0 — mother can berth the vessel same-day
          group 2 — next-day mother
        """
        day_key = self.point_b_calendar_day_key(decision_time)
        assigned_today = self.point_b_day_assigned_mothers.setdefault(day_key, set())
        horizon_8 = self.next_wall_clock_hour(decision_time, 8)
        # Next midnight in sim-hours — used to identify same-day candidates
        day_end = (int((decision_time + SIM_HOUR_OFFSET) // 24) + 1) * 24

        # PRIMARY_MOTHERS_ONLY vessels (e.g. SantaMonica) are the designated
        # "last delivery" to the primary mother — they should not be penalised
        # for pushing a near-full mother over its export trigger.  Regular
        # daughters receive a trigger-avoidance penalty so they route to the
        # mother with more headroom and leave the final top-up for the PMO vessel.
        _is_pmo = v.name in PRIMARY_MOTHERS_ONLY_VESSELS

        ranked = []
        for start, berth_t, mother_name in candidates:
            add_at = start + BERTHING_DELAY_HOURS + HOSE_CONNECTION_HOURS
            _base_stock = self.projected_mother_stock(
                mother_name,
                horizon_8,
                exclude_vessel=v.name,
            )
            projected_8 = _base_stock
            if add_at <= horizon_8 + 1e-6:
                projected_8 += v.cargo_bbl
            # For non-PMO daughters: flag when routing here would push the mother
            # over her export trigger AND she was already close enough that a
            # different vessel choice would avoid triggering altogether.
            #
            # Trigger-avoidance applies ONLY when the mother's current projected
            # stock (before this vessel) is already at or above the trigger.
            # When the trigger is only crossed because of this vessel's addition,
            # that is the intended and correct outcome of a normal fill cycle —
            # not a scheduling error to avoid.  Penalising it caused Woodstock
            # (42k) to be routed to GreenEagle even though Bryanston at 450k
            # was the right destination (450k + 42k = 492k → triggers export,
            # which is exactly what should happen).
            _exp_trig   = self.mother_export_trigger_bbl(mother_name)
            _will_trigger = (
                not _is_pmo
                and _base_stock >= _exp_trig   # already at/above trigger WITHOUT this vessel
                and projected_8 >= _exp_trig
            )
            ranked.append({
                "start": start,
                "berth_t": berth_t,
                "mother": mother_name,
                "immediate": start <= decision_time + 0.01,
                "same_day": start < day_end,
                "unused_today": mother_name not in assigned_today,
                "projected_8": projected_8,
                "will_trigger": _will_trigger,
            })

        # Sort all candidates together.
        # Key priority:
        #   1. Same-day mother → group 0
        #      Within group 0: prefer the mother NOT yet used today (load balancing)
        #      Then: earlier start; then lower projected stock; then name
        #   2. Next-day mother → group 2
        #
        # Load-balancing threshold: two primaries are "equally prompt" when their
        # start times are within LOAD_BALANCE_WINDOW_HOURS of each other.  Within
        # that window, the unused-today flag is the first tiebreaker so traffic
        # alternates between Bryanston and GreenEagle rather than funnelling to
        # whichever has the lower current stock (which always favours the one that
        # just returned from export).
        LOAD_BALANCE_WINDOW_HOURS = 4.0

        # Earliest group-0 primary start — used to detect "equally prompt" candidates
        _g0_starts = [r["start"] for r in ranked if r["same_day"]]
        _earliest_g0 = min(_g0_starts) if _g0_starts else None

        def _sort_key(r):
            group = 0 if r["same_day"] else 2

            # Fill ratio: projected stock / export trigger.  Higher ratio = closer
            # to export trigger = should receive the next daughter first, so that
            # the export cycle fires as soon as possible and the berth turns over.
            # This ensures Bryanston (450k / 465k = 0.97) is preferred over
            # GreenEagle (342k / 680k = 0.50) when both are available.
            _trig  = self.mother_export_trigger_bbl(r["mother"]) or 1
            _fill  = r["projected_8"] / _trig   # higher = closer to export

            if _is_pmo:
                _eq = (group == 0 and _earliest_g0 is not None
                       and r["start"] <= _earliest_g0 + LOAD_BALANCE_WINDOW_HOURS)
                return (
                    group,
                    0 if _eq else 1,
                    0 if r["unused_today"] else 1,
                    r["start"],
                    -_fill,          # higher fill ratio → lower sort value → preferred
                    r["mother"],
                )
            else:
                return (
                    group,
                    1 if r["will_trigger"] else 0,
                    0 if r["unused_today"] else 1,
                    r["start"],
                    -_fill,          # higher fill ratio → lower sort value → preferred
                    r["mother"],
                )

        ranked.sort(key=_sort_key)
        selected = ranked[0]
        # NOTE: _point_b_register_mother_start is NOT called here.
        # The lock fires at pump-start (HOSE_CONNECT_B → DISCHARGING) and is
        # released at CAST_OFF_COMPLETE_B so the rule tracks actual cargo flow.
        return selected, horizon_8

    def mother_fill_score(self):
        """Return a 0–100 score measuring how well primary mothers are
        accumulating cargo toward their export trigger.

        Penalises mothers below 40 % of their export trigger (starvation zone).
        Used by the objective function in run_optimizer().
        """
        scores = []
        for mn in (MOTHER_PRIMARY_NAME, MOTHER_SECONDARY_NAME):
            trig = MOTHER_EXPORT_TRIGGER_BY_NAME.get(mn, MOTHER_EXPORT_TRIGGER)
            if trig <= 0:
                continue
            fill_frac = min(1.0, self.mother_bbl.get(mn, 0.0) / trig)
            # Below 40 % fill: penalise proportionally
            deficit = max(0.0, 0.40 - fill_frac)   # 0 when fill >= 40 %
            scores.append(100.0 - deficit * 250.0)  # -1 pt per 0.4 % below 40 %
        return max(0.0, sum(scores) / max(1, len(scores)))

    def log_event(self, t, vessel_name, event, detail="", voyage_num=None, mother=None):
        # O(1) vessel lookup via index dict (built lazily, invalidated on join).
        if not hasattr(self, "_vessel_index") or len(self._vessel_index) != len(self.vessels):
            self._vessel_index = {vv.name: vv for vv in self.vessels}
        _v = self._vessel_index.get(vessel_name)
        # Resolve mother: explicit arg → vessel's current assigned_mother → None
        if mother is None and _v is not None:
            mother = _v.assigned_mother
        # Snapshot the vessel's current cargo API for this log row
        _vessel_api_snap = round(self.vessel_api.get(vessel_name, 0.0), 2) if _v is not None else 0.0
        # Resolve voyage code: explicit on vessel, else derive from current voyage_num
        _vcode = ""
        if _v is not None:
            _vcode = getattr(_v, "voyage_code", "") or ""
            if not _vcode and voyage_num:
                _vcode = make_voyage_code(vessel_name, voyage_num)
        self.log.append({
            "Time"       : self.hours_to_dt(t).strftime("%Y-%m-%d %H:%M"),
            "Day"        : int(t // 24) + 1,
            "Hour"       : f"{int(t % 24):02d}:{int((t % 1)*60):02d}",
            "Vessel"     : vessel_name,
            "Voyage"     : voyage_num,
            "VoyageCode" : _vcode,
            "Event"      : event,
            "Detail"     : detail,
            "Mother"     : mother,
            "Vessel_api" : _vessel_api_snap,
            "Storage_bbl": round(self.total_storage_bbl()),
            "SanBarth_bbl": round(self.storage_bbl[STORAGE_PRIMARY_NAME]),
            "JasmineS_bbl": round(self.storage_bbl[STORAGE_SECONDARY_NAME]),
            "Westmore_bbl": round(self.storage_bbl[STORAGE_TERTIARY_NAME]),
            "Duke_bbl": round(self.storage_bbl[STORAGE_QUATERNARY_NAME]),
            "Starturn_bbl": round(self.storage_bbl[STORAGE_QUINARY_NAME]),
            "PGM_bbl": round(self.storage_bbl[STORAGE_SENARY_NAME]),
            "Storage_Overflow_Accum_bbl": round(sum(self.storage_overflow_bbl.values())),
            "SanBarth_Overflow_Accum_bbl": round(self.storage_overflow_bbl[STORAGE_PRIMARY_NAME]),
            "JasmineS_Overflow_Accum_bbl": round(self.storage_overflow_bbl[STORAGE_SECONDARY_NAME]),
            "Westmore_Overflow_Accum_bbl": round(self.storage_overflow_bbl[STORAGE_TERTIARY_NAME]),
            "Duke_Overflow_Accum_bbl": round(self.storage_overflow_bbl[STORAGE_QUATERNARY_NAME]),
            "Starturn_Overflow_Accum_bbl": round(self.storage_overflow_bbl[STORAGE_QUINARY_NAME]),
            "PGM_Overflow_Accum_bbl": round(self.storage_overflow_bbl[STORAGE_SENARY_NAME]),
            "PointF_Overflow_Accum_bbl": round(self.point_f_overflow_accum_bbl),
            "PointF_Active_Loading_bbl": round(self.point_f_active_loading_bbl()),
            "Mother_bbl" : round(self.total_mother_bbl()),
            "Bryanston_bbl":  round(self.mother_bbl[MOTHER_PRIMARY_NAME]),
            "GreenEagle_bbl": round(self.mother_bbl[MOTHER_SECONDARY_NAME]),
            "Alkebulan_bbl":  round(self.mother_bbl[MOTHER_QUINARY_NAME]),
            "Total_Exported_bbl": self.total_exported,
        })

    def is_daylight_at(self, hour):
        """True if `hour` falls inside daylight operating window."""
        wall_h = (hour + SIM_HOUR_OFFSET) % 24
        return DAYLIGHT_START <= wall_h < DAYLIGHT_END

    # -----------------------------------------------------------------
    # Dispatch-bias helpers
    # -----------------------------------------------------------------

    def projected_stock_at(self, storage_name, horizon_h, vessel_eta_offset=0.0):
        """Project stock at *storage_name* `horizon_h` hours from now.

        Accounts for:
          - Net production inflow over the horizon
          - Draws from all committed vessels (loading / berth-waiting)
          - Optional vessel_eta_offset: time from now until the
            candidate vessel would arrive (used by dispatch look-ahead
            to forecast projected stock at vessel arrival time rather
            than just the current horizon).
        """
        stock  = self.storage_bbl[storage_name]
        rate   = self.production_rate_bph_at(storage_name, 0)
        cap    = STORAGE_CAPACITY_BY_NAME[storage_name]
        h = max(horizon_h, vessel_eta_offset)   # look at whichever is further out
        # Subtract draws from all vessels already committed to this storage.
        committed_statuses = {
            "LOADING",          # actively pumping cargo
            "HOSE_CONNECT_A",   # hoses connected, loading imminent
            "BERTHING_A",       # securing alongside, hose connection next
            "WAITING_BERTH_A",  # committed, waiting for berth to open
            "WAITING_STOCK",    # committed, waiting for stock threshold
        }
        for vv in self.vessels:
            if vv.assigned_storage != storage_name:
                continue
            if vv.status in committed_statuses:
                draw = self.effective_load_cap(vv.name, storage_name)
                stock = max(0.0, stock - draw)
        projected = stock + rate * h
        return min(projected, cap)

    def hours_to_overflow(self, storage_name):
        """Hours until storage overflows given current inflow and committed loads.

        A positive value means overflow is `n` hours away.  A very large
        value (1e9) means no overflow risk within any foreseeable horizon.
        """
        cap  = STORAGE_CAPACITY_BY_NAME[storage_name]
        rate = self.production_rate_bph_at(storage_name, 0)
        if rate <= 0:
            return 1e9
        stock = self.storage_bbl[storage_name]
        # Credit committed loads — they will remove stock before overflow
        committed_statuses = {
            "LOADING", "HOSE_CONNECT_A", "BERTHING_A",
            "WAITING_BERTH_A", "WAITING_STOCK",
        }
        committed_draw = sum(
            self.effective_load_cap(vv.name, storage_name)
            for vv in self.vessels
            if vv.assigned_storage == storage_name
            and vv.status in committed_statuses
        )
        ullage = cap - stock + committed_draw   # committed draws free space
        return max(0.0, ullage / rate)

    def area_travel_hours(self, from_area, to_area):
        """Return the conservative lower-bound travel time in hours
        between two area codes (single-char strings: A/B/C/D/E).
        Falls back to 14h if the pair is not in the table.
        """
        if from_area == to_area:
            return 0.0
        return _ROUTE_TRAVEL_HOURS.get((from_area, to_area),
               _ROUTE_TRAVEL_HOURS.get((to_area, from_area), 14.0))

    def production_rate_bias_factor(self, storage_name):
        """Return a small bias multiplier [0, DISPATCH_BIAS_MAX_FACTOR] that
        shrinks the apparent critical-gap for high-production storages.
        The bias is proportional to the normalised production rate and is
        only non-zero for SanBarth, JasmineS and Westmore (high-rate storages).
        """
        rate = STORAGE_PRODUCTION_RATE_BY_NAME.get(storage_name, 0.0)
        max_rate = max(STORAGE_PRODUCTION_RATE_BY_NAME.values()) or 1.0
        rate_norm = rate / max_rate           # 0..1 (1 = SanBarth/JasmineS)
        return DISPATCH_BIAS_MAX_FACTOR * rate_norm

    def storage_dispatch_rank(self, storage_name):
        """Return the risk-first dispatch rank tuple for a storage.

        Tuple: (overflow_imminent, unsafe_flag, effective_gap, -hours_to_overflow, -stock, name)
        Lower tuples are more urgent.  The overflow_imminent flag (0/1)
        promotes any storage within 24 h of overflow above all others
        regardless of current stock.  Within the same urgency band the
        production-rate bias compression breaks ties toward high-throughput
        storages exactly as before.
        """
        stock    = self.storage_bbl[storage_name]
        crit     = STORAGE_CRITICAL_THRESHOLD_BY_NAME[storage_name]
        h2o      = self.hours_to_overflow(storage_name)
        # Imminent: overflow within 24 h even after committed draws
        overflow_imminent = 0 if h2o > 24.0 else 1
        raw_gap  = abs(stock - crit)
        if raw_gap <= DISPATCH_BIAS_FORECAST_BBL:
            bias = self.production_rate_bias_factor(storage_name)
            effective_gap = raw_gap * (1.0 - bias)
        else:
            effective_gap = raw_gap
        unsafe = 0 if stock >= crit else 1
        return (overflow_imminent, unsafe, effective_gap, -h2o, -stock, storage_name)

    def plan_ac_waiting_assignments(self, vessels, t):
        """Greedy A/C matching for idle or waiting vessels.

        This prevents a smaller flexible vessel from reserving the most urgent
        Point A/C storage when a larger permitted vessel in the same waiting
        pool can drain materially more stock from that location.
        """
        ac_storages = [
            STORAGE_PRIMARY_NAME,
            STORAGE_SECONDARY_NAME,
            STORAGE_TERTIARY_NAME,
        ]
        pairings = []
        for vv in vessels:
            for storage_name in ac_storages:
                if not self.storage_allowed_for_vessel(storage_name, vv.name):
                    continue
                if self.storage_locked_by_active_berth(storage_name, requesting_vessel=vv.name):
                    continue
                current_bonus = 0 if vv.assigned_storage == storage_name else 1
                pairings.append((
                    self.storage_dispatch_rank(storage_name),
                    -self.effective_load_cap(vv.name, storage_name),
                    current_bonus,
                    vv.name,
                    storage_name,
                ))

        assignments = {}
        claimed_vessels = set()
        claimed_storages = set()
        for _, _, _, vessel_name, storage_name in sorted(pairings):
            if vessel_name in claimed_vessels or storage_name in claimed_storages:
                continue
            assignments[vessel_name] = storage_name
            claimed_vessels.add(vessel_name)
            claimed_storages.add(storage_name)
        return assignments

    def choose_hourly_storage_option(self, v, t, excluded_storages=None):
        """Choose hourly reassessment storage option with risk-first priority.

        Two enhancements over the baseline:

        1. PRODUCTION-RATE BIAS
           High-production storages (SanBarth / JasmineS / Westmore) receive a
           small apparent-gap compression of up to DISPATCH_BIAS_MAX_FACTOR
           (12 %) when within DISPATCH_BIAS_FORECAST_BBL of critical.  This
           means a high-production storage at, say, 35 k bbl above critical
           sorts as though it were 31 k above critical, nudging it ahead of
           a low-production peer at the same real gap.  The effect is gentle
           and never overrides a genuine Duke/Starturn emergency.

        2. POSITION-AWARE SPREAD WITH FORECASTING
           Spreading to Duke (D) or Starturn (E) is only offered to a vessel
           when:
             a) The vessel is permitted for that storage, AND
             b) No other vessel is already serving / en-route to it, AND
             c) The projected stock at D/E will be below (or within
                SPREAD_DE_URGENCY_HORIZON hours of reaching) critical by the
                time the vessel's ETA arrives there.
             d) No A/C high-production storage will itself enter an acute
                shortage within SPREAD_AC_HOLD_HORIZON hours that this vessel
                could otherwise cover.
           If none of the D/E candidates pass the urgency gate, the vessel
           stays on the best A/C option and the low-production storage is
           allowed to stretch — it will fill slowly under its own production.
        """
        # Skip reassessment only when the vessel is physically committed to Point F.
        # Bedford may be the active Ibom loader but have returned to Point A to load
        # a full cargo — in that case target_point != "F" and it must be reassessed.
        if v.target_point == "F":
            return None

        candidates_all = [
            s for s in STORAGE_NAMES
            if self.storage_allowed_for_vessel(s, v.name)
            and not self.storage_locked_by_active_berth(s, requesting_vessel=v.name)
        ]
        if not candidates_all:
            return None

        excluded = set(excluded_storages or ())
        candidates = candidates_all
        if excluded:
            non_excluded = [s for s in candidates_all if s not in excluded]
            if non_excluded:
                candidates = non_excluded

        waiting_pool = [
            vv for vv in self.vessels
            if vv.status in {"WAITING_BERTH_A", "IDLE_A", "WAITING_STOCK"}
            and vv.target_point in ("A", "C", "D", "E")
            and vv.target_point != "F"
        ]

        # ── Production-rate bias ──────────────────────────────────────────────
        # Compress the apparent gap for high-production storages when they are
        # close to critical, so they sort ahead of low-production peers.
        def risk_rank(storage_name):
            return self.storage_dispatch_rank(storage_name)

        ordered = sorted(candidates, key=risk_rank)

        # ── Position-aware spread to D/E ─────────────────────────────────────
        if len(waiting_pool) >= 2:
            v_area = STORAGE_POINT.get(v.assigned_storage, "A") if v.assigned_storage else "A"

            spread_storages_raw = [
                s for s in (STORAGE_QUATERNARY_NAME, STORAGE_QUINARY_NAME)
                if s in candidates
            ]

            # Sort spread candidates by urgency (most urgent first).
            spread_storages_raw = sorted(spread_storages_raw, key=risk_rank)

            for spread_storage in spread_storages_raw:
                de_area = STORAGE_POINT[spread_storage]   # "D" or "E"

                # (a) Skip if another vessel already committed to this storage.
                active_or_reserved = any(
                    vv.assigned_storage == spread_storage
                    and vv.name != v.name
                    and vv.status in {"WAITING_BERTH_A", "BERTHING_A",
                                      "HOSE_CONNECT_A", "LOADING",
                                      "SAILING_D_CHANNEL", "SAILING_CH_TO_BW_OUT",
                                      "SAILING_BW_TO_CH_IN", "SAILING_CH_TO_D",
                                      "SAILING_BA"}
                    for vv in self.vessels
                )
                if active_or_reserved:
                    continue

                # (b) Permission check (already in candidates, but be explicit).
                if not self.storage_allowed_for_vessel(spread_storage, v.name):
                    continue

                # (c) Position-aware urgency gate:
                #     Would this vessel actually arrive in time to help?
                #     Only spread if D/E stock will be at/below critical
                #     within SPREAD_DE_URGENCY_HORIZON hours of the vessel ETA.
                eta_to_de = self.area_travel_hours(v_area, de_area)
                proj_de   = self.projected_stock_at(spread_storage, eta_to_de)
                crit_de   = STORAGE_CRITICAL_THRESHOLD_BY_NAME[spread_storage]
                de_urgent = proj_de <= crit_de + (
                    STORAGE_PRODUCTION_RATE_BY_NAME.get(spread_storage, 0.0)
                    * SPREAD_DE_URGENCY_HORIZON
                )
                if not de_urgent:
                    # D/E is not in genuine need — skip this spread candidate.
                    continue

                # (d) A/C hold check: don't pull a vessel away from A/C if an
                #     A/C high-production storage will itself hit critical soon.
                ac_acute = False
                for ac_stor in (STORAGE_PRIMARY_NAME, STORAGE_SECONDARY_NAME,
                                STORAGE_TERTIARY_NAME):
                    if not self.storage_allowed_for_vessel(ac_stor, v.name):
                        continue
                    crit_ac   = STORAGE_CRITICAL_THRESHOLD_BY_NAME[ac_stor]
                    proj_ac   = self.projected_stock_at(ac_stor, SPREAD_AC_HOLD_HORIZON)
                    if proj_ac < crit_ac:
                        ac_acute = True
                        break
                if ac_acute and v_area in ("A", "C"):
                    # Hold this A/C vessel — A/C needs it more urgently.
                    continue

                # All gates passed — offer this vessel the spread assignment
                # if it is first in the eligible queue for this storage.
                queued = sorted(
                    [vv for vv in waiting_pool
                     if self.storage_allowed_for_vessel(spread_storage, vv.name)],
                    key=lambda x: (self.effective_load_cap(x.name, spread_storage), x.name),
                )
                if queued and queued[0].name == v.name:
                    return spread_storage

        # ── Default: pick best risk-priority candidate ───────────────────────
        # Berth availability provides a fractional tie-break bonus (0.5 rank
        # positions) but can never override a genuine urgency advantage.
        # Old code: (berth_now_penalty, urgency_idx) — berth_now completely
        # dominated urgency, routing vessels to the wrong storage whenever a
        # more urgent berth was busy.
        def rank(storage_name):
            p = STORAGE_POINT.get(storage_name, "A")
            berth_now = (
                self.is_valid_berthing_time(t, point=p)
                and t >= self.storage_berth_free_at[storage_name]
                and t >= self.next_storage_berthing_start_at[p]
            )
            ord_idx = ordered.index(storage_name) if storage_name in ordered else 99
            # Berth-available bonus: 0.5 rank improvement (fractional, never
            # enough to leapfrog a storage that is 1+ full urgency ranks ahead)
            effective_idx = ord_idx - (0.5 if berth_now else 0.0)
            return effective_idx

        return min(candidates, key=rank)

    def trigger_ac_post_breakwater_reassessment(self, t, trigger_vessel=None):
        """Activate and run immediate A/C allocation reassessment after inbound
        breakwater crossing, then schedule hourly daylight reassessment pulses."""
        self.ac_post_bw_reassess_active = True
        self.run_ac_post_breakwater_reassessment(t, reason="breakwater-cross")
        self.ac_post_bw_next_reassess_at = round(t + 1.0, 2)

    def run_ac_post_breakwater_reassessment(self, t, reason="hourly"):
        """Wake idle A/C daughters so existing IDLE_A allocation rules can reassess
        and auto-assign berthing/loading where eligible."""
        reassess_vessels = []
        for vv in self.vessels:
            if vv.status not in {"IDLE_A", "WAITING_BERTH_A", "WAITING_STOCK"}:
                continue
            if vv.target_point not in ("A", "C", "D", "E"):
                continue
            # Don't disturb a sleeping, priority-locked, or JMP-locked vessel
            if vv.resumption_priority or (vv.resumption_hour is not None and t < vv.resumption_hour):
                continue
            if getattr(vv, "_jmp_override_locked", False):
                continue   # JMP override active — do not reassign
            reassess_vessels.append(vv)

        assigned_ac = self.plan_ac_waiting_assignments(reassess_vessels, t)
        reserved_ac = set(assigned_ac.values())

        for vv in reassess_vessels:
            _new_storage = assigned_ac.get(vv.name)
            if _new_storage is None:
                _new_storage = self.choose_hourly_storage_option(vv, t, excluded_storages=reserved_ac)
            if _new_storage and vv.assigned_storage != _new_storage:
                vv.assigned_storage = _new_storage
                vv.target_point = STORAGE_POINT.get(_new_storage, "A")
                self.log_event(
                    t,
                    vv.name,
                    "ALLOCATION_REASSESS",
                    f"Post-breakwater {reason} reassessment rerouted to {_new_storage}",
                    voyage_num=vv.current_voyage,
                )
            vv.status = "IDLE_A"
            vv.next_event_time = t
            self.log_event(
                t,
                vv.name,
                "ALLOCATION_REASSESS",
                f"Post-breakwater {reason} reassessment pulse at Point {vv.target_point}",
                voyage_num=vv.current_voyage,
            )

    def maybe_run_ac_post_breakwater_reassessment(self, t):
        """Run hourly reassessment pulses in daylight after activation trigger."""
        if not self.ac_post_bw_reassess_active:
            return
        if self.ac_post_bw_next_reassess_at is None:
            self.ac_post_bw_next_reassess_at = round(t + 1.0, 2)
            return
        while t >= self.ac_post_bw_next_reassess_at - 1e-9:
            pulse_t = self.ac_post_bw_next_reassess_at
            if self.is_daylight_at(pulse_t):
                self.run_ac_post_breakwater_reassessment(pulse_t, reason="hourly")
            self.ac_post_bw_next_reassess_at = round(self.ac_post_bw_next_reassess_at + 1.0, 2)

    def run_daily_preops_storage_reassessment(self, t, day_key):
        """Daily 05:00 Day2+ allocation checkpoint for storage-side daughters.
        Re-evaluates capacity-priority storage assignment without disabling any
        other allocation/reassessment mechanisms.

        When ENABLE_VARIABILITY is True, field production rates are perturbed by
        a small daily factor (cv = PRODUCTION_VARIABILITY_CV) to model the
        day-to-day variability in field output.  The perturbation is reset each
        day so there is no cumulative drift.
        """
        # ── Apply daily production variability ───────────────────────────────
        if ENABLE_VARIABILITY:
            for _sn in STORAGE_NAMES:
                _base_rate  = STORAGE_PRODUCTION_RATE_BY_NAME[_sn]
                _pert_rate  = _variability_sample(_base_rate, PRODUCTION_VARIABILITY_CV)
                self.production_rate_override_by_name[_sn] = _pert_rate
            if hasattr(self, "_sim_stats"):
                self._sim_stats.record(
                    "production_variability", 1.0,
                    sum(self.production_rate_override_by_name.values()) /
                    max(1, sum(STORAGE_PRODUCTION_RATE_BY_NAME.values()))
                )

        reassessed = 0
        for vv in self.vessels:
            if vv.status not in {"IDLE_A", "WAITING_STOCK", "WAITING_BERTH_A"}:
                continue
            # Skip only if the vessel is physically committed to Point F:
            # either it is the active Ibom loader AND its target_point is still "F"
            # (i.e. it hasn't returned to Point A yet), or it is en-route to F.
            if (vv.name == self.point_f_active_loader and vv.target_point == "F") \
                    or vv.target_point == "F":
                continue
            # Don't disturb a sleeping, priority-locked, or JMP-locked vessel.
            # A JMP date-shift override sets _jmp_override_locked=True while the
            # vessel waits to load on a specific future date.  Without this guard,
            # the 05:00 preops reassessment silently overwrites the locked storage
            # (e.g. JasmineS → SanBarth) before the target date is reached, causing
            # the vessel to load from the wrong storage when it wakes.
            if vv.resumption_priority or (vv.resumption_hour is not None and t < vv.resumption_hour):
                continue
            if getattr(vv, "_jmp_override_locked", False):
                continue   # JMP date-shift active — preserve locked storage assignment
            target_storage, required_stock, _ = self.return_allocation_candidate(vv.cargo_capacity, vv.name)
            if target_storage is None:
                continue
            new_point = STORAGE_POINT.get(target_storage, "A")
            changed = (
                vv.assigned_storage != target_storage
                or vv.target_point != new_point
                or vv.status != "IDLE_A"
            )
            vv.assigned_storage = target_storage
            vv.target_point = new_point
            vv.status = "IDLE_A"
            vv.next_event_time = t
            if changed:
                reassessed += 1
                self.log_event(
                    t,
                    vv.name,
                    "ALLOCATION_REASSESS",
                    f"Daily 05:00 Day {day_key + 1} storage reassessment: reassigned to Point {new_point} "
                    f"via {target_storage} (threshold {required_stock:,.0f} bbl)",
                    voyage_num=vv.current_voyage,
                )
        if reassessed == 0:
            self.log_event(
                t,
                "SYSTEM",
                "ALLOCATION_REASSESS",
                f"Daily 05:00 Day {day_key + 1} storage reassessment: no changes required",
            )


    def _bryanston_call_waiting_vessel_serially(self, t, reason="serial-caller",
                                                mother_name=None):
        """Primary-mother serial berth caller.

        Originally Bryanston-only; now parametrised by `mother_name` so it can
        also actively allocate the GreenEagle berth.  When a primary's berth is
        free and has headroom, it scans for waiting Point-B cargo vessels and
        claims the single highest-priority one (longest-waiting first; then best
        export-fill fit; then larger cargo), bypassing daylight / day-lock /
        priority / nomination / candidate-slot / stale-scheduling gates but
        keeping all physical guards.

        Why GreenEagle needs this (issue 4 — LAP-004A / Watson):
        GreenEagle previously used pure first-come self-claim, so a large MTO
        transient committed to GreenEagle was leapfrogged every day by a rotation
        of smaller daughters and could sit laden for 1–2+ weeks even when
        GreenEagle had ample headroom.  A simple "yield" guard at GreenEagle
        DEADLOCKED (daughters deferred to a transient that still couldn't claim
        the unallocated berth, so the berth went idle).  Giving GreenEagle the
        same active serial caller as Bryanston resolves the starvation without the
        deadlock: the berth, once free, is positively handed to the highest-
        priority waiter (the long-waiting transient) rather than left for whoever
        self-claims first.

        Retained physical guards:
        - mother must be physically at Point B,
        - mother must not be in any export-busy state,
        - mother berth must have no physical occupant,
        - mother must have headroom for the called vessel's cargo.

        MTO accumulation behaviour is not modified here.
        """
        mn = mother_name if mother_name is not None else MOTHER_PRIMARY_NAME

        # Physical availability only. Do not use daylight/day-lock/berth-call gates.
        if not self.mother_is_at_point_b(mn, t):
            return False
        if self.export_state.get(mn) in EXPORT_BUSY_STATES:
            return False
        if self.mother_berth_current_occupant(mn) is not None:
            return False

        headroom = max(0.0, self.mother_capacity_bbl(mn) - self.mother_bbl.get(mn, 0.0))
        if headroom <= 0:
            return False

        # Include all Point-B waiting/holding statuses that represent a cargo vessel
        # available to be called by Bryanston.
        waiting_statuses = {
            "WAITING_BERTH_B",
            "WAITING_MOTHER_CAPACITY",
            "WAITING_FAIRWAY",
            "WAITING_DAYLIGHT",
            "ARRIVED_BIA",
        }
        candidates = []
        rejected = []
        for vv in self.vessels:
            if vv.status not in waiting_statuses:
                continue
            if vv.cargo_bbl <= 0:
                continue
            if getattr(vv, "_mto_transient_since_day", None) is not None:
                # A receiver still in the accumulation phase is not callable — but a
                # transient that has finished accumulating and is COMMITTED to offload
                # at Bryanston (assigned_mother == Bryanston and its full cargo fits
                # the current headroom) is ready to discharge and must be callable,
                # otherwise the serial caller keeps feeding smaller daughters ahead of
                # it every day and the large transient never wins the berth (Watson's
                # 127k queued for Bryanston for two weeks while STM/WDK/etc. were called
                # ahead of it).  Fit is already re-checked below against headroom.
                _committed_here = (vv.assigned_mother == mn
                                   and vv.cargo_bbl <= headroom + 1e-6)
                if not _committed_here:
                    rejected.append(f"{vv.name}: MTO transient receiver")
                    continue
            elif getattr(vv, "_is_mto_offload", False):
                rejected.append(f"{vv.name}: active MTO offload")
                continue
            if vv.cargo_bbl > headroom + 1e-6:
                rejected.append(f"{vv.name}: cargo {vv.cargo_bbl:,.0f} > Bryanston headroom {headroom:,.0f}")
                continue
            candidates.append(vv)

        if not candidates:
            # If vessels are visibly waiting but cannot be called, record the real reason.
            if rejected:
                day_key = self.point_b_calendar_day_key(t)
                flag = getattr(self, "_bryanston_serial_block_log", set())
                hour_key = round(t, 2)
                key = (day_key, hour_key, mn, tuple(sorted(rejected)))
                if key not in flag:
                    self.log_event(
                        t, "SYSTEM", "BRYANSTON_SERIAL_CALL_BLOCKED",
                        "Bryanston serial caller found waiting daughters but could not claim: " + "; ".join(rejected),
                        mother=mn,
                    )
                    flag.add(key)
                    self._bryanston_serial_block_log = flag
            return False

        def _rank(vv):
            waited_since = getattr(vv, "_waiting_bia_since", None)
            if waited_since is None:
                waited_since = getattr(vv, "arrival_at_b", None)
            if waited_since is None:
                waited_since = vv.next_event_time
            trigger = self.mother_export_trigger_bbl(mn)
            post_gap = abs(trigger - (self.mother_bbl.get(mn, 0.0) + vv.cargo_bbl))
            # Serial operating rule: longest waiting first; then best export-fill fit;
            # then larger cargo; then stable alphabetical tie-breaker.
            return (waited_since, post_gap, -vv.cargo_bbl, vv.name)

        selected = sorted(candidates, key=_rank)[0]

        # Remove all non-physical Bryanston-only gates. This is the core change.
        self.mother_berth_free_at[mn] = min(self.mother_berth_free_at.get(mn, 0.0), t)
        self.mother_available_at[mn] = min(self.mother_available_at.get(mn, 0.0), t)
        day_key = self.point_b_calendar_day_key(t)
        self.point_b_day_assigned_mothers.get(day_key, set()).discard(mn)
        if self._point_b_registered_day.get(mn) == day_key:
            self._point_b_registered_day.pop(mn, None)

        previous_status = selected.status
        previous_mother = selected.assigned_mother
        selected.assigned_mother = mn
        selected.status = "BERTHING_B"
        berth_delay = self._berthing_delay()
        selected.next_event_time = t + berth_delay
        selected._bryanston_serial_call = True
        selected._bryanston_serial_call_t = t

        # Reserve Bryanston's berth through expected berthing + hose + discharge + cast-off.
        hose_hours = self._hose_connect_hours()
        disch_rate = VESSEL_DISCHARGE_RATE_BPH.get(selected.name)
        disch_hours = (selected.cargo_bbl / disch_rate) if disch_rate else DISCHARGE_HOURS
        pump_end = t + berth_delay + hose_hours + disch_hours
        self.mother_berth_free_at[mn] = max(self.mother_berth_free_at.get(mn, 0.0), _berth_free_at(pump_end))

        self.log_event(
            t,
            selected.name,
            "BERTHING_START_B",
            f"Bryanston serial caller ({reason}): Bryanston called {selected.name} from {previous_status}; "
            f"previous mother={previous_mother or 'None'}; all non-physical Bryanston constraints ignored; "
            f"serial berth reserved until post-discharge/cast-off.",
            voyage_num=selected.current_voyage,
            mother=mn,
        )
        self.log_event(
            t,
            selected.name,
            "BRYANSTON_SERIAL_CALL",
            f"Bryanston actively called waiting daughter vessel. Physical checks passed: at BIA, berth free, "
            f"headroom {headroom:,.0f} bbl, selected cargo {selected.cargo_bbl:,.0f} bbl. "
            f"Daylight, berth-call, day-lock, priority and stale availability gates bypassed for Bryanston only.",
            voyage_num=selected.current_voyage,
            mother=mn,
        )
        return True

    def maybe_run_daily_preops_storage_reassessment(self, t):
        """Trigger daily storage reassessment at 05:00 from Day 2 onward."""
        wall_hour = round((t + SIM_HOUR_OFFSET) % 24, 2)
        day_key = int((t + SIM_HOUR_OFFSET) // 24)
        if day_key < 1:
            return
        if wall_hour != 5.0:
            return
        if self.daily_preops_last_day_key == day_key:
            return
        self.daily_preops_last_day_key = day_key
        self.run_daily_preops_storage_reassessment(t, day_key)

    # -- Main simulation loop ---------------------------------------------
    def run(self):
        total_hours = SIMULATION_DAYS * 24
        t = 0.0

        while t <= total_hours:
            self._current_t = t   # make current time available to helper methods
            self.maybe_run_daily_preops_storage_reassessment(t)
            self.maybe_run_ac_post_breakwater_reassessment(t)
            self._maybe_run_multiple_transient_op(t)
            self._bryanston_call_waiting_vessel_serially(t, reason="pre-state-scan")

            # ── Custom vessel join ────────────────────────────────────────────
            # At each timestep check whether any registered custom vessel is due
            # to join the fleet.  A vessel joins exactly once: it is appended to
            # self.vessels, its per-vessel storage permissions are recorded, and
            # a VESSEL_JOINED event is written to the event log so it appears in
            # the dashboard and CSV exports.
            if self._pending_custom_vessels:
                _still_pending = []
                for _spec in self._pending_custom_vessels:
                    if t < _spec._join_hour:
                        _still_pending.append(_spec)
                        continue
                    # Register storage permissions for this vessel
                    self._custom_vessel_storage_permissions[_spec.name] = set(
                        _spec.permitted_storages
                    )
                    # Instantiate and configure — starts IDLE_A, ready immediately
                    _nv = DaughterVessel(
                        _spec.name,
                        start_offset_hours=t,
                        cargo_capacity=_spec.cargo_capacity,
                    )
                    _nv.status             = "IDLE_A"
                    _nv.target_point       = "A"
                    _nv.next_event_time    = t
                    _nv._voyage_assigned   = False
                    self.vessels.append(_nv)
                    self.vessel_api[_spec.name] = 0.0
                    # Invalidate vessel index so log_event rebuilds it.
                    if hasattr(self, "_vessel_index"):
                        del self._vessel_index
                    _perm_str = (
                        ", ".join(sorted(_spec.permitted_storages))
                        if _spec.permitted_storages
                        else "SanBarth, JasmineS (default)"
                    )
                    self.log_event(
                        t, _spec.name, "VESSEL_JOINED",
                        f"Custom vessel joined fleet — capacity {_spec.cargo_capacity:,} bbl, "
                        f"permitted storages: {_perm_str}",
                    )
                self._pending_custom_vessels = _still_pending
            # 1. Continuous production at all storage locations (non-stop)
            for storage_name in STORAGE_NAMES:
                prod_rate = self.production_rate_bph_at(storage_name, t)
                prod = prod_rate * TIME_STEP_HOURS
                cap = STORAGE_CAPACITY_BY_NAME[storage_name]
                prod_api = STORAGE_API.get(storage_name, 0.0)
                self.total_produced += prod
                projected = self.storage_bbl[storage_name] + prod
                # Blend incoming production API into storage
                self.storage_api[storage_name] = self.blend_api(
                    self.storage_bbl[storage_name], self.storage_api[storage_name],
                    prod, prod_api)
                if projected > cap:
                    overflow_amount = projected - cap
                    self.total_spilled += overflow_amount
                    self.storage_overflow_bbl[storage_name] += overflow_amount
                    self.storage_overflow_events += 1
                    self.storage_bbl[storage_name] = cap
                else:
                    self.storage_bbl[storage_name] = projected

            # Point F accumulation during swap/takeover gap (reporting only)
            if self.point_f_active_loader is None and self.point_f_swap_pending_for is not None:
                self.point_f_overflow_accum_bbl += POINT_F_LOAD_RATE_BPH * TIME_STEP_HOURS

            # 2. Advance each vessel's state machine
            # ── Capacity-aware IDLE_A dispatch ordering ───────────────────
            # When multiple vessels are simultaneously ready for IDLE_A
            # dispatch, the vessel with the highest load capacity for the
            # most urgent available storage must be processed first.  Without
            # this sort, vessels are processed in VESSEL_NAMES order, meaning
            # a smaller vessel (e.g. Woodstock 42k, index 6) grabs the most
            # urgent storage (SanBarth at overflow) before a larger vessel
            # (Watson 85k, index 8) arrives — wasting 49k of drain potential.
            #
            # We split the vessel list into two groups at each t:
            #   Group A: vessels NOT ready for IDLE_A dispatch — processed
            #            first in their natural order (no behaviour change).
            #   Group B: vessels simultaneously at IDLE_A and due to fire now
            #            — sorted by urgency(best_storage) × load_cap DESC.
            _group_a = []   # non-IDLE_A or not yet due
            _group_b = []   # IDLE_A vessels due to fire this tick
            for _v in self.vessels:
                if t < _v.next_event_time or _v.status != "IDLE_A":
                    _group_a.append(_v)
                else:
                    _group_b.append(_v)

            # Score each Group B vessel by the urgency of its best available
            # unlocked storage and its load capacity at that storage.
            # urgency = overflow_risk = (stock / critical_threshold); higher
            # ratio → closer to overflow → more urgent to drain.
            def _dispatch_priority(vv):
                best_urgency = 0.0
                best_cap     = 0
                for sn in STORAGE_NAMES:
                    if not self.storage_allowed_for_vessel(sn, vv.name):
                        continue
                    if self.storage_locked_by_active_berth(sn, requesting_vessel=vv.name):
                        continue
                    crit = STORAGE_CRITICAL_THRESHOLD_BY_NAME.get(sn, 1)
                    if crit <= 0:
                        continue
                    urgency = self.storage_bbl[sn] / crit   # >1 → above critical
                    cap     = self.effective_load_cap(vv.name, sn)
                    if urgency > best_urgency or (urgency == best_urgency and cap > best_cap):
                        best_urgency = urgency
                        best_cap     = cap
                # Primary sort: urgency DESC (most overflow-risk first)
                # Secondary sort: capacity DESC (largest drain first)
                # Negate both so min() / sort() gives descending order
                return (-best_urgency, -best_cap)

            _group_b.sort(key=_dispatch_priority)
            _ordered_vessels = _group_a + _group_b

            for v in _ordered_vessels:
                if t < v.next_event_time:
                    continue

                # ── Mid-sim dormancy trigger (cargo-aware) ────────────────────
                # When the dormancy window opens, the vessel must be allowed to
                # complete any cargo it is carrying before going offline:
                #
                #   "Defer" states — vessel has cargo on board or is actively
                #   loading/discharging; dormancy is deferred until the cargo is
                #   fully delivered at BIA and the vessel has cast off empty.
                #
                #   "Immediate" states — vessel is empty and between voyages
                #   (returning from BIA, idle at storage, waiting for stock);
                #   dormancy activates at once with no cargo to protect.
                #
                # A `_dormancy_pending` flag is set on the vessel so the
                # CAST_OFF_B and CAST_OFF_COMPLETE_B handlers can activate
                # dormancy the moment the current discharge is done.
                _dorm_h = getattr(v, "dormancy_start_hour", None)
                if _dorm_h is not None and t >= _dorm_h:
                    v.dormancy_start_hour = None   # one-shot — don't re-check next tick

                    # States where cargo is present or being processed — defer
                    _defer_statuses = {
                        # Loading at storage
                        "BERTHING_A", "HOSE_CONNECT_A", "LOADING",
                        "DOCUMENTING", "WAITING_CAST_OFF",
                        # Sailing toward BIA with cargo
                        "SAILING_AB", "SAILING_AB_LEG2",
                        "SAILING_CROSS_BW_AC", "SAILING_BW_TO_FWY",
                        "WAITING_TIDAL", "WAITING_DAYLIGHT", "WAITING_FAIRWAY",
                        "SAILING_D_CHANNEL", "SAILING_CH_TO_BW_OUT",
                        "SAILING_CROSS_BW_OUT",
                        # At BIA discharging
                        "BERTHING_B", "HOSE_CONNECT_B", "DISCHARGING",
                        "WAITING_BERTH_B", "WAITING_MOTHER_CAPACITY",
                        "WAITING_MOTHER_RETURN",
                        # Cast-off from storage with cargo
                        "CAST_OFF",
                    }

                    if v.cargo_bbl > 0 or v.status in _defer_statuses:
                        # Vessel has cargo — defer dormancy until after BIA discharge
                        v._dormancy_pending = True
                        self.log_event(
                            t, v.name, "DORMANCY_DEFERRED",
                            f"Dormancy window opened but vessel has cargo on board "
                            f"({v.cargo_bbl:,.0f} bbl, status: {v.status}). "
                            f"Dormancy will activate after current cargo is discharged at BIA. "
                            f"Resumes: {self.hours_to_dt(getattr(v, '_dormancy_end_hour', 0)).strftime('%Y-%m-%d %H:%M') if getattr(v, '_dormancy_end_hour', None) else 'N/A'}",
                            voyage_num=v.current_voyage,
                        )
                    else:
                        # Vessel is empty — activate dormancy immediately
                        _end_h = getattr(v, "_dormancy_end_hour", None)
                        if _end_h is not None:
                            v.resumption_hour = _end_h
                        v.resumption_hold_logged = False
                        v.status           = "IDLE_A"
                        v.cargo_bbl        = 0
                        self.vessel_api[v.name] = 0.0
                        v.assigned_storage = None
                        v.assigned_load_hours = None
                        v.assigned_mother  = None
                        v.target_point     = "A"
                        v.next_event_time  = t
                        self.log_event(
                            t, v.name, "DORMANCY_ACTIVATED",
                            f"Mid-sim dormancy window started — vessel idle until "
                            f"{self.hours_to_dt(v.resumption_hour).strftime('%Y-%m-%d %H:%M') if v.resumption_hour else 'indefinite'} "
                            f"| priority storage on resumption: {v.resumption_storage}",
                            voyage_num=v.current_voyage,
                        )

                if v.status == "PF_LOADING":
                    increment = POINT_F_LOAD_RATE_BPH * TIME_STEP_HOURS
                    _pf_cap = getattr(v, "_pf_load_ceiling", None) or v.cargo_capacity
                    if v.cargo_bbl < _pf_cap:
                        v.cargo_bbl = min(_pf_cap, v.cargo_bbl + increment)
                    # Ibom API is constant — assign directly rather than blending
                    # from 0.0, which would produce incorrect intermediate values
                    self.vessel_api[v.name] = IBOM_API
                    _pf_trigger_at = getattr(v, "_pf_load_ceiling", None) or POINT_F_MIN_TRIGGER_BBL
                    if v.cargo_bbl >= _pf_trigger_at:
                        alternate = self.point_f_other_vessel(v.name)
                        if self.point_f_swap_pending_for != alternate:
                            self.point_f_swap_pending_for = alternate
                            self.point_f_swap_triggered_by = v.name
                            self.log_event(
                                t,
                                v.name,
                                "POINT_F_SWAP_TRIGGER",
                                f"Point F trigger at {v.cargo_bbl:,.0f} bbl (> {POINT_F_MIN_TRIGGER_BBL:,.0f}); "
                                f"{alternate} requested to take over after current voyage",
                                voyage_num=v.current_voyage,
                            )

                        alternate_vessel = next((vv for vv in self.vessels if vv.name == alternate), None)
                        alternate_arrived = (
                            alternate_vessel is not None
                            and alternate_vessel.status == "IDLE_A"
                            and alternate_vessel.target_point == "F"
                            and alternate_vessel.cargo_bbl <= 0
                        )
                        daylight_now = DAYLIGHT_START <= ((t + SIM_HOUR_OFFSET) % 24) < DAYLIGHT_END

                        if alternate_arrived and daylight_now:
                            self.point_f_active_loader = None
                            alternate_vessel.status = "PF_SWAP"
                            alternate_vessel.target_point = "F"
                            alternate_vessel.next_event_time = t + POINT_F_SWAP_HOURS
                            self.log_event(
                                t,
                                alternate_vessel.name,
                                "POINT_F_SWAP_START",
                                f"Point F takeover starts ({POINT_F_SWAP_HOURS}h)",
                                voyage_num=alternate_vessel.current_voyage,
                            )
                            v.status = "CAST_OFF"
                            v.next_event_time = t
                            continue
                    v.next_event_time = t + TIME_STEP_HOURS
                    continue

                if v.status == "PF_SWAP":
                    self.point_f_active_loader = v.name
                    self.point_f_swap_pending_for = None
                    self.point_f_swap_triggered_by = None
                    v.target_point = "F"   # ensure active Ibom loader stays on Point F
                    returned_from_overflow = min(self.point_f_overflow_accum_bbl, max(0.0, v.cargo_capacity - v.cargo_bbl))
                    v.cargo_bbl += returned_from_overflow
                    self.point_f_overflow_accum_bbl -= returned_from_overflow
                    v.status = "PF_LOADING"
                    self.vessel_api[v.name] = IBOM_API  # constant; assign directly
                    self.log_event(
                        t,
                        v.name,
                        "POINT_F_SWAP_COMPLETE",
                        f"Point F swap complete; returned {returned_from_overflow:,.0f} bbl overflow to loader | "
                        f"trigger rule: swap when load exceeds {POINT_F_MIN_TRIGGER_BBL:,.0f} bbl",
                        voyage_num=v.current_voyage,
                    )
                    v.next_event_time = t + TIME_STEP_HOURS
                    continue

                if v.status == "IDLE_A":
                    # ── Resumption hold ───────────────────────────────────────
                    # If this vessel has a future resumption hour, hold it here
                    # until t >= resumption_hour.  Log once then sleep silently.
                    if v.resumption_hour is not None and t < v.resumption_hour:
                        if not v.resumption_hold_logged:
                            v.resumption_hold_logged = True
                            self.log_event(
                                t, v.name, "RESUMPTION_HOLD",
                                f"Vessel held idle — resumption scheduled "
                                f"{self.hours_to_dt(v.resumption_hour).strftime('%Y-%m-%d %H:%M')} "
                                f"with priority load at {v.resumption_storage}",
                                voyage_num=v.current_voyage,
                            )
                        v.next_event_time = v.resumption_hour
                        continue

                    # ── Priority resumption wake ──────────────────────────────
                    # On the first tick at or after resumption_hour: lock to the
                    # designated storage, assign voyage, go straight to berthing,
                    # bypassing the serial start-gap (but honouring
                    # storage_berth_free_at to prevent physical collision).
                    if v.resumption_hour is not None and not v.resumption_priority:
                        v.resumption_priority = True

                    if v.resumption_priority:
                        _rs = v.resumption_storage
                        if not hasattr(v, '_voyage_assigned') or not v._voyage_assigned:
                            v._vessel_voyage_counter = getattr(v, '_vessel_voyage_counter', 0) + 1
                            v.current_voyage = v._vessel_voyage_counter
                            v._voyage_assigned = True
                        _rpoint = STORAGE_POINT.get(_rs, "A")
                        _rcap   = self.effective_load_cap(v.name, _rs)
                        _rload  = self.storage_load_hours(_rs, _rcap, vessel_name=v.name)
                        v.assigned_storage    = _rs
                        v.assigned_load_hours = _rload
                        v.target_point        = _rpoint
                        # Honour physical berth availability — bypass only the
                        # serial start-gap (next_storage_berthing_start_at).
                        if not self.is_valid_berthing_time(t, point=_rpoint) \
                                or t < self.storage_berth_free_at[_rs]:
                            _next_chk = self.next_daylight_hourly_berth_check(t, point=_rpoint)
                            v.next_event_time = _next_chk
                            continue
                        v.status = "BERTHING_A"
                        self.storage_berth_free_at[_rs] = (
                            t + BERTHING_DELAY_HOURS + HOSE_CONNECTION_HOURS + _rload
                        )
                        # Do NOT advance next_storage_berthing_start_at — priority vessel
                        # bypasses the inter-berth serial gap entirely.
                        v.next_event_time = t + BERTHING_DELAY_HOURS
                        _rslot = (VESSEL_NAMES.index(v.name) + 1) if v.name in VESSEL_NAMES else "C"
                        self.log_event(
                            t, v.name, "RESUMPTION_BERTHING",
                            f"Priority resumption berthing at {_rs} "
                            f"(resumed {self.hours_to_dt(v.resumption_hour).strftime('%Y-%m-%d')}, "
                            f"bypassing queue) [rotation slot {_rslot} of {NUM_DAUGHTERS}]",
                            voyage_num=v.current_voyage,
                        )
                        # Clear all resumption state — vessel runs normally from here
                        v.resumption_hour        = None
                        v.resumption_storage     = None
                        v.resumption_priority    = False
                        v.resumption_hold_logged = False
                        continue

                    if v.name == self.point_f_active_loader and v.target_point == "F":
                        # This vessel is the designated Ibom loader AND is physically
                        # at Point F — route to PF_LOADING.
                        # IMPORTANT: if target_point != "F" the vessel has returned to
                        # Point A/C after delivering a partial Ibom cargo and must be
                        # allowed to load a full Point A cargo before returning to Ibom.
                        # Do NOT redirect to PF_LOADING in that case.
                        if v.cargo_bbl < v.cargo_capacity:
                            v.cargo_bbl = min(
                                v.cargo_capacity,
                                v.cargo_bbl + (POINT_F_LOAD_RATE_BPH * TIME_STEP_HOURS),
                            )
                        wall_h = (t + SIM_HOUR_OFFSET) % 24
                        if not (DAYLIGHT_START <= wall_h < DAYLIGHT_END):
                            next_light = self.next_daylight_sail(t)
                            self.log_event(
                                t,
                                v.name,
                                "WAITING_DAYLIGHT",
                                f"Point F loading waits for daylight at {self.hours_to_dt(next_light).strftime('%Y-%m-%d %H:%M')}",
                                voyage_num=v.current_voyage,
                            )
                            v.next_event_time = t + TIME_STEP_HOURS
                            continue
                        v.status = "PF_LOADING"
                        v.target_point = "F"
                        self.vessel_api[v.name] = IBOM_API
                        v.next_event_time = t
                        continue

                    # Bedford/Balham: if not the active Ibom loader (or is the active
                    # loader but has physically returned to Point A after delivering a
                    # partial Ibom cargo), dispatch to SanBarth (Point A).
                    if (v.name in self.point_f_vessels
                            and (self.point_f_active_loader != v.name
                                 or v.target_point != "F")
                            and v.target_point != "F"):
                        v.target_point = "A"  # load SanBarth/JasmineS

                    # Only assign a new voyage number on a fresh cycle start.
                    if not hasattr(v, '_voyage_assigned') or not v._voyage_assigned:
                        v._vessel_voyage_counter = getattr(v, '_vessel_voyage_counter', 0) + 1
                        v.current_voyage = v._vessel_voyage_counter
                        v._voyage_assigned = True
                    cap = v.cargo_capacity   # default; overridden per-storage below

                    # ── JMP manual override check (must run BEFORE pre-assigned) ──
                    # Checked first so a forced assignment always wins over any
                    # stale v.assigned_storage from a previous voyage.
                    # Use calendar-day formula (epoch-aligned) so Day 4 always means
                    # the 4th calendar day regardless of when within it the vessel is idle.
                    _dispatch_day = int((t + SIM_HOUR_OFFSET) // 24) + 1
                    _ovr_entry    = STORAGE_DISPATCH_OVERRIDES.get(v.name, {}).get(_dispatch_day)
                    # Support both plain storage string and dict with load_after_hour
                    if isinstance(_ovr_entry, dict):
                        _forced_stor       = _ovr_entry.get("storage")
                        _forced_load_after = _ovr_entry.get("load_after_hour")  # sim-hour
                    else:
                        _forced_stor       = _ovr_entry
                        _forced_load_after = None
                    # Also honour a date-shift already encoded on the vessel from a prior tick
                    if _forced_stor is None and getattr(v, "_jmp_override_locked", False):
                        # Vessel is locked from a previous JMP trigger; keep waiting
                        _forced_stor       = v.assigned_storage
                        _forced_load_after = getattr(v, "_jmp_load_after_hour", None)
                    if (_forced_stor
                            and _forced_stor in STORAGE_NAMES
                            and self.storage_allowed_for_vessel(_forced_stor, v.name)):
                        # ── Date-shift hold: vessel must wait until _forced_load_after ──
                        if (_forced_load_after is not None and t < _forced_load_after):
                            # Lock the vessel and hold it idle until the target hour
                            v._jmp_override_locked  = True
                            v._jmp_load_after_hour  = _forced_load_after
                            v.assigned_storage      = _forced_stor
                            v.target_point          = STORAGE_POINT.get(_forced_stor, "A")
                            v.next_event_time       = _forced_load_after
                            self.log_event(
                                t, v.name, "WAITING_BERTH_A",
                                f"JMP override: holding until "
                                f"{self.hours_to_dt(_forced_load_after).strftime('%Y-%m-%d %H:%M')}"
                                f" then loading from {_forced_stor}",
                                voyage_num=v.current_voyage,
                            )
                            continue
                        # ── Normal override (or date-shift hour reached) ───────────
                        if not self.storage_locked_by_active_berth(
                                _forced_stor, requesting_vessel=v.name):
                            _f_cap   = self.effective_load_cap(v.name, _forced_stor)
                            _f_point = STORAGE_POINT.get(_forced_stor, "A")
                            _f_berth = max(
                                self.next_berthing_window(t, point=_f_point),
                                self.storage_berth_free_at[_forced_stor],
                                self.next_storage_berthing_start_at[_f_point],
                            )
                            _f_berth = self.next_berthing_window(_f_berth, point=_f_point)
                            # Lock: prevent preops and hourly reassessment from undoing this
                            v._jmp_override_locked  = True
                            v._jmp_load_after_hour  = None   # date-shift consumed
                            v.assigned_storage    = _forced_stor
                            v.assigned_load_hours = self.storage_load_hours(
                                _forced_stor, _f_cap, vessel_name=v.name)
                            v.target_point        = _f_point
                            v.status              = "WAITING_BERTH_A"
                            v.next_event_time     = _f_berth
                            self.log_event(
                                t, v.name, "WAITING_BERTH_A",
                                f"JMP override → {_forced_stor} (Day {_dispatch_day}); "
                                f"berthing at "
                                f"{self.hours_to_dt(_f_berth).strftime('%Y-%m-%d %H:%M')}"
                                f" [override locked — immune to reassessment]",
                                voyage_num=v.current_voyage,
                            )
                            self.next_storage_berthing_start_at[_f_point] = (
                                _f_berth + BERTHING_DELAY_HOURS
                            )
                            continue

                    # If a vessel was manually seeded at a specific storage with
                    # IDLE_A status, honour that assignment immediately: skip the
                    # stock-gate and go straight to berthing.  The dead-stock check
                    # in HOSE_CONNECT_A will hold loading until the threshold is met.
                    if v.assigned_storage and self.storage_allowed_for_vessel(v.assigned_storage, v.name):
                        _pre_assigned = v.assigned_storage
                        cap           = self.effective_load_cap(v.name, _pre_assigned)
                        _pre_stock    = self.storage_bbl[_pre_assigned]
                        _pre_point    = STORAGE_POINT.get(_pre_assigned, "A")
                        _pre_thresh   = self.loading_start_threshold(_pre_assigned, cap)
                        # Hard active-berth lock: if another vessel is physically
                        # berthed/connecting/loading here, block regardless of timing.
                        _pre_active_lock = self.storage_locked_by_active_berth(
                            _pre_assigned, requesting_vessel=v.name)
                        _berth_now_ok = (
                            not _pre_active_lock
                            and self.is_valid_berthing_time(t, point=_pre_point)
                            and t >= self.storage_berth_free_at[_pre_assigned]
                            and t >= self.next_storage_berthing_start_at[_pre_point]
                        )
                        if not _berth_now_ok:
                            _next_chk = self.next_daylight_hourly_berth_check(t, point=_pre_point)
                            v.status = "WAITING_BERTH_A"
                            v.target_point = _pre_point
                            v.next_event_time = _next_chk
                            _lock_reason = " (berth physically occupied)" if _pre_active_lock else ""
                            self.log_event(
                                t,
                                v.name,
                                "WAITING_BERTH_A",
                                f"Arrived/idle for {_pre_assigned}; berth unavailable now{_lock_reason} — hourly daylight recheck at "
                                f"{self.hours_to_dt(_next_chk).strftime('%Y-%m-%d %H:%M')}",
                                voyage_num=v.current_voyage,
                            )
                            continue
                        _pre_start = t
                        load_hours = self.storage_load_hours(_pre_assigned, cap, vessel_name=v.name)
                        v.assigned_load_hours = load_hours
                        v.status = "BERTHING_A"
                        v.target_point = _pre_point
                        self.storage_berth_free_at[_pre_assigned] = (
                            _pre_start + BERTHING_DELAY_HOURS + HOSE_CONNECTION_HOURS + load_hours
                        )
                        self.next_storage_berthing_start_at[_pre_point] = (
                            _pre_start + BERTHING_DELAY_HOURS + POST_BERTHING_START_GAP_HOURS
                        )
                        v.next_event_time = _pre_start + BERTHING_DELAY_HOURS
                        slot = (VESSEL_NAMES.index(v.name) + 1) if v.name in VESSEL_NAMES else "C"
                        self.log_event(_pre_start, v.name, "BERTHING_START_A",
                                       f"Berthing at {_pre_assigned} (pre-assigned, 30 min procedure) "
                                       f"[rotation slot {slot} of {NUM_DAUGHTERS}]",
                                       voyage_num=v.current_voyage)
                        continue

                    # Use global permitted candidate pool so A/C defaults do not
                    # starve Duke/Starturn when they are approaching unsafe levels.
                    eligible_storage_names = [
                        name for name in STORAGE_NAMES
                        if self.storage_allowed_for_vessel(name, v.name)
                    ]

                    candidate_storages = []
                    for storage_name in eligible_storage_names:
                        if not self.storage_allowed_for_vessel(storage_name, v.name):
                            continue
                        if self.storage_locked_by_active_berth(storage_name, requesting_vessel=v.name):
                            continue
                        cap = self.effective_load_cap(v.name, storage_name)
                        stock = self.storage_bbl[storage_name]
                        storage_point = STORAGE_POINT.get(storage_name, "A")
                        threshold_required = self.loading_start_threshold(storage_name, cap)
                        berth_t = self.next_berthing_window(t, point=storage_point)
                        start = max(
                            berth_t,
                            self.storage_berth_free_at[storage_name],
                            self.next_storage_berthing_start_at[storage_point],
                        )
                        # Final daylight guard — gate values may be outside berthing window
                        start = self.next_berthing_window(start, point=storage_point)
                        crit = STORAGE_CRITICAL_THRESHOLD_BY_NAME.get(storage_name, STORAGE_CAPACITY_BY_NAME.get(storage_name, 1.0))
                        risk_gap = stock - crit
                        candidate_storages.append((
                            storage_name,
                            stock,
                            berth_t,
                            start,
                            threshold_required,
                            crit,
                            risk_gap,
                        ))

                    if candidate_storages:
                        # ── Dead-stock rule ─────────────────────────────
                        # The vessel berths and connects hoses normally, but
                        # loading cannot commence until 175% of the cargo
                        # volume is available.  We enforce this here: if the
                        # stock is above the simple threshold but below the
                        # dead-stock threshold the vessel still proceeds to
                        # berth — the waiting-for-stock logic in HOSE_CONNECT_A
                        # will hold it at berth until the threshold is met.
                        candidate_storages.sort(
                            key=lambda x: (
                                # Unsafe/borderline first to suppress local deterioration.
                                0 if x[6] >= 0 else 1,
                                -x[6],
                                # Keep stock-feasible candidates advantaged for immediate throughput.
                                0 if x[1] >= x[4] else 1,
                                x[3],
                                -x[1],
                                x[0],
                            )
                        )
                        selected_storage, selected_stock, berth_t, start, threshold_required, _crit, _risk_gap = candidate_storages[0]
                        selected_point = STORAGE_POINT.get(selected_storage, "A")
                        _cand_active_lock = self.storage_locked_by_active_berth(
                            selected_storage, requesting_vessel=v.name)
                        berth_now_ok = (
                            not _cand_active_lock
                            and self.is_valid_berthing_time(t, point=selected_point)
                            and t >= self.storage_berth_free_at[selected_storage]
                            and t >= self.next_storage_berthing_start_at[selected_point]
                        )
                        v.assigned_storage = selected_storage
                        load_hours = self.storage_load_hours(selected_storage, cap, vessel_name=v.name)
                        v.assigned_load_hours = load_hours

                        if not berth_now_ok:
                            v.status = "WAITING_BERTH_A"
                            v.target_point = selected_point
                            next_check = self.next_daylight_hourly_berth_check(t, point=selected_point)
                            v.next_event_time = next_check
                            _lock_reason = " (berth physically occupied)" if _cand_active_lock else ""
                            self.log_event(
                                t,
                                v.name,
                                "WAITING_BERTH_A",
                                f"Berth unavailable at {selected_storage}{_lock_reason}; hourly daylight recheck at "
                                f"{self.hours_to_dt(next_check).strftime('%Y-%m-%d %H:%M')}",
                                voyage_num=v.current_voyage,
                            )
                            continue

                        # The berth is reserved. We do NOT pre-commit stock
                        # here because the dead-stock rule may delay the
                        # actual loading start — stock is committed only once
                        # the 175% threshold is confirmed in HOSE_CONNECT_A.
                        v.status = "BERTHING_A"
                        start = t
                        self.storage_berth_free_at[selected_storage] = (
                            start + BERTHING_DELAY_HOURS + HOSE_CONNECTION_HOURS + load_hours
                        )
                        self.next_storage_berthing_start_at[selected_point] = (
                            start + BERTHING_DELAY_HOURS + POST_BERTHING_START_GAP_HOURS
                        )
                        v.next_event_time = start + BERTHING_DELAY_HOURS

                        slot = (VESSEL_NAMES.index(v.name) + 1) if v.name in VESSEL_NAMES else "C"
                        self.log_event(start, v.name, "BERTHING_START_A",
                                       f"Berthing at {selected_storage} (30 min procedure) "
                                       f"[rotation slot {slot} of {NUM_DAUGHTERS}]",
                                       voyage_num=v.current_voyage)
                    else:
                        v.next_event_time = t + 0.5   # poll frequently so queue doesn't stall
                        threshold_by_storage = {
                            name: self.loading_start_threshold(name, cap)
                            for name in eligible_storage_names
                        }
                        min_threshold = min(threshold_by_storage.values()) if threshold_by_storage else cap
                        storage_levels = ", ".join(
                            f"{name}: {self.storage_bbl[name]:,.0f} bbl" for name in eligible_storage_names
                        )
                        self.log_event(t, v.name, "WAITING_STOCK",
                                       f"No eligible storage assignment currently available at Point {v.target_point} "
                                       f"(active berth locks and/or stock constraints; {storage_levels}; "
                                       f"threshold guide {min_threshold:,.0f} bbl) — waiting for hourly reassessment/reroute",
                                       voyage_num=v.current_voyage)

                elif v.status == "BERTHING_A":
                    v.status = "HOSE_CONNECT_A"
                    v.next_event_time = t + HOSE_CONNECTION_HOURS
                    berth_storage = v.assigned_storage or STORAGE_PRIMARY_NAME
                    self.log_event(t, v.name, "HOSE_CONNECTION_START_A",
                                   f"Hose connection initiated at {berth_storage} (2 hours)",
                                   voyage_num=v.current_voyage)

                elif v.status == "WAITING_BERTH_A":
                    # ── JMP override intercept ────────────────────────────────
                    # Redirect if a JMP override targets a different storage today.
                    # Calendar-day formula matches what the JMP displays to the user.
                    _wb_day    = int((t + SIM_HOUR_OFFSET) // 24) + 1
                    _wb_forced = STORAGE_DISPATCH_OVERRIDES.get(v.name, {}).get(_wb_day)
                    if (_wb_forced
                            and _wb_forced != v.assigned_storage
                            and _wb_forced in STORAGE_NAMES
                            and self.storage_allowed_for_vessel(_wb_forced, v.name)
                            and not self.storage_locked_by_active_berth(
                                _wb_forced, requesting_vessel=v.name)):
                        _wb_cap   = self.effective_load_cap(v.name, _wb_forced)
                        _wb_point = STORAGE_POINT.get(_wb_forced, "A")
                        _wb_berth = max(
                            self.next_berthing_window(t, point=_wb_point),
                            self.storage_berth_free_at[_wb_forced],
                            self.next_storage_berthing_start_at[_wb_point],
                        )
                        _wb_berth = self.next_berthing_window(_wb_berth, point=_wb_point)
                        v.assigned_storage    = _wb_forced
                        v.assigned_load_hours = self.storage_load_hours(
                            _wb_forced, _wb_cap, vessel_name=v.name)
                        v.target_point        = _wb_point
                        v.next_event_time     = _wb_berth
                        self.log_event(
                            t, v.name, "ALLOCATION_REASSESS",
                            f"JMP override redirected waiting berth to {_wb_forced} "
                            f"(Day {_wb_day}); berth at "
                            f"{self.hours_to_dt(_wb_berth).strftime('%Y-%m-%d %H:%M')}",
                            voyage_num=v.current_voyage,
                        )
                        self.next_storage_berthing_start_at[_wb_point] = (
                            _wb_berth + BERTHING_DELAY_HOURS
                        )
                        continue

                    selected_storage = v.assigned_storage
                    if not selected_storage or not self.storage_allowed_for_vessel(selected_storage, v.name):
                        v.status = "IDLE_A"
                        v.next_event_time = t
                        continue

                    # Don't override the locked storage for a priority or JMP-locked vessel
                    if not v.resumption_priority and not getattr(v, "_jmp_override_locked", False):
                        _alt_storage = self.choose_hourly_storage_option(v, t)
                        if _alt_storage and _alt_storage != selected_storage:
                            selected_storage = _alt_storage
                            v.assigned_storage = selected_storage
                            v.target_point = STORAGE_POINT.get(selected_storage, "A")
                            self.log_event(
                                t,
                                v.name,
                                "ALLOCATION_REASSESS",
                                f"Hourly reassessment switched waiting berth target to {selected_storage}",
                                voyage_num=v.current_voyage,
                            )

                    selected_point = STORAGE_POINT.get(selected_storage, "A")

                    # ── Stock-aware berth reassignment ────────────────────────────────
                    # If the currently assigned storage has far too little stock to
                    # be worth waiting for (e.g. Starturn at <50% of threshold
                    # while another storage is already above threshold), proactively
                    # switch — prevents vessels idling indefinitely outside an empty tank.
                    _assigned_cap = self.effective_load_cap(v.name, selected_storage)
                    _assigned_thr = self.loading_start_threshold(selected_storage, _assigned_cap)
                    _assigned_stk = self.storage_bbl[selected_storage]
                    if (not v.resumption_priority
                            and not getattr(v, "_jmp_override_locked", False)
                            and _assigned_stk < _assigned_thr * 0.5):
                        for _alt in STORAGE_NAMES:
                            if _alt == selected_storage:
                                continue
                            if not self.storage_allowed_for_vessel(_alt, v.name):
                                continue
                            if self.storage_locked_by_active_berth(_alt, requesting_vessel=v.name):
                                continue
                            _alt_cap = self.effective_load_cap(v.name, _alt)
                            _alt_thr = self.loading_start_threshold(_alt, _alt_cap)
                            if self.storage_bbl[_alt] >= _alt_thr:
                                self.log_event(
                                    t, v.name, "ALLOCATION_REASSESS",
                                    f"Stock-aware reassignment: {selected_storage} only "
                                    f"{_assigned_stk:,.0f}/{_assigned_thr:,.0f} bbl (<50% threshold); "
                                    f"switching to {_alt} ({self.storage_bbl[_alt]:,.0f} bbl ready)",
                                    voyage_num=v.current_voyage,
                                )
                                selected_storage = _alt
                                v.assigned_storage = _alt
                                v.target_point = STORAGE_POINT.get(_alt, "A")
                                selected_point = v.target_point
                                break

                    # Hard active-berth lock: block immediately if another vessel
                    # is physically berthed, connecting hoses, or actively loading —
                    # regardless of what storage_berth_free_at says.
                    # This is the primary guard against double-berthing at a storage.
                    _wb_active_lock = self.storage_locked_by_active_berth(
                        selected_storage, requesting_vessel=v.name)
                    # Priority vessels bypass the inter-berth serial gap but still
                    # respect the active-berth lock and storage_berth_free_at.
                    if v.resumption_priority:
                        berth_now_ok = (
                            not _wb_active_lock
                            and self.is_valid_berthing_time(t, point=selected_point)
                            and t >= self.storage_berth_free_at[selected_storage]
                        )
                    else:
                        berth_now_ok = (
                            not _wb_active_lock
                            and self.is_valid_berthing_time(t, point=selected_point)
                            and t >= self.storage_berth_free_at[selected_storage]
                            and t >= self.next_storage_berthing_start_at[selected_point]
                        )
                    if not berth_now_ok:
                        next_check = self.next_daylight_hourly_berth_check(t, point=selected_point)
                        v.next_event_time = next_check
                        _lock_reason = " (berth physically occupied)" if _wb_active_lock else ""
                        self.log_event(
                            t,
                            v.name,
                            "WAITING_BERTH_A",
                            f"Berth still unavailable at {selected_storage}{_lock_reason}; hourly daylight recheck at "
                            f"{self.hours_to_dt(next_check).strftime('%Y-%m-%d %H:%M')}",
                            voyage_num=v.current_voyage,
                        )
                        continue

                    cap = self.effective_load_cap(v.name, selected_storage)
                    load_hours = self.storage_load_hours(selected_storage, cap, vessel_name=v.name)
                    v.assigned_load_hours = load_hours
                    v.status = "BERTHING_A"
                    self.storage_berth_free_at[selected_storage] = (
                        t + BERTHING_DELAY_HOURS + HOSE_CONNECTION_HOURS + load_hours
                    )
                    # Priority vessels skip the serial start-gap advancement
                    if not v.resumption_priority:
                        self.next_storage_berthing_start_at[selected_point] = (
                            t + BERTHING_DELAY_HOURS + POST_BERTHING_START_GAP_HOURS
                        )
                    v.next_event_time = t + BERTHING_DELAY_HOURS
                    slot = (VESSEL_NAMES.index(v.name) + 1) if v.name in VESSEL_NAMES else "C"
                    if v.resumption_priority:
                        _rh_disp = v.resumption_hour
                        self.log_event(
                            t, v.name, "RESUMPTION_BERTHING",
                            f"Priority resumption berthing at {selected_storage} "
                            f"(resumed {self.hours_to_dt(_rh_disp).strftime('%Y-%m-%d') if _rh_disp else 'pending'}, "
                            f"bypassing queue) [rotation slot {slot} of {NUM_DAUGHTERS}]",
                            voyage_num=v.current_voyage,
                        )
                        # Clear all resumption state — vessel runs normally from here
                        v.resumption_hour        = None
                        v.resumption_storage     = None
                        v.resumption_priority    = False
                        v.resumption_hold_logged = False
                    else:
                        self.log_event(
                            t,
                            v.name,
                            "BERTHING_START_A",
                            f"Berthing at {selected_storage} after standby (30 min procedure) "
                            f"[rotation slot {slot} of {NUM_DAUGHTERS}]",
                            voyage_num=v.current_voyage,
                        )

                elif v.status == "HOSE_CONNECT_A":
                    # ── Dead-stock rule enforced here ───────────────────
                    # Loading can only commence when storage holds at least
                    # the storage-specific loading-start threshold.
                    # For Duke/Starturn this is nominated cargo + 5,000 bbl;
                    # other storages use 175% dead-stock. The cargo was
                    # NOT pre-committed in IDLE_A for the berth reservation;
                    # it is committed here once the threshold is satisfied.
                    selected_storage = v.assigned_storage or STORAGE_PRIMARY_NAME
                    cap = self.effective_load_cap(v.name, selected_storage)
                    threshold_required = self.loading_start_threshold(selected_storage, cap)
                    load_hours = v.assigned_load_hours if v.assigned_load_hours is not None else LOAD_HOURS
                    # Recompute load_hours based on effective cap (handles Point A cap)
                    load_hours = self.storage_load_hours(selected_storage, cap, vessel_name=v.name)
                    if self.storage_bbl[selected_storage] < threshold_required:
                        # ── Dead-stock escape valve ────────────────────────────────────
                        # Track when the wait started.  If a vessel has been stuck here
                        # longer than DEAD_STOCK_MAX_WAIT_HOURS (default 12h) without
                        # loading commencing, check if another storage this vessel is
                        # permitted to use already has enough stock.  If so, abort the
                        # berth, release the lock, and let the vessel reassign — this
                        # prevents Starturn/Duke (83/250 bbl/hr) from trapping fast
                        # vessels for days while SanBarth/JasmineS/Westmore are full.
                        if v.dead_stock_wait_start is None:
                            v.dead_stock_wait_start = t
                        wait_so_far = t - v.dead_stock_wait_start
                        if wait_so_far >= DEAD_STOCK_MAX_WAIT_HOURS:
                            # Find an alternative storage that is ready NOW
                            alt_storage = None
                            for alt in STORAGE_NAMES:
                                if alt == selected_storage:
                                    continue
                                if not self.storage_allowed_for_vessel(alt, v.name):
                                    continue
                                if self.storage_locked_by_active_berth(alt, requesting_vessel=v.name):
                                    continue
                                alt_cap   = self.effective_load_cap(v.name, alt)
                                alt_thr   = self.loading_start_threshold(alt, alt_cap)
                                if self.storage_bbl[alt] >= alt_thr:
                                    alt_storage = alt
                                    break
                            if alt_storage:
                                # Release the current berth lock and reassign
                                self.storage_berth_free_at[selected_storage] = t
                                v.assigned_storage = alt_storage
                                v.target_point = STORAGE_POINT.get(alt_storage, "A")
                                v.status = "IDLE_A"
                                v.next_event_time = t
                                v.dead_stock_wait_start = None
                                self.log_event(
                                    t, v.name, "ALLOCATION_REASSESS",
                                    f"Dead-stock escape: waited {wait_so_far:.1f}h at {selected_storage} "
                                    f"({self.storage_bbl[selected_storage]:,.0f}/{threshold_required:,.0f} bbl); "
                                    f"reassigned to {alt_storage} ({self.storage_bbl[alt_storage]:,.0f} bbl available)",
                                    voyage_num=v.current_voyage,
                                )
                                continue
                        # Stay at berth; poll every 30 min until stock builds
                        v.next_event_time = t + 0.5
                        self.log_event(t, v.name, "WAITING_DEAD_STOCK",
                                       f"Berthed but waiting for loading-start threshold "
                                       f"({threshold_required:,.0f} bbl required, "
                                       f"{self.storage_bbl[selected_storage]:,.0f} bbl available at {selected_storage})",
                                       voyage_num=v.current_voyage)
                        continue
                    if (
                        selected_storage == STORAGE_QUATERNARY_NAME
                        and (self.storage_bbl[selected_storage] - cap) < DUKE_MIN_REMAINING_BBL
                    ):
                        v.next_event_time = t + 0.5
                        self.log_event(
                            t,
                            v.name,
                            "WAITING_DEAD_STOCK",
                            f"Berthed but waiting for Duke reserve rule "
                            f"({DUKE_MIN_REMAINING_BBL:,.0f} bbl must remain after loading; "
                            f"current post-load would be {self.storage_bbl[selected_storage] - cap:,.0f} bbl)",
                            voyage_num=v.current_voyage,
                        )
                        continue
                    if (
                        selected_storage == STORAGE_QUINARY_NAME
                        and (self.storage_bbl[selected_storage] - cap) < STARTURN_MIN_REMAINING_BBL
                    ):
                        v.next_event_time = t + 0.5
                        self.log_event(
                            t,
                            v.name,
                            "WAITING_DEAD_STOCK",
                            f"Berthed but waiting for Starturn reserve rule "
                            f"({STARTURN_MIN_REMAINING_BBL:,.0f} bbl must remain after loading; "
                            f"current post-load would be {self.storage_bbl[selected_storage] - cap:,.0f} bbl)",
                            voyage_num=v.current_voyage,
                        )
                        continue
                    # Threshold met — commit stock and start loading
                    v.dead_stock_wait_start = None  # reset escape timer
                    self.storage_bbl[selected_storage] -= cap
                    # Vessel receives a full cargo from storage — its API equals the
                    # storage point's current API exactly (no blending at load point).
                    _load_api = self.storage_api.get(selected_storage, 0.0)
                    self.vessel_api[v.name] = _load_api
                    v.cargo_bbl = cap
                    self.total_loaded += cap
                    v.status = "LOADING"
                    # Stamp the voyage code on the vessel for referencing discharge assignment
                    v.voyage_code = make_voyage_code(v.name, v.current_voyage)
                    self.storage_berth_free_at[selected_storage] = max(
                        self.storage_berth_free_at[selected_storage], t + load_hours
                    )
                    v.next_event_time = t + load_hours
                    self.log_event(t, v.name, "LOADING_START",
                                   f"Loading {cap:,} bbl @ {_load_api:.2f}° API | {selected_storage}: "
                                   f"{self.storage_bbl[selected_storage]:,.0f} bbl "
                                   f"(loading-start threshold {threshold_required:,.0f} bbl met, rate duration {load_hours:.1f}h)",
                                   voyage_num=v.current_voyage)

                elif v.status == "LOADING":
                    # Ensure cargo_bbl reflects the full completed load.
                    # Use effective_load_cap so Bedford/Balham at Point A
                    # complete at 63k, not their physical 85k capacity.
                    _load_stor = v.assigned_storage or STORAGE_PRIMARY_NAME
                    v.cargo_bbl = self.effective_load_cap(v.name, _load_stor)
                    v.status = "DOCUMENTING"
                    v.next_event_time = t + 4
                    # Clear JMP lock — override has been honoured
                    v._jmp_override_locked = False
                    v._jmp_load_after_hour = None
                    self.log_event(t, v.name, "LOADING_COMPLETE",
                                   f"Cargo: {v.cargo_bbl:,} bbl | Begin 4h documentation",
                                   voyage_num=v.current_voyage)
                    self.log_event(t, v.name, "DOCUMENTATION_START",
                                   "4 hours allocated for paperwork",
                                   voyage_num=v.current_voyage)

                elif v.status == "DOCUMENTING":
                    cast_off_t = self.next_cast_off_window(t)
                    wait_co = cast_off_t - t
                    v.status = "CAST_OFF"
                    v.next_event_time = cast_off_t + CAST_OFF_HOURS
                    self.log_event(t, v.name, "DOCUMENTATION_COMPLETE",
                                   f"Ready for cast-off | Procedure starts "
                                   f"{self.hours_to_dt(cast_off_t).strftime('%H:%M')} (wait {wait_co:.1f}h)",
                                   voyage_num=v.current_voyage)
                    if wait_co > 0:
                        self.log_event(t, v.name, "WAITING_CAST_OFF",
                                       f"Cast-off window opens at "
                                       f"{self.hours_to_dt(cast_off_t).strftime('%Y-%m-%d %H:%M')}",
                                       voyage_num=v.current_voyage)

                elif v.status == "CAST_OFF":
                    sail_t = self.next_tidal_sail(t)
                    wait = sail_t - t
                    if v.target_point == "D":
                        v.status = "SAILING_D_CHANNEL"
                        v.next_event_time = sail_t + _sail_leg(SAIL_HOURS_D_TO_CH, self)
                    elif v.target_point == "F":
                        # Casting off from Ibom (Point F) with partial cargo —
                        # sail directly to BIA (same leg distance as B→F = 3h).
                        # Reuse SAILING_AB_LEG2 which arrives at BIA and triggers
                        # the normal Point B berthing/discharge flow.
                        v.status = "SAILING_AB_LEG2"
                        v.next_event_time = sail_t + _sail_leg(SAIL_HOURS_B_TO_F, self)
                        # Clear Point F target so return allocation sends to Point A
                        v.target_point = "B"
                    else:
                        # A/C → B: 4-leg route via breakwater and fairway buoy
                        v.status = "SAILING_AB"
                        v.next_event_time = sail_t + _sail_leg(SAIL_HOURS_A_TO_BW, self)
                    self.log_event(t, v.name, "CAST_OFF_COMPLETE",
                                   f"Cast-off complete | Departure "
                                   f"{self.hours_to_dt(sail_t).strftime('%H:%M')} (wait {wait:.1f}h)",
                                   voyage_num=v.current_voyage)
                    if wait > 0:
                        self.log_event(t, v.name, "WAITING_TIDAL",
                                       f"Daylight/tide window opens at "
                                       f"{self.hours_to_dt(sail_t).strftime('%Y-%m-%d %H:%M')} "
                                       f"({self.tidal_period_label(sail_t)}; available today: "
                                       f"{self.tidal_periods_available_for_day(sail_t)})",
                                       voyage_num=v.current_voyage)

                elif v.status == "SAILING_D_CHANNEL":
                    # Arrived Cawthorne Channel — next leg to Breakwater needs tidal gate
                    arrival = t
                    self.log_event(arrival, v.name, "ARRIVED_CAWTHORNE_CHANNEL",
                                   "Reached Cawthorne Channel (3h from Point D)",
                                   voyage_num=v.current_voyage)
                    depart_ch = self.next_tidal_sail(arrival)
                    wait_ch = depart_ch - arrival
                    if wait_ch > 0:
                        self.log_event(arrival, v.name, "WAITING_TIDAL",
                                       f"Cawthorne Channel: waiting for daylight/tide at "
                                       f"{self.hours_to_dt(depart_ch).strftime('%Y-%m-%d %H:%M')} "
                                       f"({self.tidal_period_label(depart_ch)})",
                                       voyage_num=v.current_voyage)
                    v.status = "SAILING_CH_TO_BW_OUT"
                    v.next_event_time = depart_ch + _sail_leg(SAIL_HOURS_CH_TO_BW_OUT, self)

                elif v.status == "SAILING_CH_TO_BW_OUT":
                    # Arrived at Breakwater (outbound) — tidal gate for crossing
                    arrival = t
                    self.log_event(arrival, v.name, "ARRIVED_BREAKWATER_OUT",
                                   "Reached breakwater (outbound, 1h from Cawthorne Channel)",
                                   voyage_num=v.current_voyage)
                    depart_bw = self.next_tidal_sail(arrival)
                    wait_bw = depart_bw - arrival
                    if wait_bw > 0:
                        self.log_event(arrival, v.name, "WAITING_TIDAL",
                                       f"Breakwater: waiting for daylight/tide at "
                                       f"{self.hours_to_dt(depart_bw).strftime('%Y-%m-%d %H:%M')} "
                                       f"({self.tidal_period_label(depart_bw)})",
                                       voyage_num=v.current_voyage)
                    v.status = "SAILING_CROSS_BW_OUT"
                    v.next_event_time = depart_bw + _sail_leg(SAIL_HOURS_CROSS_BW, self)

                elif v.status == "SAILING_CROSS_BW_OUT":
                    # Crossed breakwater outbound — final run to BIA (no tidal gate)
                    arrival = t
                    self.log_event(arrival, v.name, "CROSSED_BREAKWATER_OUT",
                                   "Crossed breakwater (0.5h) — clear breakwater, running to BIA (1.5h)",
                                   voyage_num=v.current_voyage)
                    v.status = "SAILING_AB_LEG2"
                    v.next_event_time = arrival + _sail_leg(SAIL_HOURS_BW_TO_B, self)

                elif v.status == "SAILING_AB":
                    # Arrived at Breakwater (outbound from A/C) — tidal gate to cross
                    arrival = t
                    self.log_event(arrival, v.name, "ARRIVED_BREAKWATER_AC_OUT",
                                   "Reached breakwater (1.5h from Point A/C)",
                                   voyage_num=v.current_voyage)
                    depart_bw = self.next_tidal_sail(arrival)
                    wait_bw   = depart_bw - arrival
                    if wait_bw > 0:
                        self.log_event(arrival, v.name, "WAITING_TIDAL",
                                       f"Breakwater: waiting for daylight/tide at "
                                       f"{self.hours_to_dt(depart_bw).strftime('%Y-%m-%d %H:%M')} "
                                       f"({self.tidal_period_label(depart_bw)})",
                                       voyage_num=v.current_voyage)
                    v.status = "SAILING_CROSS_BW_AC"
                    v.next_event_time = depart_bw + _sail_leg(SAIL_HOURS_CROSS_BW_AC, self)

                elif v.status == "SAILING_CROSS_BW_AC":
                    # Crossed breakwater outbound — run to fairway buoy (daylight only)
                    arrival = t
                    self.log_event(arrival, v.name, "CROSSED_BREAKWATER_AC_OUT",
                                   "Crossed breakwater outbound (0.5h) — heading to Fairway Buoy (2h)",
                                   voyage_num=v.current_voyage)
                    depart_fwy = self.next_daylight_sail(arrival)
                    wait_fwy   = depart_fwy - arrival
                    if wait_fwy > 0:
                        self.log_event(arrival, v.name, "WAITING_DAYLIGHT",
                                       f"Post-breakwater: waiting for daylight at "
                                       f"{self.hours_to_dt(depart_fwy).strftime('%Y-%m-%d %H:%M')}",
                                       voyage_num=v.current_voyage)
                    v.status = "SAILING_BW_TO_FWY"
                    v.next_event_time = depart_fwy + _sail_leg(SAIL_HOURS_BW_TO_FWY, self)

                elif v.status == "SAILING_BW_TO_FWY":
                    # Arrived Fairway Buoy outbound (A/C → BIA).
                    # Rule: only hold overnight if arrival is AFTER 19:00.
                    # Vessels arriving at or before 19:00 proceed directly to BIA
                    # (the 2h sail is acceptable even if it arrives after dark).
                    FAIRWAY_HOLD_HOUR = 19  # hold overnight if arrival hour >= this
                    arrival    = t
                    arrival_hod = (arrival + SIM_HOUR_OFFSET) % 24   # wall-clock hour-of-day
                    self.log_event(arrival, v.name, "ARRIVED_FAIRWAY",
                                   f"Reached Fairway Buoy — running to BIA (2h) | {v.cargo_bbl:,.0f} bbl on board",
                                   voyage_num=v.current_voyage)
                    if arrival_hod >= FAIRWAY_HOLD_HOUR:
                        # Arrived after 19:00 wall-clock — hold until next daylight
                        depart_bia   = self.next_daylight_sail(arrival + 0.01)
                        wait_bia     = depart_bia - arrival
                        self.log_event(arrival, v.name, "WAITING_FAIRWAY",
                                       f"Arrived after 19:00 ({self.hours_to_dt(arrival).strftime('%H:%M')}) — "
                                       f"holding at Fairway Buoy until {self.hours_to_dt(depart_bia).strftime('%Y-%m-%d %H:%M')}",
                                       voyage_num=v.current_voyage)
                    else:
                        # Arrived at or before 19:00 — proceed directly, no hold
                        depart_bia = arrival
                    v.status = "SAILING_AB_LEG2"
                    v.next_event_time = depart_bia + _sail_leg(SAIL_HOURS_FWY_TO_B, self)

                elif v.status == "SAILING_AB_LEG2":
                    arrival = t
                    v.arrival_at_b = arrival

                    # Ensure berthing_start is within the daylight berthing window
                    # regardless of arrival time (night, early morning, etc.)
                    berthing_start, candidates = self.point_b_candidate_slots(v, arrival)
                    if berthing_start > arrival + 0.01:
                        self.log_event(arrival, v.name, "WAITING_NIGHT",
                                       f"Arrived at {self.hours_to_dt(arrival).strftime('%H:%M')} outside berthing window — "
                                       f"waiting until {self.hours_to_dt(berthing_start).strftime('%Y-%m-%d %H:%M')}",
                                       voyage_num=v.current_voyage)

                    if not candidates:
                        mother_levels = ", ".join(
                            f"{name}: {self.mother_bbl[name]:,.0f}/{self.mother_capacity_bbl(name):,} bbl"
                            for name in MOTHER_NAMES
                        )
                        next_recheck = self.next_daylight_hourly_berth_check(arrival, point="B")
                        wait_h = max(0.0, next_recheck - arrival)
                        self.log_event(arrival, v.name, "WAITING_MOTHER_CAPACITY",
                                       f"No berth/capacity slot currently available on Point B mothers ({mother_levels}); "
                                       f"hourly daylight reassessment at "
                                       f"{self.hours_to_dt(next_recheck).strftime('%Y-%m-%d %H:%M')} "
                                       f"(wait {wait_h:.1f}h)",
                                       voyage_num=v.current_voyage)
                        v.next_event_time = next_recheck
                    else:
                        day_key = self.point_b_calendar_day_key(arrival)
                        candidate_by_mother = {
                            mother_name: (start, berth_t, mother_name)
                            for start, berth_t, mother_name in candidates
                        }

                        # Day 1 exception: disable Point B auto-prioritization and require
                        # a manual nomination for each arriving daughter vessel.
                        if STARTUP_DAY_DISABLE_POINT_B_PRIORITY and day_key == 0:
                            nominated_mother = STARTUP_DAY_POINT_B_MANUAL_NOMINATIONS.get(v.name)
                            if not nominated_mother:
                                v.next_event_time = arrival + 0.5
                                self.log_event(
                                    arrival,
                                    v.name,
                                    "WAITING_MOTHER_CAPACITY",
                                    "Startup-day manual nomination required at Point B; no auto-priority assignment applied",
                                    voyage_num=v.current_voyage,
                                )
                                continue
                            if nominated_mother not in candidate_by_mother:
                                eligible_names = ", ".join(name for name in MOTHER_NAMES if name in candidate_by_mother)
                                v.next_event_time = arrival + 0.5
                                self.log_event(
                                    arrival,
                                    v.name,
                                    "WAITING_MOTHER_CAPACITY",
                                    f"Startup-day manual nomination '{nominated_mother}' is not currently eligible; eligible now: {eligible_names}",
                                    voyage_num=v.current_voyage,
                                )
                                continue
                            selected_mother = nominated_mother
                            selected = candidate_by_mother[selected_mother]
                        else:
                            # ── Daughter discharge point override ──────────────
                            # DAUGHTER_DISCHARGE_OVERRIDES: {voyage_code: {"vessel", "mother",
                            #   "discharge_date"}} or legacy {vessel: {day: mother}}.
                            # ZeeZee is unaffected (handled by _run_zeezee separately).
                            _ddo_mother, _ddo_disc_date = self._resolve_discharge_override(
                                v.name, getattr(v, "voyage_code", ""), arrival)
                            _candidate_mothers = {m for _, _, m in candidates}
                            if _ddo_mother:
                                # ── Date-hold: vessel arrived early — wait at BIA ──
                                if not self._discharge_override_date_reached(_ddo_disc_date, arrival):
                                    # Calculate sim-hour of 08:00 on the discharge date
                                    from datetime import datetime as _ddt
                                    _disc_dt = _ddt.fromisoformat(_ddo_disc_date).replace(hour=8, minute=0)
                                    _hold_until = (_disc_dt - _SIM_EPOCH).total_seconds() / 3600.0
                                    _hold_until = max(_hold_until, arrival)
                                    v.status = "WAITING_BERTH_B"
                                    v.assigned_mother = _ddo_mother
                                    v.next_event_time = _hold_until
                                    self.log_event(
                                        arrival, v.name, "WAITING_BERTH_B",
                                        f"Discharge override [{v.voyage_code}]: holding at BIA until "
                                        f"{_ddo_disc_date} to discharge to {_ddo_mother} — "
                                        f"vessel arrived early; wait until "
                                        f"{self.hours_to_dt(_hold_until).strftime('%Y-%m-%d %H:%M')}",
                                        voyage_num=v.current_voyage, mother=_ddo_mother,
                                    )
                                    continue
                                # ── Date reached: displace incumbent and berth ──
                                if _ddo_mother in _candidate_mothers:
                                    self._displace_incumbent_at_mother(_ddo_mother, arrival)
                                    selected_mother = _ddo_mother
                                    # Recompute candidates after displacement
                                    _, candidates = self.point_b_candidate_slots(v, arrival)
                                    selected = next(
                                        (x for x in candidates if x[2] == _ddo_mother),
                                        None,
                                    )
                                    if selected is None:
                                        selected = (arrival, arrival, _ddo_mother)
                                    # Lock deferred to pump-start
                                    self.log_event(
                                        arrival, v.name, "MOTHER_PRIORITY_ASSIGNMENT",
                                        f"Discharge override [{v.voyage_code}]: forced to "
                                        f"{_ddo_mother} on {_ddo_disc_date} — incumbent displaced",
                                        voyage_num=v.current_voyage, mother=_ddo_mother,
                                    )
                                else:
                                    # Mother not at BIA (e.g. at export) — wait for 30-min rescan
                                    v.next_event_time = self.next_daylight_hourly_berth_check(arrival, point="B")
                                    self.log_event(
                                        arrival, v.name, "WAITING_BERTH_B",
                                        f"Discharge override [{v.voyage_code}]: target {_ddo_mother} "
                                        f"not available on {self.hours_to_dt(arrival).strftime('%Y-%m-%d')}; "
                                        f"rescan in 30 min at "
                                        f"{self.hours_to_dt(v.next_event_time).strftime('%Y-%m-%d %H:%M')}",
                                        voyage_num=v.current_voyage, mother=_ddo_mother,
                                    )
                                    v.status = "WAITING_BERTH_B"
                                    v.assigned_mother = _ddo_mother
                                    continue
                            else:
                                selected_meta, _ = self.select_point_b_mother(
                                    v,
                                    arrival,
                                    day_key,
                                    candidates,
                                )
                                selected_mother = selected_meta["mother"]
                                selected = (
                                    selected_meta["start"],
                                    selected_meta["berth_t"],
                                    selected_meta["mother"],
                                )

                        # ── MTO queued discharger override ───────────────────
                        # If this vessel was flagged at startup as a queued MTO
                        # discharger (_mto_target_vessel set), route it to the
                        # transient vessel instead of a primary mother.
                        _mto_tv_attr = getattr(v, "_mto_target_vessel", None)
                        if _mto_tv_attr:
                            # Find the transient receiver vessel
                            _recv_v = next(
                                (vv for vv in self.vessels if vv.name == _mto_tv_attr
                                 and getattr(vv, "_mto_transient_since_day", None) is not None),
                                None
                            )
                            if _recv_v is not None:
                                # Check receiver has headroom for this discharger
                                _trn_cap   = MTO_TRANSIENT_CAPACITY_BBL.get(
                                    _recv_v.name, _recv_v.cargo_capacity)
                                _headroom  = max(0.0, _trn_cap - _recv_v.cargo_bbl)
                                _berth_free = getattr(_recv_v, "_mto_berth_free_at", 0.0)

                                if _headroom >= v.cargo_bbl and _berth_free <= arrival:
                                    # Execute the MTO transfer immediately
                                    _dis_api   = self.vessel_api.get(v.name, 0.0)
                                    _trn_api   = self.vessel_api.get(_recv_v.name, 0.0)
                                    _trn_old   = _recv_v.cargo_bbl
                                    _xfer      = min(v.cargo_bbl, _headroom)
                                    _new_trn   = _trn_old + _xfer
                                    if _new_trn > 0:
                                        self.vessel_api[_recv_v.name] = (
                                            (_trn_old * _trn_api + _xfer * _dis_api) / _new_trn
                                        )
                                    _recv_v.cargo_bbl = _new_trn
                                    _recv_v._mto_parcels_received = getattr(
                                        _recv_v, "_mto_parcels_received", 0) + 1

                                    _mto_rate   = VESSEL_DISCHARGE_RATE_BPH.get(v.name)
                                    _pump_h     = (_xfer / _mto_rate) if _mto_rate else DISCHARGE_HOURS
                                    _cast_t     = self.next_cast_off_window(
                                        arrival + BERTHING_DELAY_HOURS + HOSE_CONNECTION_HOURS + _pump_h
                                    )
                                    _end_t      = _cast_t + CAST_OFF_HOURS
                                    _recv_v._mto_berth_free_at = _end_t

                                    v.cargo_bbl       = 0
                                    self.vessel_api[v.name] = 0.0
                                    v.status          = "CAST_OFF_B"
                                    v.next_event_time = _end_t
                                    v._mto_target_vessel = None  # clear flag

                                    # Increment parcel counter on receiver
                                    _recv_v._mto_parcels_received = getattr(
                                        _recv_v, "_mto_parcels_received", 0) + 1

                                    # After this discharge the receiver's transient
                                    # role is complete — clear flag so the auto MTO
                                    # gate no longer sees it as an active transient,
                                    # preventing duplicate Day 2 nominations.
                                    _recv_v._mto_transient_since_day = None

                                    _recv_v.status          = "WAITING_BERTH_B"
                                    _recv_v.next_event_time = self.next_daylight_hourly_berth_check(
                                        arrival, point="B"
                                    )

                                    self.log_event(
                                        arrival, v.name, "MTO_DISCHARGE_TO_TRANSIENT",
                                        f"[MTO queued arrival] {v.name} → {_recv_v.name}: "
                                        f"{_xfer:,.0f} bbl | pump {_pump_h:.1f}h | "
                                        f"cast-off {self.hours_to_dt(_end_t).strftime('%H:%M')}",
                                        voyage_num=v.current_voyage,
                                    )
                                    continue  # skip normal mother assignment
                                else:
                                    # Receiver berth busy — wait until it frees
                                    v.status          = "WAITING_BERTH_B"
                                    v.next_event_time = max(arrival, _berth_free) + 0.5
                                    v.assigned_mother = None
                                    self.log_event(
                                        arrival, v.name, "WAITING_BERTH_B",
                                        f"MTO queued for {_recv_v.name}; transient berth busy until "
                                        f"{self.hours_to_dt(_berth_free).strftime('%H:%M')} — waiting",
                                        voyage_num=v.current_voyage,
                                    )
                                    continue
                            # If receiver not found or no longer transient, fall through to normal logic
                            v._mto_target_vessel = None

                        start, berth_t, selected_mother = selected
                        v.assigned_mother = selected_mother
                        if start > arrival + 0.01:
                            v.status = "WAITING_BERTH_B"
                            v.next_event_time = self.next_daylight_hourly_berth_check(arrival, point="B")
                            self.log_event(
                                arrival,
                                v.name,
                                "WAITING_BERTH_B",
                                f"Assigned to {selected_mother}; rescan every 30 min until berth opens "
                                f"(earliest {self.hours_to_dt(start).strftime('%Y-%m-%d %H:%M')})",
                                voyage_num=v.current_voyage,
                                mother=selected_mother,
                            )
                        else:
                            v.status = "BERTHING_B"
                            _disch_rate_3 = VESSEL_DISCHARGE_RATE_BPH.get(v.name)
                            _disch_hrs_3 = (v.cargo_bbl / _disch_rate_3) if _disch_rate_3 else DISCHARGE_HOURS
                            _pump_end_3   = start + BERTHING_DELAY_HOURS + HOSE_CONNECTION_HOURS + _disch_hrs_3
                            _discharge_end = _berth_free_at(_pump_end_3)
                            self.mother_berth_free_at[selected_mother] = max(
                                self.mother_berth_free_at[selected_mother], _discharge_end
                            )
                            v.next_event_time = start + BERTHING_DELAY_HOURS
                            self.log_event(start, v.name, "BERTHING_START_B",
                                           f"Berthing at {selected_mother} (30 min procedure)",
                                           voyage_num=v.current_voyage)
                        if STARTUP_DAY_DISABLE_POINT_B_PRIORITY and day_key == 0:
                            self.log_event(
                                arrival,
                                v.name,
                                "MOTHER_PRIORITY_ASSIGNMENT",
                                f"Day 1 startup manual nomination applied: {selected_mother} (auto-prioritization disabled)",
                                voyage_num=v.current_voyage,
                            )
                        else:
                            assigned_today = self.point_b_day_assigned_mothers.get(day_key, set())
                            self.log_event(
                                arrival,
                                v.name,
                                "MOTHER_PRIORITY_ASSIGNMENT",
                                f"Day {day_key + 1} optimization assignment: "
                                f"{selected_mother} selected using 08:00 projected stock + earliest berth | "
                                f"already assigned today: {', '.join(sorted(assigned_today))}",
                                voyage_num=v.current_voyage,
                            )

                elif v.status == "WAITING_BERTH_B":
                    decision_t = t

                    # ── MTO transient vessel: opportunistic offload priority ───
                    # A transient vessel tries to berth at a primary mother on
                    # every hourly check — as soon as a window opens it takes it.
                    # It does NOT wait for "day+1"; the capacity ceiling and parcel
                    # count control only when MORE shuttles can top it up, never
                    # when it may discharge.  Priority is absolute: it displaces
                    # any incumbent at the best available berth.
                    _mto_since = getattr(v, "_mto_transient_since_day", None)
                    # Belt-and-suspenders: a vessel carrying at least 20k above
                    # its standard cargo_capacity and within its MTO_TRANSIENT_CAPACITY_BBL
                    # should be treated as an MTO transient.  Smaller overloads are
                    # regarded as normal voyages that have exceeded nominal capacity.
                    if (_mto_since is None
                            and v.cargo_bbl >= v.cargo_capacity + 20_000
                            and v.cargo_bbl <= MTO_TRANSIENT_CAPACITY_BBL.get(
                                v.name, v.cargo_capacity)):
                        v._mto_transient_since_day = 0   # treat as Day-0 transient
                        v._is_mto_offload          = True
                        v._mto_parcels_received    = getattr(
                            v, "_mto_parcels_received", 1)
                        _mto_since = 0
                    if _mto_since is not None:
                        # Guard: last MTO discharger must have cast off before the
                        # transient seeks a mother berth.  Day-1 startup receivers
                        # have _mto_berth_free_at=0.0 so this check is naturally
                        # exempt for them.  Runtime MTO sets _mto_berth_free_at=
                        # transfer_end_t; next_event_time is also transfer_end_t so
                        # this only fires if next_event_time was advanced earlier.
                        _mto_last_xfr_done = getattr(v, "_mto_berth_free_at", 0.0)
                        if t < _mto_last_xfr_done:
                            v.next_event_time = self.next_daylight_hourly_berth_check(
                                _mto_last_xfr_done, point="B"
                            )
                            continue
                        # Find the best primary mother — any mother with ANY space.
                        # The old guard (space < v.cargo_bbl) was wrong: it froze
                        # vessels indefinitely when no mother had full-cargo space.
                        # The transient discharges WHATEVER FITS, not necessarily all
                        # at once. Transfer is clamped at HOSE_CONNECT_B to available
                        # space so the mother never overflows.
                        _mto_best_mother = None
                        _mto_best_start  = None
                        # Full-fit preference: a mother whose headroom can accept the
                        # ENTIRE transient cargo.  An MTO discharge is hard-aborted
                        # (MOTHER_CAPACITY_ABORT) when cargo > headroom — it never
                        # partial-fills — so picking the earliest-slot mother without
                        # regard to headroom can send the transient to a near-full
                        # mother that aborts it every tick (e.g. Watson WTS-003A's 127k
                        # repeatedly aborting at a 50k-headroom GreenEagle on 10–11 Jun
                        # while Bryanston sat with ~475k free).  Prefer a mother that
                        # can take the whole cargo; only fall back to an insufficient-
                        # headroom mother when none can (preserving the original intent
                        # of not waiting indefinitely when every mother is near-full).
                        _mto_fit_mother = None
                        _mto_fit_start  = None
                        # Queue fallback: best primary that HAS space but whose berth
                        # is currently occupied.  Used only when no berth is claimable
                        # right now, so the transient waits in line for the soonest
                        # cast-off instead of spinning forever and never offloading.
                        _mto_queue_mother = None
                        _mto_queue_start  = None
                        _mto_fit_queue_mother = None
                        _mto_fit_queue_start  = None
                        for _mn in MOTHER_NAMES:
                            if not self.mother_is_at_point_b(_mn, decision_t):
                                continue
                            # Skip mothers in any export state — DOC/SAILING means
                            # departing; HOSE/IN_PORT means physically at the export
                            # terminal and unavailable for daughter discharge.
                            if self.export_state.get(_mn) in EXPORT_BUSY_STATES:
                                continue
                            _space = self.mother_capacity_bbl(_mn) - self.mother_bbl[_mn]
                            # Any positive headroom suffices — the HOSE_CONNECT_B
                            # handler clamps transfer to available space.
                            # Requiring full-cargo space caused MTO receivers to wait
                            # indefinitely when mothers were always near-full.
                            if _space <= 0:
                                continue
                            _slot = self.next_berthing_window(
                                max(decision_t,
                                    self.mother_berth_free_at[_mn],
                                    self.mother_available_at[_mn]),
                                point="B",
                            )
                            if self._point_b_mother_assigned_on_day(_mn, _slot):
                                continue
                            # Do not target a mother whose berth is already
                            # physically occupied by another actor mid-operation
                            # (BERTHING_B / HOSE_CONNECT_B / DISCHARGING / CAST_OFF_B).
                            # The MTO priority-berth path below displaces the
                            # incumbent unconditionally; if that incumbent is mid-
                            # cycle it gets bumped, re-berthed by the MT SanBarth
                            # idle-daughter daylight call, then bumped again next
                            # hour — an all-day berth ping-pong that completes no
                            # discharge and monopolises the berth.  Consistent with
                            # the concurrent-berth guard's rule of never interrupting
                            # an active operation, wait for the current occupant to
                            # finish rather than displacing it.
                            _occ_mn = self.mother_berth_current_occupant(_mn)
                            if _occ_mn is not None and getattr(_occ_mn, "name", None) != v.name:
                                # Berth physically occupied.  Record this mother as a
                                # QUEUE fallback (soonest cast-off) but do not target it
                                # for an immediate claim — the transient must not displace
                                # an active incumbent (that caused the day-7 berth ping-pong).
                                # The queue fallback below lets the transient wait its turn
                                # rather than skip the only space-having primary every tick
                                # and never offload its accumulated cargo.
                                _occ_free = self.next_berthing_window(
                                    max(decision_t,
                                        getattr(_occ_mn, "next_event_time", decision_t) or 0.0,
                                        self.mother_berth_free_at.get(_mn, 0.0)),
                                    point="B",
                                )
                                if _mto_queue_start is None or _occ_free < _mto_queue_start:
                                    _mto_queue_start  = _occ_free
                                    _mto_queue_mother = _mn
                                # Track the full-fit subset of queue candidates too.
                                if _space >= v.cargo_bbl and (
                                        _mto_fit_queue_start is None
                                        or _occ_free < _mto_fit_queue_start):
                                    _mto_fit_queue_start  = _occ_free
                                    _mto_fit_queue_mother = _mn
                                continue
                            if _mto_best_start is None or _slot < _mto_best_start:
                                _mto_best_start  = _slot
                                _mto_best_mother = _mn
                            # Track the full-fit subset of immediate candidates.
                            if _space >= v.cargo_bbl and (
                                    _mto_fit_start is None or _slot < _mto_fit_start):
                                _mto_fit_start  = _slot
                                _mto_fit_mother = _mn
                        # Prefer a mother that can accept the entire cargo (no abort).
                        if _mto_fit_mother is not None:
                            _mto_best_mother, _mto_best_start = _mto_fit_mother, _mto_fit_start
                        if _mto_fit_queue_mother is not None:
                            _mto_queue_mother, _mto_queue_start = _mto_fit_queue_mother, _mto_fit_queue_start
                        # If the only immediately-claimable mother CANNOT take the full
                        # cargo (it would just MOTHER_CAPACITY_ABORT every tick) but a
                        # full-fit mother is reachable by queueing for its berth, do not
                        # claim the insufficient one — fall through to the queue branch
                        # so the transient waits for the mother that can actually accept
                        # it.  This is what lets Watson's 127k queue for Bryanston
                        # (~475k free) instead of abort-looping at a near-full GreenEagle,
                        # which in turn frees GreenEagle for a smaller daughter (Woodstock).
                        _immediate_is_undersized = (
                            _mto_best_mother is not None
                            and _mto_fit_mother is None
                            and (self.mother_capacity_bbl(_mto_best_mother)
                                 - self.mother_bbl[_mto_best_mother]) < v.cargo_bbl
                        )
                        if _immediate_is_undersized and _mto_fit_queue_mother is not None:
                            _mto_best_mother = None   # force the queue-for-full-fit path
                        if _mto_best_mother is not None:
                            # Displace incumbent and release berth lock first
                            self._displace_incumbent_at_mother(_mto_best_mother, decision_t)
                            # Recompute start from NOW after displacement cleared the lock
                            _mto_actual_start = self.next_berthing_window(
                                max(decision_t,
                                    self.mother_available_at.get(_mto_best_mother, 0.0)),
                                point="B",
                            )
                            v.assigned_mother = _mto_best_mother
                            # Lock deferred to pump-start.  Do NOT reserve
                            # mother_berth_free_at here: this is only a speculative
                            # berth *claim*.  If the concurrent-berth guard aborts
                            # this attempt before pumping starts, a reservation
                            # written here is never rolled back and orphans the
                            # mother's berth (ratcheting forward on every retry),
                            # which starves genuinely eligible daughters.  The real
                            # reservation is established at DISCHARGE_START via
                            # _point_b_register_mother_start + _enforce_exclusive_day_at_mother
                            # once cargo physically flows.  Pre-pump occupancy is
                            # already enforced by mother_berth_current_occupant and
                            # the concurrent-berth guard.
                            v.status = "BERTHING_B"
                            v.next_event_time = _mto_actual_start + BERTHING_DELAY_HOURS
                            v._mto_transient_since_day  = None   # clear transient flag
                            v._mto_offload_wait_since   = None   # claimed — clear stuck timer
                            v._mto_parcels_received     = 0      # reset parcel counter
                            v._is_mto_offload           = True   # mark for voyage code suffix
                            _cur_day_key = int((decision_t + SIM_HOUR_OFFSET) // 24)
                            self.log_event(
                                decision_t, v.name, "MTO_TRANSIENT_PRIORITY_BERTH",
                                f"[MTO] Transient offloading at {_mto_best_mother} "
                                f"(Day {_cur_day_key+1}) — {v.cargo_bbl:,.0f} bbl on board | "
                                f"berth at {self.hours_to_dt(_mto_best_start).strftime('%H:%M')}",
                                voyage_num=v.current_voyage, mother=_mto_best_mother,
                            )
                            continue
                        else:
                            # No berth claimable right now.  If a primary with space
                            # exists but is currently occupied, QUEUE for the one that
                            # frees soonest: assign the transient and wait for the
                            # incumbent to cast off, then berth on a later check (no
                            # displacement).  Without this an MTO transient whose only
                            # space-having primary is continuously busy spins forever
                            # and never offloads its accumulated cargo (e.g. AMY-002
                            # holding 92k while Bryanston stays occupied and GreenEagle
                            # is full).
                            if _mto_queue_mother is not None:
                                # Track how long this transient has been unable to
                                # claim any berth.  Only escalate to a queue commitment
                                # once it has been stuck past the threshold, so routine
                                # short waits do not perturb normal berth scheduling.
                                if getattr(v, "_mto_offload_wait_since", None) is None:
                                    v._mto_offload_wait_since = decision_t
                                _stuck_h = decision_t - v._mto_offload_wait_since
                                if _stuck_h >= MTO_OFFLOAD_STUCK_ESCALATION_HOURS:
                                    v.assigned_mother = _mto_queue_mother
                                    v.next_event_time = self.next_daylight_hourly_berth_check(
                                        _mto_queue_start, point="B")
                                    self.log_event(
                                        decision_t, v.name, "WAITING_BERTH_B",
                                        f"[MTO] Transient {v.cargo_bbl:,.0f} bbl stuck "
                                        f"{_stuck_h:.0f}h — queueing for {_mto_queue_mother} "
                                        f"(berth occupied; awaiting cast-off, recheck "
                                        f"{self.hours_to_dt(v.next_event_time).strftime('%Y-%m-%d %H:%M')})",
                                        voyage_num=v.current_voyage, mother=_mto_queue_mother,
                                    )
                                    continue
                            # No qualifying mother yet — recheck every TIME_STEP_HOURS
                            # during daylight so we catch returning mothers immediately
                            # rather than waiting up to an hour.
                            _wall_now = (decision_t + SIM_HOUR_OFFSET) % 24
                            if DAYLIGHT_START <= _wall_now < DAYLIGHT_END:
                                _next_mto = decision_t + TIME_STEP_HOURS
                            else:
                                _next_mto = self.next_daylight_hourly_berth_check(decision_t, point="B")
                            v.next_event_time = _next_mto
                            continue

                    _, candidates = self.point_b_candidate_slots(v, decision_t)
                    if not candidates:
                        next_recheck = self.next_daylight_hourly_berth_check(decision_t, point="B")
                        v.next_event_time = next_recheck
                        self.log_event(
                            decision_t,
                            v.name,
                            "WAITING_MOTHER_CAPACITY",
                            f"No Point B mother currently feasible; rescan in 30 min at "
                            f"{self.hours_to_dt(next_recheck).strftime('%Y-%m-%d %H:%M')}",
                            voyage_num=v.current_voyage,
                        )
                        continue

                    day_key = self.point_b_calendar_day_key(decision_t)
                    if STARTUP_DAY_DISABLE_POINT_B_PRIORITY and day_key == 0:
                        nominated_mother = STARTUP_DAY_POINT_B_MANUAL_NOMINATIONS.get(v.name)
                        if nominated_mother not in {m for _, _, m in candidates}:
                            next_recheck = self.next_daylight_hourly_berth_check(decision_t, point="B")
                            v.next_event_time = next_recheck
                            self.log_event(
                                decision_t,
                                v.name,
                                "WAITING_MOTHER_CAPACITY",
                                f"Startup-day manual nomination '{nominated_mother}' not feasible yet; reassess at "
                                f"{self.hours_to_dt(next_recheck).strftime('%Y-%m-%d %H:%M')}",
                                voyage_num=v.current_voyage,
                            )
                            continue
                        selected_mother = nominated_mother
                        selected = next((x for x in candidates if x[2] == selected_mother), None)
                    else:
                        # ── Daughter discharge point override (hourly reassessment) ──
                        # ZeeZee is unaffected — she never enters WAITING_BERTH_B
                        # via this path (handled entirely by _run_zeezee).
                        _ddo_mother, _ddo_disc_date = self._resolve_discharge_override(
                            v.name, getattr(v, "voyage_code", ""), decision_t)
                        _candidate_mothers = {m for _, _, m in candidates}
                        if _ddo_mother:
                            # ── Still holding for target date? ─────────────────
                            if not self._discharge_override_date_reached(_ddo_disc_date, decision_t):
                                from datetime import datetime as _ddt
                                _disc_dt = _ddt.fromisoformat(_ddo_disc_date).replace(hour=8, minute=0)
                                _hold_until = (_disc_dt - _SIM_EPOCH).total_seconds() / 3600.0
                                _hold_until = max(_hold_until, decision_t)
                                v.next_event_time = _hold_until
                                self.log_event(
                                    decision_t, v.name, "WAITING_BERTH_B",
                                    f"Discharge override [{v.voyage_code}]: holding at BIA until "
                                    f"{_ddo_disc_date} to discharge to {_ddo_mother}",
                                    voyage_num=v.current_voyage, mother=_ddo_mother,
                                )
                                continue
                            # ── Date reached — displace incumbent and berth ───
                            if _ddo_mother in _candidate_mothers:
                                self._displace_incumbent_at_mother(_ddo_mother, decision_t)
                                # Recompute candidates after displacement
                                _, candidates = self.point_b_candidate_slots(v, decision_t)
                                _candidate_mothers = {m for _, _, m in candidates}
                                if _ddo_mother not in _candidate_mothers:
                                    # Mother not at BIA (export/unavailable) — keep waiting
                                    next_recheck = self.next_daylight_hourly_berth_check(decision_t, point="B")
                                    v.next_event_time = next_recheck
                                    self.log_event(
                                        decision_t, v.name, "WAITING_BERTH_B",
                                        f"Discharge override [{v.voyage_code}]: {_ddo_mother} not at BIA; "
                                        f"reassessing at {self.hours_to_dt(next_recheck).strftime('%Y-%m-%d %H:%M')}",
                                        voyage_num=v.current_voyage, mother=_ddo_mother,
                                    )
                                    continue
                                selected_mother = _ddo_mother
                                selected = next(
                                    (x for x in candidates if x[2] == _ddo_mother),
                                    (decision_t, decision_t, _ddo_mother),
                                )
                                # Lock deferred to pump-start
                                if _ddo_mother != v.assigned_mother:
                                    self.log_event(
                                        decision_t, v.name, "MOTHER_PRIORITY_ASSIGNMENT",
                                        f"Discharge override [{v.voyage_code}]: forced to "
                                        f"{_ddo_mother} on {_ddo_disc_date} — incumbent displaced",
                                        voyage_num=v.current_voyage, mother=_ddo_mother,
                                    )
                            else:
                                # Mother not available yet — keep waiting
                                next_recheck = self.next_daylight_hourly_berth_check(decision_t, point="B")
                                v.next_event_time = next_recheck
                                self.log_event(
                                    decision_t, v.name, "WAITING_BERTH_B",
                                    f"Discharge override [{v.voyage_code}]: awaiting {_ddo_mother} — "
                                    f"not currently feasible; next rescan "
                                    f"{self.hours_to_dt(next_recheck).strftime('%Y-%m-%d %H:%M')}",
                                    voyage_num=v.current_voyage, mother=_ddo_mother,
                                )
                                continue
                        else:
                            selected_meta, _ = self.select_point_b_mother(
                                v,
                                decision_t,
                                day_key,
                                candidates,
                            )
                            selected_mother = selected_meta["mother"]
                            selected = (
                                selected_meta["start"],
                                selected_meta["berth_t"],
                                selected_meta["mother"],
                            )

                    start, berth_t, selected_mother = selected
                    _prev_mother = v.assigned_mother
                    _mother_changed = selected_mother != _prev_mother and _prev_mother in MOTHER_NAMES
                    if _mother_changed:
                        self.log_event(
                            decision_t,
                            v.name,
                            "MOTHER_PRIORITY_ASSIGNMENT",
                            f"Point B rescan reallocated: {_prev_mother} → {selected_mother} "
                            f"(berth freed — earlier slot available)",
                            voyage_num=v.current_voyage,
                            mother=selected_mother,
                        )
                    v.assigned_mother = selected_mother

                    if start > decision_t + 0.01:
                        next_recheck = self.next_daylight_hourly_berth_check(decision_t, point="B")
                        v.next_event_time = next_recheck
                        # Only log when mother changed or this is the first check
                        # (half-step scanning generates too many identical log entries)
                        _last_log_start = getattr(v, "_wb_last_logged_start", None)
                        _last_log_mother = getattr(v, "_wb_last_logged_mother", None)
                        if _mother_changed or _last_log_start != start or _last_log_mother != selected_mother:
                            self.log_event(
                                decision_t,
                                v.name,
                                "WAITING_BERTH_B",
                                f"Awaiting berth at {selected_mother}; earliest {self.hours_to_dt(start).strftime('%Y-%m-%d %H:%M')}, "
                                f"next rescan {self.hours_to_dt(next_recheck).strftime('%Y-%m-%d %H:%M')}",
                                voyage_num=v.current_voyage,
                                mother=selected_mother,
                            )
                            v._wb_last_logged_start  = start
                            v._wb_last_logged_mother = selected_mother
                        continue

                    # ── Yield a primary berth to a higher-priority MTO transient ────
                    # Applies at EVERY primary (Bryanston AND GreenEagle): a smaller
                    # daughter must not self-claim a primary berth the instant it
                    # frees ahead of a large MTO consolidation that is assigned to
                    # that mother, ready, fits the headroom, and has waited at least
                    # as long.  The guard also bumps the transient's next_event_time
                    # to NOW so it re-evaluates and claims the freed berth this tick
                    # (it does not depend on the Bryanston-only serial caller), so it
                    # is safe at GreenEagle too.  Restricting it to Bryanston was the
                    # gap that let Laphroaig's 133k MTO offload (LAP-004A) be
                    # leapfrogged at GreenEagle by a daily rotation of smaller
                    # daughters and sit laden ~11 days.
                    if (selected_mother == MOTHER_PRIMARY_NAME
                            and getattr(v, "_mto_transient_since_day", None) is None):
                        _bry_hr = max(0.0, self.mother_capacity_bbl(selected_mother)
                                      - self.mother_bbl.get(selected_mother, 0.0))
                        _v_wait = (getattr(v, "arrival_at_b", None)
                                   or getattr(v, "_waiting_bia_since", None) or decision_t)
                        _mto_ahead = None
                        for _mt in self.vessels:
                            if _mt is v or getattr(_mt, "_mto_transient_since_day", None) is None:
                                continue
                            if _mt.assigned_mother != selected_mother:
                                continue
                            if _mt.status not in {"WAITING_BERTH_B", "WAITING_MOTHER_CAPACITY"}:
                                continue
                            if _mt.cargo_bbl <= 0 or _mt.cargo_bbl > _bry_hr + 1e-6:
                                continue
                            if decision_t < getattr(_mt, "_mto_berth_free_at", 0.0):
                                continue   # transient's inbound transfer not yet complete
                            _mt_wait = (getattr(_mt, "arrival_at_b", None)
                                        or getattr(_mt, "_waiting_bia_since", None) or decision_t)
                            if _mt_wait <= _v_wait + 1e-6:
                                _mto_ahead = _mt
                                break
                        if _mto_ahead is not None:
                            _mto_ahead.next_event_time = min(
                                getattr(_mto_ahead, "next_event_time", decision_t) or decision_t,
                                decision_t)
                            next_recheck = self.next_daylight_hourly_berth_check(decision_t, point="B")
                            v.next_event_time = next_recheck
                            self.log_event(
                                decision_t, v.name, "WAITING_BERTH_B",
                                f"Yielding {selected_mother} berth to committed MTO transient "
                                f"{_mto_ahead.name} ({_mto_ahead.cargo_bbl:,.0f} bbl, waiting longer) — "
                                f"next rescan {self.hours_to_dt(next_recheck).strftime('%Y-%m-%d %H:%M')}",
                                voyage_num=v.current_voyage, mother=selected_mother,
                            )
                            continue

                    # ── Issue-4 headroom reservation (both primaries) ───────────
                    # A large MTO consolidation committed to this mother can be
                    # starved not by losing the berth race but by HEADROOM erosion:
                    # while it waits, a rotation of smaller daughters berths the
                    # mother and fills it, so by the time the berth frees the
                    # transient no longer fits and is bumped to the other mother —
                    # repeatedly, for 1–2+ weeks (Watson/Sherlock 147k).  Unlike the
                    # Bryanston-only yield above (which only fires when the transient
                    # already fits *now*), this guard protects the transient's future
                    # slot: if berthing this smaller daughter would drop the mother's
                    # headroom below a waiting committed transient's cargo, the
                    # daughter must not consume that headroom here.  It defers — its
                    # own next rescan will route it to the other primary (or it waits
                    # briefly) — leaving room for the transient to berth via its
                    # normal handler.  No berth hand-off is required, so there is no
                    # deadlock (the failure mode of generalising the yield/serial
                    # caller to GreenEagle).  Applies only when the OTHER primary can
                    # take this daughter, so we never freeze a daughter that has
                    # nowhere else to go.
                    if (selected_mother in (MOTHER_PRIMARY_NAME, MOTHER_SECONDARY_NAME)
                            and getattr(v, "_mto_transient_since_day", None) is None
                            and v.cargo_bbl > 0):
                        _sel_hr = max(0.0, self.mother_capacity_bbl(selected_mother)
                                      - self.mother_bbl.get(selected_mother, 0.0))
                        _hr_after = _sel_hr - v.cargo_bbl
                        _blocking_transient = None
                        for _mt in self.vessels:
                            if _mt is v or getattr(_mt, "_mto_transient_since_day", None) is None:
                                continue
                            if _mt.assigned_mother != selected_mother:
                                continue
                            if _mt.status not in {"WAITING_BERTH_B", "WAITING_MOTHER_CAPACITY"}:
                                continue
                            if _mt.cargo_bbl <= 0:
                                continue
                            if decision_t < getattr(_mt, "_mto_berth_free_at", 0.0):
                                continue   # transient's inbound transfer not complete
                            # The transient currently fits, but would NOT after this
                            # daughter discharges → this daughter is eroding its slot.
                            if (_mt.cargo_bbl <= _sel_hr + 1e-6
                                    and _mt.cargo_bbl > _hr_after + 1e-6):
                                _blocking_transient = _mt
                                break
                        if _blocking_transient is not None:
                            # Only defer if the OTHER primary can physically take this
                            # daughter now (at BIA, not export-busy, fits) — otherwise
                            # let the daughter proceed rather than strand it.
                            _other = (MOTHER_SECONDARY_NAME
                                      if selected_mother == MOTHER_PRIMARY_NAME
                                      else MOTHER_PRIMARY_NAME)
                            _other_ok = (
                                self.mother_is_at_point_b(_other, decision_t)
                                and self.export_state.get(_other) not in EXPORT_BUSY_STATES
                                and (self.mother_capacity_bbl(_other)
                                     - self.mother_bbl.get(_other, 0.0)) >= v.cargo_bbl - 1e-6
                            )
                            if _other_ok:
                                # Nudge the transient to act now and send this daughter
                                # to the other primary on its next rescan.
                                _blocking_transient.next_event_time = min(
                                    getattr(_blocking_transient, "next_event_time", decision_t)
                                    or decision_t, decision_t)
                                v.assigned_mother = _other
                                _nr = self.next_daylight_hourly_berth_check(decision_t, point="B")
                                v.next_event_time = _nr
                                self.log_event(
                                    decision_t, v.name, "WAITING_BERTH_B",
                                    f"Reserving {selected_mother} headroom for committed MTO "
                                    f"transient {_blocking_transient.name} "
                                    f"({_blocking_transient.cargo_bbl:,.0f} bbl) — rerouting to "
                                    f"{_other}; next rescan "
                                    f"{self.hours_to_dt(_nr).strftime('%Y-%m-%d %H:%M')}",
                                    voyage_num=v.current_voyage, mother=selected_mother,
                                )
                                continue

                    v.status = "BERTHING_B"
                    _disch_rate_4 = VESSEL_DISCHARGE_RATE_BPH.get(v.name)
                    _disch_hrs_4 = (v.cargo_bbl / _disch_rate_4) if _disch_rate_4 else DISCHARGE_HOURS
                    _berth_delay_4 = self._berthing_delay()
                    _hose_hrs_4    = self._hose_connect_hours()
                    _pump_end_4   = start + _berth_delay_4 + _hose_hrs_4 + _disch_hrs_4
                    _discharge_end = _berth_free_at(_pump_end_4)
                    self.mother_berth_free_at[selected_mother] = max(
                        self.mother_berth_free_at[selected_mother], _discharge_end
                    )
                    v.next_event_time = start + _berth_delay_4
                    self.log_event(
                        start,
                        v.name,
                        "BERTHING_START_B",
                        f"Berthing at {selected_mother} ({_berth_delay_4*60:.0f} min procedure)",
                        voyage_num=v.current_voyage,
                        mother=selected_mother,
                    )

                elif v.status == "BERTHING_B":
                    if v.assigned_mother not in MOTHER_NAMES:
                        self.log_event(t, v.name, "WAITING_MOTHER_CAPACITY",
                                       "Blocked: no explicit mother assignment at Point B (fallback disabled)",
                                       voyage_num=v.current_voyage)
                        v.next_event_time = t + 0.5
                        continue
                    if not self.mother_is_at_point_b(v.assigned_mother, t):
                        _next_chk = self.next_daylight_hourly_berth_check(t, point="B")
                        v.status = "WAITING_BERTH_B"
                        v.next_event_time = _next_chk
                        self.log_event(
                            t,
                            v.name,
                            "WAITING_MOTHER_RETURN",
                            f"{v.assigned_mother} not at Point B; reassessing at {self.hours_to_dt(_next_chk).strftime('%Y-%m-%d %H:%M')}",
                            voyage_num=v.current_voyage,
                            mother=v.assigned_mother,
                        )
                        continue
                    # Gate: mothers require full return + fendering before berthing.
                    # mother_available_at = return_arrival + 2h fender.
                    _fender_at = self.mother_available_at.get(v.assigned_mother, 0.0)
                    if t < _fender_at:
                        _next_chk = self.next_daylight_hourly_berth_check(
                            _fender_at, point="B"
                        )
                        v.status = "WAITING_BERTH_B"
                        v.next_event_time = _next_chk
                        self.log_event(
                            t, v.name, "WAITING_MOTHER_RETURN",
                            f"{v.assigned_mother} fendering not yet complete; "
                            f"ready {self.hours_to_dt(_fender_at).strftime('%Y-%m-%d %H:%M')} — "
                            f"re-checking {self.hours_to_dt(_next_chk).strftime('%Y-%m-%d %H:%M')}",
                            voyage_num=v.current_voyage, mother=v.assigned_mother)
                        continue
                    # Gate: do not proceed while another vessel already occupies this berth
                    _berth_occupant = self.mother_berth_current_occupant(v.assigned_mother)
                    if _berth_occupant is not None and _berth_occupant.name != v.name:
                        _wait_until = max(_berth_occupant.next_event_time, t + 0.5)
                        _next_chk = self.next_daylight_hourly_berth_check(_wait_until, point="B")
                        v.status = "WAITING_BERTH_B"
                        v.next_event_time = _next_chk
                        self.log_event(t, v.name, "WAITING_BERTH_B",
                                       f"Berth at {v.assigned_mother} occupied by "
                                       f"{_berth_occupant.name} ({_berth_occupant.status}); "
                                       f"waiting until {self.hours_to_dt(_next_chk).strftime('%Y-%m-%d %H:%M')}",
                                       voyage_num=v.current_voyage, mother=v.assigned_mother)
                        continue
                    # Concurrent-occupancy guard: fire just before hose opens.
                    # If two actors have both reached BERTHING_B at this mother
                    # simultaneously (race condition), abort the smaller-volume vessel.
                    if self._concurrent_berth_guard(v.name, v.cargo_bbl,
                                                    v.assigned_mother, t):
                        v.status          = "WAITING_BERTH_B"
                        v.assigned_mother = None
                        v.next_event_time = self.next_daylight_hourly_berth_check(t, point="B")
                        continue
                    v.status = "HOSE_CONNECT_B"
                    _hose_h = self._hose_connect_hours()
                    v.next_event_time = t + _hose_h
                    selected_mother = v.assigned_mother
                    self.log_event(t, v.name, "HOSE_CONNECTION_START_B",
                                   f"Hose connection initiated at {selected_mother} ({_hose_h:.1f}h)",
                                   voyage_num=v.current_voyage)

                elif v.status == "HOSE_CONNECT_B":
                    if v.assigned_mother not in MOTHER_NAMES:
                        # No valid mother assignment — reset to WAITING_BERTH_B
                        # so the candidate-selection logic can assign one.
                        v.status = "WAITING_BERTH_B"
                        v.next_event_time = self.next_daylight_hourly_berth_check(t, point="B")
                        self.log_event(t, v.name, "WAITING_BERTH_B",
                                       "No mother assignment at HOSE_CONNECT_B — "
                                       "returning to WAITING_BERTH_B for reassignment",
                                       voyage_num=v.current_voyage)
                        continue
                    selected_mother = v.assigned_mother
                    if not self.mother_is_at_point_b(selected_mother, t):
                        next_recheck = self.next_daylight_hourly_berth_check(t, point="B")
                        v.status = "WAITING_BERTH_B"
                        v.next_event_time = next_recheck
                        self.log_event(
                            t,
                            v.name,
                            "WAITING_MOTHER_RETURN",
                            f"{selected_mother} not at Point B; reassessing at {self.hours_to_dt(next_recheck).strftime('%Y-%m-%d %H:%M')}",
                            voyage_num=v.current_voyage,
                            mother=selected_mother,
                        )
                        continue
                    _mother_cap = self.mother_capacity_bbl(selected_mother)
                    _mother_space = max(0.0, _mother_cap - self.mother_bbl[selected_mother])
                    _is_mto_vessel = getattr(v, "_is_mto_offload", False)

                    # ── Hard capacity ceiling — all primary mothers ───────────
                    # A vessel whose full cargo would push the mother above her
                    # rated capacity must be aborted immediately before the hose
                    # opens.  The committed-volume check in point_b_candidate_slots
                    # runs at scheduling time; by the time a vessel arrives and
                    # reaches HOSE_CONNECT_B the mother's actual stock may have
                    # changed (e.g. a concurrent discharge completed since the
                    # slot was booked).  This is the authoritative last-chance
                    # gate that enforces the physical ceiling at pump time.
                    #
                    # Scope: ALL mother vessels.
                    #
                    # Action when cargo > headroom:
                    #   • Abort — cast off and return to WAITING_BERTH_B with
                    #     assigned_mother cleared so the vessel is immediately
                    #     eligible for the next available mother with space.
                    #   • MTO transients are NOT exempt: an MTO vessel that
                    #     has accumulated more cargo than the mother can accept
                    #     must wait for the mother to export and return, or seek
                    #     the other primary mother.
                    if v.cargo_bbl > _mother_space:
                        cast_off_t = self.next_cast_off_window(t)
                        self.mother_berth_free_at[selected_mother] = cast_off_t + CAST_OFF_HOURS
                        v.status          = "CAST_OFF_B"
                        v.next_event_time = cast_off_t + CAST_OFF_HOURS
                        _abort_reason = (
                            f"{selected_mother} stock {self.mother_bbl[selected_mother]:,.0f} bbl "
                            f"+ {v.cargo_bbl:,.0f} bbl cargo = "
                            f"{self.mother_bbl[selected_mother] + v.cargo_bbl:,.0f} bbl — "
                            f"exceeds capacity {_mother_cap:,.0f} bbl "
                            f"(headroom {_mother_space:,.0f} bbl); "
                            f"aborting discharge and reassigning"
                        )
                        self.log_event(t, v.name, "MOTHER_CAPACITY_ABORT",
                                       _abort_reason,
                                       voyage_num=v.current_voyage,
                                       mother=selected_mother)
                        self.log_event(cast_off_t, v.name, "CAST_OFF_START_B",
                                       f"Cast-off from {selected_mother} "
                                       f"(capacity abort, {CAST_OFF_HOURS}h)",
                                       voyage_num=v.current_voyage,
                                       mother=selected_mother)
                        # Clear assignment so candidate-selection can find a
                        # better-fit mother (or WAITING_MOTHER_CAPACITY if none).
                        v.assigned_mother = None
                        continue

                    if _mother_space <= 0:
                        # Mother is completely full — wait and retry
                        self.log_event(t, v.name, "WAITING_MOTHER_CAPACITY",
                                       f"Cannot start discharge - {selected_mother} lacks space",
                                       voyage_num=v.current_voyage)
                        v.next_event_time = t + 6
                        continue

                    _actual_discharge = v.cargo_bbl   # always discharge full volume
                    # Blend vessel cargo API into mother vessel.
                    _vessel_api_val = self.vessel_api.get(v.name, 0.0)
                    self.mother_api[selected_mother] = self.blend_api(
                        self.mother_bbl[selected_mother], self.mother_api.get(selected_mother, 0.0),
                        _actual_discharge, _vessel_api_val)
                    self.mother_bbl[selected_mother] += _actual_discharge
                    v.cargo_bbl -= _actual_discharge
                    if v.cargo_bbl <= 0:
                        v.cargo_bbl = 0
                        self.vessel_api[v.name] = 0.0
                    v.status = "DISCHARGING"
                    _disch_rate = VESSEL_DISCHARGE_RATE_BPH.get(v.name)
                    _nominal_disch_hrs = (_actual_discharge / _disch_rate) if _disch_rate else DISCHARGE_HOURS
                    # Apply variability: discharge rate uncertainty + congestion
                    _n_waiting_b = sum(
                        1 for vv in self.vessels
                        if vv.status in {"WAITING_BERTH_B", "BERTHING_B", "HOSE_CONNECT_B"}
                        and vv is not v
                    )
                    _cong = _congestion_factor(_n_waiting_b)
                    _disch_hrs = (
                        _variability_sample(_nominal_disch_hrs, VARIABILITY_CV_DISCHARGE) * _cong
                    )
                    if hasattr(self, "_sim_stats"):
                        self._sim_stats.record("discharge", _nominal_disch_hrs, _disch_hrs)
                    # Lock the serial-discharge slot for this mother now that cargo
                    # is physically flowing.  Released at CAST_OFF_COMPLETE_B so
                    # the next vessel can start a fresh discharge on the same day.
                    self._point_b_register_mother_start(selected_mother, t)
                    # Lock berth and displace pre-pump incumbents.
                    # MTO transients get the exclusive-day rule; normal daughters
                    # get the physical-only lock (pump + cast-off duration).
                    _is_mto_discharge = getattr(v, "_is_mto_offload", False)
                    if _is_mto_discharge:
                        self._enforce_exclusive_day_at_mother(
                            selected_mother, t,
                            physical_end=_berth_free_at(t + _disch_hrs),
                        )
                    else:
                        self.mother_berth_free_at[selected_mother] = max(
                            self.mother_berth_free_at[selected_mother],
                            _berth_free_at(t + _disch_hrs),
                        )
                    v.next_event_time = t + _disch_hrs
                    # MTO transient offload: stamp "A" suffix on VoyageCode
                    # (e.g. AMY-000 → AMY-000A) so JMP can distinguish
                    # transient offloads from normal cargo deliveries.
                    _is_mto = getattr(v, "_is_mto_offload", False)
                    _log_vcode = v.current_voyage
                    if _is_mto:
                        _base_vcode = make_voyage_code(v.name, v.current_voyage)
                        v.voyage_code = _base_vcode + "A"
                    self.log_event(t, v.name, "DISCHARGE_START",
                                   f"{'[MTO offload] ' if _is_mto else ''}"
                                   f"Discharging {_actual_discharge:,} bbl @ {_vessel_api_val:.2f}° API | "
                                   f"{selected_mother}: {self.mother_bbl[selected_mother]:,.0f} bbl "
                                   f"(blended {self.mother_api[selected_mother]:.2f}° API)"
                                   + (f" | {v.cargo_bbl:,.0f} bbl residual remaining on vessel" if v.cargo_bbl > 0 else ""),
                                   voyage_num=_log_vcode,
                                   mother=selected_mother)

                elif v.status == "DISCHARGING":
                    if v.assigned_mother not in MOTHER_NAMES:
                        # No valid mother assignment — cast off and return to load
                        cast_off_t = self.next_cast_off_window(t)
                        v.status = "CAST_OFF_B"
                        v.next_event_time = cast_off_t + CAST_OFF_HOURS
                        self.log_event(t, v.name, "CAST_OFF_START_B",
                                       "No mother assignment at DISCHARGING — casting off",
                                       voyage_num=v.current_voyage)
                        continue
                    selected_mother = v.assigned_mother

                    # If vessel still has residual cargo after discharge cycle,
                    # determine correct action based on whether this is MTO.
                    if v.cargo_bbl > 0:
                        if getattr(v, "_is_mto_offload", False):
                            # Residual after MTO offload: unexpected, but clear it so
                            # the vessel can cast off cleanly and return to loading.
                            # (HOSE_CONNECT_B now always transfers full cargo so this
                            # path should not normally be reached.)
                            self.log_event(
                                t, v.name, "DISCHARGE_PARTIAL_COMPLETE",
                                f"[MTO] Residual {v.cargo_bbl:,.0f} bbl after MTO discharge "
                                f"— clearing and casting off.",
                                voyage_num=v.current_voyage,
                            )
                            v.cargo_bbl = 0
                            # Fall through to normal cast-off below
                        if True:   # unified cast-off regardless of MTO/non-MTO
                            # Non-MTO vessel with residual (edge case): cast off normally.
                            # This can occur when a mother fills up mid-pump on a
                            # normal cargo delivery. The vessel returns and the
                            # residual stays on board for the next BIA trip.
                            cast_off_t = self.next_cast_off_window(t)
                            v.status = "CAST_OFF_B"
                            v.next_event_time = cast_off_t + CAST_OFF_HOURS
                            self.log_event(
                                t, v.name, "DISCHARGE_PARTIAL_COMPLETE",
                                f"Mother filled: {v.cargo_bbl:,.0f} bbl residual retained on board",
                                voyage_num=v.current_voyage,
                            )
                            continue

                    v.cargo_bbl = 0
                    self.vessel_api[v.name] = 0.0
                    # Clear MTO offload flag and restore normal voyage code
                    if getattr(v, "_is_mto_offload", False):
                        v._is_mto_offload = False
                        v.voyage_code = make_voyage_code(v.name, v.current_voyage)
                    # Enforce daylight-only cast-off at BIA
                    cast_off_b_t = self.next_cast_off_window(t)
                    wait_co_b = cast_off_b_t - t
                    # Update berth lock to the EXACT cast-off completion time now that
                    # we know whether a nighttime wait applies. This supersedes the
                    # conservative estimate set at HOSE_CONNECT_B / WAITING_BERTH_B.
                    self.mother_berth_free_at[selected_mother] = max(
                        self.mother_berth_free_at[selected_mother],
                        _berth_free_at(t),  # t = pump complete; cast_off_b_t already computed
                    )
                    v.status = "CAST_OFF_B"
                    v.next_event_time = cast_off_b_t + CAST_OFF_HOURS
                    self.log_event(t, v.name, "DISCHARGE_COMPLETE",
                                   f"{selected_mother}: {self.mother_bbl[selected_mother]:,.0f} bbl | "
                                   f"Cast-off scheduled {self.hours_to_dt(cast_off_b_t).strftime('%H:%M')} (wait {wait_co_b:.1f}h)",
                                   voyage_num=v.current_voyage)
                    if wait_co_b > 0:
                        self.log_event(t, v.name, "WAITING_CAST_OFF",
                                       f"Night restriction — cast-off from {selected_mother} at "
                                       f"{self.hours_to_dt(cast_off_b_t).strftime('%Y-%m-%d %H:%M')}",
                                       voyage_num=v.current_voyage)
                    self.log_event(cast_off_b_t, v.name, "CAST_OFF_START_B",
                                   f"Cast-off from {selected_mother} ({CAST_OFF_HOURS}h)",
                                   voyage_num=v.current_voyage)

                elif v.status == "CAST_OFF_B":
                    # MTO discharger: vessel cast off from anchor after transferring
                    # its cargo to the transient — it was never physically berthed,
                    # so assigned_mother may be None. Skip the mother check and go
                    # straight to WAITING_RETURN_STOCK.
                    if v.assigned_mother not in MOTHER_NAMES:
                        if v.cargo_bbl == 0:
                            # Casting off from anchor after MTO transfer — no mother
                            self.log_event(t, v.name, "CAST_OFF_COMPLETE_B",
                                           "Cast-off complete (MTO discharger — no berth occupied)",
                                           voyage_num=v.current_voyage)
                            # Activate deferred dormancy if pending
                            if getattr(v, "_dormancy_pending", False):
                                v._dormancy_pending = False
                                _end_h = getattr(v, "_dormancy_end_hour", None)
                                if _end_h is not None:
                                    v.resumption_hour = _end_h
                                v.resumption_hold_logged = False
                                v.status = "IDLE_A"
                                v.assigned_mother = None
                                v.next_event_time = t
                                self.log_event(
                                    t, v.name, "DORMANCY_ACTIVATED",
                                    f"Deferred dormancy active after MTO transfer — idle until "
                                    f"{self.hours_to_dt(v.resumption_hour).strftime('%Y-%m-%d %H:%M') if v.resumption_hour else 'indefinite'}",
                                    voyage_num=v.current_voyage,
                                )
                            else:
                                v.status = "WAITING_RETURN_STOCK"
                                v.next_event_time = t
                            continue
                        # No mother but still has cargo — send to WAITING_BERTH_B for reassignment
                        v.status = "WAITING_BERTH_B"
                        v.next_event_time = self.next_daylight_hourly_berth_check(t, point="B")
                        self.log_event(t, v.name, "WAITING_BERTH_B",
                                       "No mother assignment at CAST_OFF_B — returning to WAITING_BERTH_B",
                                       voyage_num=v.current_voyage)
                        continue
                    selected_mother = v.assigned_mother
                    if v.cargo_bbl > 0:
                        # Discharge was aborted (GreenEagle capacity exceeded) — vessel still
                        # has cargo; clear mother assignment and re-queue for a different mother
                        v.assigned_mother = None
                        v.status = "WAITING_BERTH_B"
                        v.next_event_time = t
                        self.log_event(t, v.name, "CAST_OFF_COMPLETE_B",
                                       f"Cast-off from {selected_mother} complete (abort) — "
                                       f"{v.cargo_bbl:,} bbl still aboard; re-queuing for alternative mother",
                                       voyage_num=v.current_voyage, mother=selected_mother)
                        continue
                    # Only mark export_ready when the mother has reached its
                    # export trigger.  Setting it unconditionally after every
                    # cast-off was causing Bryanston to start an export cycle
                    # after receiving just one daughter cargo (85k), far below
                    # the 465k trigger.
                    # Record this cast-off time for the export intake buffer.
                    # The export DOC cannot fire until EXPORT_INTAKE_BUFFER_HOURS
                    # after this timestamp regardless of export_ready state.
                    self.export_intake_last_cast_off[selected_mother] = t
                    _trigger = self.mother_export_trigger_bbl(selected_mother)
                    if self.mother_bbl[selected_mother] >= _trigger:
                        if not self.export_ready[selected_mother]:
                            self.export_ready_since[selected_mother] = t
                        self.export_ready[selected_mother] = True
                    # Release the serial-discharge lock so the next waiting
                    # vessel can berth this mother on the same calendar day
                    # — enabling two or more serial discharges per day.
                    self._point_b_deregister_mother(selected_mother, t)
                    self.log_event(t, v.name, "CAST_OFF_COMPLETE_B",
                                   "Cast-off from mother complete; returning to storage",
                                   voyage_num=v.current_voyage)

                    # MTO transient re-anchor: if this cast-off was triggered by
                    # an abort (insufficient mother space), the vessel still holds
                    # its consolidated cargo and must return to WAITING_BERTH_B to
                    # seek a qualifying mother — not sail back to storage.
                    #
                    # The cargo-remaining check is essential: after a SUCCESSFUL MTO
                    # offload the vessel is empty (cargo_bbl == 0).  _is_mto_offload is
                    # cleared at DISCHARGE_COMPLETE but _mto_transient_since_day was not,
                    # so without this guard an emptied transient re-anchors with 0 bbl
                    # and loops forever — repeatedly berthing a mother, "discharging
                    # 0 bbl", casting off and re-anchoring (e.g. Watson WTS-003 churning
                    # at GreenEagle on 24–25 Jun after delivering 116k to Bryanston on
                    # the 22nd).  An empty transient has finished its MTO role and must
                    # sail back to storage to reload, so clear the transient flags here.
                    if (getattr(v, "_mto_transient_since_day", None) is not None
                            and v.cargo_bbl > 0):
                        v.status = "WAITING_BERTH_B"
                        v.next_event_time = self.next_daylight_hourly_berth_check(t, point="B")
                        self.log_event(
                            t, v.name, "MTO_REANCHOR",
                            f"[MTO] Re-anchoring at BIA with {v.cargo_bbl:,.0f} bbl on board — "
                            f"awaiting a primary mother with sufficient space for full cargo",
                            voyage_num=v.current_voyage,
                        )
                        continue
                    # MTO offload complete (cargo fully delivered): drop the transient
                    # flags so the now-empty vessel is treated as a normal returning
                    # daughter and sails back to storage to reload.
                    if getattr(v, "_mto_transient_since_day", None) is not None:
                        v._mto_transient_since_day = None
                        v._mto_offload_wait_since  = None

                    v.status = "WAITING_RETURN_STOCK"
                    v.next_event_time = t

                    # ── Deferred dormancy activation ──────────────────────────
                    # If dormancy was deferred (vessel had cargo when window opened),
                    # activate it now — the cargo has been delivered and the vessel
                    # is empty.  Override the WAITING_RETURN_STOCK state.
                    if getattr(v, "_dormancy_pending", False):
                        v._dormancy_pending = False
                        _end_h = getattr(v, "_dormancy_end_hour", None)
                        if _end_h is not None:
                            v.resumption_hour = _end_h
                        v.resumption_hold_logged = False
                        v.status           = "IDLE_A"
                        v.cargo_bbl        = 0
                        self.vessel_api[v.name] = 0.0
                        v.assigned_storage = None
                        v.assigned_load_hours = None
                        v.assigned_mother  = None
                        v.target_point     = "A"
                        v.next_event_time  = t
                        self.log_event(
                            t, v.name, "DORMANCY_ACTIVATED",
                            f"Deferred dormancy now active — cargo discharged, vessel idle until "
                            f"{self.hours_to_dt(v.resumption_hour).strftime('%Y-%m-%d %H:%M') if v.resumption_hour else 'indefinite'} "
                            f"| priority storage on resumption: {v.resumption_storage}",
                            voyage_num=v.current_voyage,
                        )

                elif v.status == "WAITING_RETURN_STOCK":
                    selected_mother = v.assigned_mother if v.assigned_mother in MOTHER_NAMES else "UNASSIGNED"
                    # If this is a point_f vessel not currently assigned to Ibom,
                    # force it back to SanBarth (Point A) for its SanBarth cycle.
                    # If swap is pending for this vessel it will sail to Ibom
                    # directly — skip storage allocation entirely for that case.
                    # Otherwise, if in SanBarth support mode restrict to Point A.
                    if self.point_f_swap_pending_for == v.name:
                        # Will be intercepted below — just need a dummy allocation
                        # to satisfy the flow; use SanBarth as placeholder.
                        target_storage    = "SanBarth"
                        required_stock    = 0
                        threshold_by_storage = {}
                    else:
                        _pf_sanbarth_mode = (
                            v.name in self.point_f_vessels
                            and (self.point_f_active_loader != v.name
                                 or v.target_point != "F")
                        )
                        if _pf_sanbarth_mode:
                            v.target_point = "A"
                        _pt_restrict = "A" if _pf_sanbarth_mode else None
                        target_storage, required_stock, threshold_by_storage = self.return_allocation_candidate(v.cargo_capacity, v.name, point_restrict=_pt_restrict)
                    if target_storage is None:
                        if not threshold_by_storage:
                            self.log_event(
                                t,
                                v.name,
                                "WAITING_RETURN_STOCK",
                                "Waiting at Point B for permitted return storage allocation",
                                voyage_num=v.current_voyage,
                            )
                            v.next_event_time = t + 0.5
                            continue
                        storage_levels = ", ".join(
                            f"{name}: {self.storage_bbl[name]:,.0f} bbl "
                            f"(need {threshold_by_storage[name]:,.0f})"
                            for name in threshold_by_storage
                        )
                        self.log_event(
                            t,
                            v.name,
                            "WAITING_RETURN_STOCK",
                            f"Waiting at Point B for return allocation stock "
                            f"(storage-specific loading thresholds): {storage_levels}",
                            voyage_num=v.current_voyage,
                        )
                        v.next_event_time = t + 0.5
                        continue

                    # ── Position-aware D/E override for vessels departing BIA ─
                    # A vessel returning from Point B is always at area "B".
                    # If return_allocation_candidate picked D or E, verify that
                    # the storage will actually be in genuine need by ETA; if
                    # not, redirect to the best A/C storage instead so the vessel
                    # promotes back-to-back loading at high-production points.
                    _tgt_area = STORAGE_POINT.get(target_storage, "A")
                    _pf_san = "_pf_sanbarth_mode" in dir() and _pf_sanbarth_mode
                    if _tgt_area in ("D", "E") and not _pf_san:
                        _eta_de  = self.area_travel_hours("B", _tgt_area)
                        _proj_de = self.projected_stock_at(target_storage, _eta_de)
                        _crit_de = STORAGE_CRITICAL_THRESHOLD_BY_NAME[target_storage]
                        _de_ok   = _proj_de <= _crit_de + (
                            STORAGE_PRODUCTION_RATE_BY_NAME.get(target_storage, 0.0)
                            * SPREAD_DE_URGENCY_HORIZON
                        )
                        if not _de_ok:
                            # D/E not urgent — redirect to best A/C storage
                            _ac_cands = [
                                nm for nm in STORAGE_NAMES
                                if STORAGE_POINT.get(nm) in ("A", "C")
                                and self.storage_allowed_for_vessel(nm, v.name)
                            ]
                            if _ac_cands:
                                _ac_eligible = [
                                    nm for nm in _ac_cands
                                    if self.storage_bbl[nm] >= self.loading_start_threshold(
                                        nm, self.effective_load_cap(v.name, nm))
                                ]
                                _ac_pool = _ac_eligible if _ac_eligible else _ac_cands
                                def _ac_rank(nm):
                                    stk = self.storage_bbl[nm]
                                    crit = STORAGE_CRITICAL_THRESHOLD_BY_NAME[nm]
                                    unsafe = 0 if stk >= crit else 1
                                    raw_g = abs(stk - crit)
                                    eff_g = raw_g * (1.0 - self.production_rate_bias_factor(nm)) if raw_g <= DISPATCH_BIAS_FORECAST_BBL else raw_g
                                    return (unsafe, eff_g, -stk, nm)
                                _ac_best = min(_ac_pool, key=_ac_rank)
                                self.log_event(
                                    t, v.name, "RETURN_POINT_ALLOCATED",
                                    f"D/E spread suppressed (no urgency at {target_storage} by ETA "
                                    f"{_eta_de:.0f}h, proj {_proj_de:,.0f} bbl): "
                                    f"redirecting to {_ac_best} for back-to-back A/C loading",
                                    voyage_num=v.current_voyage,
                                )
                                target_storage = _ac_best
                                required_stock = self.loading_start_threshold(
                                    _ac_best, self.effective_load_cap(v.name, _ac_best))

                    # Point F swap pending: vessel must sail BIA → Ibom directly
                    if self.point_f_swap_pending_for == v.name:
                        sail_t = self.next_daylight_sail(t)
                        wait   = sail_t - t
                        v.target_point     = "F"
                        v.status           = "SAILING_B_TO_F"
                        v.next_event_time  = sail_t + SAIL_HOURS_B_TO_F
                        self.log_event(t, v.name, "SAILING_B_TO_F_START",
                                       f"Ibom swap ordered — sailing BIA → Ibom "
                                       f"({SAIL_HOURS_B_TO_F}h, depart "
                                       f"{self.hours_to_dt(sail_t).strftime('%H:%M')})",
                                       voyage_num=v.current_voyage)
                        if wait > 0:
                            self.log_event(t, v.name, "WAITING_DAYLIGHT",
                                           f"Daylight window opens at "
                                           f"{self.hours_to_dt(sail_t).strftime('%Y-%m-%d %H:%M')}",
                                           voyage_num=v.current_voyage)
                        continue

                    v.target_point = STORAGE_POINT.get(target_storage, "A")
                    v.assigned_storage = target_storage
                    # BIA -> Fairway Buoy (A/C return leg 1) is daylight-only,
                    # while other return routes keep tidal gating.
                    if v.target_point in ("A", "C"):
                        sail_t = self.next_daylight_sail(t)
                    else:
                        sail_t = self.next_tidal_sail(t)
                    wait   = sail_t - t
                    self.log_event(t, v.name, "RETURN_POINT_ALLOCATED",
                                   f"Allocated to Point {v.target_point} on departure from {selected_mother} | "
                                   f"Designated return storage: {target_storage} "
                                   f"({self.storage_bbl[target_storage]:,.0f} bbl, "
                                   f"loading-start threshold {required_stock:,.0f} bbl, "
                                   f"critical {STORAGE_CRITICAL_THRESHOLD_BY_NAME[target_storage]:,.0f} bbl)",
                                   voyage_num=v.current_voyage)
                    if wait > 0:
                        if v.target_point in ("A", "C"):
                            self.log_event(t, v.name, "WAITING_DAYLIGHT",
                                           f"Daylight window opens at "
                                           f"{self.hours_to_dt(sail_t).strftime('%Y-%m-%d %H:%M')}",
                                           voyage_num=v.current_voyage)
                        else:
                            self.log_event(t, v.name, "WAITING_TIDAL",
                                           f"Daylight/tide window opens at "
                                           f"{self.hours_to_dt(sail_t).strftime('%Y-%m-%d %H:%M')} "
                                           f"({self.tidal_period_label(sail_t)}; available today: "
                                           f"{self.tidal_periods_available_for_day(sail_t)})",
                                           voyage_num=v.current_voyage)
                    if v.target_point == "D":
                        # 4-leg return: BIA → BW (1.5h) → cross BW (0.5h, tidal) →
                        #               CH (1h, tidal) → Point D (3h, tidal)
                        v.status = "SAILING_B_TO_BW_IN"
                        v.next_event_time = sail_t + _sail_leg(SAIL_HOURS_B_TO_BW, self)
                    elif v.target_point in ("E", "G"):
                        # Starturn (E) / PGM (G) — short direct return 3h, no breakwater
                        v.status = "SAILING_BA"
                        v.next_event_time = sail_t + 3
                    else:
                        # A/C return: BIA → FWY (2h) → BW (2h) → cross BW (0.5h, tidal) → A/C (1.5h)
                        v.status = "SAILING_B_TO_FWY"
                        v.next_event_time = sail_t + _sail_leg(SAIL_HOURS_B_TO_FWY, self)

                elif v.status == "SAILING_B_TO_FWY":
                    # BIA → Fairway Buoy (2h, A/C return leg 1)
                    arrival = t
                    self.log_event(arrival, v.name, "ARRIVED_FAIRWAY_RETURN",
                                   "Reached Fairway Buoy returning (2h from BIA)",
                                   voyage_num=v.current_voyage)
                    # Leg 2: Fairway Buoy → Breakwater (2h, daylight)
                    depart_fwy = self.next_daylight_sail(arrival)
                    wait_fwy   = depart_fwy - arrival
                    if wait_fwy > 0:
                        self.log_event(arrival, v.name, "WAITING_FAIRWAY",
                                       f"Holding at Fairway Buoy (return) until daylight at "
                                       f"{self.hours_to_dt(depart_fwy).strftime('%Y-%m-%d %H:%M')}",
                                       voyage_num=v.current_voyage)
                    v.status = "SAILING_FWY_TO_BW"
                    v.next_event_time = depart_fwy + _sail_leg(SAIL_HOURS_FWY_TO_BW, self)

                elif v.status == "SAILING_FWY_TO_BW":
                    # Arrived at Breakwater inbound (A/C return leg 2) — tidal gate to cross
                    arrival = t
                    self.log_event(arrival, v.name, "ARRIVED_BREAKWATER_AC_IN",
                                   "Reached breakwater inbound (2h from Fairway Buoy)",
                                   voyage_num=v.current_voyage)
                    depart_bw = self.next_tidal_sail(arrival)
                    wait_bw   = depart_bw - arrival
                    if wait_bw > 0:
                        self.log_event(arrival, v.name, "WAITING_TIDAL",
                                       f"Breakwater inbound: waiting for daylight/tide at "
                                       f"{self.hours_to_dt(depart_bw).strftime('%Y-%m-%d %H:%M')} "
                                       f"({self.tidal_period_label(depart_bw)})",
                                       voyage_num=v.current_voyage)
                    v.status = "SAILING_CROSS_BW_IN_AC"
                    v.next_event_time = depart_bw + SAIL_HOURS_CROSS_BW_AC

                elif v.status == "SAILING_CROSS_BW_IN_AC":
                    # Crossed breakwater inbound — final run to Point A/C (1.5h, no gate)
                    arrival = t
                    self.log_event(arrival, v.name, "CROSSED_BREAKWATER_AC_IN",
                                   "Crossed breakwater inbound (0.5h) — running to Point A/C (1.5h)",
                                   voyage_num=v.current_voyage)
                    self.assign_ac_point_post_breakwater(v, arrival)
                    self.trigger_ac_post_breakwater_reassessment(arrival, trigger_vessel=v.name)
                    v.status = "SAILING_BA"
                    v.next_event_time = arrival + _sail_leg(SAIL_HOURS_BW_TO_A, self)

                elif v.status == "SAILING_B_TO_BW_IN":
                    # Arrived at clear breakwater (inbound from BIA, 1.5h)
                    arrival = t
                    self.log_event(arrival, v.name, "ARRIVED_BREAKWATER_IN",
                                   "Reached clear breakwater inbound (1.5h from BIA)",
                                   voyage_num=v.current_voyage)
                    depart_bw = self.next_tidal_sail(arrival)
                    wait_bw   = depart_bw - arrival
                    if wait_bw > 0:
                        self.log_event(arrival, v.name, "WAITING_TIDAL",
                                       f"Breakwater inbound: waiting for daylight/tide at "
                                       f"{self.hours_to_dt(depart_bw).strftime('%Y-%m-%d %H:%M')} "
                                       f"({self.tidal_period_label(depart_bw)})",
                                       voyage_num=v.current_voyage)
                    v.status = "SAILING_CROSS_BW_IN"
                    v.next_event_time = depart_bw + _sail_leg(SAIL_HOURS_CROSS_BW, self)

                elif v.status == "SAILING_CROSS_BW_IN":
                    # Crossed breakwater inbound — next leg to Cawthorne Channel (tidal)
                    arrival = t
                    self.log_event(arrival, v.name, "CROSSED_BREAKWATER_IN",
                                   "Crossed breakwater inbound (0.5h) — heading to Cawthorne Channel",
                                   voyage_num=v.current_voyage)
                    depart_bw_ch = self.next_tidal_sail(arrival)
                    wait_bw_ch   = depart_bw_ch - arrival
                    if wait_bw_ch > 0:
                        self.log_event(arrival, v.name, "WAITING_TIDAL",
                                       f"Post-breakwater: waiting for daylight/tide at "
                                       f"{self.hours_to_dt(depart_bw_ch).strftime('%Y-%m-%d %H:%M')} "
                                       f"({self.tidal_period_label(depart_bw_ch)})",
                                       voyage_num=v.current_voyage)
                    v.status = "SAILING_BW_TO_CH_IN"
                    v.next_event_time = depart_bw_ch + _sail_leg(SAIL_HOURS_BW_TO_CH_IN, self)

                elif v.status == "SAILING_BW_TO_CH_IN":
                    # Arrived Cawthorne Channel inbound — final leg to Point D (tidal)
                    arrival = t
                    self.log_event(arrival, v.name, "ARRIVED_CAWTHORNE_CHANNEL_IN",
                                   "Reached Cawthorne Channel inbound (1h from breakwater)",
                                   voyage_num=v.current_voyage)
                    depart_ch_d = self.next_tidal_sail(arrival)
                    wait_ch_d   = depart_ch_d - arrival
                    if wait_ch_d > 0:
                        self.log_event(arrival, v.name, "WAITING_TIDAL",
                                       f"Cawthorne Channel: waiting for daylight/tide at "
                                       f"{self.hours_to_dt(depart_ch_d).strftime('%Y-%m-%d %H:%M')} "
                                       f"({self.tidal_period_label(depart_ch_d)})",
                                       voyage_num=v.current_voyage)
                    v.status = "SAILING_CH_TO_D"
                    v.next_event_time = depart_ch_d + _sail_leg(SAIL_HOURS_CH_TO_D, self)

                elif v.status == "SAILING_CH_TO_D":
                    # Arrived Point D — reset for next loading cycle
                    v.status = "IDLE_A"
                    v.target_point = "D"          # always stay on Duke
                    v.assigned_storage = None
                    v.assigned_load_hours = None
                    v.assigned_mother = None
                    v._voyage_assigned = False
                    self.log_event(t, v.name, "ARRIVED_LOADING_POINT",
                                   f"Arrived Point D (Awoba) — ready for next cycle",
                                   voyage_num=v.current_voyage)
                    v.next_event_time = t

                elif v.status == "SAILING_B_TO_F":
                    # Arrived at Ibom — execute swap immediately
                    v.status          = "IDLE_A"
                    v.target_point    = "F"
                    v._voyage_assigned = False
                    self.log_event(t, v.name, "ARRIVED_IBOM",
                                   "Arrived at Ibom (Point F) for swap takeover",
                                   voyage_num=v.current_voyage)
                    v.next_event_time = t

                elif v.status == "SAILING_BA":
                    v.status = "IDLE_A"
                    v.assigned_load_hours = None
                    v.assigned_mother = None
                    v._voyage_assigned = False  # allow next cycle to get a new voyage number
                    # Point F vessels that are not the active Ibom loader must be
                    # directed to SanBarth (Point A) — reset target_point here so
                    # IDLE_A dispatch immediately sees SanBarth/JasmineS as eligible.
                    # Also redirect if this vessel IS the active Ibom loader but has
                    # just returned from delivering a partial Ibom cargo (target_point
                    # was set to "B" when casting off from Point F — it is no longer
                    # "F", meaning it needs a full Point A cycle before returning).
                    if (v.name in self.point_f_vessels
                            and self.point_f_swap_pending_for != v.name
                            and (self.point_f_active_loader != v.name
                                 or v.target_point != "F")):
                        v.target_point = "A"
                    self.log_event(t, v.name, "ARRIVED_LOADING_POINT",
                                   f"Arrived Point {v.target_point} storage area — ready for next cycle",
                                   voyage_num=v.current_voyage)
                    v.next_event_time = t

            # 2b. Check mother unavailability windows — log transitions and
            #     reserve berth when a window ends so daughters don't try to
            #     berth before the mother is fully settled back.
            for _uname, _windows in self.mother_unavailability_windows.items():
                for _ws, _we in _windows:
                    # Log entry into window (once, at the first step inside)
                    if abs(t - _ws) < TIME_STEP_HOURS * 0.5:
                        self.log_event(t, _uname, "MOTHER_UNAVAILABLE_START",
                                       f"{_uname} entering scheduled unavailability window — "
                                       f"unavailable until "
                                       f"{self.hours_to_dt(_we).strftime('%Y-%m-%d %H:%M')}")
                        # Reserve berth through the whole window so no daughter
                        # is assigned until the window ends
                        self.mother_berth_free_at[_uname] = max(
                            self.mother_berth_free_at.get(_uname, 0.0), _we
                        )
                        self.mother_available_at[_uname] = max(
                            self.mother_available_at.get(_uname, 0.0), _we
                        )
                    # Log exit from window (once, at the first step after)
                    if abs(t - _we) < TIME_STEP_HOURS * 0.5:
                        self.log_event(t, _uname, "MOTHER_UNAVAILABLE_END",
                                       f"{_uname} resuming normal operations after scheduled "
                                       f"unavailability window")

            # 3. Advance mother export state machines independently
            active_export_mother = next(
                (name for name in MOTHER_NAMES if self.export_state[name] is not None),
                None,
            )

            # ── Forced export override ─────────────────────────────────────────
            # Operator can schedule a specific mother to sail on a particular day
            # via EXPORT_FORCE_SCHEDULE.  When the sim clock crosses a forced
            # departure hour the mother is put into DOC state immediately,
            # bypassing export_ready and departure-eligibility tests.
            # Constraints honoured: no active daughter discharge, daylight window,
            # no other export already active (one export at a time).
            if active_export_mother is None:
                for _fm, _fhours in EXPORT_FORCE_SCHEDULE.items():
                    if self.export_state[_fm] is not None:
                        continue   # already in an export cycle
                    if not self.mother_is_at_point_b(_fm, t):
                        continue   # mother is away
                    for _fh in sorted(_fhours):
                        # Fire when the sim clock has just passed the target hour
                        # (within one timestep tolerance)
                        if t - TIME_STEP_HOURS < _fh <= t:
                            _fdaughter_active = any(
                                vv.assigned_mother == _fm and
                                vv.status in {"BERTHING_B", "HOSE_CONNECT_B", "DISCHARGING"}
                                for vv in self.vessels
                            )
                            if _fdaughter_active:
                                self.log_event(t, _fm, "EXPORT_FORCE_WAIT_DISCHARGE",
                                               f"Forced export override: waiting for active daughter "
                                               f"discharge to complete before starting DOC")
                                # Reschedule by one timestep so the loop retries
                                EXPORT_FORCE_SCHEDULE[_fm] = [
                                    h if h != _fh else t + TIME_STEP_HOURS
                                    for h in _fhours
                                ]
                                break
                            # ── Intake buffer check (forced path) ────────────────
                            _forced_last_co = self.export_intake_last_cast_off.get(_fm, 0.0)
                            _forced_clear_at = _forced_last_co + EXPORT_INTAKE_BUFFER_HOURS
                            if t < _forced_clear_at:
                                self.log_event(t, _fm, "EXPORT_INTAKE_BUFFER",
                                               f"Forced export: waiting for {EXPORT_INTAKE_BUFFER_HOURS}h "
                                               f"intake buffer after last cast-off "
                                               f"(clear at {self.hours_to_dt(_forced_clear_at).strftime('%H:%M')})")
                                EXPORT_FORCE_SCHEDULE[_fm] = [
                                    h if h != _fh else _forced_clear_at
                                    for h in _fhours
                                ]
                                break
                            wall_h = (t + SIM_HOUR_OFFSET) % 24
                            if not (DAYLIGHT_START <= wall_h < DAYLIGHT_END):
                                # Reschedule to next daylight tick
                                next_light = self.next_daylight_sail(t)
                                EXPORT_FORCE_SCHEDULE[_fm] = [
                                    h if h != _fh else next_light
                                    for h in _fhours
                                ]
                                self.log_event(t, _fm, "EXPORT_FORCE_WAIT_DAYLIGHT",
                                               f"Forced export override: waiting for daylight at "
                                               f"{self.hours_to_dt(next_light).strftime('%Y-%m-%d %H:%M')}")
                                break
                            # All clear — force DOC now
                            self.export_state[_fm]       = "DOC"
                            self.export_ready[_fm]       = False
                            self.export_ready_since[_fm] = None
                            self.export_end_time[_fm]    = t + EXPORT_DOC_HOURS
                            # ── Block the berth immediately ───────────────────
                            # Set mother_berth_free_at to a far-future value so
                            # no new daughter can claim this berth while the
                            # mother is in DOC / SAILING / HOSE / IN_PORT.
                            # Daughters already berthed/discharging finish
                            # normally (they hold their own next_event_time).
                            # The berth is released when the mother returns
                            # (mother_available_at is set by the return logic).
                            _forced_vol = min(
                                self.mother_bbl[_fm],
                                self.mother_capacity_bbl(_fm),
                            )
                            _lock_until = t + (EXPORT_DOC_HOURS
                                               + EXPORT_SAIL_HOURS
                                               + EXPORT_HOSE_HOURS
                                               + (_forced_vol / max(1, EXPORT_RATE_BPH))
                                               + EXPORT_SAIL_HOURS + 2.0)
                            self.mother_berth_free_at[_fm] = max(
                                self.mother_berth_free_at.get(_fm, 0.0),
                                _lock_until,
                            )
                            self.log_event(t, _fm, "EXPORT_DOC_START",
                                           f"FORCED export departure override — "
                                           f"documentation ({EXPORT_DOC_HOURS}h) | "
                                           f"Stock: {self.mother_bbl[_fm]:,.0f} bbl | "
                                           f"Berth locked until return")
                            active_export_mother = _fm   # block normal selection below
                            break
                    if active_export_mother is not None:
                        break
            if active_export_mother is None and t >= self.next_export_allowed_at:
                # ── Proactive trigger check ───────────────────────────────────
                # In case a mother reached her export trigger via a partial
                # discharge without going through CAST_OFF_B (the normal path),
                # ensure export_ready is set here.
                for _mn in MOTHER_NAMES:
                    if (self.export_state[_mn] is None
                            and not self.export_ready[_mn]
                            and self.mother_bbl[_mn] >= self.mother_export_trigger_bbl(_mn)):
                        self.export_ready[_mn] = True
                        if not self.export_ready_since[_mn]:
                            self.export_ready_since[_mn] = t

                # ── Export unavailability block ───────────────────────────────
                # If the current sim hour falls within any export unavailability
                # window, suppress new export departures entirely.  Mothers that
                # are already mid-export cycle (DOC/SAILING/HOSE/IN_PORT) are
                # NOT interrupted — they complete normally.  We just skip the
                # ready_candidates selection so no NEW exports start.
                _eu_windows = getattr(self, 'export_unavailability_windows', [])
                _export_blocked = any(
                    _eu_s <= t < _eu_e for (_eu_s, _eu_e) in _eu_windows
                )
                if _export_blocked:
                    # Log once per timestep entry into the window
                    for (_eu_s, _eu_e) in _eu_windows:
                        if abs(t - _eu_s) < TIME_STEP_HOURS * 0.5:
                            self.log_event(
                                t, "SYSTEM", "EXPORT_UNAVAILABLE_START",
                                f"Export unavailability window active until "
                                f"{self.hours_to_dt(_eu_e).strftime('%Y-%m-%d %H:%M')} — "
                                f"mother vessels held at BIA"
                            )
                    # Log exit from window once
                    for (_eu_s, _eu_e) in _eu_windows:
                        if abs(t - _eu_e) < TIME_STEP_HOURS * 0.5:
                            self.log_event(
                                t, "SYSTEM", "EXPORT_UNAVAILABLE_END",
                                "Export unavailability window ended — normal export operations resume"
                            )

                ready_candidates = []
                for mother_name in MOTHER_NAMES:
                    if (
                        self.export_state[mother_name] is None
                        and self.export_ready[mother_name]
                        and self.mother_export_departure_eligible(mother_name)
                        and t >= self.mother_available_at[mother_name]
                        and not _export_blocked
                    ):
                        # Block export DOC if any daughter is actively berthed here.
                        daughter_active_here = any(
                            vv.assigned_mother == mother_name
                            and vv.status in {"BERTHING_B", "HOSE_CONNECT_B", "DISCHARGING"}
                            for vv in self.vessels
                        )
                        if daughter_active_here:
                            self.log_event(
                                t, mother_name, "EXPORT_WAIT_DISCHARGE",
                                "Export ready but waiting for active "
                                "daughter berthing/discharge operations to complete"
                            )
                            continue
                        ready_since = self.export_ready_since[mother_name]
                        if ready_since is None:
                            ready_since = t
                        ready_candidates.append((ready_since, mother_name))

                if ready_candidates:
                    ready_candidates.sort(key=lambda x: (x[0], x[1]))
                    selected_export_mother = ready_candidates[0][1]
                    wall_h = (t + SIM_HOUR_OFFSET) % 24
                    if DAYLIGHT_START <= wall_h < DAYLIGHT_END:
                        # ── Intake buffer check ───────────────────────────────
                        # Do not fire DOC until EXPORT_INTAKE_BUFFER_HOURS after
                        # the last daughter cast-off, so the
                        # final intake volume has settled before documentation.
                        _last_co = self.export_intake_last_cast_off.get(
                            selected_export_mother, 0.0)
                        _intake_clear_at = _last_co + EXPORT_INTAKE_BUFFER_HOURS
                        if t < _intake_clear_at:
                            self.log_event(
                                t, selected_export_mother, "EXPORT_INTAKE_BUFFER",
                                f"Export ready but waiting for {EXPORT_INTAKE_BUFFER_HOURS}h "
                                f"intake buffer after last cast-off "
                                f"(clear at {self.hours_to_dt(_intake_clear_at).strftime('%H:%M')})"
                            )
                        else:
                            self.export_state[selected_export_mother] = "DOC"
                            self.export_ready[selected_export_mother] = False
                            self.export_ready_since[selected_export_mother] = None
                            self.export_end_time[selected_export_mother] = t + EXPORT_DOC_HOURS
                            # Lock the berth for the full export round-trip so that
                            # no new daughter can berth while she is away.  Daughters
                            # already mid-discharge finish normally; no new ones start.
                            _export_vol_capped = min(
                                self.mother_bbl[selected_export_mother],
                                self.mother_capacity_bbl(selected_export_mother),
                            )
                            _nat_lock = t + (EXPORT_DOC_HOURS
                                             + EXPORT_SAIL_HOURS
                                             + EXPORT_HOSE_HOURS
                                             + (_export_vol_capped / max(1, EXPORT_RATE_BPH))
                                             + EXPORT_SAIL_HOURS + 2.0)
                            self.mother_berth_free_at[selected_export_mother] = max(
                                self.mother_berth_free_at.get(selected_export_mother, 0.0),
                                _nat_lock,
                            )
                            self.log_event(t, selected_export_mother, "EXPORT_DOC_START",
                                           f"Export documentation ({EXPORT_DOC_HOURS}h) | "
                                           f"Berth locked until return")
                    else:
                        next_light = self.next_daylight_sail(t)
                        if next_light > t:
                            self.log_event(t, selected_export_mother, "EXPORT_WAIT_DAYLIGHT",
                                           f"Export ready but waiting for daylight at "
                                           f"{self.hours_to_dt(next_light).strftime('%Y-%m-%d %H:%M')}")

            for mother_name in MOTHER_NAMES:
                state = self.export_state[mother_name]
                if state == "DOC":
                    if t >= self.export_end_time[mother_name]:
                        daughter_active_here = any(
                            vv.assigned_mother == mother_name
                            and vv.status in {"BERTHING_B", "HOSE_CONNECT_B", "DISCHARGING"}
                            for vv in self.vessels
                        )
                        _any_active = daughter_active_here
                        # Hard timeout: never wait more than 24h after DOC completes.
                        # Without this, a continuous stream of daughters keeps the
                        # mother in DOC forever and the export never sails.
                        _doc_complete_t = self.export_start_time.get(mother_name) or t
                        _waited = t - (_doc_complete_t + EXPORT_DOC_HOURS)
                        if _any_active and _waited < 24.0:
                            self.export_end_time[mother_name] = t + TIME_STEP_HOURS
                            self.log_event(
                                t, mother_name, "EXPORT_WAIT_DISCHARGE",
                                "Export docs complete but waiting for active "
                                "daughter discharge operations",
                            )
                            continue
                        sail_start = self.next_export_sail_start(t)
                        if sail_start > t:
                            self.log_event(t, mother_name, "EXPORT_WAIT_SAIL_WINDOW",
                                           f"Export docs complete; waiting to start sail at "
                                           f"{self.hours_to_dt(sail_start).strftime('%Y-%m-%d %H:%M')}")
                        self.export_state[mother_name] = "SAILING"
                        self.export_start_time[mother_name] = sail_start
                        self.export_end_time[mother_name] = sail_start + EXPORT_SAIL_HOURS
                        self.log_event(sail_start, mother_name, "EXPORT_SAIL_START",
                                       f"Sailing to export terminal ({EXPORT_SAIL_HOURS}h)")

                elif state == "SAILING":
                    if t >= self.export_end_time[mother_name]:
                        self.export_state[mother_name] = "HOSE"
                        self.export_start_time[mother_name] = t
                        self.export_end_time[mother_name] = t + EXPORT_HOSE_HOURS
                        self.log_event(t, mother_name, "EXPORT_ARRIVED",
                                       f"Arrived at export terminal; initiating hose connection ({EXPORT_HOSE_HOURS}h)")
                        self.log_event(t, mother_name, "EXPORT_HOSE_START",
                                       f"Hose connection ({EXPORT_HOSE_HOURS}h)")

                elif state == "HOSE":
                    if t >= self.export_end_time[mother_name]:
                        self.export_state[mother_name] = "IN_PORT"
                        self.export_start_time[mother_name] = t
                        self.log_event(t, mother_name, "EXPORT_HOSE_COMPLETE",
                                       "Hose connection complete; ready to export")

                elif state == "IN_PORT":
                    amount = min(self.mother_bbl[mother_name], EXPORT_RATE_BPH * TIME_STEP_HOURS)
                    if amount > 0:
                        self.total_exported_api_bbl += amount * self.mother_api.get(mother_name, 0.0)
                        self.mother_bbl[mother_name] -= amount
                        self.total_exported += amount
                        self.log_event(t, mother_name, "EXPORT_PROGRESS",
                                       f"Exported {amount:,} bbl in port; Remaining: {self.mother_bbl[mother_name]:,.0f} bbl")
                    if self.mother_bbl[mother_name] <= 0:
                        export_complete_t = t
                        # Mother is now empty but still PHYSICALLY at the export
                        # terminal — she must sail ~6h back to BIA and complete
                        # fendering before she can receive any cargo again.  Use a
                        # dedicated RETURNING state (not None) for this window so
                        # every loading guard (daughter discharge, MTO offload,
                        # full-fit routing) treats her as
                        # unavailable.  Previously the state went straight to None
                        # at export-complete, which briefly made her look available
                        # while she was still hours away at the terminal — that let
                        # daughters "load" a mother that was still at
                        # export (the 4-Jun/5-Jun GreenEagle violations).  The state
                        # is cleared to None only at EXPORT_FENDERING_COMPLETE below.
                        self.export_state[mother_name] = "RETURNING"
                        self.export_start_time[mother_name] = None
                        self.export_end_time[mother_name] = None
                        self.log_event(t, mother_name, "EXPORT_COMPLETE",
                                       f"Export complete; Remaining on board: {self.mother_bbl[mother_name]:,.0f} bbl")
                        return_depart = self.next_daylight_sail(t)
                        if return_depart > t:
                            self.log_event(t, mother_name, "EXPORT_WAIT_DAYLIGHT_RETURN",
                                           f"Waiting for daylight to depart export terminal at "
                                           f"{self.hours_to_dt(return_depart).strftime('%Y-%m-%d %H:%M')}")
                        return_arrival = return_depart + EXPORT_SAIL_HOURS
                        self.mother_available_at[mother_name] = return_arrival + 2
                        _fender_done = return_arrival + 2
                        # Release the berth lock so daughters can berth again
                        # once fendering is complete.  Use direct assignment —
                        # the old min() kept the far-future DOC-phase lock value,
                        # which prevented any new daughters from ever berthing.
                        self.mother_berth_free_at[mother_name] = _fender_done
                        self.log_event(return_depart, mother_name, "EXPORT_RETURN_START",
                                       f"Departing export terminal ({EXPORT_SAIL_HOURS}h transit)")
                        self.log_event(return_arrival, mother_name, "EXPORT_RETURN_ARRIVE",
                                       f"Arrived at {mother_name}; beginning 2h fendering")
                        self.log_event(self.mother_available_at[mother_name], mother_name, "EXPORT_FENDERING_COMPLETE",
                                       "Fendering complete; ready to receive daughters")
                        self.next_export_allowed_at = max(
                            self.next_export_allowed_at,
                            export_complete_t + EXPORT_SERIES_BUFFER_HOURS,
                        )
                        self.last_export_mother = mother_name
                        self.log_event(
                            self.next_export_allowed_at,
                            mother_name,
                            "EXPORT_SERIES_BUFFER_COMPLETE",
                            f"Mandatory post-export buffer complete ({EXPORT_SERIES_BUFFER_HOURS}h from export discharge completion) — next export sailing may begin",
                        )

                elif state == "RETURNING":
                    # Empty mother sailing back from the export terminal and
                    # fendering at BIA.  She remains unavailable for loading for
                    # this entire window; only when fendering is complete
                    # (mother_available_at) does she become a valid berth
                    # target again.  Clearing the state here — rather than at
                    # export-complete — is what blocks all loads during the return.
                    if t >= self.mother_available_at.get(mother_name, 0.0):
                        self.export_state[mother_name] = None

            # 3b. Advance ZeeZee third-party discharge state machine
            self._run_zeezee(t)

            # 4. Debit overflow accumulation and credit stock when space is available
            for storage_name in STORAGE_NAMES:
                overflow_backlog = self.storage_overflow_bbl[storage_name]
                if overflow_backlog <= 0:
                    continue
                cap = STORAGE_CAPACITY_BY_NAME[storage_name]
                space_available = max(0.0, cap - self.storage_bbl[storage_name])
                if space_available <= 0:
                    continue
                credit_amount = min(space_available, overflow_backlog)
                self.storage_bbl[storage_name] += credit_amount
                self.storage_overflow_bbl[storage_name] -= credit_amount

            # 5. Check storage critical thresholds (entry/exit)
            for storage_name in STORAGE_NAMES:
                threshold = STORAGE_CRITICAL_THRESHOLD_BY_NAME[storage_name]
                is_critical_now = self.storage_bbl[storage_name] > threshold
                if is_critical_now and not self.storage_critical_active[storage_name]:
                    self.storage_critical_active[storage_name] = True
                    self.log_event(
                        t,
                        storage_name,
                        "STORAGE_CRITICAL_ENTER",
                        f"Critical stock reached: {self.storage_bbl[storage_name]:,.0f} bbl > {threshold:,.0f} bbl",
                    )
                elif (not is_critical_now) and self.storage_critical_active[storage_name]:
                    self.storage_critical_active[storage_name] = False
                    self.log_event(
                        t,
                        storage_name,
                        "STORAGE_CRITICAL_EXIT",
                        f"Critical stock cleared: {self.storage_bbl[storage_name]:,.0f} bbl <= {threshold:,.0f} bbl",
                    )

            # 6. Snapshot for timeline
            # Pre-cache repeated lookups to avoid redundant function calls
            # and dict traversals per step (~4,320 steps per 90-day run).
            _t_dt   = self.hours_to_dt(t)
            _t_day  = int(t // 24) + 1
            _s_bbl  = self.storage_bbl
            _s_ovf  = self.storage_overflow_bbl
            _m_bbl  = self.mother_bbl
            _s_api  = self.storage_api
            _m_api  = self.mother_api
            _v_api  = self.vessel_api
            _ovf_total = round(_s_ovf[STORAGE_PRIMARY_NAME] + _s_ovf[STORAGE_SECONDARY_NAME]
                               + _s_ovf[STORAGE_TERTIARY_NAME] + _s_ovf[STORAGE_QUATERNARY_NAME]
                               + _s_ovf[STORAGE_QUINARY_NAME])
            vessel_statuses = {}
            for v in self.vessels:
                _vn = v.name
                vessel_statuses[_vn]                = v.status
                vessel_statuses[f"{_vn}_cargo_bbl"] = round(v.cargo_bbl)
                vessel_statuses[f"{_vn}_api"]       = round(_v_api.get(_vn, 0.0), 2)
            self.timeline.append({
                "Time"       : _t_dt,
                "Day"        : _t_day,
                "Storage_bbl": round(sum(self.storage_bbl.values())),
                "SanBarth_bbl": round(_s_bbl[STORAGE_PRIMARY_NAME]),
                "JasmineS_bbl": round(_s_bbl[STORAGE_SECONDARY_NAME]),
                "Westmore_bbl": round(_s_bbl[STORAGE_TERTIARY_NAME]),
                "Duke_bbl": round(_s_bbl[STORAGE_QUATERNARY_NAME]),
                "Starturn_bbl": round(_s_bbl[STORAGE_QUINARY_NAME]),
                "PGM_bbl": round(_s_bbl[STORAGE_SENARY_NAME]),
                "Storage_Overflow_Accum_bbl": _ovf_total,
                "SanBarth_Overflow_Accum_bbl": round(_s_ovf[STORAGE_PRIMARY_NAME]),
                "JasmineS_Overflow_Accum_bbl": round(_s_ovf[STORAGE_SECONDARY_NAME]),
                "Westmore_Overflow_Accum_bbl": round(_s_ovf[STORAGE_TERTIARY_NAME]),
                "Duke_Overflow_Accum_bbl": round(_s_ovf[STORAGE_QUATERNARY_NAME]),
                "Starturn_Overflow_Accum_bbl": round(_s_ovf[STORAGE_QUINARY_NAME]),
                "PGM_Overflow_Accum_bbl": round(_s_ovf[STORAGE_SENARY_NAME]),
                "PointF_Overflow_Accum_bbl": round(self.point_f_overflow_accum_bbl),
                "PointF_Active_Loading_bbl": round(self.point_f_active_loading_bbl()),
                "Mother_bbl" : round(sum(self.mother_bbl.values())),
                "Bryanston_bbl":  round(_m_bbl[MOTHER_PRIMARY_NAME]),
                "GreenEagle_bbl": round(_m_bbl[MOTHER_SECONDARY_NAME]),
                "Alkebulan_bbl":  round(_m_bbl[MOTHER_QUINARY_NAME]),
                "Total_Exported": self.total_exported,
                "SanBarth_api"   : round(_s_api.get(STORAGE_PRIMARY_NAME,   0.0), 2),
                "JasmineS_api" : round(_s_api.get(STORAGE_SECONDARY_NAME, 0.0), 2),
                "Westmore_api" : round(_s_api.get(STORAGE_TERTIARY_NAME,  0.0), 2),
                "Duke_api"     : round(_s_api.get(STORAGE_QUATERNARY_NAME,0.0), 2),
                "Starturn_api" : round(_s_api.get(STORAGE_QUINARY_NAME,   0.0), 2),
                "PGM_api"      : round(_s_api.get(STORAGE_SENARY_NAME,    0.0), 2),
                "Bryanston_api":  round(_m_api.get(MOTHER_PRIMARY_NAME,    0.0), 2),
                "GreenEagle_api": round(_m_api.get(MOTHER_SECONDARY_NAME,  0.0), 2),
                "Alkebulan_api":  round(_m_api.get(MOTHER_QUINARY_NAME,    0.0), 2),
                **vessel_statuses,
                # ZeeZee snapshot — only present when she is visiting
                **({  "ZeeZee": self.zeezee.status,
                      "ZeeZee_cargo_bbl": round(self.zeezee.cargo_bbl),
                      "ZeeZee_api": round(self.zeezee.api, 2)}
                   if self.zeezee is not None else {}),
            })

            self._bryanston_call_waiting_vessel_serially(t, reason="post-state-scan")

            t = round(t + TIME_STEP_HOURS, 2)

        self.final_storage_api = dict(self.storage_api)
        self.final_vessel_api  = dict(self.vessel_api)
        self.final_mother_api  = dict(self.mother_api)
        self.avg_exported_api  = (
            self.total_exported_api_bbl / self.total_exported
            if self.total_exported > 0 else 0.0
        )
        # ── Stochastic summary ────────────────────────────────────────────────
        # calibration_report() is always available (returns {} in deterministic
        # mode).  The app reads self._variability_summary after run() returns.
        self._variability_summary = {
            "enabled":              ENABLE_VARIABILITY,
            "weather_hold_h_total": round(getattr(self, "_weather_hold_hours_total", 0.0), 2),
            "calibration":          self.calibration_report(),
        }

        return pd.DataFrame(self.log), pd.DataFrame(self.timeline)


# =============================================================================
# ── WORLD-CLASS LAYER: MONTE CARLO RISK ENVELOPE & VALIDATION HARNESS ─────────
#
# Design philosophy (why this is a separate layer, not a rewrite):
#
#   The deterministic engine above IS the executable plan — it already enforces
#   every hard operational constraint (berth occupancy, daylight berthing/cast-off
#   windows, per-vessel load/discharge rates, hose/fender/documentation durations,
#   concurrent-berth exclusion, MTO sequencing) and is reproducible.  Real marine
#   planning desks publish exactly such a single best-estimate plan.
#
#   What a deterministic plan cannot show is the RISK around it — how late a
#   vessel might actually offload once weather, equipment, congestion and human
#   lag are layered on.  The engine already contains a fully-wired stochastic
#   layer (triangular per-operation variability, exponential weather holds,
#   equipment delays, human-decision lag, congestion multipliers, production
#   fluctuation) gated behind ENABLE_VARIABILITY.  The missing piece — the genuine
#   gap — is a Monte Carlo driver that runs many independent stochastic
#   replications and reduces them to a P50/P90 risk envelope, plus a validation
#   harness that scores the deterministic plan against that envelope.
#
#   This gives the two artefacts a real operations system needs side by side:
#     • the executable PLAN (deterministic), and
#     • the CONFIDENCE BAND around it (stochastic Monte Carlo).
# =============================================================================

def _run_single_replication(seed, days=None):
    """Run one independent simulation replication and return (log_df, timeline_df).

    Honours the module-level ENABLE_VARIABILITY switch.  When variability is on,
    the supplied seed makes each replication independently reproducible; when it
    is off, every replication is identical to the deterministic plan (so a Monte
    Carlo run with variability disabled degenerates, correctly, to the plan).
    """
    if seed is not None:
        random.seed(seed)
    sim = Simulation()
    return sim.run()


def _replication_metrics(log_df):
    """Reduce one replication's event log to the operational KPIs that matter.

    Returns a dict of scalar metrics.  Kept deliberately small and robust so the
    Monte Carlo aggregator can build distributions over each KPI.
    """
    import re as _re
    ev = log_df["Event"].astype(str)
    detail = log_df["Detail"].astype(str)

    # Real (non-zero) discharges to mothers.
    _disch_mask = ev.eq("DISCHARGE_START")
    real_disch = int(sum(1 for d in detail[_disch_mask]
                         if not _re.search(r"Discharging 0 bbl", d)))

    # Cargo delivered directly to mother vessels.
    direct = 0
    for d, m in zip(detail[_disch_mask], log_df["Mother"][_disch_mask].astype(str)):
        g = _re.search(r"Discharging ([\d,]+)", d)
        if g and m in ("Bryanston", "GreenEagle"):
            direct += int(g.group(1).replace(",", ""))
    to_primary = direct

    # Total cargo loaded (throughput into the daughter fleet).
    loaded = 0
    for d in detail[ev.eq("LOADING_COMPLETE")]:
        g = _re.search(r"Cargo: ([\d,]+)", d)
        if g:
            loaded += int(g.group(1).replace(",", ""))

    # Operational stress signals.
    cap_aborts   = int(ev.eq("MOTHER_CAPACITY_ABORT").sum())
    berth_aborts = int(ev.eq("CONCURRENT_BERTH_ABORT").sum())
    weather_holds = int(ev.astype(str).str.contains("WEATHER", case=False, na=False).sum())
    errors = int(ev.str.contains("ERROR|EXCEPTION", case=False, na=False).sum())

    return {
        "real_discharges":   real_disch,
        "cargo_to_primary":  to_primary,
        "cargo_loaded":      loaded,
        "capacity_aborts":   cap_aborts,
        "berth_aborts":      berth_aborts,
        "weather_holds":     weather_holds,
        "errors":            errors,
    }


def run_monte_carlo(n_replications=50, base_seed=12345, days=None, progress=False):
    """Run a Monte Carlo ensemble and return a per-KPI risk envelope.

    Each replication is an independent stochastic realisation of the SAME plan
    (requires ENABLE_VARIABILITY=True to vary; otherwise all replications are
    identical to the deterministic plan).  Results are reduced to percentiles —
    the operational risk band a planner actually needs:

        P10  – optimistic   (only 10% of outcomes are better)
        P50  – median       (the expected real-world outcome)
        P90  – conservative (90% of outcomes are at least this good / this few)

    Returns a dict: { kpi_name: {mean, std, p10, p50, p90, min, max}, ... }
    plus a 'replications' count and the raw per-rep frame under '_raw'.
    """
    import numpy as _np
    rows = []
    for i in range(n_replications):
        seed = (base_seed + i) if base_seed is not None else None
        log_df, _ = _run_single_replication(seed, days=days)
        rows.append(_replication_metrics(log_df))
        if progress:
            print(f"  MC replication {i+1}/{n_replications} done")
    raw = pd.DataFrame(rows)
    envelope = {"replications": n_replications, "_raw": raw}
    for col in raw.columns:
        vals = raw[col].to_numpy(dtype=float)
        envelope[col] = {
            "mean": float(_np.mean(vals)),
            "std":  float(_np.std(vals, ddof=1)) if len(vals) > 1 else 0.0,
            "p10":  float(_np.percentile(vals, 10)),
            "p50":  float(_np.percentile(vals, 50)),
            "p90":  float(_np.percentile(vals, 90)),
            "min":  float(_np.min(vals)),
            "max":  float(_np.max(vals)),
        }
    return envelope


def validate_plan_against_envelope(plan_metrics, envelope):
    """Score the deterministic plan against the Monte Carlo risk envelope.

    This is the validation methodology a real operations desk uses: it asks,
    for each KPI, where the deterministic plan sits within the distribution of
    achievable real-world outcomes.  A plan that sits far on the optimistic tail
    (e.g. above P90 for throughput, below P10 for delays) is flagging an
    over-optimistic assumption — the classic 'mathematically optimal but not
    operationally achievable' failure the planner must see.

    Returns a per-KPI dict: {plan, p50, p90, plan_vs_p50_pct, realism_flag}.
    """
    out = {}
    for kpi, plan_val in plan_metrics.items():
        if kpi not in envelope or kpi in ("replications", "_raw"):
            continue
        band = envelope[kpi]
        p50 = band["p50"]
        gap_pct = (100.0 * (plan_val - p50) / p50) if p50 else 0.0
        # Throughput KPIs: plan above the optimistic tail is over-optimistic.
        # Stress KPIs (aborts/errors): plan below the band understates risk.
        if kpi in ("real_discharges", "cargo_to_primary", "cargo_loaded"):
            flag = "OPTIMISTIC" if plan_val > band["p90"] + 1e-9 else (
                   "CONSERVATIVE" if plan_val < band["p10"] - 1e-9 else "REALISTIC")
        else:
            flag = "UNDERSTATES_RISK" if plan_val < band["p10"] - 1e-9 else "REALISTIC"
        out[kpi] = {
            "plan":            round(plan_val, 1),
            "p50":             round(p50, 1),
            "p90":             round(band["p90"], 1),
            "plan_vs_p50_pct": round(gap_pct, 1),
            "realism_flag":    flag,
        }
    return out


# -----------------------------------------------------------------
# RUN SIMULATION
# -----------------------------------------------------------------
print("=" * 65)
print("  OIL TANKER DAUGHTER VESSEL OPERATION SIMULATION  (v5)")
print("=" * 65)

if POINT_B_DISTRIBUTION_TEST_MODE:
    SIMULATION_DAYS = POINT_B_DISTRIBUTION_TEST_DAYS
    print("[INFO] Point B distribution test mode enabled")
    print(f"[INFO] Simulation days overridden to {SIMULATION_DAYS}")

sim = Simulation()
log_df, timeline_df = sim.run()

# Print summary table
print(f"\n{'-'*65}")
print("DETAILED EVENT LOG (first 80 events)")
print(f"{'-'*65}")
display_cols = ["Time", "Vessel", "Voyage", "Event", "Detail", "Storage_bbl", "Mother_bbl"]
print(log_df[display_cols].head(80).to_string(index=False))

print(f"\n{'-'*65}")
print("SIMULATION SUMMARY")
print(f"{'-'*65}")
total_loads     = len(log_df[log_df["Event"] == "LOADING_START"])
total_discharge = len(log_df[log_df["Event"] == "DISCHARGE_START"])
total_exports   = len(log_df[log_df["Event"] == "EXPORT_COMPLETE"])
print(f"  Simulation Period    : {SIMULATION_DAYS} days")
print(f"  Total Loadings       : {total_loads}")
print(f"  Total Discharges     : {total_discharge}")
print(f"  Total Volume Loaded  : {sim.total_loaded:,} bbl")
print(f"  Mother Export Voyages: {total_exports}")
print(f"  Total Volume Exported: {sim.total_exported:,} bbl")
print(f"  Total Volume Produced: {sim.total_produced:,.0f} bbl")
print(f"  Produced Spill/Overflow: {sim.total_spilled:,.0f} bbl")
print(f"  Final Storage Level (Total Point A+C+D+E): {sim.total_storage_bbl():,.0f} bbl")
print(f"    - {STORAGE_PRIMARY_NAME:<8}: {sim.storage_bbl[STORAGE_PRIMARY_NAME]:,.0f} bbl")
print(f"    - {STORAGE_SECONDARY_NAME:<8}: {sim.storage_bbl[STORAGE_SECONDARY_NAME]:,.0f} bbl")
print(f"    - {STORAGE_TERTIARY_NAME:<8}: {sim.storage_bbl[STORAGE_TERTIARY_NAME]:,.0f} bbl")
print(f"    - {STORAGE_QUATERNARY_NAME:<8}: {sim.storage_bbl[STORAGE_QUATERNARY_NAME]:,.0f} bbl")
print(f"    - {STORAGE_QUINARY_NAME:<8}: {sim.storage_bbl[STORAGE_QUINARY_NAME]:,.0f} bbl")
print(f"  Final Mother Level (Total Point B): {sim.total_mother_bbl():,.0f} bbl")
print(f"    - {MOTHER_PRIMARY_NAME:<9}: {sim.mother_bbl[MOTHER_PRIMARY_NAME]:,.0f} bbl")
print(f"    - {MOTHER_SECONDARY_NAME:<9}: {sim.mother_bbl[MOTHER_SECONDARY_NAME]:,.0f} bbl")
print(f"    - {MOTHER_QUINARY_NAME:<9}: {sim.mother_bbl[MOTHER_QUINARY_NAME]:,.0f} bbl")
print(f"  Storage Overflow     : {sim.storage_overflow_events} events")

print(f"\n{'-'*65}")
print("BERTHING ORDER AT MOTHER VESSELS (all voyages)")
print(f"{'-'*65}")
berth_mask = log_df["Event"] == "BERTHING_START_B"
print(log_df[berth_mask][display_cols].to_string(index=False))

# -----------------------------------------------------------------
# CHARTS
# -----------------------------------------------------------------

# ── Unique base colours per daughter vessel ──────────────────────
VESSEL_COLORS = {
    "Sherlock"  : "#e74c3c",   # red family
    "Laphroaig" : "#2ecc71",   # green family
    "Rathbone"  : "#9b59b6",   # purple family
    "SantaMonica": "#6c5ce7",  # indigo family
    "Bedford"   : "#f39c12",   # amber family
    "Balham"    : "#1abc9c",   # teal family
    "Woodstock" : "#e91e63",   # pink family
    "Bagshot"   : "#00bcd4",   # cyan family
    "Watson"    : "#95a5a6",   # slate/gray family
    "Amyla"   : "#7f8c8d",   # steel gray family
    "FatimaZarah": "#84cc16",   # lime family
}

# Each vessel gets a palette of shades derived from its base colour.
# Ordered from light (idle/waiting) → vivid (active ops) → dark (return)
import colorsys

def hex_to_rgb(h):
    h = h.lstrip('#')
    return tuple(int(h[i:i+2], 16)/255 for i in (0, 2, 4))

def shade(hex_color, lightness_factor):
    """Return a lighter/darker shade of hex_color by scaling lightness."""
    r, g, b = hex_to_rgb(hex_color)
    h, l, s = colorsys.rgb_to_hls(r, g, b)
    l2 = max(0.0, min(1.0, l * lightness_factor))
    r2, g2, b2 = colorsys.hls_to_rgb(h, l2, s)
    return "#{:02x}{:02x}{:02x}".format(int(r2*255), int(g2*255), int(b2*255))

# Map every status to a lightness factor for each vessel's palette
STATUS_LIGHTNESS = {
    "IDLE_A"                  : 2.0,    # lightest — at rest at storage
    "WAITING_STOCK"           : 1.8,
    "WAITING_BERTH_A"         : 1.7,
    "WAITING_DEAD_STOCK"      : 1.6,    # berthed but stock too low
    "BERTHING_A"              : 1.3,
    "HOSE_CONNECT_A"          : 1.1,
    "LOADING"                 : 1.0,    # base colour — active loading
    "DOCUMENTING"             : 0.9,
    "WAITING_CAST_OFF"        : 0.85,
    "CAST_OFF"                : 0.8,
    "SAILING_AB"              : 0.7,
    "SAILING_AB_LEG2"         : 0.65,
    "WAITING_FAIRWAY"         : 0.6,
    "WAITING_BERTH_B"         : 0.6,
    "WAITING_MOTHER_RETURN"   : 0.55,
    "WAITING_MOTHER_CAPACITY" : 0.5,
    "BERTHING_B"              : 0.5,
    "HOSE_CONNECT_B"          : 0.45,
    "DISCHARGING"             : 0.4,    # darkest active — discharging
    "CAST_OFF_B"              : 0.38,
    "SAILING_BA"              : 0.5,
    "IDLE_B"                  : 0.55,
    "WAITING_DAYLIGHT"        : 1.5,
}

def vessel_status_color(vessel_name, status):
    base = VESSEL_COLORS.get(vessel_name, "#95a5a6")
    factor = STATUS_LIGHTNESS.get(status, 1.0)
    return shade(base, factor)

fig, axes = plt.subplots(3, 1, figsize=(18, 16))
fig.patch.set_facecolor("#1a1a2e")
for ax in axes:
    ax.set_facecolor("#16213e")
    ax.tick_params(colors="white")
    ax.xaxis.label.set_color("white")
    ax.yaxis.label.set_color("white")
    ax.title.set_color("white")
    for spine in ax.spines.values():
        spine.set_edgecolor("#444")

fig.suptitle("Oil Tanker Daughter Vessel Operation — 30-Day Simulation (v5)",
             fontsize=15, fontweight="bold", y=0.99, color="white")

# ── Chart 1: Storage vessel volume ───────────────────────────────
ax1 = axes[0]
ax1.fill_between(timeline_df["Time"], timeline_df["Storage_bbl"],
                 alpha=0.25, color="#e67e22")
ax1.plot(timeline_df["Time"], timeline_df["Storage_bbl"],
            color="#e67e22", linewidth=2, label="Point A/C/D/E Total Storage Volume")
ax1.plot(timeline_df["Time"], timeline_df["SanBarth_bbl"],
            color="#f1c40f", linewidth=1.4, alpha=0.9, label=f"{STORAGE_PRIMARY_NAME} Volume")
ax1.plot(timeline_df["Time"], timeline_df["JasmineS_bbl"],
            color="#8e44ad", linewidth=1.4, alpha=0.9, label=f"{STORAGE_SECONDARY_NAME} Volume")
ax1.plot(timeline_df["Time"], timeline_df["Westmore_bbl"],
            color="#27ae60", linewidth=1.4, alpha=0.9, label=f"{STORAGE_TERTIARY_NAME} Volume")
ax1.plot(timeline_df["Time"], timeline_df["Duke_bbl"],
            color="#3498db", linewidth=1.4, alpha=0.9, label=f"{STORAGE_QUATERNARY_NAME} Volume")
ax1.plot(timeline_df["Time"], timeline_df["Starturn_bbl"],
            color="#d35400", linewidth=1.4, alpha=0.9, label=f"{STORAGE_QUINARY_NAME} Volume")
ax1.axhline(SANBARTH_STORAGE_CAPACITY_BBL, color="#e74c3c", linestyle="--", alpha=0.7,
                label=f"SanBarth Capacity ({SANBARTH_STORAGE_CAPACITY_BBL:,} bbl)")
ax1.axhline(DUKE_STORAGE_CAPACITY_BBL, color="#3498db", linestyle="--", alpha=0.7,
                label=f"Duke Capacity ({DUKE_STORAGE_CAPACITY_BBL:,} bbl)")
ax1.axhline(STARTURN_STORAGE_CAPACITY_BBL, color="#d35400", linestyle="--", alpha=0.7,
                label=f"Starturn Capacity ({STARTURN_STORAGE_CAPACITY_BBL:,} bbl)")

# Dead-stock lines per vessel (175% of each cargo)
ds_colors = {"Sherlock": "#e74c3c", "Laphroaig": "#2ecc71",
             "Rathbone": "#9b59b6", "SantaMonica": "#6c5ce7", "Bedford": "#f39c12",
         "Balham": "#1abc9c", "Woodstock": "#e91e63", "Bagshot": "#00bcd4", "Watson": "#95a5a6", "Amyla": "#7f8c8d",
         "FatimaZarah": "#84cc16"}
for vname, vcap in [("Sherlock", DAUGHTER_CARGO_BBL),
                     ("Laphroaig", DAUGHTER_CARGO_BBL),
                     ("Rathbone", VESSEL_CAPACITIES.get("Rathbone", DAUGHTER_CARGO_BBL)),
                     ("SantaMonica", VESSEL_CAPACITIES.get("SantaMonica", DAUGHTER_CARGO_BBL)),
                     ("Bedford",  VESSEL_CAPACITIES.get("Bedford",  DAUGHTER_CARGO_BBL)),
                     ("Balham",   VESSEL_CAPACITIES.get("Balham",   DAUGHTER_CARGO_BBL)),
                     ("Woodstock", VESSEL_CAPACITIES.get("Woodstock", DAUGHTER_CARGO_BBL)),
             ("Bagshot",  VESSEL_CAPACITIES.get("Bagshot",  DAUGHTER_CARGO_BBL)),
             ("Watson",   VESSEL_CAPACITIES.get("Watson",   DAUGHTER_CARGO_BBL)),
             ("Amyla",  VESSEL_CAPACITIES.get("Amyla",  DAUGHTER_CARGO_BBL)),
             ("FatimaZarah", VESSEL_CAPACITIES.get("FatimaZarah", DAUGHTER_CARGO_BBL))]:
    ds = DEAD_STOCK_FACTOR * vcap
    ax1.axhline(ds, color=ds_colors[vname], linestyle=":",
                alpha=0.8, linewidth=1.2,
                label=f"{vname} dead-stock ({ds:,.0f} bbl)")

ax1.set_ylabel("Volume (bbls)", fontsize=10, color="white")
ax1.set_title(
    f"Point A/C/D/E Storage — Prod std {PRODUCTION_RATE_BPH:,}, Duke {DUKE_PRODUCTION_RATE_BPH:,}, Starturn {STARTURN_PRODUCTION_RATE_BPH:,} bbl/hr",
    fontsize=11,
)
ax1.legend(loc="upper right", fontsize=7, facecolor="#0f3460", labelcolor="white", ncol=2)
ax1.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f"{int(x):,}"))
ax1.grid(True, alpha=0.2, color="#444")

# ── Chart 2: Mother vessel volume ────────────────────────────────
ax2 = axes[1]
ax2.fill_between(timeline_df["Time"], timeline_df["Mother_bbl"],
                 alpha=0.25, color="#2980b9")
ax2.plot(timeline_df["Time"], timeline_df["Mother_bbl"],
            color="#2980b9", linewidth=2, label="Point B Total Mother Volume")
ax2.plot(timeline_df["Time"], timeline_df["Bryanston_bbl"],
            color="#16a085", linewidth=1.4, alpha=0.9, label=f"{MOTHER_PRIMARY_NAME} Volume")
ax2.plot(timeline_df["Time"], timeline_df["GreenEagle_bbl"],
            color="#8e44ad", linewidth=1.4, alpha=0.9, label=f"{MOTHER_SECONDARY_NAME} Volume")
ax2.plot(timeline_df["Time"], timeline_df["Alkebulan_bbl"],
            color="#16a085", linewidth=1.4, alpha=0.9, label=f"{MOTHER_QUINARY_NAME} Volume")
ax2.axhline(MOTHER_EXPORT_TRIGGER_BY_NAME[MOTHER_PRIMARY_NAME], color="#e74c3c", linestyle="--", alpha=0.7,
          label=(f"{MOTHER_PRIMARY_NAME} Export Trigger "
              f"({MOTHER_EXPORT_TRIGGER_BY_NAME[MOTHER_PRIMARY_NAME]:,} bbl)"))
ax2.axhline(MOTHER_EXPORT_TRIGGER_BY_NAME[MOTHER_SECONDARY_NAME], color="#8e44ad", linestyle="--", alpha=0.7,
          label=(f"{MOTHER_SECONDARY_NAME} Export Trigger "
              f"({MOTHER_EXPORT_TRIGGER_BY_NAME[MOTHER_SECONDARY_NAME]:,} bbl)"))
ax2.axhline(MOTHER_CAPACITY_BY_NAME[MOTHER_PRIMARY_NAME], color="#922b21", linestyle="-.", alpha=0.5,
          label=f"{MOTHER_PRIMARY_NAME} Max Capacity ({MOTHER_CAPACITY_BY_NAME[MOTHER_PRIMARY_NAME]:,} bbl)")
ax2.axhline(MOTHER_CAPACITY_BY_NAME[MOTHER_SECONDARY_NAME], color="#7f8c8d", linestyle="-.", alpha=0.5,
          label=(f"{MOTHER_SECONDARY_NAME} Max Capacity "
              f"({MOTHER_CAPACITY_BY_NAME[MOTHER_SECONDARY_NAME]:,} bbl)"))
ax2.set_ylabel("Volume (bbls)", fontsize=10, color="white")
ax2.set_title(
    f"Point B Mothers ({MOTHER_PRIMARY_NAME} + {MOTHER_SECONDARY_NAME} + {MOTHER_QUINARY_NAME}) — Volume Level",
    fontsize=11,
)
ax2.legend(loc="upper right", fontsize=8, facecolor="#0f3460", labelcolor="white")
ax2.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f"{int(x):,}"))
ax2.grid(True, alpha=0.2, color="#444")

# ── Chart 3: Gantt — vessel-colour-coded status bars ─────────────
ax3 = axes[2]
vessel_names = [v.name for v in sim.vessels]
y_pos = {name: i for i, name in enumerate(vessel_names)}

for _, row in timeline_df.iterrows():
    for vn in vessel_names:
        if vn in row and pd.notna(row[vn]):
            color = vessel_status_color(vn, row[vn])
            ax3.barh(y_pos[vn], TIME_STEP_HOURS / 24,
                     left=row["Day"] - 1 + (row["Time"].hour + row["Time"].minute/60) / 24,
                     color=color, edgecolor="none", height=0.65)

ax3.set_yticks(list(y_pos.values()))
ax3.set_yticklabels(list(y_pos.keys()), color="white", fontsize=11, fontweight="bold")
for label, vn in zip(ax3.get_yticklabels(), vessel_names):
    label.set_color(VESSEL_COLORS.get(vn, "white"))

ax3.set_xlabel("Simulation Day", fontsize=10, color="white")
ax3.set_title("Daughter Vessel Status Timeline — colour = vessel, shade = activity", fontsize=11)
ax3.set_xlim(0, SIMULATION_DAYS)
ax3.grid(True, alpha=0.15, color="#444", axis="x")

# Build legend: vessel colour swatches + key status shades
legend_items = []
for vn in vessel_names:
    base = VESSEL_COLORS.get(vn, "#95a5a6")
    legend_items.append(mpatches.Patch(color=base, label=f"── {vn} ──"))
    for status, label in [
        ("IDLE_A",              "Idle at storage (light)"),
        ("LOADING",             "Loading (base colour)"),
        ("WAITING_DEAD_STOCK",  "Waiting dead-stock"),
        ("SAILING_AB",          "Sailing A→B"),
        ("DISCHARGING",         "Discharging (dark)"),
        ("SAILING_BA",          "Returning B→(A/C/D/E)"),
    ]:
        legend_items.append(
            mpatches.Patch(color=vessel_status_color(vn, status),
                           label=f"  {label}")
        )

ax3.legend(handles=legend_items, loc="lower right", fontsize=6.5,
           facecolor="#0f3460", labelcolor="white", ncol=4,
           handlelength=1.5, handleheight=1.2)

plt.tight_layout(rect=[0, 0, 1, 0.97])

import os
# Prefer a writable output directory next to this script.
# If unavailable, fall back to the user's home directory.
script_dir = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.path.join(script_dir, "outputs")
try:
    os.makedirs(OUTPUT_DIR, exist_ok=True)
except PermissionError:
    OUTPUT_DIR = os.path.join(os.path.expanduser("~"), "tanker_outputs")
    os.makedirs(OUTPUT_DIR, exist_ok=True)
chart_path = os.path.join(OUTPUT_DIR, "tanker_simulation_charts_v5.png")
plt.savefig(chart_path, dpi=150, bbox_inches="tight", facecolor=fig.get_facecolor())
print(f"\n[OK] Charts saved to {chart_path}")

def safe_csv_write(df, base_filename):
    path = os.path.join(OUTPUT_DIR, base_filename)
    try:
        df.to_csv(path, index=False)
        return path
    except PermissionError:
        stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        stem, ext = os.path.splitext(base_filename)
        fallback_name = f"{stem}_{stamp}{ext}"
        fallback_path = os.path.join(OUTPUT_DIR, fallback_name)
        df.to_csv(fallback_path, index=False)
        print(f"[WARN] {base_filename} is locked. Saved fallback file: {fallback_path}")
        return fallback_path

if POINT_B_DISTRIBUTION_TEST_MODE:
    event_log_path = safe_csv_write(log_df, "tanker_event_log_point_b_3day_test.csv")
    timeline_path = safe_csv_write(timeline_df, "tanker_timeline_point_b_3day_test.csv")
else:
    event_log_path = safe_csv_write(log_df, "tanker_event_log_v5.csv")
    timeline_path = safe_csv_write(timeline_df, "tanker_timeline_v5.csv")
print("[OK] CSVs saved.")

