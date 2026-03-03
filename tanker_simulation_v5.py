"""
=============================================================
OIL TANKER DAUGHTER VESSEL OPERATION SIMULATION  (v4)
=============================================================
Simulates the continuous loading/offloading cycle between:
    - Storage Vessel (Chapel / Point A)  - capacity 800,000 bbls
    - Daughter Vessels: Sherlock, Laphroaig, Rathbone, Bedford, Balham, Woodstock, Bagshot
    - Mother Vessel (Bryanston / Point B) - capacity 550,000 bbls

v5 changes — Multi-point independent storage loading at Point A/C/D/E:
    Point A has two active storage load points (Chapel and JasmineS).
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
from datetime import datetime, timedelta
import random

# -----------------------------------------------------------------
# CONFIGURATION
# -----------------------------------------------------------------
SIMULATION_DAYS        = 30          # How many days to simulate
DAUGHTER_CARGO_BBL     = 85_000      # Fixed load per voyage

VESSEL_NAMES           = [
    "Sherlock",    # loads 1st in every cycle
    "Laphroaig",   # loads 2nd
    "Rathbone",    # loads 3rd
    "Bedford",     # loads 4th
    "Balham",      # loads 5th
    "Woodstock",   # loads 6th
    "Bagshot",     # loads 7th
    "Watson",      # loads 8th (Point A only)
]
VESSEL_CAPACITIES      = {
    "Rathbone" : 44_000,
    "Bedford"  : 85_000,
    "Balham"   : 85_000,
    "Woodstock": 42_000,
    "Bagshot"  : 43_000,
    "Watson"   : 85_000,
}
NUM_DAUGHTERS          = len(VESSEL_NAMES)

MAX_DAUGHTER_CARGO = max(VESSEL_CAPACITIES.values(), default=DAUGHTER_CARGO_BBL)

STORAGE_CAPACITY_BBL   = 270_000
MOTHER_CAPACITY_BBL    = 550_000
STORAGE_INIT_BBL       = 400_000
MOTHER_INIT_BBL        = 0
PRODUCTION_RATE_BPH    = 1_700        # barrels per hour (replaces daily rate)
WESTMORE_PRODUCTION_RATE_BPH = 833
DUKE_PRODUCTION_RATE_BPH = 250
DUKE_STORAGE_CAPACITY_BBL = 90_000
STARTURN_PRODUCTION_RATE_BPH = 83
STARTURN_STORAGE_CAPACITY_BBL = 70_000
DUKE_STARTURN_DEAD_STOCK_BBL = 42_000
DEAD_STOCK_FACTOR      = 1.75         # vessel must wait until 175% of its cargo is available
SAIL_HOURS_A_TO_B      = 6
SAIL_HOURS_B_TO_A      = 6
SAIL_HOURS_D_TO_CHANNEL = 3
SAIL_HOURS_CHANNEL_TO_B = 3
BERTHING_DELAY_HOURS   = 0.5
POST_BERTHING_START_GAP_HOURS = 0.5
POST_MOTHER_BERTHING_START_GAP_HOURS = 1.0
HOSE_CONNECTION_HOURS  = 2.0
LOAD_HOURS             = 12
DUKE_LOAD_RATE_BPH     = 3_500
STARTURN_LOAD_RATE_BPH = 2_500
POINT_F_LOAD_RATE_BPH  = 165
POINT_F_SWAP_HOURS     = 2
POINT_F_MIN_TRIGGER_BBL = 65_000
STARTURN_PRE_TANK_TOP_TRIGGER_RATIO = 0.90
DUKE_PRE_TANK_TOP_TRIGGER_RATIO = 0.90
PRE_TANK_TOP_TRIGGER_RATIO_DEFAULT = 0.90
DUKE_MIN_REMAINING_BBL = 7_500
STARTURN_MIN_REMAINING_BBL = 5_000
DISCHARGE_HOURS        = 12
CAST_OFF_HOURS         = 0.2
CAST_OFF_START         = 6
CAST_OFF_END           = 17.5
BERTHING_START         = 6
BERTHING_END           = 18
DAYLIGHT_START         = 6
DAYLIGHT_END           = 18
MOTHER_EXPORT_TRIGGER  = MOTHER_CAPACITY_BBL - MAX_DAUGHTER_CARGO
MOTHER_EXPORT_VOLUME   = 400_000
TIME_STEP_HOURS        = 0.5
EXPORT_RATE_BPH        = 16_000
EXPORT_DOC_HOURS       = 2
EXPORT_SAIL_HOURS      = 6
EXPORT_SAIL_WINDOW_START = 6
EXPORT_SAIL_WINDOW_END   = 15
EXPORT_HOSE_HOURS       = 4
EXPORT_SERIES_BUFFER_HOURS = 56

STORAGE_PRIMARY_NAME = "Chapel"
STORAGE_SECONDARY_NAME = "JasmineS"
STORAGE_TERTIARY_NAME = "Westmore"
STORAGE_QUATERNARY_NAME = "Duke"
STORAGE_QUINARY_NAME = "Starturn"
WESTMORE_PERMITTED_VESSELS = {"Sherlock", "Laphroaig", "Bagshot", "Rathbone", "Watson"}
DUKE_PERMITTED_VESSELS = {"Woodstock", "Bagshot", "Rathbone"}
STARTURN_PERMITTED_VESSELS = {"Woodstock", "Rathbone"}
POINT_A_ONLY_VESSELS = {"Watson"}
STORAGE_NAMES = [
    STORAGE_PRIMARY_NAME,
    STORAGE_SECONDARY_NAME,
    STORAGE_TERTIARY_NAME,
    STORAGE_QUATERNARY_NAME,
    STORAGE_QUINARY_NAME,
]
STORAGE_POINT = {
    STORAGE_PRIMARY_NAME: "A",
    STORAGE_SECONDARY_NAME: "A",
    STORAGE_TERTIARY_NAME: "C",
    STORAGE_QUATERNARY_NAME: "D",
    STORAGE_QUINARY_NAME: "E",
}
STORAGE_CAPACITY_BY_NAME = {name: STORAGE_CAPACITY_BBL for name in STORAGE_NAMES}
STORAGE_CAPACITY_BY_NAME[STORAGE_SECONDARY_NAME] = 290_000
STORAGE_CAPACITY_BY_NAME[STORAGE_TERTIARY_NAME] = 290_000
STORAGE_CAPACITY_BY_NAME[STORAGE_QUATERNARY_NAME] = DUKE_STORAGE_CAPACITY_BBL
STORAGE_CAPACITY_BY_NAME[STORAGE_QUINARY_NAME] = STARTURN_STORAGE_CAPACITY_BBL
STORAGE_PRODUCTION_RATE_BY_NAME = {name: PRODUCTION_RATE_BPH for name in STORAGE_NAMES}
STORAGE_PRODUCTION_RATE_BY_NAME[STORAGE_TERTIARY_NAME] = WESTMORE_PRODUCTION_RATE_BPH
STORAGE_PRODUCTION_RATE_BY_NAME[STORAGE_QUATERNARY_NAME] = DUKE_PRODUCTION_RATE_BPH
STORAGE_PRODUCTION_RATE_BY_NAME[STORAGE_QUINARY_NAME] = STARTURN_PRODUCTION_RATE_BPH
STORAGE_CRITICAL_THRESHOLD_BY_NAME = {
    STORAGE_PRIMARY_NAME: 270_000,
    STORAGE_SECONDARY_NAME: 290_000,
    STORAGE_TERTIARY_NAME: 290_000,
    STORAGE_QUATERNARY_NAME: 90_000,
    STORAGE_QUINARY_NAME: 70_000,
}
MOTHER_PRIMARY_NAME = "Bryanston"
MOTHER_SECONDARY_NAME = "Alkebulan"
MOTHER_TERTIARY_NAME = "GreenEagle"
MOTHER_NAMES = [MOTHER_PRIMARY_NAME, MOTHER_SECONDARY_NAME, MOTHER_TERTIARY_NAME]
MOTHER_DISCHARGE_SEQUENCE = [MOTHER_PRIMARY_NAME, MOTHER_SECONDARY_NAME, MOTHER_TERTIARY_NAME]

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
    "EXPORT_SAIL"       : "Sailing to export terminal",
    "EXPORT_HOSE"       : "Hose connection at export terminal",
    "SAILING_AB"        : "Sailing A -> Fairway Buoy",
    "SAILING_AB_LEG2"   : "Sailing Fairway -> B",
    "SAILING_D_CHANNEL" : "Sailing D -> Cawthorne Channel",
    "WAITING_BERTH_B"   : "Waiting for berthing window at Point B mother",
    "BERTHING_B"        : "Berthing at Point B mother",
    "HOSE_CONNECT_B"    : "Hose connection at Point B mother",
    "IDLE_B"            : "Idle at Point B mother",
    "DISCHARGING"       : "Discharging to Point B mother",
    "SAILING_BA"        : "Returning B -> selected loading point (A/C/D/E)",
    "WAITING_DAYLIGHT"  : "Waiting for Daylight Window",
    "WAITING_FAIRWAY"   : "Waiting at Fairway Buoy",
    "WAITING_MOTHER_CAPACITY" : "Waiting for space on mother vessel",
    "WAITING_MOTHER_RETURN" : "Waiting for mother to return from export",
    "WAITING_DEAD_STOCK"    : "Berthed — waiting for dead-stock threshold",
    "WAITING_RETURN_STOCK"  : "Waiting at Point B until return destination can load immediately",
    "PF_LOADING"            : "Loading at Point F",
    "PF_SWAP"               : "Point F swap/takeover in progress",
}


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
        self.queue_position = None
        self.assigned_storage = None
        self.assigned_load_hours = None
        self.assigned_mother = None
        self.target_point = "A"
        # FIX 1: track exact arrival hour at Point B for FIFO queue ordering
        self.arrival_at_b = None

    def __repr__(self):
        return f"{self.name}[{self.status}|cargo={self.cargo_bbl:,}bbl]"


class Simulation:
    def __init__(self):
        self.storage_bbl = {
            name: min(STORAGE_INIT_BBL, STORAGE_CAPACITY_BY_NAME[name])
            for name in STORAGE_NAMES
        }
        self.mother_bbl = {name: MOTHER_INIT_BBL for name in MOTHER_NAMES}
        self.total_exported = 0
        self.total_produced = 0
        self.total_spilled = 0
        self.storage_overflow_bbl = {name: 0.0 for name in STORAGE_NAMES}
        self.point_f_overflow_accum_bbl = 0.0
        self.storage_overflow_events = 0
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
        self.point_b_day_allocation_count = {}
        self.next_mother_discharge_sequence_idx = 0
        self.storage_critical_active = {name: False for name in STORAGE_NAMES}
        self.point_f_vessels = ["Bedford", "Balham"]
        self.point_f_active_loader = "Balham"
        self.point_f_swap_pending_for = None
        self.point_f_swap_triggered_by = None

        offsets = [0] * NUM_DAUGHTERS      # all vessels wake up simultaneously
        self.vessels = []
        for i in range(NUM_DAUGHTERS):
            name = VESSEL_NAMES[i]
            cap = VESSEL_CAPACITIES.get(name, DAUGHTER_CARGO_BBL)
            self.vessels.append(DaughterVessel(name, offsets[i], cargo_capacity=cap))
        self.total_loaded = 0
        for vv in self.vessels:
            if vv.name == "Balham":
                vv.target_point = "F"
                vv.cargo_bbl = 10_000
                vv.next_event_time = 0.0
            elif vv.name == "Woodstock":
                vv.target_point = "D"
                vv.next_event_time = 0.0

    def point_f_other_vessel(self, vessel_name):
        return next((name for name in self.point_f_vessels if name != vessel_name), None)

    def point_f_active_loading_bbl(self):
        for vv in self.vessels:
            if vv.name == self.point_f_active_loader and vv.status in {"PF_LOADING", "IDLE_A"}:
                return vv.cargo_bbl
        return 0.0

    def total_storage_bbl(self):
        return sum(self.storage_bbl.values())

    def total_mother_bbl(self):
        return sum(self.mother_bbl.values())

    def storage_load_hours(self, storage_name, cargo_bbl):
        if storage_name == STORAGE_QUATERNARY_NAME:
            return cargo_bbl / DUKE_LOAD_RATE_BPH
        if storage_name == STORAGE_QUINARY_NAME:
            return cargo_bbl / STARTURN_LOAD_RATE_BPH
        return LOAD_HOURS

    def is_mother_idle(self, mother_name, at_hour):
        if self.export_state[mother_name] is not None:
            return False
        if at_hour < self.mother_available_at[mother_name]:
            return False
        if at_hour < self.mother_berth_free_at[mother_name]:
            return False
        active_daughter_ops = {
            "WAITING_BERTH_B",
            "BERTHING_B",
            "HOSE_CONNECT_B",
            "DISCHARGING",
            "CAST_OFF_B",
        }
        for vv in self.vessels:
            if vv.assigned_mother == mother_name and vv.status in active_daughter_ops:
                return False
        return True

    def next_five_pm(self, current_hour):
        days_elapsed = int(current_hour // 24)
        five_pm_today = days_elapsed * 24 + 17
        if current_hour <= five_pm_today:
            return five_pm_today
        return (days_elapsed + 1) * 24 + 17

    def mother_idle_by_hour(self, mother_name, cutoff_hour):
        if self.export_state[mother_name] is not None:
            return False
        idle_from = max(self.mother_available_at[mother_name], self.mother_berth_free_at[mother_name])
        return idle_from <= cutoff_hour

    def loading_start_threshold(self, storage_name, cargo_bbl):
        if storage_name in (STORAGE_QUATERNARY_NAME, STORAGE_QUINARY_NAME):
            required = max(cargo_bbl + DUKE_STARTURN_DEAD_STOCK_BBL,
                           STORAGE_CRITICAL_THRESHOLD_BY_NAME[storage_name])
            if storage_name == STORAGE_QUATERNARY_NAME:
                required = max(required, cargo_bbl + DUKE_MIN_REMAINING_BBL)
            if storage_name == STORAGE_QUINARY_NAME:
                required = max(required, cargo_bbl + STARTURN_MIN_REMAINING_BBL)
            return min(required, STORAGE_CAPACITY_BY_NAME[storage_name])
        required = DEAD_STOCK_FACTOR * cargo_bbl
        return min(required, STORAGE_CAPACITY_BY_NAME[storage_name])

    def storage_allowed_for_vessel(self, storage_name, vessel_name):
        if vessel_name in POINT_A_ONLY_VESSELS and STORAGE_POINT.get(storage_name) != "A":
            return False
        if storage_name == STORAGE_TERTIARY_NAME and vessel_name not in WESTMORE_PERMITTED_VESSELS:
            return False
        if storage_name == STORAGE_QUATERNARY_NAME and vessel_name not in DUKE_PERMITTED_VESSELS:
            return False
        if storage_name == STORAGE_QUINARY_NAME and vessel_name not in STARTURN_PERMITTED_VESSELS:
            return False
        return True

    def storage_min_remaining_after_load(self, storage_name):
        if storage_name == STORAGE_QUATERNARY_NAME:
            return DUKE_MIN_REMAINING_BBL
        if storage_name == STORAGE_QUINARY_NAME:
            return STARTURN_MIN_REMAINING_BBL
        return 0.0

    def return_allocation_candidate(self, cargo_bbl, vessel_name):
        # Consider every storage this vessel is permitted to load from.
        allowed_storages = [
            name for name in STORAGE_NAMES
            if self.storage_allowed_for_vessel(name, vessel_name)
        ]
        threshold_by_storage = {
            name: self.loading_start_threshold(name, cargo_bbl)
            for name in allowed_storages
        }

        pre_tank_top_candidates = []

        trigger_ratio_by_storage = {
            STORAGE_QUATERNARY_NAME: DUKE_PRE_TANK_TOP_TRIGGER_RATIO,
            STORAGE_QUINARY_NAME: STARTURN_PRE_TANK_TOP_TRIGGER_RATIO,
        }
        for storage_name in allowed_storages:
            stock = self.storage_bbl[storage_name]
            cap = STORAGE_CAPACITY_BY_NAME[storage_name]
            trigger_ratio = trigger_ratio_by_storage.get(storage_name, PRE_TANK_TOP_TRIGGER_RATIO_DEFAULT)
            pre_tank_top_trigger = cap * trigger_ratio
            reserve_required = self.storage_min_remaining_after_load(storage_name)
            if (
                stock >= pre_tank_top_trigger
                and stock >= (cargo_bbl + reserve_required)
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
            return None, None, threshold_by_storage

        def rank_key(storage_name):
            stock = self.storage_bbl[storage_name]
            critical = STORAGE_CRITICAL_THRESHOLD_BY_NAME[storage_name]
            above_critical = 0 if stock >= critical else 1
            critical_distance = abs(stock - critical)
            return (above_critical, critical_distance, -stock, storage_name)

        selected = min(eligible, key=rank_key)
        return selected, threshold_by_storage[selected], threshold_by_storage

    # -- Helpers ----------------------------------------------------------
    def hours_to_dt(self, h):
        return datetime(2025, 1, 1) + timedelta(hours=h)

    def next_daylight_sail(self, current_hour):
        day_h = current_hour % 24
        if DAYLIGHT_START <= day_h < DAYLIGHT_END:
            return current_hour
        days_elapsed = int(current_hour // 24)
        if day_h < DAYLIGHT_START:
            return days_elapsed * 24 + DAYLIGHT_START
        else:
            return (days_elapsed + 1) * 24 + DAYLIGHT_START

    def next_export_sail_start(self, current_hour):
        day_h = current_hour % 24
        if EXPORT_SAIL_WINDOW_START <= day_h < EXPORT_SAIL_WINDOW_END:
            return current_hour
        days_elapsed = int(current_hour // 24)
        if day_h < EXPORT_SAIL_WINDOW_START:
            return days_elapsed * 24 + EXPORT_SAIL_WINDOW_START
        else:
            return (days_elapsed + 1) * 24 + EXPORT_SAIL_WINDOW_START

    def next_cast_off_window(self, current_hour):
        day_h = current_hour % 24
        if CAST_OFF_START <= day_h < CAST_OFF_END:
            return current_hour
        days_elapsed = int(current_hour // 24)
        if day_h < CAST_OFF_START:
            return days_elapsed * 24 + CAST_OFF_START
        else:
            return (days_elapsed + 1) * 24 + CAST_OFF_START

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

    def is_any_vessel_discharging(self):
        for v in self.vessels:
            if v.status == "DISCHARGING":
                return True
        return False

    def is_valid_berthing_time(self, hour, point=None):
        day_h = hour % 24
        return BERTHING_START <= day_h < BERTHING_END and not self.is_any_vessel_casting_off(point)

    def next_berthing_window(self, current_hour, point=None):
        day_h = current_hour % 24
        if BERTHING_START <= day_h < BERTHING_END and not self.is_any_vessel_casting_off(point):
            return current_hour
        check_time = current_hour
        for _ in range(48):
            check_h = check_time % 24
            if BERTHING_START <= check_h < BERTHING_END and not self.is_any_vessel_casting_off(point):
                return check_time
            check_time += 1
        days = int(current_hour // 24)
        return (days + 1) * 24 + BERTHING_START

    def log_event(self, t, vessel_name, event, detail="", voyage_num=None):
        self.log.append({
            "Time"       : self.hours_to_dt(t).strftime("%Y-%m-%d %H:%M"),
            "Day"        : int(t // 24) + 1,
            "Hour"       : f"{int(t % 24):02d}:{int((t % 1)*60):02d}",
            "Vessel"     : vessel_name,
            "Voyage"     : voyage_num,
            "Event"      : event,
            "Detail"     : detail,
            "Storage_bbl": round(self.total_storage_bbl()),
            "Chapel_bbl": round(self.storage_bbl[STORAGE_PRIMARY_NAME]),
            "JasmineS_bbl": round(self.storage_bbl[STORAGE_SECONDARY_NAME]),
            "Westmore_bbl": round(self.storage_bbl[STORAGE_TERTIARY_NAME]),
            "Duke_bbl": round(self.storage_bbl[STORAGE_QUATERNARY_NAME]),
            "Starturn_bbl": round(self.storage_bbl[STORAGE_QUINARY_NAME]),
            "Storage_Overflow_Accum_bbl": round(sum(self.storage_overflow_bbl.values())),
            "Chapel_Overflow_Accum_bbl": round(self.storage_overflow_bbl[STORAGE_PRIMARY_NAME]),
            "JasmineS_Overflow_Accum_bbl": round(self.storage_overflow_bbl[STORAGE_SECONDARY_NAME]),
            "Westmore_Overflow_Accum_bbl": round(self.storage_overflow_bbl[STORAGE_TERTIARY_NAME]),
            "Duke_Overflow_Accum_bbl": round(self.storage_overflow_bbl[STORAGE_QUATERNARY_NAME]),
            "Starturn_Overflow_Accum_bbl": round(self.storage_overflow_bbl[STORAGE_QUINARY_NAME]),
            "PointF_Overflow_Accum_bbl": round(self.point_f_overflow_accum_bbl),
            "PointF_Active_Loading_bbl": round(self.point_f_active_loading_bbl()),
            "Mother_bbl" : round(self.total_mother_bbl()),
            "Bryanston_bbl": round(self.mother_bbl[MOTHER_PRIMARY_NAME]),
            "Alkebulan_bbl": round(self.mother_bbl[MOTHER_SECONDARY_NAME]),
            "GreenEagle_bbl": round(self.mother_bbl[MOTHER_TERTIARY_NAME]),
            "Total_Exported_bbl": self.total_exported,
        })

    # -- Main simulation loop ---------------------------------------------
    def run(self):
        total_hours = SIMULATION_DAYS * 24
        t = 0.0

        while t <= total_hours:
            # 1. Continuous production at all storage locations (non-stop)
            for storage_name in STORAGE_NAMES:
                prod = STORAGE_PRODUCTION_RATE_BY_NAME[storage_name] * TIME_STEP_HOURS
                cap = STORAGE_CAPACITY_BY_NAME[storage_name]
                self.total_produced += prod
                projected = self.storage_bbl[storage_name] + prod
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
            for v in self.vessels:
                if t < v.next_event_time:
                    continue

                if v.status == "PF_LOADING":
                    increment = POINT_F_LOAD_RATE_BPH * TIME_STEP_HOURS
                    if v.cargo_bbl < v.cargo_capacity:
                        v.cargo_bbl = min(v.cargo_capacity, v.cargo_bbl + increment)
                    if v.cargo_bbl > POINT_F_MIN_TRIGGER_BBL:
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
                            and alternate_vessel.cargo_bbl <= 0
                        )
                        daylight_now = DAYLIGHT_START <= (t % 24) < DAYLIGHT_END

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
                    returned_from_overflow = min(self.point_f_overflow_accum_bbl, max(0.0, v.cargo_capacity - v.cargo_bbl))
                    v.cargo_bbl += returned_from_overflow
                    self.point_f_overflow_accum_bbl -= returned_from_overflow
                    v.status = "PF_LOADING"
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
                    if v.name == self.point_f_active_loader:
                        if v.cargo_bbl < v.cargo_capacity:
                            v.cargo_bbl = min(
                                v.cargo_capacity,
                                v.cargo_bbl + (POINT_F_LOAD_RATE_BPH * TIME_STEP_HOURS),
                            )
                        day_h = t % 24
                        if not (DAYLIGHT_START <= day_h < DAYLIGHT_END):
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
                        v.next_event_time = t
                        continue

                    # Only assign a new voyage number on a fresh cycle start.
                    if not hasattr(v, '_voyage_assigned') or not v._voyage_assigned:
                        self.voyage_counter += 1
                        v.current_voyage = self.voyage_counter
                        v._voyage_assigned = True
                    cap = v.cargo_capacity

                    eligible_storage_names = [
                        name for name in STORAGE_NAMES
                        if STORAGE_POINT.get(name) == v.target_point
                    ]
                    if not eligible_storage_names:
                        eligible_storage_names = STORAGE_NAMES

                    candidate_storages = []
                    for storage_name in eligible_storage_names:
                        if not self.storage_allowed_for_vessel(storage_name, v.name):
                            continue
                        stock = self.storage_bbl[storage_name]
                        if stock < cap:
                            continue
                        storage_point = STORAGE_POINT.get(storage_name, "A")
                        threshold_required = self.loading_start_threshold(storage_name, cap)
                        berth_t = self.next_berthing_window(t, point=storage_point)
                        start = max(
                            berth_t,
                            self.storage_berth_free_at[storage_name],
                            self.next_storage_berthing_start_at[storage_point],
                        )
                        if not self.is_valid_berthing_time(start, point=storage_point):
                            start = self.next_berthing_window(start, point=storage_point)
                        if start < self.next_storage_berthing_start_at[storage_point]:
                            start = self.next_storage_berthing_start_at[storage_point]
                            if not self.is_valid_berthing_time(start, point=storage_point):
                                start = self.next_berthing_window(start, point=storage_point)
                        candidate_storages.append((storage_name, stock, berth_t, start, threshold_required))

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
                                0 if x[1] >= x[4] else 1,
                                x[3],
                                -x[1]
                            )
                        )
                        selected_storage, selected_stock, berth_t, start, threshold_required = candidate_storages[0]
                        wait_berth_window = berth_t - t
                        v.assigned_storage = selected_storage
                        load_hours = self.storage_load_hours(selected_storage, cap)
                        v.assigned_load_hours = load_hours

                        # The berth is reserved. We do NOT pre-commit stock
                        # here because the dead-stock rule may delay the
                        # actual loading start — stock is committed only once
                        # the 175% threshold is confirmed in HOSE_CONNECT_A.
                        v.status = "BERTHING_A"
                        selected_point = STORAGE_POINT.get(selected_storage, "A")
                        self.storage_berth_free_at[selected_storage] = (
                            start + BERTHING_DELAY_HOURS + HOSE_CONNECTION_HOURS + load_hours
                        )
                        self.next_storage_berthing_start_at[selected_point] = (
                            start + BERTHING_DELAY_HOURS + POST_BERTHING_START_GAP_HOURS
                        )
                        v.next_event_time = start + BERTHING_DELAY_HOURS

                        slot = VESSEL_NAMES.index(v.name) + 1
                        if wait_berth_window > 0.1:
                            self.log_event(t, v.name, "WAITING_BERTH_A",
                                           f"Waiting for berthing window at {selected_storage} | "
                                           f"Available at {self.hours_to_dt(berth_t).strftime('%Y-%m-%d %H:%M')}",
                                           voyage_num=v.current_voyage)
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
                                       f"Insufficient stock at Point {v.target_point} ({storage_levels} available, "
                                       f"need {cap:,} bbl min / {min_threshold:,.0f} bbl loading-start threshold) — waiting",
                                       voyage_num=v.current_voyage)

                elif v.status == "BERTHING_A":
                    v.status = "HOSE_CONNECT_A"
                    v.next_event_time = t + HOSE_CONNECTION_HOURS
                    berth_storage = v.assigned_storage or STORAGE_PRIMARY_NAME
                    self.log_event(t, v.name, "HOSE_CONNECTION_START_A",
                                   f"Hose connection initiated at {berth_storage} (2 hours)",
                                   voyage_num=v.current_voyage)

                elif v.status == "HOSE_CONNECT_A":
                    # ── Dead-stock rule enforced here ───────────────────
                    # Loading can only commence when storage holds at least
                    # the storage-specific loading-start threshold.
                    # For Duke/Starturn this is critical stock;
                    # other storages use 175% dead-stock. The cargo was
                    # NOT pre-committed in IDLE_A for the berth reservation;
                    # it is committed here once the threshold is satisfied.
                    cap = v.cargo_capacity
                    selected_storage = v.assigned_storage or STORAGE_PRIMARY_NAME
                    threshold_required = self.loading_start_threshold(selected_storage, cap)
                    load_hours = v.assigned_load_hours if v.assigned_load_hours is not None else LOAD_HOURS
                    if self.storage_bbl[selected_storage] < threshold_required:
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
                    self.storage_bbl[selected_storage] -= cap
                    v.cargo_bbl = cap
                    self.total_loaded += cap
                    v.status = "LOADING"
                    self.storage_berth_free_at[selected_storage] = max(
                        self.storage_berth_free_at[selected_storage], t + load_hours
                    )
                    v.next_event_time = t + load_hours
                    self.log_event(t, v.name, "LOADING_START",
                                   f"Loading {cap:,} bbl | {selected_storage}: {self.storage_bbl[selected_storage]:,.0f} bbl "
                                   f"(loading-start threshold {threshold_required:,.0f} bbl met, rate duration {load_hours:.1f}h)",
                                   voyage_num=v.current_voyage)

                elif v.status == "LOADING":
                    v.status = "DOCUMENTING"
                    v.next_event_time = t + 4
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
                    sail_t = self.next_daylight_sail(t)
                    wait = sail_t - t
                    if v.target_point == "D":
                        v.status = "SAILING_D_CHANNEL"
                        v.next_event_time = sail_t + SAIL_HOURS_D_TO_CHANNEL
                    else:
                        v.status = "SAILING_AB"
                        v.next_event_time = sail_t + SAIL_HOURS_A_TO_B
                    self.log_event(t, v.name, "CAST_OFF_COMPLETE",
                                   f"Cast-off complete | Departure "
                                   f"{self.hours_to_dt(sail_t).strftime('%H:%M')} (wait {wait:.1f}h)",
                                   voyage_num=v.current_voyage)
                    if wait > 0:
                        self.log_event(t, v.name, "WAITING_DAYLIGHT",
                                       f"Daylight window opens at "
                                       f"{self.hours_to_dt(sail_t).strftime('%Y-%m-%d %H:%M')}",
                                       voyage_num=v.current_voyage)

                elif v.status == "SAILING_D_CHANNEL":
                    arrival = t
                    self.log_event(arrival, v.name, "ARRIVED_CAWTHORNE_CHANNEL",
                                   "Reached Cawthorne channel (3h from Point D)",
                                   voyage_num=v.current_voyage)
                    day_h = arrival % 24
                    if DAYLIGHT_START <= day_h < DAYLIGHT_END:
                        continue_depart = arrival
                    else:
                        continue_depart = self.next_daylight_sail(arrival)
                        self.log_event(arrival, v.name, "WAITING_DAYLIGHT",
                                       f"At Cawthorne channel, waiting for daylight until "
                                       f"{self.hours_to_dt(continue_depart).strftime('%Y-%m-%d %H:%M')}",
                                       voyage_num=v.current_voyage)
                    v.status = "SAILING_AB_LEG2"
                    v.next_event_time = continue_depart + SAIL_HOURS_CHANNEL_TO_B

                elif v.status == "SAILING_AB":
                    arrival = t
                    self.log_event(arrival, v.name, "ARRIVED_FAIRWAY",
                                   "Reached fairway buoy (2h from Point B)",
                                   voyage_num=v.current_voyage)
                    hour = arrival % 24
                    if hour >= 19:
                        days = int(arrival // 24)
                        continue_depart = (days + 1) * 24 + 6
                        self.log_event(arrival, v.name, "WAITING_FAIRWAY",
                                       f"Arrived fairway after 19:00, holding until "
                                       f"{self.hours_to_dt(continue_depart).strftime('%Y-%m-%d %H:%M')}",
                                       voyage_num=v.current_voyage)
                    else:
                        continue_depart = arrival
                    v.status = "SAILING_AB_LEG2"
                    v.next_event_time = continue_depart + 2

                elif v.status == "SAILING_AB_LEG2":
                    arrival = t
                    v.arrival_at_b = arrival

                    hour = arrival % 24
                    if hour >= 18:
                        days = int(arrival // 24)
                        berthing_start = (days + 1) * 24 + 7
                        self.log_event(arrival, v.name, "WAITING_NIGHT",
                                       f"Arrived at {self.hours_to_dt(arrival).strftime('%H:%M')} after 18:00, "
                                       f"waiting until {self.hours_to_dt(berthing_start).strftime('%Y-%m-%d %H:%M')}",
                                       voyage_num=v.current_voyage)
                    else:
                        berthing_start = arrival

                    candidates = []
                    for mother_name in MOTHER_NAMES:
                        if self.mother_bbl[mother_name] + v.cargo_bbl > MOTHER_CAPACITY_BBL:
                            continue
                        earliest = max(berthing_start, self.mother_available_at[mother_name])
                        berth_t = self.next_berthing_window(earliest, point="B")
                        start = max(
                            berth_t,
                            self.mother_berth_free_at[mother_name],
                            self.mother_available_at[mother_name],
                            self.next_mother_berthing_start_at,
                        )
                        if not self.is_valid_berthing_time(start, point="B"):
                            start = self.next_berthing_window(start, point="B")
                        if start < self.next_mother_berthing_start_at:
                            start = self.next_mother_berthing_start_at
                            if not self.is_valid_berthing_time(start, point="B"):
                                start = self.next_berthing_window(start, point="B")
                        candidates.append((start, berth_t, mother_name))

                    if not candidates:
                        mother_levels = ", ".join(
                            f"{name}: {self.mother_bbl[name]:,.0f}/{MOTHER_CAPACITY_BBL:,} bbl"
                            for name in MOTHER_NAMES
                        )
                        self.log_event(arrival, v.name, "WAITING_MOTHER_CAPACITY",
                                       f"Insufficient capacity on Point B mothers ({mother_levels})",
                                       voyage_num=v.current_voyage)
                        v.next_event_time = arrival + 6
                    else:
                        day_key = int(arrival // 24)
                        assigned_today = self.point_b_day_allocation_count.get(day_key, 0)
                        ranked_mothers = sorted(MOTHER_NAMES, key=lambda name: (-self.mother_bbl[name], name))
                        rotation_idx = assigned_today % len(ranked_mothers)
                        prioritized_order = ranked_mothers[rotation_idx:] + ranked_mothers[:rotation_idx]

                        candidate_by_mother = {mother_name: (start, berth_t, mother_name) for start, berth_t, mother_name in candidates}
                        selected = None
                        used_idle_before_5pm_rule = False
                        used_idle_recheck_rule = False
                        used_sequence_rule = False

                        cutoff_5pm = self.next_five_pm(arrival)
                        idle_before_5pm_names = [
                            mother_name
                            for _, _, mother_name in candidates
                            if self.mother_idle_by_hour(mother_name, cutoff_5pm)
                        ]
                        if idle_before_5pm_names:
                            selected_idle_5pm = max(
                                idle_before_5pm_names,
                                key=lambda name: (self.mother_bbl[name], name),
                            )
                            selected = candidate_by_mother[selected_idle_5pm]
                            used_idle_before_5pm_rule = True

                        sequence_order = (
                            MOTHER_DISCHARGE_SEQUENCE[self.next_mother_discharge_sequence_idx:]
                            + MOTHER_DISCHARGE_SEQUENCE[:self.next_mother_discharge_sequence_idx]
                        )
                        if selected is None:
                            for mother_name in sequence_order:
                                if mother_name in candidate_by_mother:
                                    selected = candidate_by_mother[mother_name]
                                    used_sequence_rule = True
                                    break

                        if selected is None:
                            idle_candidate_names = [
                                mother_name
                                for _, _, mother_name in candidates
                                if self.is_mother_idle(mother_name, arrival)
                            ]
                            if idle_candidate_names:
                                selected_idle = max(
                                    idle_candidate_names,
                                    key=lambda name: (self.mother_bbl[name], name),
                                )
                                selected = candidate_by_mother[selected_idle]
                                used_idle_recheck_rule = True

                        if selected is None:
                            for mother_name in prioritized_order:
                                if mother_name in candidate_by_mother:
                                    selected = candidate_by_mother[mother_name]
                                    break

                        if selected is None:
                            candidates.sort(key=lambda x: (x[0], x[2]))
                            selected = candidates[0]

                        start, berth_t, selected_mother = selected
                        self.point_b_day_allocation_count[day_key] = assigned_today + 1
                        seq_pos = MOTHER_DISCHARGE_SEQUENCE.index(selected_mother)
                        self.next_mother_discharge_sequence_idx = (seq_pos + 1) % len(MOTHER_DISCHARGE_SEQUENCE)
                        v.assigned_mother = selected_mother
                        v.status = "BERTHING_B"
                        self.mother_berth_free_at[selected_mother] = (
                            start + BERTHING_DELAY_HOURS + HOSE_CONNECTION_HOURS + DISCHARGE_HOURS
                        )
                        self.next_mother_berthing_start_at = (
                            start + BERTHING_DELAY_HOURS + POST_MOTHER_BERTHING_START_GAP_HOURS
                        )
                        v.next_event_time = start + BERTHING_DELAY_HOURS
                        if berth_t > berthing_start + 0.1:
                            self.log_event(berthing_start, v.name, "WAITING_BERTH_B",
                                           f"Waiting for berthing window at {selected_mother} | "
                                           f"Available at {self.hours_to_dt(berth_t).strftime('%Y-%m-%d %H:%M')}",
                                           voyage_num=v.current_voyage)
                        self.log_event(
                            arrival,
                            v.name,
                            "MOTHER_PRIORITY_ASSIGNMENT",
                            f"Day {day_key + 1} priority slot {assigned_today + 1}: assigned to {selected_mother} "
                            f"from stock ranking {', '.join(ranked_mothers)} "
                            f"({'idle-before-5pm-highest-volume' if used_idle_before_5pm_rule else ('sequence-priority' if used_sequence_rule else ('idle-highest-volume recheck' if used_idle_recheck_rule else 'non-idle fallback'))})",
                            voyage_num=v.current_voyage,
                        )
                        self.log_event(
                            arrival,
                            v.name,
                            "MOTHER_SEQUENCE_ASSIGNMENT",
                            f"Discharge sequence target order {', '.join(sequence_order)} | "
                            f"selected {selected_mother} "
                            f"({'idle-before-5pm-highest-volume' if used_idle_before_5pm_rule else ('idle-highest-volume' if used_idle_recheck_rule else ('sequence' if used_sequence_rule else 'fallback'))})",
                            voyage_num=v.current_voyage,
                        )
                        self.log_event(start, v.name, "BERTHING_START_B",
                                       f"Berthing at {selected_mother} (30 min procedure)",
                                       voyage_num=v.current_voyage)

                elif v.status == "BERTHING_B":
                    v.status = "HOSE_CONNECT_B"
                    v.next_event_time = t + HOSE_CONNECTION_HOURS
                    selected_mother = v.assigned_mother or MOTHER_PRIMARY_NAME
                    self.log_event(t, v.name, "HOSE_CONNECTION_START_B",
                                   f"Hose connection initiated at {selected_mother} (2 hours)",
                                   voyage_num=v.current_voyage)

                elif v.status == "HOSE_CONNECT_B":
                    selected_mother = v.assigned_mother or MOTHER_PRIMARY_NAME
                    if self.mother_bbl[selected_mother] + v.cargo_bbl > MOTHER_CAPACITY_BBL:
                        self.log_event(t, v.name, "WAITING_MOTHER_CAPACITY",
                                       f"Cannot start discharge - {selected_mother} lacks space",
                                       voyage_num=v.current_voyage)
                        v.next_event_time = t + 6
                    else:
                        self.mother_bbl[selected_mother] += v.cargo_bbl
                        v.status = "DISCHARGING"
                        self.mother_berth_free_at[selected_mother] = max(
                            self.mother_berth_free_at[selected_mother], t + DISCHARGE_HOURS
                        )
                        v.next_event_time = t + DISCHARGE_HOURS
                        self.log_event(t, v.name, "DISCHARGE_START",
                                       f"Discharging {v.cargo_bbl:,} bbl | {selected_mother}: {self.mother_bbl[selected_mother]:,.0f} bbl",
                                       voyage_num=v.current_voyage)

                elif v.status == "DISCHARGING":
                    selected_mother = v.assigned_mother or MOTHER_PRIMARY_NAME
                    v.cargo_bbl = 0
                    v.status = "CAST_OFF_B"
                    v.next_event_time = t + CAST_OFF_HOURS
                    self.log_event(t, v.name, "DISCHARGE_COMPLETE",
                                   f"{selected_mother}: {self.mother_bbl[selected_mother]:,.0f} bbl | Begin post-discharge cast-off",
                                   voyage_num=v.current_voyage)
                    self.log_event(t, v.name, "CAST_OFF_START_B",
                                   f"Cast-off from {selected_mother} ({CAST_OFF_HOURS}h)",
                                   voyage_num=v.current_voyage)

                elif v.status == "CAST_OFF_B":
                    selected_mother = v.assigned_mother or MOTHER_PRIMARY_NAME
                    if not self.export_ready[selected_mother]:
                        self.export_ready_since[selected_mother] = t
                    self.export_ready[selected_mother] = True
                    self.log_event(t, v.name, "CAST_OFF_COMPLETE_B",
                                   "Cast-off from mother complete; returning to storage",
                                   voyage_num=v.current_voyage)
                    v.status = "WAITING_RETURN_STOCK"
                    v.next_event_time = t

                elif v.status == "WAITING_RETURN_STOCK":
                    selected_mother = v.assigned_mother or MOTHER_PRIMARY_NAME
                    target_storage, required_stock, threshold_by_storage = self.return_allocation_candidate(v.cargo_capacity, v.name)
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

                    v.target_point = STORAGE_POINT.get(target_storage, "A")
                    sail_t = self.next_daylight_sail(t)
                    wait = sail_t - t
                    return_sail_hours = 3 if v.target_point == "E" else SAIL_HOURS_B_TO_A
                    v.status = "SAILING_BA"
                    v.next_event_time = sail_t + return_sail_hours
                    self.log_event(t, v.name, "RETURN_POINT_ALLOCATED",
                                   f"Allocated to Point {v.target_point} on departure from {selected_mother} | "
                                   f"Immediate-load eligible storage: {target_storage} "
                                   f"({self.storage_bbl[target_storage]:,.0f} bbl, "
                                   f"loading-start threshold {required_stock:,.0f} bbl, "
                                   f"critical {STORAGE_CRITICAL_THRESHOLD_BY_NAME[target_storage]:,.0f} bbl)",
                                   voyage_num=v.current_voyage)
                    if wait > 0:
                        self.log_event(t, v.name, "WAITING_DAYLIGHT",
                                       f"Daylight window opens at "
                                       f"{self.hours_to_dt(sail_t).strftime('%Y-%m-%d %H:%M')}",
                                       voyage_num=v.current_voyage)

                elif v.status == "SAILING_BA":
                    v.status = "IDLE_A"
                    v.assigned_storage = None
                    v.assigned_load_hours = None
                    v.assigned_mother = None
                    v._voyage_assigned = False  # allow next cycle to get a new voyage number
                    self.log_event(t, v.name, "ARRIVED_LOADING_POINT",
                                   f"Arrived Point {v.target_point} storage area — ready for next cycle",
                                   voyage_num=v.current_voyage)
                    v.next_event_time = t

            # 3. Advance mother export state machines independently
            active_export_mother = next(
                (name for name in MOTHER_NAMES if self.export_state[name] is not None),
                None,
            )
            if active_export_mother is None and t >= self.next_export_allowed_at:
                ready_candidates = []
                for mother_name in MOTHER_NAMES:
                    if (
                        self.export_state[mother_name] is None
                        and self.export_ready[mother_name]
                        and self.mother_bbl[mother_name] >= MOTHER_EXPORT_TRIGGER
                        and t >= self.mother_available_at[mother_name]
                    ):
                        discharging_here = any(
                            vv.status == "DISCHARGING" and (vv.assigned_mother == mother_name)
                            for vv in self.vessels
                        )
                        if discharging_here:
                            self.log_event(t, mother_name, "EXPORT_WAIT_DISCHARGE",
                                           "Export ready but waiting for daughter discharge to complete")
                            continue
                        ready_since = self.export_ready_since[mother_name]
                        if ready_since is None:
                            ready_since = t
                        ready_candidates.append((ready_since, mother_name))

                if ready_candidates:
                    ready_candidates.sort(key=lambda x: (x[0], x[1]))
                    selected_export_mother = ready_candidates[0][1]
                    day_h = t % 24
                    if DAYLIGHT_START <= day_h < DAYLIGHT_END:
                        self.export_state[selected_export_mother] = "DOC"
                        self.export_ready[selected_export_mother] = False
                        self.export_ready_since[selected_export_mother] = None
                        self.export_end_time[selected_export_mother] = t + EXPORT_DOC_HOURS
                        self.log_event(t, selected_export_mother, "EXPORT_DOC_START",
                                       f"Export documentation ({EXPORT_DOC_HOURS}h)")
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
                        self.mother_bbl[mother_name] -= amount
                        self.total_exported += amount
                        self.log_event(t, mother_name, "EXPORT_PROGRESS",
                                       f"Exported {amount:,} bbl in port; Remaining: {self.mother_bbl[mother_name]:,.0f} bbl")
                    if self.mother_bbl[mother_name] <= 0:
                        self.export_state[mother_name] = None
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
                        self.log_event(return_depart, mother_name, "EXPORT_RETURN_START",
                                       f"Departing export terminal ({EXPORT_SAIL_HOURS}h transit)")
                        self.log_event(return_arrival, mother_name, "EXPORT_RETURN_ARRIVE",
                                       f"Arrived at {mother_name}; beginning 2h fendering")
                        self.log_event(self.mother_available_at[mother_name], mother_name, "EXPORT_FENDERING_COMPLETE",
                                       "Fendering complete; ready to receive daughters")
                        self.next_export_allowed_at = (
                            self.mother_available_at[mother_name] + EXPORT_SERIES_BUFFER_HOURS
                        )
                        self.last_export_mother = mother_name
                        self.log_event(
                            self.next_export_allowed_at,
                            mother_name,
                            "EXPORT_SERIES_BUFFER_COMPLETE",
                            f"Serial export buffer complete ({EXPORT_SERIES_BUFFER_HOURS}h) — next mother export may begin",
                        )

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
            vessel_statuses = {v.name: v.status for v in self.vessels}
            self.timeline.append({
                "Time"       : self.hours_to_dt(t),
                "Day"        : int(t // 24) + 1,
                "Storage_bbl": round(self.total_storage_bbl()),
                "Chapel_bbl": round(self.storage_bbl[STORAGE_PRIMARY_NAME]),
                "JasmineS_bbl": round(self.storage_bbl[STORAGE_SECONDARY_NAME]),
                "Westmore_bbl": round(self.storage_bbl[STORAGE_TERTIARY_NAME]),
                "Duke_bbl": round(self.storage_bbl[STORAGE_QUATERNARY_NAME]),
                "Starturn_bbl": round(self.storage_bbl[STORAGE_QUINARY_NAME]),
                "Storage_Overflow_Accum_bbl": round(sum(self.storage_overflow_bbl.values())),
                "Chapel_Overflow_Accum_bbl": round(self.storage_overflow_bbl[STORAGE_PRIMARY_NAME]),
                "JasmineS_Overflow_Accum_bbl": round(self.storage_overflow_bbl[STORAGE_SECONDARY_NAME]),
                "Westmore_Overflow_Accum_bbl": round(self.storage_overflow_bbl[STORAGE_TERTIARY_NAME]),
                "Duke_Overflow_Accum_bbl": round(self.storage_overflow_bbl[STORAGE_QUATERNARY_NAME]),
                "Starturn_Overflow_Accum_bbl": round(self.storage_overflow_bbl[STORAGE_QUINARY_NAME]),
                "PointF_Overflow_Accum_bbl": round(self.point_f_overflow_accum_bbl),
                "PointF_Active_Loading_bbl": round(self.point_f_active_loading_bbl()),
                "Mother_bbl" : round(self.total_mother_bbl()),
                "Bryanston_bbl": round(self.mother_bbl[MOTHER_PRIMARY_NAME]),
                "Alkebulan_bbl": round(self.mother_bbl[MOTHER_SECONDARY_NAME]),
                "GreenEagle_bbl": round(self.mother_bbl[MOTHER_TERTIARY_NAME]),
                "Total_Exported": self.total_exported,
                **vessel_statuses
            })

            t = round(t + TIME_STEP_HOURS, 2)

        return pd.DataFrame(self.log), pd.DataFrame(self.timeline)


# -----------------------------------------------------------------
# RUN SIMULATION
# -----------------------------------------------------------------
print("=" * 65)
print("  OIL TANKER DAUGHTER VESSEL OPERATION SIMULATION  (v5)")
print("=" * 65)

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
print(f"    - {MOTHER_TERTIARY_NAME:<9}: {sim.mother_bbl[MOTHER_TERTIARY_NAME]:,.0f} bbl")
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
    "Bedford"   : "#f39c12",   # amber family
    "Balham"    : "#1abc9c",   # teal family
    "Woodstock" : "#e91e63",   # pink family
    "Bagshot"   : "#00bcd4",   # cyan family
    "Watson"    : "#95a5a6",   # slate/gray family
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
ax1.plot(timeline_df["Time"], timeline_df["Chapel_bbl"],
            color="#f1c40f", linewidth=1.4, alpha=0.9, label=f"{STORAGE_PRIMARY_NAME} Volume")
ax1.plot(timeline_df["Time"], timeline_df["JasmineS_bbl"],
            color="#8e44ad", linewidth=1.4, alpha=0.9, label=f"{STORAGE_SECONDARY_NAME} Volume")
ax1.plot(timeline_df["Time"], timeline_df["Westmore_bbl"],
            color="#27ae60", linewidth=1.4, alpha=0.9, label=f"{STORAGE_TERTIARY_NAME} Volume")
ax1.plot(timeline_df["Time"], timeline_df["Duke_bbl"],
            color="#3498db", linewidth=1.4, alpha=0.9, label=f"{STORAGE_QUATERNARY_NAME} Volume")
ax1.plot(timeline_df["Time"], timeline_df["Starturn_bbl"],
            color="#d35400", linewidth=1.4, alpha=0.9, label=f"{STORAGE_QUINARY_NAME} Volume")
ax1.axhline(STORAGE_CAPACITY_BBL, color="#e74c3c", linestyle="--", alpha=0.7,
                label=f"Std Storage Capacity ({STORAGE_CAPACITY_BBL:,} bbl)")
ax1.axhline(DUKE_STORAGE_CAPACITY_BBL, color="#3498db", linestyle="--", alpha=0.7,
                label=f"Duke Capacity ({DUKE_STORAGE_CAPACITY_BBL:,} bbl)")
ax1.axhline(STARTURN_STORAGE_CAPACITY_BBL, color="#d35400", linestyle="--", alpha=0.7,
                label=f"Starturn Capacity ({STARTURN_STORAGE_CAPACITY_BBL:,} bbl)")

# Dead-stock lines per vessel (175% of each cargo)
ds_colors = {"Sherlock": "#e74c3c", "Laphroaig": "#2ecc71",
             "Rathbone": "#9b59b6", "Bedford": "#f39c12",
         "Balham": "#1abc9c", "Woodstock": "#e91e63", "Bagshot": "#00bcd4", "Watson": "#95a5a6"}
for vname, vcap in [("Sherlock", DAUGHTER_CARGO_BBL),
                     ("Laphroaig", DAUGHTER_CARGO_BBL),
                     ("Rathbone", VESSEL_CAPACITIES.get("Rathbone", DAUGHTER_CARGO_BBL)),
                     ("Bedford",  VESSEL_CAPACITIES.get("Bedford",  DAUGHTER_CARGO_BBL)),
                     ("Balham",   VESSEL_CAPACITIES.get("Balham",   DAUGHTER_CARGO_BBL)),
                     ("Woodstock", VESSEL_CAPACITIES.get("Woodstock", DAUGHTER_CARGO_BBL)),
             ("Bagshot",  VESSEL_CAPACITIES.get("Bagshot",  DAUGHTER_CARGO_BBL)),
             ("Watson",   VESSEL_CAPACITIES.get("Watson",   DAUGHTER_CARGO_BBL))]:
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
ax2.plot(timeline_df["Time"], timeline_df["Alkebulan_bbl"],
            color="#c0392b", linewidth=1.4, alpha=0.9, label=f"{MOTHER_SECONDARY_NAME} Volume")
ax2.plot(timeline_df["Time"], timeline_df["GreenEagle_bbl"],
            color="#8e44ad", linewidth=1.4, alpha=0.9, label=f"{MOTHER_TERTIARY_NAME} Volume")
ax2.axhline(MOTHER_EXPORT_TRIGGER, color="#e74c3c", linestyle="--", alpha=0.7,
                label=f"Per-Mother Export Trigger ({MOTHER_EXPORT_TRIGGER:,} bbl)")
ax2.axhline(MOTHER_CAPACITY_BBL, color="#922b21", linestyle="-.", alpha=0.5,
                label=f"Per-Mother Max Capacity ({MOTHER_CAPACITY_BBL:,} bbl)")
ax2.set_ylabel("Volume (bbls)", fontsize=10, color="white")
ax2.set_title(
    f"Point B Mothers ({MOTHER_PRIMARY_NAME} + {MOTHER_SECONDARY_NAME} + {MOTHER_TERTIARY_NAME}) — Volume Level",
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

event_log_path = safe_csv_write(log_df, "tanker_event_log_v5.csv")
timeline_path = safe_csv_write(timeline_df, "tanker_timeline_v5.csv")
print("[OK] CSVs saved.")

