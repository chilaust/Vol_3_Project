"""Build one race-session table with one row per driver telemetry timestamp.

Run from the Austin folder with the project's Python environment active:
    python master_table.py

Raw input files stay unchanged. The joined output is written beside the raw
session folder as outputs/joined_driver_timeline.parquet.
"""

from pathlib import Path
import pandas as pd


AUSTIN_DIR = Path(__file__).resolve().parent
OUTPUTS_DIR = AUSTIN_DIR / "outputs"
SOURCE_DIR = OUTPUTS_DIR / "2026_Silverstone_Race"
OUTPUT_FILE = OUTPUTS_DIR / "joined_driver_timeline.parquet"

# Explicit time matching rules; adjust only if your group chooses different ones.
TELEMETRY_TOLERANCE = pd.Timedelta(seconds=1)
INTERVAL_TOLERANCE = pd.Timedelta(seconds=6)   # interval feed updates about every 4 sec
WEATHER_TOLERANCE = pd.Timedelta(seconds=90)  # weather feed updates about every 60 sec
OVERTAKE_WINDOW = pd.Timedelta(seconds=5)      # flag telemetry within +/- 5 sec of event

ID_COLUMNS = ("session_key", "driver_number")
DRIVER_MODE_FIELDS = (
    "active_aero_mode",
    "overtake_eligible",
    "overtake_mode_active",
)


def read_parquet(name, date_column=None):
    """Read a source table and standardize IDs/timestamps without altering values."""
    path = SOURCE_DIR / f"{name}.parquet"
    if not path.exists():
        raise FileNotFoundError(f"Missing source table: {path}")

    frame = pd.read_parquet(path)
    for column in ID_COLUMNS:
        if column in frame.columns:
            # Nullable integer IDs prevent int/float join-key mismatches.
            frame[column] = pd.to_numeric(frame[column], errors="coerce").astype("Int64")
    if date_column and date_column in frame.columns:
        frame[date_column] = pd.to_datetime(
            frame[date_column], utc=True, errors="coerce"
        )
    return frame


def clean_timed_source(frame, time_column, session_key, start, end,
                       lookback=pd.Timedelta(0), lookahead=pd.Timedelta(0),
                       driver_level=True):
    """Drop rows with unusable join keys; retain null feature values."""
    keys = ["session_key"] + (["driver_number"] if driver_level else [])
    before = len(frame)
    frame = frame.dropna(subset=[time_column, *keys]).copy()
    frame = frame.loc[frame["session_key"] == session_key]
    frame = frame.loc[
        frame[time_column].between(start - lookback, end + lookahead, inclusive="both")
    ].copy()
    print(f"{time_column}: kept {len(frame):,} of {before:,} rows in the session window")
    return frame


def asof_join(left, right, *, left_on, right_on, by,
              direction="backward", tolerance=None):
    """Time-align a source table; invalid right-side keys are excluded first."""
    by_columns = [by] if isinstance(by, str) else list(by)
    left = left.copy()
    right = right.copy()

    # Standardize join IDs as strings inside the merge. Their output values are
    # converted back to integers at the end; they are identifiers, not measures.
    for column in by_columns:
        if column in left.columns:
            left[column] = left[column].astype("string")
        if column in right.columns:
            right[column] = right[column].astype("string")

    # Lap number and lap_start are numeric keys; normalize their numeric types.
    if pd.api.types.is_numeric_dtype(left[left_on]) or pd.api.types.is_numeric_dtype(right[right_on]):
        left[left_on] = pd.to_numeric(left[left_on], errors="coerce").astype("float64")
        right[right_on] = pd.to_numeric(right[right_on], errors="coerce").astype("float64")

    right = right.dropna(subset=[right_on, *by_columns])
    left = left.dropna(subset=[left_on, *by_columns])
    return pd.merge_asof(
        left.sort_values(left_on),
        right.sort_values(right_on),
        left_on=left_on,
        right_on=right_on,
        by=by_columns,
        direction=direction,
        tolerance=tolerance,
        allow_exact_matches=True,
        suffixes=("", "_source"),
    )


def main():
    # Session boundaries prevent practice, warm-up, and pre-race samples from
    # becoming master rows with no corresponding race measurements.
    session = read_parquet("session")
    if len(session) != 1:
        raise ValueError(f"Expected one session row, found {len(session)}")
    session = session.iloc[0]
    session_key = int(session["session_key"])
    session_start = pd.to_datetime(session["date_start"], utc=True)
    session_end = pd.to_datetime(session["date_end"], utc=True)
    print(f"Session: {session['circuit_short_name']} {session['year']} {session['session_name']}")
    print(f"Window: {session_start} to {session_end}")

    # Load and clean the high-frequency streams. Keep a short pre-session lookback
    # for matching the first master timestamps, but never use it as a master row.
    loc = read_parquet("location", "date")
    car = read_parquet("car_data", "date")
    loc = clean_timed_source(
        loc, "date", session_key, session_start, session_end,
        lookback=TELEMETRY_TOLERANCE,
    )
    car = clean_timed_source(
        car, "date", session_key, session_start, session_end,
        lookback=TELEMETRY_TOLERANCE,
    )

    # Select the denser stream within the actual race window, not the whole weekend.
    loc_master = loc.loc[loc["date"].between(session_start, session_end)].copy()
    car_master = car.loc[car["date"].between(session_start, session_end)].copy()
    loc_counts = loc_master.groupby("driver_number").size()
    car_counts = car_master.groupby("driver_number").size()
    loc_density = loc_counts.mean() if not loc_counts.empty else 0
    car_density = car_counts.mean() if not car_counts.empty else 0

    if loc_density >= car_density:
        master_name = "location"
        master = loc_master
        car_source = car
        table = master[["date", "session_key", "driver_number", "x", "y", "z"]].copy()
        # X/Y are required for a usable location row; Z may legitimately be absent.
        table = table.dropna(subset=["date", "session_key", "driver_number", "x", "y"])
        table = table.rename(columns={"x": "X", "y": "Y", "z": "Z"})
    else:
        master_name = "car_data"
        master = car_master
        car_source = master
        table = master[["date", "session_key", "driver_number"]].copy()
        location_source = loc[["date", "session_key", "driver_number", "x", "y", "z"]]
        table = asof_join(
            table, location_source,
            left_on="date", right_on="date",
            by=["session_key", "driver_number"],
            tolerance=TELEMETRY_TOLERANCE,
        ).rename(columns={"x": "X", "y": "Y", "z": "Z"})

    # One row per session/driver/date. Drop exact duplicate master keys only.
    table = table.dropna(subset=["date", "session_key", "driver_number"])
    table = table.drop_duplicates(["session_key", "driver_number", "date"], keep="last")
    table = table.sort_values("date").reset_index(drop=True)
    print(f"Master timeline: {master_name}; {len(table):,} rows")

    # Car attributes: align car_data to the chosen telemetry timestamp.
    # Carry through these fields if the source endpoint provides them. The
    # current OpenF1 car_data schema does not include the 2026 per-car modes,
    # so the three mode columns are added below as null unless a source row
    # actually supplies them.
    base_car_fields = ["throttle", "drs", "rpm", "n_gear", "speed", "brake"]
    car_mode_fields = [field for field in DRIVER_MODE_FIELDS if field in car_source.columns]
    car_fields = base_car_fields + car_mode_fields
    car_source = car_source[["date", "session_key", "driver_number", *car_fields]]
    table = asof_join(
        table, car_source,
        left_on="date", right_on="date",
        by=["session_key", "driver_number"],
        tolerance=TELEMETRY_TOLERANCE,
    )
    for field in DRIVER_MODE_FIELDS:
        if field not in table.columns:
            table[field] = pd.NA

    # Gap attributes: use the most recent interval reading for the same driver.
    # Null for the leader and '+N LAP' strings are meaningful source values.
    intervals = read_parquet("intervals", "date")
    intervals = clean_timed_source(
        intervals, "date", session_key, session_start, session_end,
        lookback=INTERVAL_TOLERANCE,
    )
    intervals = intervals[["date", "session_key", "driver_number", "gap_to_leader", "interval"]]
    table = asof_join(
        table, intervals,
        left_on="date", right_on="date",
        by=["session_key", "driver_number"],
        tolerance=INTERVAL_TOLERANCE,
    ).rename(columns={"interval": "gap_to_next"})

    # Lap number: locate the lap whose start/end interval contains the timestamp.
    laps = read_parquet("laps", "date_start")
    laps = clean_timed_source(
        laps, "date_start", session_key, session_start, session_end,
        driver_level=True,
    )
    laps["lap_number"] = pd.to_numeric(laps["lap_number"], errors="coerce")
    laps["lap_duration"] = pd.to_numeric(laps["lap_duration"], errors="coerce")
    laps = laps.dropna(subset=["lap_number", "date_start"])
    laps = laps.sort_values(["session_key", "driver_number", "date_start"])
    laps["_next_lap_start"] = laps.groupby(
        ["session_key", "driver_number"], observed=True
    )["date_start"].shift(-1)
    laps["_duration_end"] = laps["date_start"] + pd.to_timedelta(laps["lap_duration"], unit="s")
    laps["_lap_end"] = laps["_next_lap_start"].fillna(laps["_duration_end"])
    lap_source = laps[["date_start", "session_key", "driver_number", "lap_number", "_lap_end"]]
    table = asof_join(
        table, lap_source,
        left_on="date", right_on="date_start",
        by=["session_key", "driver_number"],
        direction="backward",
    )
    invalid_lap = table["_lap_end"].isna() | (table["date"] >= table["_lap_end"])
    table.loc[invalid_lap, "lap_number"] = pd.NA
    table = table.drop(columns=["date_start", "_lap_end"])

    # This is a race-lap table, so exclude grid/formation and post-finish
    # telemetry that cannot be assigned to a recorded lap. Keep other missing
    # fields; they may be meaningful or reflect source coverage.
    rows_without_lap = int(table["lap_number"].isna().sum())
    table = table.loc[table["lap_number"].notna()].copy()
    print(f"Removed {rows_without_lap:,} telemetry rows outside reported lap intervals")

    # Passing: attach the nearest event for the same passing driver within +/- 5s.
    # This is an event-window label, not proof every event was an on-track pass.
    overtakes = read_parquet("overtakes", "date").rename(
        columns={"overtaking_driver_number": "driver_number"}
    )
    overtakes = clean_timed_source(
        overtakes, "date", session_key, session_start, session_end,
        lookback=OVERTAKE_WINDOW, lookahead=OVERTAKE_WINDOW,
    )
    events = overtakes.rename(columns={"date": "_overtake_date"})[
        ["_overtake_date", "session_key", "driver_number", "overtaken_driver_number"]
    ]
    table = asof_join(
        table, events,
        left_on="date", right_on="_overtake_date",
        by=["session_key", "driver_number"],
        direction="nearest",
        tolerance=OVERTAKE_WINDOW,
    )
    table["overtake_flag"] = table["_overtake_date"].notna().astype("int8")
    table = table.drop(columns="_overtake_date")

    # Race control exposes track-wide Overtake system availability. This is
    # not a per-driver eligibility or activation signal. Carry the last
    # enabled/disabled notice forward until the next notice; before the first
    # notice, the state remains unknown.
    race_control = read_parquet("race_control", "date")
    # Keep earlier messages from this same session: the race can begin with a
    # control state announced before the session's official start timestamp.
    race_control = race_control.dropna(subset=["date", "session_key"])
    race_control = race_control.loc[
        (race_control["session_key"] == session_key)
        & (race_control["date"] <= session_end)
    ].copy()
    race_control["_message_upper"] = (
        race_control["message"].fillna("").astype("string").str.strip().str.upper()
    )
    is_enabled = race_control["_message_upper"].str.match(r"^OVERTAKE ENABLED(?:\b|$)")
    is_disabled = race_control["_message_upper"].str.match(r"^OVERTAKE DISABLED(?:\b|$)")
    control_events = race_control.loc[is_enabled | is_disabled].copy()
    control_events["overtake_system_enabled"] = is_enabled.loc[control_events.index]
    control_events = control_events.rename(columns={"date": "_overtake_control_date"})[
        ["_overtake_control_date", "session_key", "overtake_system_enabled"]
    ]
    table = asof_join(
        table, control_events,
        left_on="date", right_on="_overtake_control_date",
        by=["session_key"],
        direction="backward",
    )
    table["overtake_system_enabled"] = table["overtake_system_enabled"].astype("boolean")
    table = table.drop(columns="_overtake_control_date")

    # Weather: session-level, latest prior reading. Allow one update interval of lookback.
    weather = read_parquet("weather", "date")
    weather = clean_timed_source(
        weather, "date", session_key, session_start, session_end,
        lookback=WEATHER_TOLERANCE, driver_level=False,
    )
    weather_fields = ["pressure", "wind_speed", "air_temperature", "rainfall", "humidity", "track_temperature"]
    table = asof_join(
        table,
        weather[["date", "session_key", *weather_fields]],
        left_on="date", right_on="date",
        by=["session_key"],
        tolerance=WEATHER_TOLERANCE,
    )

    # Tires: a stint applies to this driver's laps from lap_start through lap_end.
    stints = read_parquet("stints")
    stints = stints.loc[stints["session_key"] == session_key].copy()
    for column in ["lap_start", "lap_end", "stint_number", "tyre_age_at_start"]:
        stints[column] = pd.to_numeric(stints[column], errors="coerce")
    stints = stints.dropna(subset=["session_key", "driver_number", "lap_start", "lap_end"])

    table = table.reset_index(drop=True)
    table["stint_number"] = pd.NA
    table["compound"] = pd.NA
    table["tire_age"] = pd.NA
    valid_laps = table.loc[table["lap_number"].notna()].copy()
    valid_laps["_row_id"] = valid_laps.index
    if not valid_laps.empty:
        valid_laps = valid_laps.drop(columns=["stint_number", "compound", "tire_age"])
        stint_source = stints[[
            "session_key", "driver_number", "lap_start", "lap_end",
            "stint_number", "compound", "tyre_age_at_start",
        ]]
        stint_match = asof_join(
            valid_laps, stint_source,
            left_on="lap_number", right_on="lap_start",
            by=["session_key", "driver_number"],
            direction="backward",
        )
        in_stint = stint_match["lap_end"].notna() & (
            stint_match["lap_number"] <= stint_match["lap_end"]
        )
        for column in ["stint_number", "compound"]:
            table.loc[stint_match["_row_id"], column] = (
                stint_match[column].where(in_stint).to_numpy()
            )
        age = stint_match["tyre_age_at_start"] + (
            stint_match["lap_number"] - stint_match["lap_start"]
        )
        table.loc[stint_match["_row_id"], "tire_age"] = age.where(in_stint).to_numpy()

    # Pit: mark pit_lap from the event table and flag timestamps in its lane window.
    pit = read_parquet("pit", "date")
    pit = pit.loc[pit["session_key"] == session_key].copy()
    pit["session_key"] = pit["session_key"].astype("string")
    pit["driver_number"] = pit["driver_number"].astype("string")
    pit["lap_number"] = pd.to_numeric(pit["lap_number"], errors="coerce")
    pit["lane_duration"] = pd.to_numeric(pit["lane_duration"], errors="coerce")
    pit_keys = {
        (int(row.session_key), int(row.driver_number), int(row.lap_number))
        for row in pit.itertuples()
        if pd.notna(row.session_key) and pd.notna(row.driver_number) and pd.notna(row.lap_number)
    }
    table["pit_lap"] = [
        lap if pd.notna(lap) and (int(session), int(driver), int(lap)) in pit_keys else pd.NA
        for session, driver, lap in zip(table["session_key"], table["driver_number"], table["lap_number"])
    ]
    table["in_pit_flag"] = 0
    for event in pit.itertuples():
        if pd.isna(event.date) or pd.isna(event.lane_duration) or pd.isna(event.driver_number):
            continue
        lane_end = event.date + pd.to_timedelta(event.lane_duration, unit="s")
        mask = (
            (table["session_key"] == event.session_key)
            & (table["driver_number"] == event.driver_number)
            & table["date"].between(event.date, lane_end, inclusive="both")
        )
        table.loc[mask, "in_pit_flag"] = 1

    # Final table: keep fields in the project's requested order and preserve
    # nulls where no valid source observation matched the master timestamp.
    columns = [
        "date", "session_key", "driver_number", "X", "Y", "Z", "lap_number",
        "gap_to_leader", "gap_to_next", "overtake_flag", "overtaken_driver_number",
        "throttle", "drs", "active_aero_mode", "overtake_eligible",
        "overtake_mode_active", "overtake_system_enabled",
        "rpm", "n_gear", "speed", "brake", "pressure",
        "wind_speed", "air_temperature", "rainfall", "humidity", "track_temperature",
        "tire_age", "compound", "stint_number", "pit_lap", "in_pit_flag",
    ]
    table = table.reindex(columns=columns)

    # Restore numeric ID columns for the saved file; they were joined as strings.
    for column in ["session_key", "driver_number", "overtaken_driver_number",
                   "lap_number", "stint_number", "pit_lap"]:
        table[column] = pd.to_numeric(table[column], errors="coerce").astype("Int64")

    table = table.sort_values(["date", "driver_number"]).reset_index(drop=True)
    table.to_parquet(OUTPUT_FILE, index=False)

    print(f"Saved {len(table):,} rows to {OUTPUT_FILE}")
    print("Non-null row counts (nulls can still be meaningful, e.g. leader gaps):")
    for column in [
        "X", "Y", "lap_number", "gap_to_leader", "gap_to_next", "throttle", "drs",
        "active_aero_mode", "overtake_eligible", "overtake_mode_active",
        "overtake_system_enabled", "speed", "pressure", "compound", "pit_lap",
    ]:
        print(f"  {column}: {table[column].notna().sum():,} / {len(table):,}")


if __name__ == "__main__":
    main()
