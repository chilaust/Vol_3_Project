"""Download one historical OpenF1 session and save source tables as Parquet."""

from __future__ import annotations

import argparse
import re
import time
from pathlib import Path

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

BASE_URL = "https://api.openf1.org/v1"
HTTP = requests.Session()
HTTP.mount(
    "https://",
    HTTPAdapter(max_retries=Retry(
        total=6, connect=6, read=6, status=6,
        backoff_factor=1.5, status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset({"GET"}),
    )),
)


def safe_name(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9_-]+", "_", value).strip("_")


def get_rows(endpoint: str, params: dict) -> list[dict]:
    last_error = None
    for attempt in range(6):
        try:
            response = HTTP.get(f"{BASE_URL}/{endpoint}", params=params, timeout=120)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as error:
            last_error = error
            time.sleep(2 ** attempt)
    raise RuntimeError(f"Failed to retrieve {endpoint} after retries: {last_error}")


def save_table(rows: list[dict], path: Path) -> None:
    # Even empty endpoints get a file, so it's clear which sources were requested.
    frame = pd.DataFrame(rows)
    # OpenF1 can return a number for a time gap and text such as "+1 LAP" in
    # the same field. Keep these mixed raw values as strings for Parquet.
    for column in frame.select_dtypes(include="object").columns:
        kinds = {type(value) for value in frame[column].dropna()}
        if len(kinds) > 1:
            frame[column] = frame[column].map(lambda value: None if pd.isna(value) else str(value))
    frame.to_parquet(path, index=False)
    print(f"{path.name}: {len(rows):,} rows")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("year", type=int)
    parser.add_argument("circuit", help='Circuit short name, e.g. "Silverstone"')
    parser.add_argument("--session-type", default="Race", help="Session type (default: Race)")
    parser.add_argument("--session-name", default="Race", help="Session name (default: Race; excludes Sprint)")
    parser.add_argument("--output", default="outputs", help="Output directory")
    parser.add_argument(
        "--refresh",
        action="store_true",
        help="Download every endpoint again and replace existing Parquet files",
    )
    args = parser.parse_args()

    sessions = get_rows(
        "sessions", {"year": args.year, "session_type": args.session_type}
    )
    matching = [
        row for row in sessions
        if row.get("circuit_short_name", "").casefold() == args.circuit.casefold()
        and row.get("session_name", "").casefold() == args.session_name.casefold()
    ]
    if len(matching) != 1:
        choices = ", ".join(sorted({row.get("circuit_short_name", "") for row in sessions}))
        raise SystemExit(
            f"Expected one matching session, found {len(matching)}. "
            f"Circuits in {args.year}: {choices}"
        )

    session = matching[0]
    session_key = session["session_key"]
    folder = Path(args.output) / f"{args.year}_{safe_name(args.circuit)}_{safe_name(args.session_type)}"
    folder.mkdir(parents=True, exist_ok=True)

    save_table([session], folder / "session.parquet")

    # One request per endpoint for session-sized tables.
    for endpoint in (
        "drivers", "laps", "intervals", "position", "stints", "weather",
        "race_control", "overtakes", "pit", "session_result",
    ):
        destination = folder / f"{endpoint}.parquet"
        if destination.exists() and not args.refresh:
            print(f"Keeping existing {destination.name}")
            continue
        rows = get_rows(endpoint, {"session_key": session_key})
        save_table(rows, destination)
        time.sleep(0.4)

    # Telemetry endpoints are large, so request each driver's stream separately.
    drivers = get_rows("drivers", {"session_key": session_key})
    driver_numbers = sorted({row["driver_number"] for row in drivers})
    for endpoint in ("car_data", "location"):
        destination = folder / f"{endpoint}.parquet"
        if destination.exists() and not args.refresh:
            print(f"Keeping existing {destination.name}")
            continue
        rows = []
        for index, number in enumerate(driver_numbers, start=1):
            print(f"Downloading {endpoint}: driver {number} ({index}/{len(driver_numbers)})")
            rows.extend(get_rows(endpoint, {"session_key": session_key, "driver_number": number}))
            time.sleep(0.4)
        save_table(rows, destination)

    mode_note = (
        "2026 mode coverage: OpenF1 car_data includes the legacy drs field, which is "
        "null for this session. Its documented car_data fields do not include per-car "
        "Active Aero mode, Overtake eligibility, or Overtake Mode activation. The "
        "race_control table does include track-wide OVERTAKE ENABLED/DISABLED messages; "
        "these describe system availability, not an individual driver's activation.\n"
    )
    (folder / "README.txt").write_text(
        "Raw OpenF1 session export. Each Parquet file corresponds to a source endpoint.\n"
        "No pairwise gaps, normalized track positions, passing labels, or other project features were added.\n"
        + mode_note
        + f"Season: {args.year}\nCircuit: {session.get('circuit_short_name')}\n"
        f"Session type: {args.session_type}\nSession key: {session_key}\n",
        encoding="utf-8",
    )
    print(f"Saved files in {folder.resolve()}")


if __name__ == "__main__":
    main()
