from pathlib import Path
import fastf1
import pandas as pd


def main():
  base_dir = Path(__file__).resolve().parent
  cache_dir = base_dir / 'fastf1_cache'
  data_dir = base_dir / 'data'

  cache_dir.mkdir(exist_ok=True)
  data_dir.mkdir(exist_ok=True)

  # Enable local caching so re-runs are near-instant
  fastf1.Cache.enable_cache(str(cache_dir))

  print(
      'Downloading Silverstone 2026 Race session (takes 1-3 mins on first'
      ' run)...'
  )
  session = fastf1.get_session(2026, 'Silverstone', 'R')
  session.load(telemetry=True, laps=True, weather=False, messages=False)

  all_telemetry = []

  for drv in session.drivers:
    drv_laps = session.laps.pick_drivers(drv)
    if drv_laps.empty:
      continue

    drv_code = drv_laps['Driver'].iloc[0]
    print(f'Extracting raw telemetry for {drv_code} (#{drv})...')

    try:
      # Extracts raw merged car + pos telemetry for all laps of this driver
      telemetry = drv_laps.get_telemetry()
      telemetry['Driver'] = drv_code
      telemetry['DriverNumber'] = drv
      all_telemetry.append(telemetry)
    except Exception as e:
      print(f'Skipping {drv_code} due to missing telemetry: {e}')

  print('Concatenating all drivers into a single DataFrame...')
  full_df = pd.concat(all_telemetry, ignore_index=True)

  output_path = data_dir / 'silverstone_2026_telemetry.parquet'
  full_df.to_parquet(output_path, index=False)
  print(f'\nSuccess! Saved {len(full_df):,} rows to {output_path}')


if __name__ == '__main__':
  main()