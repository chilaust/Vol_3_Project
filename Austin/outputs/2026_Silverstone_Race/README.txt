Raw OpenF1 session export. Each Parquet file corresponds to a source endpoint.
No pairwise gaps, normalized track positions, passing labels, or other project features were added.
2026 mode coverage: OpenF1 car_data includes the legacy drs field, which is null for this session. Its documented car_data fields do not include per-car Active Aero mode, Overtake eligibility, or Overtake Mode activation. The race_control table does include track-wide OVERTAKE ENABLED/DISABLED messages; these describe system availability, not an individual driver's activation.
Season: 2026
Circuit: Silverstone
Session type: Race
Session key: 11326
