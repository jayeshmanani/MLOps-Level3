# Bike Rental - Week 2

This folder contains the Week 2 data-preprocessing pipeline for the Bike Rental project (see the full assignment in [project_handout/README.md](project_handout/README.md)). The pipeline is implemented as Dagster assets and uses small helper modules to load, merge and transform CSV data into a final preprocessed dataset.

**What this repo contains**
- Asset definitions (Dagster): [src/bike_rental/definitions.py](src/bike_rental/definitions.py)
- Asset implementations: [src/bike_rental/defs/assets](src/bike_rental/defs/assets)
- Resource for file I/O: [src/bike_rental/defs/resources/csv_io.py](src/bike_rental/defs/resources/csv_io.py)
- Raw input CSVs: [data/](data)

## Quickstart — run locally

Prerequisites:
- Python 3.10–3.12
- `uv` (project runner) — follow https://docs.astral.sh/uv/getting-started/installation/

1. Install dependencies (from project root):

```bash
uv sync
```

2. Start the Dagster development server (opens the UI):

```bash
uv run dg dev
```

3. Open the Dagster UI at http://localhost:3000 and materialize assets using the Assets view (use the Materialize button or run individual assets).

Notes: you can also invoke materialization from the CLI or write a small Python script to call Dagster programmatically, but the UI is the simplest for exploration.

## Data layout

- `data/registered_bike_rentals.csv` - booked rentals
- `data/direct_pickup_bike_rentals.csv` - direct pickups
- `data/weather.csv` - historical weather
- `data/holidays.csv` - holiday calendar

All assets read these CSVs by path (relative to the repository root) using the `csv_io` resource. The final preprocessed CSV is written to `data/raw/raw_final_transformed_data.csv` by the final asset.

## Resource and configuration

This pipeline uses a small Dagster resource to centralize CSV I/O:

- Resource: `CSVIO` implemented in [src/bike_rental/defs/resources/csv_io.py](src/bike_rental/defs/resources/csv_io.py)

How assets receive the resource:
- Assets accept the resource directly as a function argument, e.g. `def bike_rental_hourly(csv_io: CSVIO)`. Dagster injects the resource named `csv_io` from `Definitions`.

How the resource is registered:
- `Definitions` registers `csv_io` in [src/bike_rental/definitions.py](src/bike_rental/definitions.py). By default this project registers `CSVIO(base_path='.')`, so no extra run config is necessary. If you want to override the base path, you can either change `definitions.py` or register an explicit run config.

Example run config (optional) if you register a configurable resource instead of an instance:

```yaml
resources:
	csv_io:
		config:
			base_path: "./"
```

## How the code is organized

- Pure transforms and helpers: [src/bike_rental/defs/assets/helper.py](src/bike_rental/defs/assets/helper.py)
- Asset implementations (reading, merging, cleaning): [src/bike_rental/defs/assets/*.py](src/bike_rental/defs/assets)
- Definitions (asset registry + resources): [src/bike_rental/definitions.py](src/bike_rental/definitions.py)

## Where to read more
- Assignment and background: [project_handout/README.md](project_handout/README.md)
- Dagster docs: https://docs.dagster.io/
