# garmin-strava-dashboard

Minimal Dagster scaffold for Garmin + Strava ingestion.

## Setup

1. Install `uv` if it is not already installed:

```bash
python -m pip install --user uv
```

2. From the repo root, run:

```bash
uv sync
```

This will:
- create the project virtual environment in `.venv`
- install the dependencies from `pyproject.toml`
- generate or update `uv.lock`

3. Copy `.env.example` to `.env` and add your API tokens and DuckDB path.

4. Use `workspace.yaml` to load the Dagster workspace.

5. Add assets, jobs, resources, and schedules inside `dagster_project/` as you build the pipeline.

## Running commands

Use `uv run` to execute tools inside the synced environment:

```bash
uv run python -m dagster dev
```

After adding `dagster-dg-cli`, `dg` commands will also be available:

```bash
uv run dg --help
```
