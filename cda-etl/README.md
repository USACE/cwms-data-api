# CDA ETL

This project downloads CWMS data from a source CDA REST API, stages the retrieved JSON on the local filesystem, and then uploads the staged records to a destination CDA REST API.

The workflow is intentionally split into two phases:

1. Stage data from the source API onto disk.
2. Publish the staged files to the destination API.

When `SOURCE_CDA_URL` is configured, the stage phase always re-downloads source data and overwrites staged files for projects, locations, and timeseries.

If `SOURCE_CDA_URL` is not configured, the pipeline skips the download phase and publishes whatever is already staged on disk.

## What It Does

The ETL process handles these CWMS resource types:

- Locations
- Projects
- Timeseries data
- Clobs
- Location levels
- Ratings
- Properties (office- and project-level)
- Outlets
- Turbines, and turbine changes
- Locks (physical lock structures)
- Gate changes
- Water users, water contracts (including pump associations and water supply accounting)
- Location groups (with assigned locations), office-level
- Timeseries groups (with assigned timeseries), office-level

The data is organized by office, project, and resource type, then written to a filesystem staging area before being posted to the destination CDA API.

## Configuration Overview

The main runtime configuration is stored in a YAML file, defaulting to `sample-app.yml` in the working directory.

The application reads the YAML path from the `ETL_CONFIG_PATH` environment variable. If the variable is not set, it looks for `sample-app.yml` next to where the process starts.

### Example Structure

```yaml
version: 1
settings:
  startTime: "2026-01-01"
  endTime: "now"
  maxThreads: 10
  logLevel: INFO
  path: "./data"
offices:
  - id: SWT
    enabled: true
    projects:
      - id: EUFA
        enabled: true
        locations:
          - id: EUFA-Dam
            enabled: true
        timeseries:
          - id: EUFA.Elev.Inst.1Hour.0.Ccp-Rev
            enabled: true
```

### YAML Fields

- `version`: Config version. Must be `1`.
- `settings.startTime`: Default start time used for timeseries/window-based downloads when an item does not define its own download window.
- `settings.endTime`: Default end time used for timeseries/window-based downloads when an item does not define its own download window.
- `settings.maxThreads`: Maximum number of worker threads used for staging and publishing.
- `settings.logLevel`: Logging level for the application.
- `settings.path`: Filesystem root used for staged JSON files.
- `offices`: List of office definitions. Each office may also declare `properties`, `locationGroups`, and `timeseriesGroups` (office-wide, not tied to one project).
- `projects`: Projects under each office.
- `locations`: Locations under each project.
- `timeseries`: Timeseries under each project.
- `clobs`, `locationLevels`, `ratings`, `properties`: Under each project, same shape as the office-level `properties`.
- `outlets`, `turbines`, `locks`: Flat lists of `{id, enabled}` under each project - literal ids, no time window.
- `gateChanges`, `turbineChanges`: A single `{enabled, download: {startTime, endTime}}` mapping per project (not a list) - one time-windowed feed for the whole project.
- An outlet's effective rating spec is not stored on the outlet itself - CDA derives it from a `Rating`-category location group: the outlet carries a `rating-group-id`, and that group's `shared-loc-alias-id` is the rating spec id. Storing gate changes requires both to already exist on the destination, so neither needs to be hand-listed in `outlets`/`locationGroups`/`ratings` - staging/publishing outlets automatically discovers and stages/publishes the associated `Rating` location group and rating alongside them.
- `waterUsers`: Under each project - a list of `{id, enabled, contracts: [...]}`. Each contract is `{id, enabled, pumps: [...], accounting: {enabled, startTime, endTime}}`. A pump is `{id, type, enabled}` (`type` is `IN`, `OUT`, or `OUT BELOW`); a pump with `enabled: false` is disassociated from the destination contract during publish rather than skipped. `accounting.enabled` defaults to `false` when the `accounting` block is omitted entirely.
- `locationGroups`, `timeseriesGroups`: Under each office - `{categoryId, id, enabled}` entries, or `{categoryId, all: true}` for every group in a category, mirroring how `properties` categories work.

### Enabled Flags

The `enabled` field is optional everywhere. If it is omitted, the item is treated as enabled.

### Filesystem Staging

Staged data is written under the directory configured by `settings.path`.

For timeseries data, the stored file name does not include the time window. During staging with `SOURCE_CDA_URL` configured, each run overwrites the staged file with a fresh source download.

## Runtime Parameters

### Required for Destination Upload

- `DEST_CDA_URL`: Destination CDA REST API root.

Environment variable values are trimmed. Empty or whitespace-only values are treated as unset.

### Optional Source Download

- `SOURCE_CDA_URL`: Source CDA REST API root. If set, source data is always re-downloaded and staged files are overwritten each run. If omitted (or set to an empty value), the download phase is skipped.
- `SOURCE_CDA_API_KEY`: API key for the source CDA REST API.
- `DEST_CDA_API_KEY`: API key for the destination CDA REST API.

### Other Runtime Settings

- `ETL_CONFIG_PATH`: Path to the YAML config file. Defaults to `sample-app.yml`.
- `LOG_LEVEL`: Console log level for the application process. Defaults to `INFO`.

## Docker Usage

### docker run

Mount the YAML file into the container and point `ETL_CONFIG_PATH` at it.

```powershell
docker run --rm `
  -v ${PWD}\data\sample-data\sample-app.yml:/app/sample-app.yml `
  -e ETL_CONFIG_PATH=/app/sample-app.yml `
  -e SOURCE_CDA_URL=https://source.example/cwms-data `
  -e SOURCE_CDA_API_KEY=your-source-key `
  -e DEST_CDA_URL=https://dest.example/cwms-data `
  -e DEST_CDA_API_KEY=your-dest-key `
  cwms-data-api/etl
```

If you do not want to download from the source API, omit `SOURCE_CDA_URL` and the pipeline will publish only staged files.

### docker-compose

The included `docker-compose.yml` mounts `ETL_CONFIG_PATH` for the yml config file path.

You still need to supply the API endpoint environment variables when running Compose.

## Gradle Commands

The Gradle build file provides Docker-based convenience tasks.

### Build the image

```bash
./gradlew dockerBuild
```

### Run the ETL container

```bash
./gradlew runEtl
```

Optional Gradle property:

- `-PetlEnvFile=<path>`: Override the environment file passed to `docker run`. Default: `etl.env`

Example:

```bash
./gradlew runEtl -PetlEnvFile=etl.env.example
```

### Run the unit tests in Docker

```bash
./gradlew runEtlUnitTests
```

This uses Docker, mounts the local `src` and `tests` directories, and runs `pytest` inside the container.

### Run the full verification task

```bash
./gradlew check
```

`check` depends on `runEtlUnitTests` in this project.

## Local Development

For local Python execution, ensure the environment variables for source and destination CDA endpoints are set, then run:

```bash
python src/cda_etl/main.py
```

The process will load the YAML config, stage files under `settings.path`, and publish to the destination CDA API.
