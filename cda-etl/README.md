# CDA ETL

This project downloads CWMS data from a source CDA REST API, stages the retrieved JSON on the local filesystem, and then uploads the staged records to a destination CDA REST API.

The workflow is intentionally split into two phases:

1. Stage data from the source API onto disk.
2. Publish the staged files to the destination API.

When `SOURCE_CDA_URL` is configured, the stage phase always re-downloads source data and overwrites staged files for projects, locations, and timeseries.

If `SOURCE_CDA_URL` is not configured, the pipeline skips the download phase and publishes whatever is already staged on disk.

## What It Does

The ETL process currently handles three CWMS resource types:

- Locations
- Projects
- Timeseries data

The data is organized by office, project, and resource type, then written to a filesystem staging area before being posted to the destination CDA API.

## Configuration Overview

The main runtime configuration is stored in a YAML file, defaulting to `regi.yml` in the working directory.

The application reads the YAML path from the `REGI_CONFIG_PATH` environment variable. If the variable is not set, it looks for `regi.yml` next to where the process starts.

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
- `settings.startTime`: Default start time used for timeseries downloads when a timeseries does not define its own download window.
- `settings.endTime`: Default end time used for timeseries downloads when a timeseries does not define its own download window.
- `settings.maxThreads`: Maximum number of worker threads used for staging and publishing.
- `settings.logLevel`: Logging level for the application.
- `settings.path`: Filesystem root used for staged JSON files.
- `offices`: List of office definitions.
- `projects`: Projects under each office.
- `locations`: Locations under each project.
- `timeseries`: Timeseries under each project.

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

- `REGI_CONFIG_PATH`: Path to the YAML config file. Defaults to `regi.yml`.
- `LOG_LEVEL`: Console log level for the application process. Defaults to `INFO`.

## Docker Usage

### docker run

Mount the YAML file into the container and point `REGI_CONFIG_PATH` at it.

```powershell
docker run --rm `
  -v ${PWD}\data\regi\regi.yml:/app/regi.yml `
  -e REGI_CONFIG_PATH=/app/regi.yml `
  -e SOURCE_CDA_URL=https://source.example/cwms-data `
  -e SOURCE_CDA_API_KEY=your-source-key `
  -e DEST_CDA_URL=https://dest.example/cwms-data `
  -e DEST_CDA_API_KEY=your-dest-key `
  cwms-data-api/etl
```

If you do not want to download from the source API, omit `SOURCE_CDA_URL` and the pipeline will publish only staged files.

### docker-compose

The included `docker-compose.yml` mounts `regi.yml` into the container and sets `REGI_CONFIG_PATH=/app/regi.yml`.

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
