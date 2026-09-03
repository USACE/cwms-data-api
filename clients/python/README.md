# cda-python

Python SDK generated from the CWMS Data API (CDA) OpenAPI specification. Install
the `cda-python` distribution and import `cda_python`. The SDK provides generated
API methods and models; higher-level workflows can wrap it separately.

This follows the [cwmsjs generator](../typescript): Gradle exports the local CDA
specification, validates it, and runs OpenAPI Generator 7.15.0. Generated Python
source and API/model documentation stay under `build/` and are not committed.

## Build and test

Use Python 3.10 or newer, the repository's Java/Node build prerequisites, and a
running Docker engine for CDA's existing OpenAPI export task. From the repository
root:

```sh
./gradlew :clients:python:build --init-script init.gradle
```

On Windows use `./gradlew.bat`. To select a Python interpreter, pass
`-PpythonExecutable=/path/to/python`. Gradle creates its own virtual environment
under `clients/python/build/venv`; it does not install into your system Python.

The build generates source and documentation, builds a wheel and source archive,
installs the wheel, and runs HTTP contract tests against a local test server.
The SDK tests require no external CDA instance or credentials.

To build against a previously exported specification without Docker, pass
`-PpythonOpenApiSpec=/absolute/path/to/openapi.json`. Relative paths are resolved
from `clients/python`. This explicitly replaces the local export for that build;
the generator never silently downloads a different API version.

| Output | Location |
| --- | --- |
| Generated project | `clients/python/build/cda-python/` |
| API and model documentation | `clients/python/build/cda-python/docs/` |
| Wheel and source archive | `clients/python/build/dist/` |

Use `:clients:python:generatePythonClient` to generate only the source and docs.
Use `:clients:python:clean` to remove all Python build outputs.

## Install locally

This package has **not been set up or published on PyPI**. Install the wheel from
`clients/python/build/dist/` by supplying its actual filename:

```sh
python -m pip install /path/to/cda_python-<version>-py3-none-any.whl
```

```python
from cda_python import ApiClient, Configuration
from cda_python.api.offices_api import OfficesApi

config = Configuration(host="https://cwms-data.usace.army.mil/cwms-data")
with ApiClient(config) as client:
    offices = OfficesApi(client).get_offices(has_data=True)
    print([office.name for office in offices])
```

For authenticated requests, configure `config.api_key["ApiKey"]` with your key
and `config.api_key_prefix["ApiKey"] = "apikey"`. The host includes the deployment
context, such as `/cwms-data`; generated operation paths omit that prefix.

## Specification adjustments

Python attributes use snake case while serialization preserves CDA's JSON keys.
The preparation script separates rating inheritance from the rating union to
avoid circular imports, preserves discriminator values, and describes time-series
rows as numeric arrays that can contain null values. These adaptations affect
only the Python generator input. Office types accept both the descriptive labels
in the schema and the codes returned by `has-data=true` (for example, `DIS`).

The generator version is `0.1.0`. As with cwmsjs, the package also records the CDA
revision. Python uses a PEP 440 local version, for example
`0.1.0+2026.9.3` when built with `-PversionOverride=2026.09.03`.
Development branch punctuation is normalized to dots. PyPI publishing and a
public release-version policy remain follow-up work.

## Naming

Generated libraries use the `cda-*` naming convention to distinguish them from
existing CWMS projects. See the [proposal to use a generated SDK underneath
cwms-python](https://github.com/HydrologicEngineeringCenter/cwms-python/issues/299).
The existing TypeScript package is named `cwmsjs`.
