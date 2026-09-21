# CWMSjs

_CWMS Data API (CDA) client library created with OpenAPI generator in TypeScript for use with web browsers._

## Getting Started

- Install CWMSjs to your React project with:
   `npm install cwmsjs`  
   (This will grab the latest)

- Import the API endpoint you wish to use:
  - Available endpoints are listed in the generated TypeDoc output. API classes use names like `TimeSeriesApi`.
  - At the top of your js/jsx file:
    ```javascript
    import { TimeSeriesApi } from "cwmsjs";
    ```
  - Then initialize the `TimeSeriesApi` with:
    ```javascript
    const ts_api = new TimeSeriesApi();
    ```
  - Fetch time series data with:
    ```javascript
    await ts_api
      .getTimeSeries({
        office: "SWT",
        name: "KEYS.Elev.Inst.1Hour.0.Ccp-Rev",
      })
      .then((data) => {
        console.log(data);
      })
      .catch((e) => {
        console.log("My Error", e);
      });
    ```

- Use CWMSjs from plain HTML with the generated browser bundle:
    ```html
    <script src="https://cdn.jsdelivr.net/npm/cwmsjs@<version>/dist/cwmsjs.min.js"></script>
    <script>
      const tsApi = new cwmsjs.TimeSeriesApi();
    </script>
    ```

The npm package supports both bundler/module usage and plain HTML usage. Bundlers should import from `cwmsjs`; the CDN/browser bundle exposes the same APIs on the global `cwmsjs` object.

Documentation is generated during the Gradle build for both developers and new users:

- New Users: generated examples in `clients/typescript/cwmsjs/docs/examples/`
- Advanced users: generated TypeDoc output in `clients/typescript/cwmsjs/docs/`

## API Adjustments

Some tweaks are made to the base cwms-data-api syntax during the build process to improve quality of life while working in JavaScript. Users should be aware of the following:

### camelCase

Most response object keys in CDA are written in snake-case. All response object keys have been converted to camelCase in order to play more nicely with javascript's object dot notation (`object.key`).

### "TimeSeries" standardization

Throughout CDA, "time series" is arbitrarily referred to in both a one-word ("timeseries") and two-word ("time series") format. During the cwmsjs build, all instances of "time series" for method names, types, etc. are standardized to the two-word form.

## Developers

### Versioning

In order to accommodate changes both to the generator and to CDA itself, cwmsjs combines the client generator SemVer with the CDA version suffix:
`[cwmsjs generator SemVer]-[CDA version/calver suffix]`

The Gradle build passes the CDA project version into the client package step. When running the package update script directly, set `CDA_CLIENT_VERSION_SUFFIX` or pass `--version-suffix=<version>`.

## Building CWMSjs from source

- Clone this repository
- Run the generator and TypeScript build with:
  `./gradlew :clients:typescript:build`

All generated files (source, library, and docs) will be in `[repo]/clients/typescript/cwmsjs`
