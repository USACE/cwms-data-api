# CWMSjs

_CWMS Data API (CDA) client library created with OpenAPI generator in TypeScript for use with web browsers._

## Getting Started

- Install CWMSjs to your react project with:  
   `npm install cwmsjs`  
   (This will grab the latest)

- Import the API endpoint you wish to use:
  - Avaiable endpoints are here (Classes ending in API): [Endpoints](https://hydrologicengineeringcenter.github.io/cwms-data-api-client-javascript/modules.html)
  - At the top of your js/jsx/file type:
    ```javascript
    import { TimeSeriesAPI } from "cwmsjs";
    ```
  - Then initialize the TimeSeriesAPI with:
    ```javascript
    const ts_api = new TimeSeriesAPI();
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

Documentation is available for both developers and new users:

- New Users : [Examples](https://hydrologicengineeringcenter.github.io/cwms-data-api-client-javascript/examples/)
- Advanced users : [Type Documentation / Developer Docs](https://hydrologicengineeringcenter.github.io/cwms-data-api-client-javascript/)

## API Adjustments
Some tweaks are made to the base cwms-data-api syntax during the build process to improve quality of life while working in JavaScript.  Users should be aware of the following:

### camelCase
Most response object keys in CDA are written in snake-case.  All response object keys have been converted to camelCase in order to play more nicely with javascript's object dot notation (`object.key`).

### "TimeSeries" standardization
Throughout CDA, "time series" is arbitrarily referred to in both a one-word ("timeseries") and two-word ("time series") format.  During the cwmsjs build, all instances of "time series" for method names, types, etc. are standardized to the two-word form.

## Developers
### Versioning
In order to accommodate changes both to the generator and to CDA itself, cwmsjs is versioned in the following format:
`[generator SemVer]-[CDA schema version]`

The generator now uses the live OpenAPI `info.version` published by CDA. If that field is unavailable, it falls back to the current date.

### Publishing
Contributors with authorization can publish a new version of cwmsjs by manually running the "Build, Test, and Publish" GitHub Action.

The workflow will build an updated cwmsjs library using the current generator and national CDA instance OpenAPI spec.  If all tests pass, the library will be published to npm and updated documentation will be deployed to GitHub Pages.

## Building CWMSjs from source

- Clone this repository
- Install dependencies with: `npm install`
- Optionally set `CWMS_SCHEMA_URL` if you need to build from a non-default CDA schema endpoint
- Run the generator with:
  `npm run build`

All generated files (source, library, and docs) will be in `[repo]/cwmsjs`
