# CDA Landing Page Source

_The React+Vite project for CDA_

## Setup

Setup the project by running:

1. `cd cda-gui`
2. `npm install` (With nodejs installed)

## Development

To run the project in dev:
`npm run dev`

This starts Vite in `localhost` mode using `.env.localhost` and displays **Local**
in the header.

## Production Files

To build the project:
`npm run build`

To see the available scripts for this project, including how to run and deploy, look at the `package.json` file.

The shared header shows the environment selected by the Vite build mode:
`development` displays **Development**, `test` displays **Test**, and `production`
displays **Production**. `npm run dev` (`localhost`) and `dev-cda-compose` display
**Local**; unrecognized modes display **Unknown**. This uses the same mode as the
API configuration, without making an additional API request.

Build CWBI development with `npm run build:development` and test with
`npm run build:test`. WAR builds select the equivalent mode with
`-PcdaGuiMode=development` / `-PcdaGuiMode=test` or `CDA_GUI_MODE`; the default is
`production`.

### GitHub Actions releases and deployments

The reusable release workflow selects the GUI mode from the branch or release tag
being built and passes it to both the WAR build and the Docker build:

| Release branch or tag                                   | Vite mode     | Header      | Deployment environment |
| ------------------------------------------------------- | ------------- | ----------- | ---------------------- |
| `develop`, `YYYY.MM.DD-dev` (including letter suffixes) | `development` | Development | Dev                    |
| `test`, `YYYY.MM.DD-test` (including letter suffixes)   | `test`        | Test        | Test                   |
| Other refs, including `YYYY.MM.DD` and `YYYY.MM.DD-a`   | `production`  | Production  | Prod                   |

The nightly workflow builds `develop-nightly` from `develop` and deploys it to
Dev. Tagged releases and manually dispatched release builds use the same mapping.
The Deploy workflow copies an already-built image from GHCR to ECR; selecting a
GitHub deployment environment does not rebuild the GUI. Select the matching image
tag and deployment environment when deploying. Existing images need a new build
to pick up the environment badge and mode selection.

For a direct Docker build, use `--build-arg CDA_GUI_MODE=development` or
`--build-arg CDA_GUI_MODE=test`; Docker builds default to `production`.

## UI Tests and Development

The UI tests use Storybook, Vitest, and Playwright. Stories provide an isolated place to
render components, and their `play` functions run as browser tests without requiring a
live CDA API.

Install the Chromium browser used by the test runner once after installing dependencies:

```sh
npx playwright install chromium
```

To open Storybook and see the stories and their interaction results:

```sh
npm run storybook
```

Then open http://localhost:6006 and select **Pages > Home > Landing Page**.

To run the same stories as headless browser tests:

```sh
npm run test-storybook -- --run
```

To verify that the static Storybook site can be built:

```shell
npm run build-storybook
```

To verify background token refresh against the running UI and Swagger editor:

```sh
npm run test:auth
```

These Chromium tests start Vite and mock CDA and Keycloak responses. They advance
the browser clock through five-minute refresh intervals and verify that drafts
survive and subsequent requests use the rotated token. No live login or database
is required.

## Formatting

Formatting is done with [husky](https://typicode.github.io/husky/) and styled using [prettier](https://prettier.io/docs/). Husky requires a minimum Git version of 2.9

When you commit, after running the install command above, husky will format files for you! But you can also run the formatter with:
`npx prettier --write myfile.html` or `npm run prepare`

Or check files with:
`format:check`
