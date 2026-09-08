# CDA Landing Page Source

_The React+Vite project for CDA_

## Setup

Setup the project by running:

1. `cd cda-gui`
2. `npm install` (With nodejs installed)

## Development

To run the project in dev:
`npm run dev`

## Production Files

To build the project:
`npm run build`

To see the available scripts for this project, including how to run and deploy, look at the `package.json` file.

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

## Formatting

Formatting is done with [husky](https://typicode.github.io/husky/) and styled using [prettier](https://prettier.io/docs/). Husky requires a minimum Git version of 2.9

When you commit, after running the install command above, husky will format files for you! But you can also run the formatter with:
`npx prettier --write myfile.html` or `npm run prepare`

Or check files with:
`format:check`
