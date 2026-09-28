# CDA Landing Page Source

_The React+Vite project for CDA_

## Setup

Setup the project by running:

1. `cd cda-gui`
2. `npm install` (With nodejs installed)

## Development

For user registration, office assignment, and the required Admin and PD roles,
see the [user onboarding guide](../docs/source/user-onboarding.md).

To run the project in dev:
`npm run dev`

## Local onboarding demo

Run `npm run dev:onboarding` and open
http://127.0.0.1:18742/cwms-data/user-roles. Click **Log in** to use the sample
administrator, then **Onboard users**. No Docker, real identity provider, or
database is needed. This development script serves an in-memory API; it does not
validate real database authorization and is not included in production builds.

| Sample user        | Starting state                                         |
| ------------------ | ------------------------------------------------------ |
| `alex.hq`          | HQ with All Users and CWMS Users; onboarding candidate |
| `blair.unassigned` | No office; onboarding candidate                        |
| `casey.baseline`   | HQ with All Users only; onboarding candidate           |
| `devon.tulsa`      | Existing SWT read/write user                           |
| `ellis.multi`      | Existing HQ, SWT, and SPK memberships                  |
| `frankie.hq.admin` | Established HQ administrator; excluded from onboarding |
| `gray.sacramento`  | Existing SPK read-only user                            |

The sample administrator has Admin and PD in SWT and SPK. Saves update the sample
users until the server restarts. **Reset sample users** restores the initial data.
Run `npm run test:user-roles` to exercise onboarding in Chromium with these fixtures.

## Production build

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
