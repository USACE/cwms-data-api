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


## Formatting

Formatting is done with [husky](https://typicode.github.io/husky/) and styled using [prettier](https://prettier.io/docs/). Husky requires a minimum Git version of 2.9

When you commit, after running the install command above, husky will format files for you! But you can also run the formatter with:
`npx prettier --write myfile.html` or `npm run prepare`

Or check files with:
`format:check`