# CDA Landing Page Source
*The React+Vite project for CDA*


To run the project in dev:
`npm run dev`

To build the project:
`npm run build`
*NOTE*: Building the project will also deploy it to the required tomcat webapps directory `webapp`

To see the available scripts for this project, including how to run and deploy, look at the `package.json` file. 

## Extra Notes
*Paths are relative to project root*
* **Swagger Usage Locally:** Running NPM dev launches a local vite dev server that lets you test the components/web interface locally on your system. Depending on how you setup SwaggerUI in the `./pages/swagger-ui/index.jsx` will change whether or not it is able to grab data on your local test instance. (i.e. you'd need CDA running locally and answering on `./swagger-docs` for this to work)
* **Building WAR with new Web Interface:** In order to add your `npm run build` to the war you will have to:
  * Run `npm run build` from `./cwms-data-api/src/main/webapp-src`
  * Delete the old contents from the last web deploy i.e. `./cwms-data-api/src/main/webapp/assets`
  * Copy the contents of `./cwms-data-api/src/main/webapp-src/dist` to `./cwms-data-api/src/main/webapp`
* Deploy war as normal from here

