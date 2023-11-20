# CWMS-Data-Api Docker Compose environment.

Due to the nature of the needs of this system it is not possible to just up and run `docker-compose up`, some manual setup will be required.

## Here are the following pre-steps
1. Add `<real host ip> cwms-data.localhost auth.localhost traefik.localhost` to the /etc/hosts file  (**Warning: 127.0.0.1 doesn't work!**)
2. In the compose_files/pki directory run `./genall.sh`. This will create the initial PKI infrastructure
3. Create an environment file with appropriate references for your environment and testing.


## Starting the system

run `docker-compose --env-file <env file> up -d --force-recreate`

The first time this is run it will take ~40 minutes while Oracle Initializes and the schema is installed. Subsequent runs will be faster.
The force recreate is required as we are dumping our local rootca into the java keystore of the data-api image so the query to keycloak 
can be verified correctly.

## What is provided.

1. A CWMS Oracle Database
2. An instance of the CWMS Data API
3. An instance of keycloak that can be used to login. (The swagger-ui will allow entering a username and password and set the appropriate variables.)

The following users and permissions are available:

| User        | Password    | Office | Permissions    |
| ----------- | ----------- | ------ | ------------   |
| l2hectest   | l2hectest   | SPK    | General User   |
| l1hectest   | l1hectest   | SPL    | No permissions |
| m5hectest   | m5hectest   | SWT    | General User   |


## Inventory of services


|service|host-port|container-port|description|test urls|
|----|--|---|--|--|
|[traefik](./compose_files/traefik/traefik.yml)|8444|8443|entry point - web traffic|https://cwms-data.localhost:8444/cwms-data/ https://auth.localhost:8444/auth/realms/cwms https://auth.localhost:8444/auth/realms/cwms/.well-known/openid-configuration|
|db||1521|oracle database|
|[api](./cwms-data-api/src/docker/Dockerfile)||7000|tomcat CWMS Data API |
|[auth](./compose_files/keycloak/Dockerfile)||8080|authentication-token service (keycloak)|
|db_install|||connects to db and installs CWMS schema|
|db_webuser_ permissions|||connects to db and sets permissions |


