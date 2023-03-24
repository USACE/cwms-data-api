# CWMS DATA API (CDA) 
*Formerly known as RADAR*

![Build CWMS RADAR](https://github.com/USACE/cwms-radar-api/workflows/Build%20CWMS%20RADAR/badge.svg)

This is a new implementation of a REST API for data retrieval of USACE Water Data.

See the [📃 Wiki](https://github.com/USACE/cwms-radar-api/wiki) for how to get started


## Development notes

Development and runtime currently requires java 8.


To build the war:

     ./gradlew build

This will compile the jar and run the basic unit tests.

## Testing

To run the integration tests:

     ./gradlew integrationtests

If the testcontainers bypass options are not used (see gradle.properties.example) the tests
will attempt to pull a build oracle image, the cwms schema installer image, and create a database for you.
This takes 30-40 minutes on first run. Subsequent runs are a minimum of ~7 minutes for the schema install.

This is primarily to support CI/CI flushing the docker environment and to make it easier to start from scratch.
Using a bypass database the current test suite is about 2 minutes. 

The "integrationtests" task is a standard Gradle Test type. You can use test filters to reduce the testing to
a specific suite, class, or method as with any other testing further reducing cycle time.


### Creating new tests

Extend integration tests from `DataApiTestIT`. This class contains the ExtendWith for handling Database setup and 
several helper functions to register users and data to be deleted.


There are several SQL templates used by the `DataApiTestIT` class in the `test/resources/helpers` directory.
District or other specific data is also appropriate to use if necessary.

However, all locations should be registered with `DataApiTestIT#createLocation` in a:

- `@BeforeEach` handler if all tests will need the same location
- the start of the individual tests

This will allow Junit to cascade delete the locations so the test can be cycled again. Tests MAY do their own location deletes. 
For DataApiTestIT failure to delete a location that does not exist is considered a normal condition and ignored.

We use [RestAssured](https://rest-assured.io/) to do the testing with logging set to "print request/response on failure" on by default. 
(See RadarApiSetupCallBack.java for more additional defaults.)

We do not currently have examples of parameterized tests, but the use there of is highly encouraged to make adding simple test 
cases easier.

### Test Users

For operations requiring users with write privileges or other privileges for testing, they are registered as enums in
`fixtures/TestAccounts` 

    NOTE: future work will likely allow just passing in the RestAssured request to the user to configure the request appropriate
    e.g, Session Cookie vs Authorization header.

Currently only the API Key is supported, future intention is to parameterize the tests with the user type.

Current and new tests should feel free to use any office ID desired, the integration tests are setup as a multi office database. 
However it MUST be explicit on each request.

### DataApiTestIT expansion

If expanding the functionality of the Base class, do not depend on the SQL wrappers. Either use direct JDBC, or [JDBI3](https://jdbi.org/)
This is to isolate specific possible errors with various APIs and reduces points of failure in initial setup for traceability.