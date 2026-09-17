# Production configuration follow-up

Reviewed September 16, 2026, after the user reported approximately 900 or more
connections during the incident, a pool maximum temporarily raised to 250 and
subsequently possibly reduced, 32 GB memory, and a possible replica. These are
incident reports, not verified live settings. The connection metric source remains
unknown; HTTP connections, active requests, and Oracle sessions are different.

## Repository evidence

The authoritative repository supplied by the user is `cwbi-infrastructure/cwms-data`.
Findings below use its freshly fetched `cwbi-prod` branch, commit
`51db741ddc7ea5b70ed90e809f255fcca4570d80` (September 16). The earlier review used
`cwbi-dev-infrastructure/cwms-data`; its dev head matches the authoritative dev
branch, but missed the newer production database resize. Remote refs were fetched
into `M:\Programming\cwms-data-infra` without changing its working tree or origin.

| Component | Declared setting | Source |
| --- | --- | --- |
| API compute | Fargate x86_64, 2 vCPUs, 4,096 MiB task memory | `infrastructure/stacks/ecs/api.py:69-80` |
| API instances | One desired task; no service autoscaling found | `infrastructure/stacks/ecs/api.py:287-307` |
| API deployment minimum healthy | Hard-coded 0%; production YAML declares 50%, which this constructor does not consume | `api.py:296`, `environment/prod.yaml:31` |
| Pool | Runtime values injected from Secrets Manager | `api.py:192-203` |
| Pool creation defaults | Initial 5, max active 30, min idle 5, max idle 10 | `infrastructure/stacks/app_secrets.py:32-35` |
| Production Oracle | Oracle EE 19c Spatial, `db.r6i.2xlarge` | `environment/prod.yaml:49-50` |
| Production storage | gp3, 16,000 IOPS, throughput 750; 1,000 GB initial / 3,000 GB maximum | `environment/prod.yaml:52-56` |
| Database HA | `multiple_az: false`; no read replica definition found | `environment/prod.yaml:57`, `infrastructure/stacks/rds.py:215` |
| JVM heap | No explicit heap sizing found in the CDK API environment or the inspected CDA Docker startup script | Live JVM flags remain unknown |

Production PR #21, merged September 16 at 19:28:46 UTC, changes the database
from `db.m6i.2xlarge` (8 vCPUs, 32 GiB) to `db.r6i.2xlarge` (8 vCPUs, 64 GiB).
The reported 32 GB matches the earlier database configuration, while the API
task remains **4 GiB**. This is an inference about the incident report; a merged
infrastructure change does not prove the resize has deployed. The API task memory
limit is also not the same as its Java maximum heap.

The repository defines one API task and no RDS standby/read replica. A runtime
override, separate service, or later manual change could explain the reported
replica. Even a Multi-AZ DB-instance standby, if enabled outside this code, would
provide failover rather than serve read queries.

The available local AWS CLI identities did not match any of this repository's
configured CWMS deployment accounts. No ECS/RDS runtime or secret values were
retrieved, and no infrastructure was changed. In particular, the pool defaults
above do not establish its live maximum or disprove the reported 250 setting.

Sources:
- [Production database resize PR #21](https://github.com/cwbi-infrastructure/cwms-data/pull/21)
- [API task/service at reviewed commit](https://github.com/cwbi-infrastructure/cwms-data/blob/51db741ddc7ea5b70ed90e809f255fcca4570d80/infrastructure/stacks/ecs/api.py)
- [Production configuration](https://github.com/cwbi-infrastructure/cwms-data/blob/51db741ddc7ea5b70ed90e809f255fcca4570d80/environment/prod.yaml)
- [Pool defaults](https://github.com/cwbi-infrastructure/cwms-data/blob/51db741ddc7ea5b70ed90e809f255fcca4570d80/infrastructure/stacks/app_secrets.py)
- [AWS RDS instance specifications](https://aws.amazon.com/rds/instance-types/)
- [RDS Multi-AZ DB-instance standby behavior](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/Concepts.MultiAZSingleStandby.html)

## Two-CPU executor behavior: verified local probe

The earlier HTTP comparison deliberately used four visible processors. That is
**not equivalent** to the two-vCPU task declared here. Java 11's default async
executor uses a thread per task when common-pool parallelism is below two.
CDA's time-series controller invokes `CompletableFuture.supplyAsync` without an
explicit executor. Under the defaults, two visible processors yield common-pool
parallelism one and therefore trigger this fallback.

A bounded standalone Java 11.0.32.1 probe submitted 250 latch-blocked tasks using
the same async API, waited one second, cancelled every future with `cancel(true)`,
then released the latch and verified the running tasks drained:

| JVM-visible CPUs | Common-pool parallelism | Tasks running before cancel | Still running after cancel | Interruptions |
| ---: | ---: | ---: | ---: | ---: |
| 4 | 3 | 3 | 3 | 0 |
| 2 | 1 | 250 | 250 | 0 |

This is an **executor mechanism probe**, not an HTTP/Oracle load test, and does not
prove that production uses these defaults. No database connections were opened.
Source/results are `ExecutorProbe.java` and `executor-cpu-comparison.json` in
`C:\Users\krowv\Code\cda-memory-evidence`.

Consequences if the live JVM sees two CPUs and has no executor override:

- The previous finding of three workers limiting database work applies to the
  four-CPU tests, not this two-CPU deployment configuration.
- Raising the JDBC pool from 30 to 250 could permit much more simultaneous database
  work, increase result-buffer/native/thread memory, and overload Oracle. It does
  not repair cancellation. The actual concurrency also depends on servlet threads,
  CSV traffic, request completion/timeouts, other endpoints, and task count.
- HTTP timeouts can leave already-started work running. A large pool and accumulated
  requests can therefore cause a different failure pattern than a small executor
  queue. The probe alone does not demonstrate 900 simultaneous CDA requests.

Reference: [Java 11 CompletableFuture executor and cancellation contract](https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/util/concurrent/CompletableFuture.html).

## Revised next comparison and deployment recommendation

Keep #1902 first and the memory fix separately reviewable, but prioritize an
explicit read admission with proper JDBC cancellation. Do not depend
on CPU-count-sensitive common-pool defaults or restore a 250-connection pool as a
capacity fix. Pool size must be budgeted across all tasks/services using Oracle.

Before choosing final AWS sizing, obtain the live ECS task revision/image digest,
running/desired task count, CPU/memory, JVM available processors and max heap,
effective pool maximum, and the name/time range/scope of the 900-connection metric.
These are runtime fields; the CDK source cannot establish them.

The next local sensitivity comparison should use two visible CPUs and compare
pool 30 (source default) against 250 (reported incident setting), escalating mixed
HTTP concurrency through 100, 250, 500, 900, and 1,000. Explicitly label 900 as a
stress scenario until the metric is identified. Hold heap constant between pool
variants, model the 4 GiB container separately from heap, and repeat with a second
API instance against the same database if two live API tasks are confirmed.
The local Oracle Free fixture cannot reproduce the declared 8-vCPU/64-GiB Oracle
capacity (or the earlier 32-GiB instance). A large-heap allocation on the local JVM
would not change that limitation.
These two-CPU HTTP/pool scenarios have **not yet been run**.

For availability, configure at least two API tasks across availability zones and
make task count/health settings actually consume environment values. This should
be a separate infrastructure change; doubling tasks also doubles the aggregate
pool ceiling unless per-task limits are adjusted. Final CPU/heap/pool numbers
require the revised load evidence and live deployment settings, not the database's
32 GiB memory figure alone.

## Local admission and deadline implementation

The next local change replaces the default CompletableFuture execution in
`GET /timeseries` with synchronous request-owned work. A semaphore admits at most
eight reads per controller instance by default, including JSON, XML, legacy
formats, and CSV. It does not queue excess work: those requests receive 503 with
`Retry-After: 1` before creating their DAO or checking out a database connection.
Clients should retry with exponential backoff and jitter. The permit remains held
through response writing and resource cleanup.

Operators can set the JVM system property
`-Dcwms.cda.timeseries.maxConcurrentReads=8` (allowed range 1 through 64).
This is a provisional limit for load testing, not a measured production optimum.
The existing `-Dcwms.cda.api.apiTimeoutMs=45000` now supplies a request-owned JDBC
deadline (allowed range 1 through 300000 milliseconds). JDBC safeguards wrap the
data source before session preparation. They set statement query timeouts,
schedule statement cancellation, check cursor advancement, and temporarily bound
network reads. Connection return waits for any cancellation already in progress;
the original network timeout is restored before returning the connection.
The incremental gap-generation loop also checks the deadline.

This does not establish a strict end-to-end HTTP deadline. Servlet queueing occurs
before admission; pool checkout still uses the pool's own maximum wait; blocking
client writes still depend on container/network settings. Cancellation can take
time to finish. Streaming responses cannot change to a JSON error after headers
have committed. Other endpoint controllers are outside this admission limit.
The follow-up [size guards](read-limits-investigation.md) cover unlimited pages,
the eager fallback, CSV batches/exports, and legacy CLOB responses. The subsequent
[two-CPU mixed-load investigation](guard-concurrency-investigation.md) records
900/1000-client load, overload responses, retries, and remaining throughput limits.

### Guard validation, September 17 UTC

The full build passed with Java 11 and UTC: 760 API unit tests passed, 40 were
skipped; the focused Oracle suite passed 66 tests with one skipped. OpenAPI
validation, generated TypeScript client builds, documentation, and ETL checks
also passed. The unchanged GUI reused its existing build. Existing repository
Checkstyle warnings remain. Local logs are `read-guard-validation.log` under the
external evidence directory.

The admission unit test made 1,000 attempts through 32 caller threads while
holding accepted permits: exactly eight were admitted. This is not a
1,000-connection HTTP load test. Additional tests cover rejection before DAO
creation, permit recovery after failure, cursor/gap deadlines, and blocking
connection return until in-progress cancellation completes.

A real Tomcat pool with one connection and validation on every borrow ran a
ten-second `DBMS_SESSION.SLEEP` call with a one-second deadline:

| Oracle disableOob | Elapsed until failure | Oracle error | Next pool checkout |
| --- | ---: | --- | --- |
| false | 2,023 ms | ORA-18730 | Replacement session successfully queried |
| true | 2,014 ms | ORA-18730 | Replacement session successfully queried |

Both runs used the network-timeout fallback and destroyed the original physical
connection. The pool returned to zero active connections and the replacement
session worked. Neither run demonstrated clean ORA-01013 cancellation or proved
when server-side execution stopped. An earlier test deliberately required clean
cancellation and failed; its evidence is preserved in
`read-deadline-initial-oracle-failure.xml`. The replacement test checks the two
distinct valid cleanup outcomes instead of assuming clean cancellation.

Oracle documents that [statement cancellation depends on the database and network
responding](https://docs.oracle.com/en/database/oracle/oracle-database/21/jjdbc/JDBC-troubleshooting.html).
JDBC's [network timeout contract closes the connection when the timeout
expires](https://docs.oracle.com/en/java/javase/11/docs/api/java.sql/java/sql/Connection.html).
Production validation must verify failed-session disposal and server-side work
cessation using its driver, pool settings, and network path. These results are
not a production-readiness claim.

The subsequent full size-guard build added a CPU-intensive SQL calculation over
two 10,000-row inputs. With a one-second deadline, Oracle returned ORA-01013 after
1,021 ms (`disableOob=false`) and 1,007 ms (`true`). Both cases preserved the same
physical session, restored its prior network timeout, and successfully queried
it again. The sleep cases still required the network fallback (2,006/2,015 ms,
ORA-18730). Thus clean cancellation was demonstrated for the expensive SQL probe,
while the slower PL/SQL cancellation case exercised pool replacement. These are
distinct behaviors, not interchangeable timeout guarantees. Evidence is in
`read-limits-full-validation.log` and the `ReadDeadlineTestIT` XML results.

The later `pool-cached-validation.log` and XML snapshot repeat both kinds of
cancellation with `validationInterval=0` and `30000`, each with OOB enabled and
disabled. All eight cases pass: clean SQL cancellation preserves the session,
and the network-timeout cases obtain a working replacement session. The Docker
image copies the test-resource Tomcat context through `prepareDockerBuild`; that
context enables validation on borrow but does not override the pool's cached
validation interval. This additional test did not require changing that setting.
It still does not verify a deployed image, secret override, or production network.
