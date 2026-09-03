# Running Benchmarks

The authorization proxy includes a benchmark suite built with [k6](https://k6.io/). These tests simulate realistic traffic patterns to measure latency, throughput, and cache effectiveness.

## Prerequisites

You will need k6 installed on your machine. On macOS:

```bash
brew install k6
```

On other platforms, see the [k6 installation guide](https://grafana.com/docs/k6/latest/set-up/install-k6/).

The benchmark assumes all services are running via Podman or Docker Compose:

```bash
cd cwms-access-management
podman compose -f docker-compose.podman.yml up -d
```

Verify services are healthy:

```bash
curl http://localhost:3001/health
curl http://localhost:8080/auth/realms/cwms
```

## Quick Benchmark

For a quick sanity check, run the 30-second benchmark:

```bash
cd cwms-access-management/tools/benchmark
k6 run quick-benchmark.js
```

This runs 10 virtual users making a mix of requests:
- Health checks (baseline latency)
- Authenticated requests through the proxy
- Direct authorization endpoint calls

The quick benchmark authenticates against Keycloak using the test user credentials, so it exercises the full authorization flow.

## Full Benchmark Suite

The full suite runs five scenarios sequentially, taking approximately three minutes:

```bash
cd cwms-access-management/tools/benchmark
./run-benchmark.sh
```

Or run it directly:

```bash
k6 run scenarios.js
```

### Test Scenarios

| Scenario | Duration | VUs | What It Tests |
|----------|----------|-----|---------------|
| Public Endpoints | 30s | 10 | Baseline proxy overhead on `/health` and `/ready` |
| Warm Cache | 30s | 20 | Repeated requests with same user (cache hits) |
| Cold Cache | 50 requests | 10 | Unique queries forcing cache misses |
| Authorization Endpoint | 30s | 15 | Direct `/authorize` API without proxying |
| Stress Test | 60s | 5-50 | Ramping load to find breaking point |

## Environment Configuration

The benchmarks read configuration from environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `PROXY_URL` | `http://localhost:3001` | Authorization proxy address |
| `KEYCLOAK_URL` | `http://localhost:8080` | Keycloak server address |
| `KEYCLOAK_REALM` | `cwms` | OAuth realm name |
| `KEYCLOAK_CLIENT_ID` | `cwms` | OAuth client ID |

To run against a different environment:

```bash
PROXY_URL=http://staging-proxy:3001 \
KEYCLOAK_URL=http://staging-auth:8080 \
k6 run scenarios.js
```

## Test Users

The benchmarks use three test accounts with different permission levels:

| User Key | Username | Office | Purpose |
|----------|----------|--------|---------|
| `damOperator` | `m5hectest` | SWT | Primary test user, CWMS Users role |
| `waterManager` | `l2hectest` | SPK | Tests cross-office access |
| `viewerUser` | `l1hectest` | SPL | Tests limited permissions |

These users must exist in both Keycloak and the CWMS database. The Docker Compose setup configures them automatically.

## Reading Results

After running, k6 outputs a summary like:

```
     checks.........................: 99.85% 10882 out 10898
     http_req_duration..............: avg=5.72ms   p(95)=12.4ms
     http_req_failed................: 0.00%  0 out of 10940
     authorization_latency..........: avg=3.1ms    p(95)=8ms
     cache_hit_rate.................: 98.50%
```

Key metrics to watch:

| Metric | Good | Concerning |
|--------|------|------------|
| `http_req_duration` p95 | <100ms | >500ms |
| `http_req_failed` | 0% | >1% |
| `cache_hit_rate` | >95% | <80% |

## Collecting Prometheus Metrics

The proxy exposes metrics at `/metrics` that can be scraped during the benchmark:

```bash
# Before running benchmark
curl http://localhost:3001/metrics > before.txt

# Run benchmark
k6 run quick-benchmark.js

# After running benchmark
curl http://localhost:3001/metrics > after.txt
```

For JSON format (easier to parse):

```bash
curl http://localhost:3001/metrics/json | jq
```

## Troubleshooting

### Token Acquisition Fails

If you see `invalid_client` errors, verify the OAuth client exists in Keycloak:

```bash
curl -X POST "http://localhost:8080/auth/realms/cwms/protocol/openid-connect/token" \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "username=m5hectest&password=m5hectest&grant_type=password&client_id=cwms"
```

### Connection Refused

Ensure all services are running:

```bash
podman ps | grep -E 'authorizer-proxy|auth|opa|redis'
```

### High Error Rates

Check proxy logs for details:

```bash
podman logs -f authorizer-proxy
```

