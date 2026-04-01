# Performance Testing

Understanding how the authorization proxy performs under load helps inform capacity planning and identify potential bottlenecks. This section covers the benchmarking tools, how to run performance tests, and what the results mean.

## Why Performance Testing Matters

The authorization proxy sits in the critical path for every request to the CWMS Data API. Even small latency increases can compound across thousands of requests. Performance testing helps answer questions like:

- How much overhead does the proxy add to each request?
- At what point does the system start to degrade under load?
- Is the caching strategy effective?
- How does OPA policy evaluation scale?

## Metrics Collected

The proxy exposes Prometheus-compatible metrics at the `/metrics` endpoint. These provide insight into both real-time behavior and historical trends.

| Metric Category | What It Measures |
|-----------------|------------------|
| Request latency | Time to process each request, broken down by endpoint |
| Cache performance | Hit and miss rates for both Redis and OPA caches |
| OPA evaluation | Time spent evaluating authorization policies |
| API calls | Latency when fetching user context from the backend |
| Connection tracking | Number of concurrent connections being handled |

## Testing Approach

Performance tests use [k6](https://k6.io/), a load testing tool that runs scenarios with simulated virtual users. The tests authenticate against Keycloak, make requests through the proxy, and measure response times.

The test suite includes several scenarios that exercise different aspects of the system:

| Scenario | Purpose |
|----------|---------|
| Public endpoints | Baseline measurement of proxy overhead |
| Authenticated with warm cache | Typical production behavior where users are already cached |
| Authenticated with cold cache | Worst-case latency when cache misses require backend calls |
| Direct authorization | Isolated measurement of policy evaluation |
| Stress test | System behavior under increasing load |

## Quick Start

For those wanting to run a quick benchmark locally:

```bash
cd cwms-access-management/tools/benchmark

# Run a 30-second quick test
k6 run quick-benchmark.js
```

Detailed instructions and result interpretation are covered in the following pages.

```{toctree}
:maxdepth: 2

running-benchmarks
benchmark-results
metrics-reference
```
