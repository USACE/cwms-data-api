# Metrics Reference

The authorization proxy exposes Prometheus-compatible metrics at the `/metrics` endpoint. These metrics provide visibility into request latency, cache effectiveness, and authorization decisions.

## Accessing Metrics

### Prometheus Format

```bash
curl http://localhost:3001/metrics
```

Returns metrics in Prometheus text exposition format:

```
# HELP authorizer_proxy_http_requests_total Total HTTP requests
# TYPE authorizer_proxy_http_requests_total counter
authorizer_proxy_http_requests_total{method="GET",route="/health",status_code="200"} 6695
```

### JSON Format

```bash
curl http://localhost:3001/metrics/json | jq
```

Returns metrics as a JSON object for easier programmatic access:

```json
{
  "http_requests_total": {
    "GET /health 200": 6695,
    "POST /authorize 200": 2092
  },
  "cache_hits_total": {
    "user_context": 4235
  }
}
```

## Available Metrics

### HTTP Request Metrics

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `authorizer_proxy_http_requests_total` | Counter | method, route, status_code | Total HTTP requests received |
| `authorizer_proxy_http_request_duration_seconds` | Histogram | method, route, status_code | Request latency distribution |
| `authorizer_proxy_active_connections` | Gauge | - | Current number of active connections |

The request duration histogram uses default Prometheus buckets: 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10 seconds.

### Cache Metrics

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `authorizer_proxy_cache_hits_total` | Counter | cache_type | Successful cache lookups |
| `authorizer_proxy_cache_misses_total` | Counter | cache_type | Cache lookup failures |
| `authorizer_proxy_cache_operation_duration_seconds` | Histogram | operation, result | Time spent on cache operations |

Cache types include:
- `user_context` - Redis cache for user profile data

Cache operations include:
- `get` - Cache read operations
- `set` - Cache write operations

### OPA Metrics

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `authorizer_proxy_opa_evaluations_total` | Counter | resource, action, decision | Policy evaluations by outcome |
| `authorizer_proxy_opa_evaluation_duration_seconds` | Histogram | resource, action, decision | Time spent evaluating policies |
| `authorizer_proxy_opa_cache_hits_total` | Counter | - | OPA decision cache hits |
| `authorizer_proxy_opa_cache_misses_total` | Counter | - | OPA decision cache misses |

The decision label values are `allow` or `deny`.

### API Call Metrics

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `authorizer_proxy_api_calls_total` | Counter | endpoint, status | Calls to downstream APIs |
| `authorizer_proxy_api_call_duration_seconds` | Histogram | endpoint, status | Downstream API latency |

Endpoints tracked include:
- `/user/profile` - User context lookup from CWMS Data API

### Authorization Decision Metrics

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `authorizer_proxy_authorization_decisions_total` | Counter | result | Authorization outcomes (allow/deny/error) |

## Querying Metrics

### Cache Hit Rate

Calculate the user context cache hit rate:

```promql
sum(rate(authorizer_proxy_cache_hits_total{cache_type="user_context"}[5m])) /
(sum(rate(authorizer_proxy_cache_hits_total{cache_type="user_context"}[5m])) +
 sum(rate(authorizer_proxy_cache_misses_total{cache_type="user_context"}[5m])))
```

### Request Latency Percentiles

Get the 95th percentile request latency:

```promql
histogram_quantile(0.95, rate(authorizer_proxy_http_request_duration_seconds_bucket[5m]))
```

### OPA Evaluation Time

Average OPA evaluation time by decision:

```promql
rate(authorizer_proxy_opa_evaluation_duration_seconds_sum[5m]) /
rate(authorizer_proxy_opa_evaluation_duration_seconds_count[5m])
```

### Error Rate

Percentage of requests returning 5xx errors:

```promql
sum(rate(authorizer_proxy_http_requests_total{status_code=~"5.."}[5m])) /
sum(rate(authorizer_proxy_http_requests_total[5m]))
```

## Grafana Dashboard

For visualization, import these metrics into Grafana. A sample dashboard configuration might include:

| Panel | Query | Visualization |
|-------|-------|---------------|
| Request Rate | `sum(rate(authorizer_proxy_http_requests_total[1m]))` | Time series |
| Latency p95 | `histogram_quantile(0.95, rate(authorizer_proxy_http_request_duration_seconds_bucket[1m]))` | Time series |
| Cache Hit Rate | See formula above | Gauge (0-100%) |
| Authorization Decisions | `sum by (result)(rate(authorizer_proxy_authorization_decisions_total[5m]))` | Pie chart |

## Alerting Rules

Example Prometheus alerting rules:

```yaml
groups:
  - name: authorizer-proxy
    rules:
      - alert: HighLatency
        expr: histogram_quantile(0.95, rate(authorizer_proxy_http_request_duration_seconds_bucket[5m])) > 0.5
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "Authorization proxy p95 latency above 500ms"

      - alert: LowCacheHitRate
        expr: |
          sum(rate(authorizer_proxy_cache_hits_total[5m])) /
          (sum(rate(authorizer_proxy_cache_hits_total[5m])) +
           sum(rate(authorizer_proxy_cache_misses_total[5m]))) < 0.8
        for: 10m
        labels:
          severity: warning
        annotations:
          summary: "Cache hit rate below 80%"

      - alert: HighErrorRate
        expr: |
          sum(rate(authorizer_proxy_http_requests_total{status_code=~"5.."}[5m])) /
          sum(rate(authorizer_proxy_http_requests_total[5m])) > 0.05
        for: 5m
        labels:
          severity: critical
        annotations:
          summary: "Error rate above 5%"
```

