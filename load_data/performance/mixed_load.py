"""Drive ONLY the disposable local benchmark server; requires aiohttp.

The API and load generator run in separate processes. Each JSON response is
checked for total, row count and first/last timestamps. Detailed records are
flushed after each response so API termination does not erase the evidence.
"""
import argparse
import asyncio
import collections
import datetime as dt
import json
import math
from pathlib import Path
import random
import time
from urllib.parse import urlsplit

import aiohttp


PROFILES = (
    ("short", 1440, 500, "application/json;version=2", 50),
    ("medium", 43200, 5000, "application/json;version=2", 25),
    ("long_window", 1000000, 500, "application/json;version=2", 15),
    ("json_export", 100000, -1, "application/json;version=2", 5),
    ("csv_export", 100000, 1000, "text/csv", 5),
)


def percentile(values, percent):
    return sorted(values)[max(0, math.ceil(len(values) * percent / 100) - 1)] if values else None


def summarize(records):
    latencies = [r["seconds"] for r in records]
    successes = [r["seconds"] for r in records if r["status"] == 200 and r["valid"]]
    return {"requests": len(records), "statuses": dict(collections.Counter(str(r["status"]) for r in records)),
            "retryableOverloadResponses": sum(r["status"] == 503 and r.get("retryAfter") == "1" for r in records),
            "validSuccesses": len(successes), "invalid200": sum(r["status"] == 200 and not r["valid"] for r in records),
            "p50Seconds": percentile(latencies, 50), "p95Seconds": percentile(latencies, 95),
            "p99Seconds": percentile(latencies, 99), "successP95Seconds": percentile(successes, 95),
            "bytes": sum(r["bytes"] for r in records),
            "errors": dict(collections.Counter(r["error"] for r in records if r["error"]))}


class Load:
    def __init__(self, ready, output, timeout, units="mixed"):
        self.ready, self.output, self.timeout = ready, output, timeout
        self.units = units
        if urlsplit(ready["baseUrl"]).hostname not in ("localhost", "127.0.0.1", "::1"):
            raise ValueError("This harness only allows the local disposable fixture")
        self.start = dt.datetime.fromisoformat(ready["start"].replace("Z", "+00:00"))
        self.log = (output / "requests.jsonl").open("w", encoding="utf-8", buffering=1)
        self.sequence = 0
        self.wire_active = 0
        self.wire_peak = 0
        self.created = 0
        self.phases = []

    async def headers_sent(self, session, context, params):
        if not context.trace_request_ctx["sent"]:
            context.trace_request_ctx["sent"] = True
            self.wire_active += 1
            self.wire_peak = max(self.wire_peak, self.wire_active)

    async def connection_created(self, session, context, params):
        self.created += 1

    def plan(self, count, seed, probe=False):
        rng = random.Random(seed)
        profiles = [p for p in PROFILES for _ in range(p[4])]
        rng.shuffle(profiles)
        result = []
        for index in range(count):
            name, points, page, accept, _ = profiles[index % 100]
            if probe:
                name, points, page, accept = "recovery", 60, 60, "application/json;version=2"
            offset = rng.randrange(max(1, self.ready["points"] - points + 1))
            begin = self.start + dt.timedelta(minutes=offset)
            end = begin + dt.timedelta(minutes=points - 1)
            params = {"office": "SPK", "name": self.ready["series"][index % len(self.ready["series"])],
                      "units": "ft" if self.units == "mixed" and index % 10 == 0 else "EN",
                      "begin": begin.isoformat(),
                      "end": end.isoformat(), "page-size": page}
            result.append({"profile": name, "points": points, "page": page, "accept": accept,
                           "beginMs": int(begin.timestamp() * 1000), "params": params})
        return result

    @staticmethod
    def validate(body, plan):
        if plan["accept"] == "text/csv":
            # CSV has a header and a row per observed minute; inspect exact count
            # after a smoke request establishes this fixture's header convention.
            count = len(body.splitlines()) - 1
            return count == plan["points"], {"csvRows": count}
        data = json.loads(body)
        values = data.get("values", [])
        count = plan["points"] if plan["page"] < 0 else min(plan["page"], plan["points"])
        valid = (data.get("total") == plan["points"] and len(values) == count
                 and values[0][0] == plan["beginMs"]
                 and values[-1][0] == plan["beginMs"] + (count - 1) * 60000)
        return valid, {"reportedTotal": data.get("total"), "returnedRows": len(values)}

    async def request(self, session, phase, plan):
        self.sequence += 1
        identifier = self.sequence
        trace = {"sent": False}
        started = time.perf_counter()
        record = {"id": identifier, "phase": phase, "profile": plan["profile"],
                  "windowPoints": plan["points"], "pageSize": plan["page"], "units": plan["params"]["units"],
                  "startedEpochMs": int(time.time() * 1000), "status": 0, "bytes": 0,
                  "valid": False, "error": None}
        try:
            async with session.get(self.ready["baseUrl"] + "/timeseries", params=plan["params"],
                                   headers={"Accept": plan["accept"], "Accept-Encoding": "identity"},
                                   trace_request_ctx=trace) as response:
                record["status"] = response.status
                record["retryAfter"] = response.headers.get("Retry-After")
                record["ttfbSeconds"] = time.perf_counter() - started
                body = await response.read()
                record["bytes"] = len(body)
                record["seconds"] = time.perf_counter() - started
                if trace["sent"]:
                    self.wire_active -= 1
                    trace["sent"] = False
                if response.status == 200:
                    record["valid"], details = await asyncio.to_thread(self.validate, body, plan)
                    record.update(details)
                    if not record["valid"]:
                        record["error"] = "response_validation"
                        (self.output / f"invalid-{identifier}.body").write_bytes(body)
                else:
                    try:
                        record["error"] = json.loads(body).get("message", "HTTP error")
                    except (ValueError, AttributeError):
                        record["error"] = "non_json_http_error"
        except Exception as error:
            record["seconds"] = time.perf_counter() - started
            record["error"] = type(error).__name__
        finally:
            if trace["sent"]:
                self.wire_active -= 1
        record["endedEpochMs"] = int(time.time() * 1000)
        self.log.write(json.dumps(record) + "\n")
        return record

    async def phase(self, session, name, concurrency, count=None, duration=None, rate=None):
        self.wire_peak, self.created = 0, 0
        started = time.perf_counter()
        epoch = int(time.time() * 1000)
        records = []
        seed = 20260916
        if rate:
            tasks = []
            plans = self.plan(int(duration * rate), seed)
            for index, plan in enumerate(plans):
                await asyncio.sleep(max(0, started + index / rate - time.perf_counter()))
                tasks.append(asyncio.create_task(self.request(session, name, plan)))
            records = await asyncio.gather(*tasks)
        else:
            plans = iter(self.plan(count or 100000, seed))

            async def worker():
                while duration is None or time.perf_counter() - started < duration:
                    try:
                        plan = next(plans)
                    except StopIteration:
                        return
                    records.append(await self.request(session, name, plan))

            await asyncio.gather(*(worker() for _ in range(concurrency)))
        elapsed = time.perf_counter() - started
        summary = {"phase": name, "concurrencyLimit": concurrency, "arrivalRate": rate,
                   "startedEpochMs": epoch, "endedEpochMs": int(time.time() * 1000),
                   "elapsedSeconds": elapsed, "peakRequestsSentAwaitingCompletion": self.wire_peak,
                   "connectionsCreated": self.created, "requestsPerSecond": len(records) / elapsed,
                   **summarize(records),
                   "profiles": {p: summarize([r for r in records if r["profile"] == p])
                                for p in sorted({r["profile"] for r in records})}}
        self.phases.append(summary)
        (self.output / "load-summary.json").write_text(json.dumps(self.phases, indent=2), encoding="utf-8")
        print(json.dumps({k: v for k, v in summary.items() if k not in ("profiles", "errors")}), flush=True)

    async def recover(self, session, preceding):
        records = []
        started = time.perf_counter()
        for index in range(10):
            plan = self.plan(1, index, probe=True)[0]
            records.append(await self.request(session, "recovery-" + preceding, plan))
            if time.perf_counter() - started > 90:
                break
            await asyncio.sleep(3)
        result = {"phase": "recovery-" + preceding, **summarize(records)}
        self.phases.append(result)
        (self.output / "load-summary.json").write_text(json.dumps(self.phases, indent=2), encoding="utf-8")
        print(json.dumps(result), flush=True)

    async def retry_burst(self, session, clients, budget):
        """Measure logical completion separately from the number of HTTP attempts."""
        phase = f"retry-burst-{clients}"
        started_epoch = int(time.time() * 1000)

        async def logical(index, plan):
            started = time.perf_counter()
            attempts = 0
            rng = random.Random(20260916 + index)

            async def attempt_until_done():
                nonlocal attempts
                while True:
                    attempts += 1
                    result = await self.request(session, phase, plan)
                    if result["status"] != 503:
                        return result
                    delay = min(10, 2 ** min(attempts - 1, 3))
                    await asyncio.sleep(delay + rng.random())

            try:
                result = await asyncio.wait_for(attempt_until_done(), timeout=budget)
                valid = result["status"] == 200 and result["valid"]
                status = result["status"]
            except asyncio.TimeoutError:
                valid, status = False, "logical_timeout"
            return {"logicalId": index, "attempts": attempts, "valid": valid, "status": status,
                    "seconds": time.perf_counter() - started, "profile": plan["profile"]}

        results = await asyncio.gather(*(logical(i, plan)
                                        for i, plan in enumerate(self.plan(clients, 20260916))))
        successful = [r["seconds"] for r in results if r["valid"]]
        summary = {"phase": phase, "logicalRequests": clients, "deadlineSeconds": budget,
                   "startedEpochMs": started_epoch, "endedEpochMs": int(time.time() * 1000),
                   "validSuccesses": len(successful), "httpAttempts": sum(r["attempts"] for r in results),
                   "finalStatuses": dict(collections.Counter(str(r["status"]) for r in results)),
                   "successP95Seconds": percentile(successful, 95), "results": results}
        (self.output / "retry-summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
        print(json.dumps({k: v for k, v in summary.items() if k != "results"}), flush=True)

    async def run(self, args):
        trace = aiohttp.TraceConfig()
        trace.on_request_headers_sent.append(self.headers_sent)
        trace.on_connection_create_end.append(self.connection_created)
        connector = aiohttp.TCPConnector(limit=0, limit_per_host=0)
        timeout = aiohttp.ClientTimeout(total=self.timeout, connect=30)
        async with aiohttp.ClientSession(connector=connector, timeout=timeout, trace_configs=[trace]) as session:
            if args.smoke:
                for profile in PROFILES:
                    plan = self.plan(100, 20260916)
                    selected = next(p for p in plan if p["profile"] == profile[0])
                    result = await self.request(session, "smoke", selected)
                    print(json.dumps(result), flush=True)
                return
            for concurrency in args.levels:
                name = f"burst-{concurrency}"
                await self.phase(session, name, concurrency, count=max(20, concurrency * 2))
                await self.recover(session, name)
            if args.soak:
                await self.phase(session, "soak-100", 100, duration=args.soak)
                await self.recover(session, "soak-100")
            if args.arrival:
                await self.phase(session, "arrival-20rps", 0, duration=args.arrival, rate=20)
                await self.recover(session, "arrival-20rps")
            if args.retry_burst:
                await self.retry_burst(session, args.retry_burst, args.retry_deadline)
                await self.recover(session, f"retry-burst-{args.retry_burst}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("server", type=Path, help="Directory containing the fixture's ready.json")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--levels", type=int, nargs="+", default=[1, 10, 50, 100, 200, 400])
    parser.add_argument("--soak", type=int, default=120)
    parser.add_argument("--arrival", type=int, default=30)
    parser.add_argument("--timeout", type=int, default=120)
    parser.add_argument("--smoke", action="store_true")
    parser.add_argument("--units", choices=["mixed", "EN"], default="mixed")
    parser.add_argument("--retry-burst", type=int, default=0)
    parser.add_argument("--retry-deadline", type=int, default=120)
    options = parser.parse_args()
    options.output.mkdir(parents=True, exist_ok=True)
    ready = json.loads((options.server / "ready.json").read_text())
    (options.output / "server.json").write_text(json.dumps(ready, indent=2))
    load = Load(ready, options.output, options.timeout, options.units)
    try:
        asyncio.run(load.run(options))
    finally:
        load.log.close()
