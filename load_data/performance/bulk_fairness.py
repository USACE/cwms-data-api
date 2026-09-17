"""Exercise large-window admission and small-read progress on an owned local fixture."""
import argparse
import asyncio
from collections import Counter
import datetime
import json
from pathlib import Path
import time
from urllib.parse import urlsplit

import aiohttp


async def run(server, output):
    ready = json.loads((server / "ready.json").read_text())
    target = urlsplit(ready["baseUrl"])
    if target.hostname not in {"localhost", "127.0.0.1"} or target.scheme != "http":
        raise ValueError("Only an owned local HTTP fixture is supported")
    start = datetime.datetime.fromisoformat(ready["start"].replace("Z", "+00:00"))
    samples = []

    async def request(session, wave, index, large):
        points, page = (1000000, 500) if large else (1440, 1)
        sample = {"wave": wave, "kind": "large" if large else "small",
                  "startedEpochMs": int(time.time() * 1000), "valid": False}
        try:
            async with session.get(ready["baseUrl"] + "/timeseries/", params={
                "office": "SPK", "name": ready["series"][index % len(ready["series"])],
                "begin": ready["start"], "end": (start + datetime.timedelta(minutes=points - 1)).isoformat(),
                "unit": "EN", "page-size": page, "trim": "false"
            }, headers={"Accept": "application/json;version=2"}) as response:
                sample["status"] = response.status
                sample["retryAfter"] = response.headers.get("Retry-After")
                body = await response.json()
                sample["valid"] = (response.status == 200 and body.get("total") == points
                                   and len(body.get("values", [])) == page)
        except Exception as error:
            sample["status"] = type(error).__name__
        sample["elapsedMs"] = int(time.time() * 1000) - sample["startedEpochMs"]
        samples.append(sample)

    try:
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=60)) as session:
            for wave in range(2):
                tasks = [asyncio.create_task(request(session, wave, i, True)) for i in range(8)]
                # Allow metadata-based admission to settle before probing reserved capacity.
                await asyncio.sleep(1)
                while any(not task.done() for task in tasks):
                    await request(session, wave, 0, False)
                    await asyncio.sleep(.2)
                await asyncio.gather(*tasks)
                for _ in range(10):
                    await request(session, wave, 0, False)
                large = [s for s in samples if s["wave"] == wave and s["kind"] == "large"]
                small = [s for s in samples if s["wave"] == wave and s["kind"] == "small"]
                if (sum(s["valid"] for s in large) != 2
                        or any(not s["valid"] and (s["status"] != 503 or s["retryAfter"] != "1") for s in large)
                        or not all(s["valid"] for s in small)):
                    raise RuntimeError("Bulk admission or small-read progress did not meet expectations")
    finally:
        summary = {"samples": samples, "groups": {
            f"{wave}/{kind}": {"statuses": dict(Counter(str(s["status"]) for s in samples
                                                        if s["wave"] == wave and s["kind"] == kind)),
                                "maxMs": max(s["elapsedMs"] for s in samples
                                             if s["wave"] == wave and s["kind"] == kind)}
            for wave, kind in {(s["wave"], s["kind"]) for s in samples}}}
        output.write_text(json.dumps(summary, indent=2), encoding="utf-8")
        print(json.dumps(summary["groups"]))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("server", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    asyncio.run(run(args.server, args.output))
