"""Check an owned local fixture's recovery while export clients stop reading bodies."""
import argparse
import asyncio
import collections
import datetime
import json
from pathlib import Path
import socket
import time
import urllib.parse

import aiohttp


async def run(server, output, hold):
    ready = json.loads((server / "ready.json").read_text())
    target = urllib.parse.urlsplit(ready["baseUrl"])
    if target.hostname not in {"localhost", "127.0.0.1"} or target.scheme != "http":
        raise ValueError("This probe only accepts a local HTTP fixture")
    start = datetime.datetime.fromisoformat(ready["start"].replace("Z", "+00:00"))
    route = target.path.rstrip("/") + "/timeseries/"
    opened = []
    probes = []
    started = int(time.time() * 1000)

    def parameters(index, points, page_size):
        return {"office": "SPK", "name": ready["series"][index % len(ready["series"])],
                "begin": ready["start"], "end": (start + datetime.timedelta(minutes=points - 1)).isoformat(),
                "unit": "EN", "page-size": page_size, "trim": "false"}

    async def slow(index):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 8192)
        sock.setblocking(False)
        try:
            await asyncio.wait_for(asyncio.get_running_loop().sock_connect(sock, ("127.0.0.1", target.port)), 5)
            reader, writer = await asyncio.open_connection(sock=sock, limit=16384)
            opened.append(writer)
            csv = index % 2 == 1
            accept = "text/csv" if csv else "application/json;version=2"
            query = urllib.parse.urlencode(parameters(index, 100000, 1000 if csv else -1))
            request = (f"GET {route}?{query} HTTP/1.1\r\nHost: localhost:{target.port}\r\n"
                       f"Accept: {accept}\r\nConnection: close\r\n\r\n")
            writer.write(request.encode("ascii"))
            await writer.drain()
            header = await asyncio.wait_for(reader.readuntil(b"\r\n\r\n"), 30)
            # StreamReader stops transport reads once its small buffer fills. Keep it
            # alive but deliberately do not consume any further response body.
            return {"client": index, "format": "csv" if csv else "json",
                    "status": int(header.split(b" ", 2)[1]), "headersEpochMs": int(time.time() * 1000),
                    "reader": reader}
        except Exception:
            sock.close()
            raise

    async def probe(session, phase):
        sample = {"phase": phase, "epochMs": int(time.time() * 1000), "status": 0, "valid": False}
        try:
            async with session.get(ready["baseUrl"] + "/timeseries/", params=parameters(0, 1440, 1),
                                   headers={"Accept": "application/json;version=2"}) as response:
                sample["status"] = response.status
                body = await response.json()
                sample["valid"] = response.status == 200 and body.get("total") == 1440 and len(body.get("values", [])) == 1
                sample["retryAfter"] = response.headers.get("Retry-After")
        except Exception as error:
            sample["error"] = type(error).__name__
        sample["endedEpochMs"] = int(time.time() * 1000)
        probes.append(sample)

    clients = []
    errors = []
    held_from = None
    try:
        outcomes = await asyncio.gather(*(slow(index) for index in range(8)), return_exceptions=True)
        clients = [result for result in outcomes if isinstance(result, dict)]
        errors = [type(result).__name__ for result in outcomes if isinstance(result, Exception)]
        if len(clients) != 8 or any(client["status"] != 200 for client in clients):
            raise RuntimeError("The probe did not establish eight successful export responses")
        held_from = int(time.time() * 1000)
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=5)) as session:
            until = time.monotonic() + hold
            while time.monotonic() < until:
                await probe(session, "unread-bodies")
                await asyncio.sleep(1)
            for writer in opened:
                writer.close()
            for _ in range(10):
                await probe(session, "after-disconnect")
                await asyncio.sleep(1)
    finally:
        for writer in opened:
            writer.close()
        for writer in opened:
            try:
                await asyncio.wait_for(writer.wait_closed(), 3)
            except Exception:
                pass
        summary = {"startedEpochMs": started, "endedEpochMs": int(time.time() * 1000),
                   "unreadBodiesStartedEpochMs": held_from, "clientErrors": errors,
                   "holdSeconds": hold, "clients": [{k: v for k, v in c.items() if k != "reader"} for c in clients],
                   "phases": {phase: {"statuses": dict(collections.Counter(str(p["status"]) for p in probes if p["phase"] == phase)),
                                      "validSuccesses": sum(p["valid"] for p in probes if p["phase"] == phase)}
                              for phase in {p["phase"] for p in probes}}, "probes": probes}
        output.write_text(json.dumps(summary, indent=2), encoding="utf-8")
        print(json.dumps({k: v for k, v in summary.items() if k != "probes"}))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("server", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--hold", type=int, default=60)
    args = parser.parse_args()
    if not 1 <= args.hold <= 120:
        parser.error("hold must be between 1 and 120 seconds")
    asyncio.run(run(args.server, args.output, args.hold))
