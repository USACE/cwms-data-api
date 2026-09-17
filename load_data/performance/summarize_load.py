"""Join client phases to same-host JVM metrics; write a compact comparison table."""
import argparse
import json
from pathlib import Path


def summarize(server, client):
    samples = [json.loads(line) for line in (server / "server-metrics.jsonl").read_text().splitlines()]
    phases = json.loads((client / "load-summary.json").read_text())
    result = []
    for phase in phases:
        if "startedEpochMs" not in phase:
            continue
        window = [s for s in samples if phase["startedEpochMs"] <= s["epochMs"] <= phase["endedEpochMs"]]
        row = {k: phase[k] for k in ["phase", "requests", "statuses", "validSuccesses", "invalid200",
                                    "p50Seconds", "p95Seconds", "p99Seconds", "requestsPerSecond",
                                    "peakRequestsSentAwaitingCompletion"]}
        if window:
            for label, metric in [("peakHeapMiB", "heapUsedBytes"), ("peakPoolActive", "poolActive"),
                                  ("peakPoolWaiters", "poolWaitCount"), ("peakQueuedTasks", "commonPoolQueued"),
                                  ("peakThreads", "threads")]:
                row[label] = max(s[metric] for s in window) / (1048576 if label == "peakHeapMiB" else 1)
            row["gcMs"] = window[-1]["gcMs"] - window[0]["gcMs"]
            row["cpuCoreEquivalent"] = ((window[-1]["cpuTimeNs"] - window[0]["cpuTimeNs"]) / 1e6
                                        / max(1, window[-1]["epochMs"] - window[0]["epochMs"]))
        result.append(row)
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("server", type=Path)
    parser.add_argument("client", type=Path)
    args = parser.parse_args()
    report = summarize(args.server, args.client)
    (args.client / "combined-summary.json").write_text(json.dumps(report, indent=2))
    for row in report:
        print(json.dumps(row))
