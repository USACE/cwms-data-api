"""Sample an explicitly selected disposable Oracle container during a local run."""
import argparse
import json
from pathlib import Path
import subprocess
import time

parser = argparse.ArgumentParser()
parser.add_argument("container")
parser.add_argument("output", type=Path)
args = parser.parse_args()
with (args.output / "oracle-metrics.jsonl").open("w", encoding="utf-8", buffering=1) as log:
    deadline = time.monotonic() + 3600
    while time.monotonic() < deadline and not (args.output / "stop").exists():
        result = subprocess.run(
            ["rtk", "proxy", "docker", "stats", args.container, "--no-stream", "--format", "{{json .}}"],
            capture_output=True, text=True, timeout=20)
        if result.returncode or not result.stdout.strip():
            break
        sample = json.loads(result.stdout)
        sample["epochMs"] = int(time.time() * 1000)
        log.write(json.dumps(sample) + "\n")
        time.sleep(4)
