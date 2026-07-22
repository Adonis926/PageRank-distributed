#!/usr/bin/env python3
"""Fail closed when a Motivation result directory is incomplete or corrupt."""

from __future__ import annotations

import argparse
import csv
import json
import re
from collections import Counter
from pathlib import Path


FATAL_DMESG = re.compile(
    r"BUG:|Oops:|kernel panic|general protection fault|use-after-free|"
    r"cxlfork.*(?:error|failed)", re.IGNORECASE)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("result_root", type=Path)
    args = parser.parse_args()
    root = args.result_root.resolve()
    required = ["metadata.txt", "run-manifest.csv", "tasks.csv", "jobs.csv",
                "summary.csv", "job-summary.csv", "validation.json"]
    missing = [name for name in required if not (root / name).is_file()]
    if missing:
        print("missing result files: " + ", ".join(missing))
        return 1

    validation = json.loads((root / "validation.json").read_text())
    errors = list(validation.get("validation_errors", []))
    with (root / "tasks.csv").open(newline="", encoding="utf-8") as stream:
        rows = list(csv.DictReader(stream))
    counts = Counter(row["scenario"] for row in rows
                     if int(row.get("warmup", "0")) == 0)
    warmups = Counter(row["scenario"] for row in rows
                      if int(row.get("warmup", "0")) != 0)
    with (root / "jobs.csv").open(newline="", encoding="utf-8") as stream:
        jobs = list(csv.DictReader(stream))
    job_counts = Counter(job["scenario"] for job in jobs)

    with (root / "run-manifest.csv").open(newline="", encoding="utf-8") as stream:
        for entry in csv.DictReader(stream):
            if entry["type"] != "probe":
                scenario = entry["scenario"]
                if job_counts[scenario] != 1:
                    errors.append(
                        f"{scenario}: WordCount job record count is {job_counts[scenario]}"
                    )
                output_diff = root / "raw" / scenario / "output.diff"
                if not output_diff.is_file() or output_diff.stat().st_size != 0:
                    errors.append(f"{scenario}: WordCount output is not verified")
                continue
            scenario = entry["scenario"]
            expected = int(entry["expected_measured"])
            expected_warmups = int(entry["expected_warmup"])
            if counts[scenario] != expected:
                errors.append(
                    f"{scenario}: measured count {counts[scenario]} != {expected}"
                )
            if warmups[scenario] != expected_warmups:
                errors.append(
                    f"{scenario}: warmup count {warmups[scenario]} != {expected_warmups}"
                )

    for path in sorted((root / "system").glob("after-vm*.log")):
        for number, line in enumerate(path.read_text(errors="replace").splitlines(), 1):
            if FATAL_DMESG.search(line):
                errors.append(f"{path.name}:{number}: fatal dmesg: {line}")

    for scenario in sorted((root / "raw").glob("probe_cxlfork_*")):
        release = scenario / "template-release.log"
        inspect = scenario / "template-release-inspect-after.log"
        if not release.is_file() or not inspect.is_file():
            errors.append(f"{scenario.name}: missing release lifecycle evidence")

    if errors:
        for error in errors[:100]:
            print(f"check failed: {error}")
        return 1
    print(f"result check passed: {len(rows)} task records")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
