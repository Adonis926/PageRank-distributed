#!/usr/bin/env python3
"""Fail closed when a CXLfork lifecycle result is incomplete."""

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
    required = [
        "metadata.txt", "run-manifest.csv", "tasks.csv", "workers.csv",
        "kernel.csv", "lifecycle.csv", "jobs.csv", "summary.csv",
        "validation.json",
    ]
    errors = [f"missing {name}" for name in required
              if not (root / name).is_file()]
    if errors:
        for error in errors:
            print(f"check failed: {error}")
        return 1

    validation = json.loads((root / "validation.json").read_text())
    errors.extend(validation.get("validation_errors", []))
    with (root / "tasks.csv").open(newline="", encoding="utf-8") as stream:
        tasks = list(csv.DictReader(stream))
    with (root / "lifecycle.csv").open(newline="", encoding="utf-8") as stream:
        lifecycles = {row["scenario"]: row for row in csv.DictReader(stream)}
    with (root / "jobs.csv").open(newline="", encoding="utf-8") as stream:
        jobs = list(csv.DictReader(stream))
    job_counts = Counter(row["scenario"] for row in jobs)
    phase_counts = Counter((row["scenario"], row["phase"]) for row in tasks)

    with (root / "run-manifest.csv").open(newline="", encoding="utf-8") as stream:
        for entry in csv.DictReader(stream):
            scenario = entry["scenario"]
            expected_cold = int(entry["expected_cold"])
            expected_steady = int(entry["expected_steady"])
            if scenario not in lifecycles:
                errors.append(f"{scenario}: missing lifecycle row")
            if job_counts[scenario] != 1:
                errors.append(f"{scenario}: job count is {job_counts[scenario]}")
            if phase_counts[(scenario, "cold")] != expected_cold:
                errors.append(f"{scenario}: cold task count mismatch")
            if phase_counts[(scenario, "steady")] != expected_steady:
                errors.append(f"{scenario}: steady task count mismatch")
            directory = root / "raw" / scenario
            output_diff = directory / "output.diff"
            if not output_diff.is_file() or output_diff.stat().st_size != 0:
                errors.append(f"{scenario}: WordCount output is not verified")
            for name in ["template-publish.log", "template-release.log",
                         "template-inspect-before-release.log",
                         "template-inspect-after-release.log", "master.log",
                         "agent0.log", "agent1.log"]:
                if not (directory / name).is_file():
                    errors.append(f"{scenario}: missing {name}")
            before = directory / "template-inspect-before-release.log"
            if before.is_file():
                text = before.read_text(errors="replace")
                match = re.search(r"reader_count=(\d+)", text)
                if match is not None and int(match.group(1)) != 0:
                    errors.append(f"{scenario}: reader_count is not zero")
            after = directory / "template-inspect-after-release.log"
            if after.is_file() and after.stat().st_size == 0:
                errors.append(f"{scenario}: missing failed-inspect evidence")

    for path in sorted((root / "system").glob("after-vm*.log")):
        for number, line in enumerate(
                path.read_text(errors="replace").splitlines(), 1):
            if FATAL_DMESG.search(line):
                errors.append(f"{path.name}:{number}: fatal dmesg: {line}")

    if errors:
        for error in errors[:100]:
            print(f"check failed: {error}")
        return 1
    print(f"lifecycle result check passed: {len(lifecycles)} cycles, "
          f"{len(tasks)} task records")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
