#!/usr/bin/env python3
"""Convert Motivation benchmark logs into checked per-task and summary CSVs."""

from __future__ import annotations

import argparse
import csv
import json
import math
import statistics
from pathlib import Path


TASK_PREFIX = "task_result_v3"
HYDRA_PREFIX = "hydra_task_v1"
RESPONSE_PREFIX = "hydra_response_v1"

DERIVED_FIELDS = [
    "warmup",
    "target_parse_ns",
    "target_payload_copy_ns",
    "target_command_transfer_ns",
    "target_supervisor_gate_ns",
    "target_worker_wakeup_ns",
    "target_bind_ns",
    "target_to_state_ready_ns",
    "target_to_business_ns",
    "target_to_first_state_access_ns",
    "target_business_ns",
    "target_reap_ns",
    "target_refill_ns",
    "target_until_agent_collect_ns",
    "master_request_send_wall_ns",
    "master_response_consume_ns",
    "target_response_send_wall_ns",
    "cross_os_unassigned_ns",
    "kernel_accounted_ns",
    "kernel_unaccounted_ns",
    "kernel_roundtrip_fraction_ppm",
    "restore_target_activation_fraction_ppm",
]

SUMMARY_METRICS = [
    "roundtrip_ns",
    "hydra_server_stack_ns",
    "target_to_state_ready_ns",
    "target_to_business_ns",
    "target_to_first_state_access_ns",
    "restore_wrapper_ns",
    "resume_ioctl_ns",
    "kernel_total_ns",
    "target_bind_ns",
    "target_reap_ns",
    "target_refill_ns",
    "master_request_send_wall_ns",
    "target_response_send_wall_ns",
    "cross_os_unassigned_ns",
]

KERNEL_EXCLUSIVE_FIELDS = [
    "kernel_lock_ns",
    "kernel_lifecycle_ns",
    "kernel_manifest_ns",
    "kernel_image_ns",
    "kernel_checksum_ns",
    "kernel_conflict_ns",
    "kernel_vma_ns",
    "kernel_upper_pt_ns",
    "kernel_shared_leaf_ns",
    "kernel_pte_validate_ns",
    "kernel_pte_install_ns",
    "kernel_fallback_other_ns",
    "kernel_dirty_prefault_ns",
    "kernel_global_ns",
    "kernel_other_ns",
]


def parse_record(line: str, prefix: str) -> dict[str, int] | None:
    if not line.startswith(prefix + " "):
        return None
    record: dict[str, int] = {}
    for token in line.split()[1:]:
        if "=" not in token:
            continue
        key, value = token.split("=", 1)
        try:
            record[key] = int(value, 0)
        except ValueError:
            continue
    return record if "task" in record else None


def delta(record: dict[str, int], end: str, begin: str) -> int | None:
    a = record.get(begin, 0)
    b = record.get(end, 0)
    if a == 0 or b == 0 or b < a:
        return None
    return b - a


def put_delta(record: dict[str, int], name: str, end: str, begin: str) -> None:
    value = delta(record, end, begin)
    if value is not None:
        record[name] = value


def percentile(values: list[int], probability: float) -> float:
    ordered = sorted(values)
    if len(ordered) == 1:
        return float(ordered[0])
    position = probability * (len(ordered) - 1)
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return float(ordered[lower])
    fraction = position - lower
    return ordered[lower] * (1.0 - fraction) + ordered[upper] * fraction


def load_scenario(directory: Path) -> list[dict[str, int | str]]:
    task_records: dict[int, dict[str, int]] = {}
    hydra_records: dict[int, dict[str, int]] = {}
    response_records: dict[int, dict[str, int]] = {}

    for path in sorted(directory.glob("*.log")):
        with path.open("r", encoding="utf-8", errors="replace") as stream:
            for line in stream:
                parsed = parse_record(line, TASK_PREFIX)
                if parsed is not None:
                    task_records[parsed["task"]] = parsed
                    continue
                parsed = parse_record(line, HYDRA_PREFIX)
                if parsed is not None:
                    hydra_records[parsed["task"]] = parsed
                    continue
                parsed = parse_record(line, RESPONSE_PREFIX)
                if parsed is not None:
                    response_records[parsed["task"]] = parsed

    rows: list[dict[str, int | str]] = []
    for task_id, task in sorted(task_records.items()):
        row: dict[str, int | str] = {"scenario": directory.name, **task}
        for key, value in hydra_records.get(task_id, {}).items():
            if key != "task":
                row[f"hydra_{key}"] = value
        for key, value in response_records.get(task_id, {}).items():
            if key != "task":
                row[f"response_send_{key}"] = value

        row["warmup"] = 1 if int(row.get("trace_flags", 0)) & 1 else 0
        put_delta(row, "target_parse_ns", "parse_done_ns", "agent_recv_ns")
        put_delta(row, "target_payload_copy_ns", "payload_done_ns",
                  "porter_submit_ns")
        put_delta(row, "target_command_transfer_ns", "supervisor_recv_ns",
                  "payload_done_ns")
        put_delta(row, "target_supervisor_gate_ns", "slot_publish_ns",
                  "supervisor_recv_ns")
        put_delta(row, "target_worker_wakeup_ns", "worker_entry_ns",
                  "slot_publish_ns")
        put_delta(row, "target_bind_ns", "bind_end_ns", "bind_begin_ns")
        put_delta(row, "target_to_state_ready_ns", "state_ready_ns",
                  "agent_recv_ns")
        put_delta(row, "target_to_business_ns", "business_entry_ns",
                  "agent_recv_ns")
        put_delta(row, "target_to_first_state_access_ns",
                  "first_state_access_ns", "agent_recv_ns")
        put_delta(row, "target_business_ns", "task_done_ns",
                  "business_entry_ns")
        put_delta(row, "target_reap_ns", "reap_end_ns", "reap_begin_ns")
        put_delta(row, "target_refill_ns", "refill_end_ns", "refill_begin_ns")
        put_delta(row, "target_until_agent_collect_ns", "agent_collect_ns",
                  "agent_recv_ns")
        put_delta(row, "master_request_send_wall_ns",
                  "hydra_request_publish_done_ns",
                  "hydra_request_call_begin_ns")
        put_delta(row, "master_response_consume_ns",
                  "hydra_response_release_done_ns",
                  "hydra_response_recv_ns")
        put_delta(row, "target_response_send_wall_ns",
                  "response_send_publish_done_ns",
                  "response_send_call_begin_ns")

        accounted = sum(int(row.get(field, 0))
                        for field in KERNEL_EXCLUSIVE_FIELDS)
        row["kernel_accounted_ns"] = accounted
        kernel_total = int(row.get("kernel_total_ns", 0))
        row["kernel_unaccounted_ns"] = kernel_total - accounted

        roundtrip = int(row.get("roundtrip_ns", 0))
        request_wall = int(row.get("master_request_send_wall_ns", 0))
        server_stack = int(row.get("hydra_server_stack_ns", 0))
        response_wall = int(row.get("target_response_send_wall_ns", 0))
        response_consume = int(row.get("master_response_consume_ns", 0))
        if roundtrip:
            row["cross_os_unassigned_ns"] = (
                roundtrip - request_wall - server_stack - response_wall -
                response_consume
            )
            row["kernel_roundtrip_fraction_ppm"] = (
                kernel_total * 1_000_000 // roundtrip
            )
        target_activation = int(row.get("target_to_state_ready_ns", 0))
        if target_activation:
            row["restore_target_activation_fraction_ppm"] = (
                int(row.get("restore_wrapper_ns", 0)) * 1_000_000 //
                target_activation
            )
        rows.append(row)
    return rows


def validate_row(row: dict[str, int | str]) -> list[str]:
    errors: list[str] = []
    task = row.get("task", "?")
    label = f"{row['scenario']}:{task}"
    if int(row.get("status", 1)) != 0:
        errors.append(f"{label}: non-zero task status")
    if int(row.get("template_binding_flags", 0)) != 15:
        errors.append(f"{label}: application binding flags are not 15")
    if int(row.get("template_access_count", 0)) < 1:
        errors.append(f"{label}: restored/local template state was not consumed")

    ordered = [
        "agent_recv_ns", "parse_done_ns", "porter_submit_ns",
        "payload_done_ns", "supervisor_recv_ns", "slot_publish_ns",
        "worker_observe_ns", "worker_entry_ns", "restore_begin_ns",
        "restore_end_ns", "bind_begin_ns", "bind_end_ns", "state_ready_ns",
        "business_entry_ns", "task_done_ns", "collect_begin_ns",
        "collect_end_ns", "slot_done_ns", "supervisor_done_ns",
        "agent_collect_ns",
    ]
    previous_name = ""
    previous = 0
    for name in ordered:
        value = int(row.get(name, 0))
        if value == 0:
            if (name.startswith("restore_") or name.startswith("collect_")) and \
                    int(row.get("launch", 0)) != 3:
                continue
            errors.append(f"{label}: missing timestamp {name}")
            continue
        if previous and value < previous:
            errors.append(
                f"{label}: target-clock order {name} < {previous_name}"
            )
        previous_name, previous = name, value

    if int(row.get("launch", 0)) == 3:
        if int(row.get("restore_id", 0)) == 0:
            errors.append(f"{label}: CXLfork restore_id is zero")
        if int(row.get("shared_leaf_count", 0)) == 0:
            errors.append(f"{label}: no shared leaf was attached")
        if int(row.get("kernel_unaccounted_ns", 1)) != 0:
            errors.append(f"{label}: kernel exclusive buckets do not sum to total")
    return errors


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("result_root", type=Path)
    parser.add_argument("--allow-validation-errors", action="store_true")
    args = parser.parse_args()
    root = args.result_root.resolve()
    raw = root / "raw"
    if not raw.is_dir():
        parser.error(f"missing raw directory: {raw}")

    rows: list[dict[str, int | str]] = []
    for scenario in sorted(path for path in raw.iterdir() if path.is_dir()):
        rows.extend(load_scenario(scenario))
    if not rows:
        parser.error("no task_result_v3 records found")

    errors = [error for row in rows for error in validate_row(row)]
    fixed = ["scenario", "task", "kind", "warmup", "launch", "policy",
             "validation", "status", "node", "worker", "pid"]
    all_fields = {str(key) for row in rows for key in row}
    fieldnames = fixed + [name for name in DERIVED_FIELDS if name not in fixed]
    fieldnames += sorted(all_fields - set(fieldnames))
    with (root / "tasks.csv").open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=fieldnames,
                                extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)

    summaries: list[dict[str, int | float | str]] = []
    for scenario in sorted({str(row["scenario"]) for row in rows}):
        selected = [row for row in rows
                    if row["scenario"] == scenario and
                    int(row.get("warmup", 0)) == 0 and
                    int(row.get("status", 1)) == 0]
        for metric in SUMMARY_METRICS:
            values = [int(row[metric]) for row in selected
                      if metric in row and int(row[metric]) >= 0]
            if not values:
                continue
            summaries.append({
                "scenario": scenario,
                "metric": metric,
                "n": len(values),
                "mean": statistics.fmean(values),
                "stdev": statistics.stdev(values) if len(values) > 1 else 0.0,
                "p50": percentile(values, 0.50),
                "p95": percentile(values, 0.95),
                "p99": percentile(values, 0.99) if len(values) >= 500 else "",
                "min": min(values),
                "max": max(values),
            })
    summary_fields = ["scenario", "metric", "n", "mean", "stdev", "p50",
                      "p95", "p99", "min", "max"]
    with (root / "summary.csv").open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=summary_fields)
        writer.writeheader()
        writer.writerows(summaries)

    validation = {
        "format": "cxlfork-motivation-validation-v1",
        "tasks": len(rows),
        "measured_tasks": sum(int(row.get("warmup", 0)) == 0 for row in rows),
        "validation_errors": errors,
        "clock_rule": (
            "Absolute CLOCK_MONOTONIC timestamps are compared only inside one "
            "VM. Cross-OS cost uses durations and the master-observed round trip."
        ),
        "p99_rule": "P99 is emitted only when a scenario has at least 500 samples.",
    }
    (root / "validation.json").write_text(
        json.dumps(validation, indent=2, sort_keys=True) + "\n",
        encoding="utf-8")
    if errors:
        for error in errors[:50]:
            print(f"validation error: {error}")
        if not args.allow_validation_errors:
            return 1
    print(f"analyzed {len(rows)} tasks across "
          f"{len({row['scenario'] for row in rows})} scenarios")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
