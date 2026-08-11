#!/usr/bin/env python3
"""Analyze the single-mode CXLfork worker lifecycle benchmark."""

from __future__ import annotations

import argparse
import csv
import json
import math
import statistics
from pathlib import Path


TASK_PREFIX = "task_result_v4"
TASK_LIFECYCLE_PREFIX = "lifecycle_task_v1"
HYDRA_PREFIX = "hydra_task_v1"
RESPONSE_PREFIX = "hydra_response_v1"
PORTER_PREFIX = "porter_lifecycle_v1"
TEMPLATE_PREFIX = "mr_template_v2"
JOB_PREFIX = "master_job_v1"

KERNEL_EXCLUSIVE_FIELDS = [
    "kernel_lock_ns", "kernel_lifecycle_ns", "kernel_manifest_ns",
    "kernel_image_ns", "kernel_checksum_ns", "kernel_conflict_ns",
    "kernel_vma_ns", "kernel_upper_pt_ns", "kernel_shared_leaf_ns",
    "kernel_pte_validate_ns", "kernel_pte_install_ns",
    "kernel_fallback_other_ns", "kernel_dirty_prefault_ns",
    "kernel_global_ns", "kernel_other_ns",
]

SUMMARY_METRICS = [
    "roundtrip_ns", "hydra_server_stack_ns", "target_to_business_ns",
    "target_to_first_state_access_ns", "target_business_ns",
    "porter_start_total_ns", "initial_pool_spawn_ns",
    "first_worker_ready_ns", "target_worker_wakeup_ns",
    "restore_wrapper_ns", "resume_ioctl_ns", "kernel_total_ns",
    "target_bind_ns", "target_reap_ns", "target_refill_ns",
    "business_to_pool_ready_ns", "master_request_send_wall_ns",
    "target_response_send_wall_ns", "master_response_consume_ns",
    "cross_os_unassigned_ns", "cross_vm_delay_estimate_ns",
]

PREPARE_METRICS = [
    "wrapper_total_ns", "device_open_ns", "query_caps_ns",
    "template_build_ns", "prepare_ioctl_ns", "prepare_total_ns",
    "prepare_lock_ns", "prepare_lifecycle_ns",
    "prepare_vma_preflight_ns", "prepare_pte_walk_ns", "prepare_gup_ns",
    "prepare_page_copy_ns", "prepare_pte_recheck_ns",
    "prepare_metadata_ns", "prepare_checksum_ns", "prepare_publish_ns",
    "prepare_other_ns",
]

TEARDOWN_METRICS = [
    "porter_stop_total_ns", "workers_stop_total_ns",
    "supervisor_wait_total_ns",
]


def parse_values(line: str, prefix: str) -> dict[str, int] | None:
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
    return record


def delta(record: dict[str, int | str], end: str, begin: str) -> int | None:
    start = int(record.get(begin, 0))
    finish = int(record.get(end, 0))
    if start == 0 or finish == 0 or finish < start:
        return None
    return finish - start


def put_delta(record: dict[str, int | str], name: str,
              end: str, begin: str) -> None:
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


def load_manifest(root: Path) -> dict[str, dict[str, str]]:
    with (root / "run-manifest.csv").open(newline="", encoding="utf-8") as f:
        return {row["scenario"]: row for row in csv.DictReader(f)}


def load_scenario(directory: Path, manifest: dict[str, str]) -> tuple[
        list[dict[str, int | str]], dict[str, int | str],
        list[dict[str, int | str]]]:
    tasks: dict[int, dict[str, int]] = {}
    task_lifecycle: dict[int, dict[str, int]] = {}
    hydra: dict[int, dict[str, int]] = {}
    response: dict[int, dict[str, int]] = {}
    porter: dict[str, int] = {}
    template: dict[str, int] = {}
    jobs: list[dict[str, int | str]] = []

    for path in sorted(directory.glob("*.log")):
        with path.open("r", encoding="utf-8", errors="replace") as stream:
            for line in stream:
                parsed = parse_values(line, TASK_PREFIX)
                if parsed is not None and "task" in parsed:
                    tasks[parsed["task"]] = parsed
                    continue
                parsed = parse_values(line, TASK_LIFECYCLE_PREFIX)
                if parsed is not None and "task" in parsed:
                    task_lifecycle[parsed["task"]] = parsed
                    continue
                parsed = parse_values(line, HYDRA_PREFIX)
                if parsed is not None and "task" in parsed:
                    hydra[parsed["task"]] = parsed
                    continue
                parsed = parse_values(line, RESPONSE_PREFIX)
                if parsed is not None and "task" in parsed:
                    response[parsed["task"]] = parsed
                    continue
                parsed = parse_values(line, PORTER_PREFIX)
                if parsed is not None and parsed.get("role") == 2:
                    porter = parsed
                    continue
                parsed = parse_values(line, TEMPLATE_PREFIX)
                if parsed is not None:
                    template = parsed
                    continue
                parsed = parse_values(line, JOB_PREFIX)
                if parsed is not None:
                    jobs.append({
                        "scenario": directory.name,
                        "cycle": int(manifest["cycle"]),
                        **parsed,
                    })

    rows: list[dict[str, int | str]] = []
    for task_id, task in sorted(tasks.items()):
        row: dict[str, int | str] = {
            "scenario": directory.name,
            "cycle": int(manifest["cycle"]),
            **task,
        }
        for key, value in task_lifecycle.get(task_id, {}).items():
            if key != "task":
                row[key] = value
        for key, value in hydra.get(task_id, {}).items():
            if key != "task":
                row[f"hydra_{key}"] = value
        for key, value in response.get(task_id, {}).items():
            if key != "task":
                row[f"response_send_{key}"] = value

        is_reducer = int(row.get("node", -1)) == 1 and \
            int(row.get("launch", -1)) == 3 and int(row.get("kind", -1)) == 2
        if is_reducer:
            row["phase"] = "cold" if int(row.get("cold", 0)) else "steady"
        else:
            row["phase"] = "support"

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
        put_delta(row, "target_to_business_ns", "business_entry_ns",
                  "agent_recv_ns")
        put_delta(row, "target_to_first_state_access_ns",
                  "first_state_access_ns", "agent_recv_ns")
        put_delta(row, "target_business_ns", "task_done_ns",
                  "business_entry_ns")
        put_delta(row, "target_reap_ns", "reap_end_ns", "reap_begin_ns")
        put_delta(row, "target_refill_ns", "refill_end_ns", "refill_begin_ns")
        put_delta(row, "business_to_pool_ready_ns", "pool_ready_ns",
                  "task_done_ns")
        put_delta(row, "master_request_send_wall_ns",
                  "hydra_request_publish_done_ns",
                  "hydra_request_call_begin_ns")
        put_delta(row, "master_response_consume_ns",
                  "hydra_response_release_done_ns", "hydra_response_recv_ns")
        put_delta(row, "target_response_send_wall_ns",
                  "response_send_publish_done_ns",
                  "response_send_call_begin_ns")
        put_delta(row, "porter_start_total_ns", "porter_start_end_ns",
                  "porter_start_begin_ns")
        put_delta(row, "porter_shared_map_ns", "shared_map_end_ns",
                  "shared_map_begin_ns")
        put_delta(row, "porter_socketpair_ns", "socketpair_end_ns",
                  "socketpair_begin_ns")
        put_delta(row, "porter_supervisor_fork_ns",
                  "supervisor_fork_return_ns", "supervisor_fork_begin_ns")
        put_delta(row, "initial_pool_spawn_ns", "initial_pool_ready_ns",
                  "pool_spawn_begin_ns")
        put_delta(row, "first_worker_ready_ns", "first_worker_ready_ns",
                  "pool_spawn_begin_ns")
        put_delta(row, "executed_worker_fork_ns", "executed_fork_return_ns",
                  "executed_fork_begin_ns")
        put_delta(row, "executed_worker_startup_ns", "executed_ready_ns",
                  "executed_child_entry_ns")
        put_delta(row, "replacement_worker_fork_ns",
                  "replacement_fork_return_ns", "replacement_fork_begin_ns")
        put_delta(row, "replacement_worker_startup_ns",
                  "replacement_ready_ns", "replacement_child_entry_ns")

        accounted = sum(int(row.get(field, 0))
                        for field in KERNEL_EXCLUSIVE_FIELDS)
        row["kernel_accounted_ns"] = accounted
        row["kernel_unaccounted_ns"] = \
            int(row.get("kernel_total_ns", 0)) - accounted

        roundtrip = int(row.get("roundtrip_ns", 0))
        if roundtrip:
            request_wall = int(row.get("master_request_send_wall_ns", 0))
            server_stack = int(row.get("hydra_server_stack_ns", 0))
            response_wall = int(row.get("target_response_send_wall_ns", 0))
            consume = int(row.get("master_response_consume_ns", 0))
            row["cross_os_unassigned_ns"] = (
                roundtrip - request_wall - server_stack - response_wall - consume
            )
            row["kernel_roundtrip_fraction_ppm"] = \
                int(row.get("kernel_total_ns", 0)) * 1_000_000 // roundtrip

        t1 = int(row.get("hydra_request_ready_ns", 0))
        t2 = int(row.get("hydra_server_recv_ns", 0))
        t3 = int(row.get("hydra_response_ready_ns", 0))
        t4 = int(row.get("hydra_response_recv_ns", 0))
        if t1 and t2 and t3 and t4 and t4 >= t1 and t3 >= t2:
            network = (t4 - t1) - (t3 - t2)
            if network >= 0:
                row["clock_offset_estimate_ns"] = \
                    ((t2 - t1) + (t3 - t4)) // 2
                row["cross_vm_delay_estimate_ns"] = network
                row["one_way_estimate_ns"] = network // 2
                row["one_way_error_bound_ns"] = network // 2
        rows.append(row)

    lifecycle: dict[str, int | str] = {
        "scenario": directory.name,
        "cycle": int(manifest["cycle"]),
        "expected_cold": int(manifest["expected_cold"]),
        "expected_steady": int(manifest["expected_steady"]),
    }
    for key, value in template.items():
        lifecycle[f"template_{key}"] = value
    for key, value in porter.items():
        lifecycle[key] = value
    put_delta(lifecycle, "porter_stop_total_ns", "stop_end_ns",
              "stop_begin_ns")
    put_delta(lifecycle, "workers_stop_total_ns", "workers_stop_end_ns",
              "workers_stop_begin_ns")
    put_delta(lifecycle, "supervisor_wait_total_ns", "supervisor_wait_end_ns",
              "supervisor_wait_begin_ns")
    lifecycle["observed_cold"] = sum(row["phase"] == "cold" for row in rows)
    lifecycle["observed_steady"] = sum(row["phase"] == "steady" for row in rows)
    return rows, lifecycle, jobs


def validate_task(row: dict[str, int | str]) -> list[str]:
    if row.get("phase") == "support":
        return []
    label = f"{row['scenario']}:{row.get('task', '?')}"
    errors: list[str] = []
    if int(row.get("status", 1)) != 0:
        errors.append(f"{label}: non-zero status")
    if int(row.get("template_binding_flags", 0)) != 15:
        errors.append(f"{label}: binding flags are not 15")
    if int(row.get("template_access_count", 0)) < 1:
        errors.append(f"{label}: restored state was not consumed")
    if int(row.get("restore_id", 0)) == 0:
        errors.append(f"{label}: restore_id is zero")
    if int(row.get("shared_leaf_count", 0)) == 0:
        errors.append(f"{label}: no shared leaf attached")
    if int(row.get("fallback_pages", 0)) != 0:
        errors.append(f"{label}: fallback reconstruction was used")
    if int(row.get("kernel_unaccounted_ns", 1)) != 0:
        errors.append(f"{label}: kernel buckets do not sum to total")
    ordered = [
        "agent_recv_ns", "parse_done_ns", "porter_submit_ns",
        "payload_done_ns", "supervisor_recv_ns", "slot_publish_ns",
        "worker_observe_ns", "worker_entry_ns", "restore_begin_ns",
        "restore_end_ns", "bind_begin_ns", "bind_end_ns",
        "business_entry_ns", "task_done_ns", "collect_begin_ns",
        "collect_end_ns", "slot_done_ns", "supervisor_done_ns",
        "reap_begin_ns", "reap_end_ns", "refill_begin_ns",
        "refill_end_ns", "agent_collect_ns",
    ]
    previous_name = ""
    previous = 0
    for name in ordered:
        value = int(row.get(name, 0))
        if value == 0:
            errors.append(f"{label}: missing timestamp {name}")
        elif previous and value < previous:
            errors.append(f"{label}: {name} precedes {previous_name}")
        if value:
            previous_name, previous = name, value
    if row["phase"] == "cold":
        for name in ["porter_start_begin_ns", "supervisor_entry_ns",
                     "pool_spawn_begin_ns", "first_worker_ready_ns",
                     "initial_pool_ready_ns", "porter_start_end_ns"]:
            if int(row.get(name, 0)) == 0:
                errors.append(f"{label}: cold task is missing {name}")
    elif int(row.get("porter_start_begin_ns", 0)) != 0:
        errors.append(f"{label}: steady task carries cold-start timestamps")
    return errors


def summarize(group: str, metric: str, values: list[int]) -> dict[str, object]:
    mean = statistics.fmean(values)
    stdev = statistics.stdev(values) if len(values) > 1 else 0.0
    margin = 1.96 * stdev / math.sqrt(len(values))
    return {
        "phase": group, "metric": metric, "n": len(values), "mean": mean,
        "stdev": stdev, "ci95_low": mean - margin,
        "ci95_high": mean + margin, "p50": percentile(values, 0.50),
        "p95": percentile(values, 0.95),
        "p99": percentile(values, 0.99) if len(values) >= 500 else "",
        "min": min(values), "max": max(values),
    }


def write_csv(path: Path, rows: list[dict[str, object]],
              fixed: list[str] | None = None) -> None:
    fields = fixed[:] if fixed else []
    all_fields = {key for row in rows for key in row}
    fields += sorted(all_fields - set(fields))
    with path.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=fields,
                                extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("result_root", type=Path)
    args = parser.parse_args()
    root = args.result_root.resolve()
    manifest = load_manifest(root)

    tasks: list[dict[str, int | str]] = []
    lifecycles: list[dict[str, int | str]] = []
    jobs: list[dict[str, int | str]] = []
    for scenario, entry in manifest.items():
        directory = root / "raw" / scenario
        rows, lifecycle, scenario_jobs = load_scenario(directory, entry)
        tasks.extend(rows)
        lifecycles.append(lifecycle)
        jobs.extend(scenario_jobs)

    reducer_tasks = [row for row in tasks if row["phase"] != "support"]
    errors = [error for row in reducer_tasks for error in validate_task(row)]
    for row in lifecycles:
        label = str(row["scenario"])
        if int(row.get("observed_cold", 0)) != int(row["expected_cold"]):
            errors.append(f"{label}: cold task count mismatch")
        if int(row.get("observed_steady", 0)) != int(row["expected_steady"]):
            errors.append(f"{label}: steady task count mismatch")
        if int(row.get("template_prepare_total_ns", 0)) == 0:
            errors.append(f"{label}: missing template prepare stats")
        if int(row.get("porter_stop_total_ns", 0)) == 0:
            errors.append(f"{label}: missing porter teardown stats")

    write_csv(root / "tasks.csv", tasks,
              ["scenario", "cycle", "phase", "task", "kind", "node",
               "worker", "pid", "status"])
    write_csv(root / "lifecycle.csv", lifecycles,
              ["scenario", "cycle", "expected_cold", "observed_cold",
               "expected_steady", "observed_steady"])
    write_csv(root / "jobs.csv", jobs,
              ["scenario", "cycle", "status", "begin_ns", "end_ns",
               "total_ns", "map_tasks", "reducers"])

    worker_by_pid: dict[int, dict[str, object]] = {}
    for row in reducer_tasks:
        for relation, prefix in [("executed", "executed"),
                                 ("replacement", "replacement")]:
            pid = int(row.get(f"{prefix}_pid", 0))
            if pid <= 0:
                continue
            worker = worker_by_pid.setdefault(pid, {
                "scenario": row["scenario"], "cycle": row["cycle"],
                "pid": pid, "worker": row.get("worker", ""),
                "created_after_task": "", "executed_task": "",
            })
            if relation == "replacement":
                worker["created_after_task"] = row["task"]
            else:
                worker["executed_task"] = row["task"]
            for name in ["spawn_flags", "fork_begin_ns", "fork_return_ns",
                         "child_entry_ns", "device_open_ns", "query_caps_ns",
                         "policy_ns", "ready_ns"]:
                worker[name] = row.get(f"{prefix}_{name}", 0)
    workers = list(worker_by_pid.values())
    write_csv(root / "workers.csv", workers,
              ["scenario", "cycle", "pid", "worker",
               "created_after_task", "executed_task"])

    kernel_rows = [{
        key: value for key, value in row.items()
        if key in {"scenario", "cycle", "phase", "task", "restore_id"} or
        key.startswith("kernel_") or key in {
            "path_mask", "shared_leaf_count", "shared_data_pages",
            "fallback_pages", "local_copy_pages", "leaf_copy_pages",
            "validated_ptes"}
    } for row in reducer_tasks]
    write_csv(root / "kernel.csv", kernel_rows,
              ["scenario", "cycle", "phase", "task", "restore_id"])

    summaries: list[dict[str, object]] = []
    for phase in ["cold", "steady"]:
        selected = [row for row in reducer_tasks
                    if row["phase"] == phase and int(row.get("status", 1)) == 0]
        for metric in SUMMARY_METRICS:
            values = [int(row[metric]) for row in selected
                      if metric in row and int(row[metric]) >= 0]
            if values:
                summaries.append(summarize(phase, metric, values))
    for metric in PREPARE_METRICS:
        key = f"template_{metric}"
        values = [int(row[key]) for row in lifecycles if int(row.get(key, 0))]
        if values:
            summaries.append(summarize("prepare", metric, values))
    for metric in TEARDOWN_METRICS:
        values = [int(row[metric]) for row in lifecycles
                  if int(row.get(metric, 0))]
        if values:
            summaries.append(summarize("teardown", metric, values))
    write_csv(root / "summary.csv", summaries,
              ["phase", "metric", "n", "mean", "stdev", "ci95_low",
               "ci95_high", "p50", "p95", "p99", "min", "max"])

    validation = {
        "format": "cxlfork-lifecycle-validation-v1",
        "cycles": len(lifecycles),
        "tasks": len(tasks),
        "reducer_tasks": len(reducer_tasks),
        "cold_tasks": sum(row["phase"] == "cold" for row in reducer_tasks),
        "steady_tasks": sum(row["phase"] == "steady" for row in reducer_tasks),
        "jobs": len(jobs),
        "validation_errors": errors,
        "clock_rule": (
            "Absolute CLOCK_MONOTONIC timestamps are compared only inside one "
            "VM. Cross-VM delay is diagnostic and is not added to top-level RTT."
        ),
        "p99_rule": "P99 is emitted only for at least 500 samples.",
    }
    (root / "validation.json").write_text(
        json.dumps(validation, indent=2, sort_keys=True) + "\n",
        encoding="utf-8")
    if errors:
        for error in errors[:100]:
            print(f"validation error: {error}")
        return 1
    print(f"analyzed {len(lifecycles)} lifecycles, "
          f"{len(reducer_tasks)} reducer tasks and {len(workers)} workers")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
