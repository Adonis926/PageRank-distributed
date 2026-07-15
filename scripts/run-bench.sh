#!/usr/bin/env bash

set -euo pipefail

die() {
  printf 'error: %s\n' "$*" >&2
  exit 1
}

now_ms() {
  date +%s%3N
}

normalize_wordcount() {
  awk 'NF >= 2 { print $1 "\t" $2 }' "$1" | LC_ALL=C sort > "$2"
}

usage() {
  cat <<'USAGE'
usage: mr-distributed/scripts/run-bench.sh [options]

Required:
  --input-dir=DIR
  --expected=FILE
  --output-dir=DIR
  --input-bytes=N

Local file-backend mode:
  --mode=local
  --local-workers=N

QEMU/SSH control-plane mode:
  --mode=ssh
  --master-host=HOST[:PORT]
  --worker=HOST:MASTER_BACKEND_PATH:AGENT_ENDPOINT_PATH
  --worker=[HOST:PORT]:MASTER_BACKEND_PATH:AGENT_ENDPOINT_PATH
  --worker=HOST,MASTER_BACKEND_PATH,AGENT_ENDPOINT_PATH
  --ssh-user=USER
  --ssh-key=PATH
  --remote-dir=DIR
  --remote-repo=DIR
  --remote-hydrarpc-root=DIR
  --no-remote-build

Common distributed options:
  --repo=DIR
  --backend=file|devdax|ivshmem
  --reducers=N
  --payload-max=N
  --map-chunk=N
  --queue-capacity=N
  --size=N
  --workers-per-agent=N
  --shuffle-connections=N
  --queue-policy=shared_pair|per_connection
  --verify=last|all|none
  --repeat=N
  --warmup=N

This script does not start QEMU VMs by default. It uses SSH to start commands
inside already-running VMs and leaves HydraRPC backend paths as the measured
agent endpoint/data plane.
Prefer the comma-separated --worker form for devdax/ivshmem paths, because PCI
BDF paths contain ':' characters. Remote configure/build output is copied into
logs/prepare_<host>.log instead of stdout. For devdax, --size must match the
mapped device size.
USAGE
}

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
repo=$(cd "$SCRIPT_DIR/.." && pwd)
input_dir=""
expected=""
output_dir=""
input_bytes=0
repeat=3
warmup=1
mode=local
local_workers=2
workers_per_agent=1
master_host=local
workers=()
backend=file
shuffle_backend=file
reducers=""
payload_max=1048576
map_chunk=""
queue_capacity=4096
map_size=67108864
shuffle_connections=1
queue_policy=shared_pair
verify=last
ssh_user=""
ssh_key=""
remote_dir="/tmp/mr-bench"
remote_repo=""
remote_hydrarpc_root=""
remote_build=1

while (($#)); do
  case "$1" in
    --help|-h) usage; exit 0 ;;
    --repo=*) repo=${1#*=} ;;
    --input-dir=*) input_dir=${1#*=} ;;
    --expected=*) expected=${1#*=} ;;
    --output-dir=*) output_dir=${1#*=} ;;
    --input-bytes=*) input_bytes=${1#*=} ;;
    --repeat=*) repeat=${1#*=} ;;
    --warmup=*) warmup=${1#*=} ;;
    --mode=*) mode=${1#*=} ;;
    --local-workers=*) local_workers=${1#*=} ;;
    --workers-per-agent=*) workers_per_agent=${1#*=} ;;
    --master-host=*) master_host=${1#*=} ;;
    --worker=*) workers+=("${1#*=}") ;;
    --backend=*) backend=${1#*=} ;;
    --shuffle-backend=*) shuffle_backend=${1#*=} ;;
    --reducers=*) reducers=${1#*=} ;;
    --payload-max=*) payload_max=${1#*=} ;;
    --map-chunk=*) map_chunk=${1#*=} ;;
    --queue-capacity=*) queue_capacity=${1#*=} ;;
    --size=*) map_size=${1#*=} ;;
    --shuffle-connections=*) shuffle_connections=${1#*=} ;;
    --queue-policy=*) queue_policy=${1#*=} ;;
    --verify=*) verify=${1#*=} ;;
    --ssh-user=*) ssh_user=${1#*=} ;;
    --ssh-key=*) ssh_key=${1#*=} ;;
    --remote-dir=*) remote_dir=${1#*=} ;;
    --remote-repo=*) remote_repo=${1#*=} ;;
    --remote-hydrarpc-root=*) remote_hydrarpc_root=${1#*=} ;;
    --no-remote-build) remote_build=0 ;;
    *) die "unknown argument: $1" ;;
  esac
  shift
done

[[ -n "$input_dir" && -d "$input_dir" ]] || die "--input-dir is required"
[[ -n "$expected" && -f "$expected" ]] || die "--expected is required"
[[ -n "$output_dir" ]] || die "--output-dir is required"
[[ "$mode" == local || "$mode" == ssh ]] || die "invalid --mode=$mode"
[[ "$repeat" =~ ^[0-9]+$ && "$repeat" -gt 0 ]] || die "--repeat must be a positive integer"
[[ "$warmup" =~ ^[0-9]+$ ]] || die "--warmup must be a non-negative integer"
[[ "$local_workers" =~ ^[0-9]+$ && "$local_workers" -gt 0 ]] || die "--local-workers must be a positive integer"
[[ "$workers_per_agent" =~ ^[0-9]+$ && "$workers_per_agent" -gt 0 ]] || die "--workers-per-agent must be a positive integer"
[[ "$payload_max" =~ ^[0-9]+$ && "$payload_max" -gt 0 ]] || die "--payload-max must be a positive integer"
[[ "$queue_capacity" =~ ^[0-9]+$ && "$queue_capacity" -gt 0 ]] || die "--queue-capacity must be a positive integer"
[[ "$map_size" =~ ^[0-9]+$ && "$map_size" -gt 0 ]] || die "--size must be a positive integer"
[[ "$shuffle_connections" =~ ^[0-9]+$ && "$shuffle_connections" -gt 0 ]] || die "--shuffle-connections must be a positive integer"
[[ "$input_bytes" =~ ^[0-9]+$ && "$input_bytes" -gt 0 ]] || die "--input-bytes must be a positive integer"
[[ "$verify" == last || "$verify" == all || "$verify" == none ]] || die "--verify must be last, all, or none"
mkdir -p "$output_dir"/logs

mapfile -t input_files < <(find "$input_dir" -maxdepth 1 -type f -name '*.txt' | LC_ALL=C sort)
((${#input_files[@]} > 0)) || die "no input files found in $input_dir"
combined_input="$output_dir/combined-input.txt"
cat "${input_files[@]}" > "$combined_input"

ssh_base_opts=()
if [[ -n "$ssh_key" ]]; then
  ssh_base_opts+=(-i "$ssh_key")
fi

split_ssh_endpoint() {
  local endpoint=$1
  local user=""
  local hostport
  local host
  local port=""

  if [[ "$endpoint" == \[*\] ]]; then
    endpoint=${endpoint#\[}
    endpoint=${endpoint%\]}
  fi

  if [[ "$endpoint" == *@* ]]; then
    user=${endpoint%%@*}
    hostport=${endpoint#*@}
  else
    user=$ssh_user
    hostport=$endpoint
  fi

  if [[ "$hostport" == *:* ]]; then
    host=${hostport%:*}
    port=${hostport##*:}
  else
    host=$hostport
  fi

  printf '%s\t%s\t%s\n' "$user" "$host" "$port"
}

ssh_endpoint_target() {
  local endpoint=$1
  local parsed user host
  parsed=$(split_ssh_endpoint "$endpoint")
  user=$(printf '%s' "$parsed" | awk -F '\t' '{print $1}')
  host=$(printf '%s' "$parsed" | awk -F '\t' '{print $2}')

  if [[ -n "$user" ]]; then
    printf '%s@%s' "$user" "$host"
  else
    printf '%s' "$host"
  fi
}

ssh_endpoint_port() {
  local endpoint=$1
  local parsed
  parsed=$(split_ssh_endpoint "$endpoint")
  printf '%s' "$parsed" | awk -F '\t' '{print $3}'
}

remote_cmd() {
  local host=$1
  shift
  if [[ "$host" == local || "$host" == localhost || -z "$host" ]]; then
    bash -lc "$*"
  else
    local port
    port=$(ssh_endpoint_port "$host")
    ssh_opts=("${ssh_base_opts[@]}")
    [[ -n "$port" ]] && ssh_opts+=(-p "$port")
    ssh "${ssh_opts[@]}" "$(ssh_endpoint_target "$host")" "$*"
  fi
}

remote_copy_to() {
  local src=$1
  local host=$2
  local dest=$3
  if [[ "$host" == local || "$host" == localhost || -z "$host" ]]; then
    mkdir -p "$(dirname "$dest")"
    cp -R "$src" "$dest"
  else
    local port
    port=$(ssh_endpoint_port "$host")
    scp_opts=("${ssh_base_opts[@]}")
    [[ -n "$port" ]] && scp_opts+=(-P "$port")
    scp "${scp_opts[@]}" -r "$src" "$(ssh_endpoint_target "$host"):$dest"
  fi
}

remote_copy_from() {
  local host=$1
  local src=$2
  local dest=$3
  if [[ "$host" == local || "$host" == localhost || -z "$host" ]]; then
    cp "$src" "$dest"
  else
    local port
    port=$(ssh_endpoint_port "$host")
    scp_opts=("${ssh_base_opts[@]}")
    [[ -n "$port" ]] && scp_opts+=(-P "$port")
    scp "${scp_opts[@]}" "$(ssh_endpoint_target "$host"):$src" "$dest"
  fi
}

remote_file_exists() {
  local host=$1
  local path=$2
  remote_cmd "$host" "test -f '$path'"
}

copy_remote_log() {
  local host=$1
  local remote_path=$2
  local local_path=$3
  if remote_file_exists "$host" "$remote_path"; then
    remote_copy_from "$host" "$remote_path" "$local_path" || true
  else
    printf 'remote log not found: %s:%s\n' "$host" "$remote_path" > "$local_path"
  fi
}

build_local() {
  local start end
  start=$(now_ms)
  cmake -S "$repo" -B "$repo/build" > "$output_dir/logs/build.log" 2>&1
  cmake --build "$repo/build" >> "$output_dir/logs/build.log" 2>&1
  end=$(now_ms)
  echo $((end - start))
}

make_agent_paths_local() {
  local run_id=$1
  local paths=()
  local i
  for ((i = 0; i < local_workers; i++)); do
    paths+=("/tmp/mr-bench-${run_id}-agent${i}.raw")
  done
  (IFS=,; printf '%s' "${paths[*]}")
}

run_local_once() {
  local phase=$1
  local index=$2
  local master_output=$3
  local run_id="${phase}-${index}-$$"
  local agent_paths
  agent_paths=$(make_agent_paths_local "$run_id")
  IFS=, read -r -a path_array <<< "$agent_paths"

  agent_pids=()
  local i
  for ((i = 0; i < ${#path_array[@]}; i++)); do
    "$repo/build/mr_hydra" agent \
      --backend="$backend" \
      --path="${path_array[$i]}" \
      --node-id="$i" \
      --agent-count="$local_workers" \
      --workers="$workers_per_agent" \
      --global-worker-base="$((i * workers_per_agent))" \
      --store-dir="$output_dir/store/agent${i}_${phase}_${index}" \
      --shuffle-backend="$shuffle_backend" \
      --shuffle-connections="$shuffle_connections" \
      --queue-policy="$queue_policy" \
      --payload-max="$payload_max" \
      --queue-capacity="$queue_capacity" \
      --size="$map_size" \
      --quiet > "$output_dir/logs/${phase}_${index}_worker${i}.log" 2>&1 &
    agent_pids+=("$!")
  done

  start=$(now_ms)
  "$repo/build/mr_hydra" master \
    --backend="$backend" \
    --input="$combined_input" \
    --output="$master_output" \
    --agents="$agent_paths" \
    --payload-max="$payload_max" \
    --queue-capacity="$queue_capacity" \
    --size="$map_size" \
    --connections="$shuffle_connections" \
    --queue-policy="$queue_policy" \
    ${reducers:+--reducers="$reducers"} \
    ${map_chunk:+--map-chunk="$map_chunk"} \
    --quiet > "$output_dir/logs/${phase}_${index}_master.log" 2>&1 &
  master_pid=$!

  local rc=0
  wait "$master_pid" || rc=$?
  end=$(now_ms)
  for pid in "${agent_pids[@]}"; do
    wait "$pid" || rc=$?
  done
  rm -f "${path_array[@]}"
  echo "$((end - start)):$rc"
}

parse_worker_spec() {
  local spec=$1
  local host rest
  if [[ "$spec" == *,*,* ]]; then
    host=${spec%%,*}
    rest=${spec#*,}
    local master_path=${rest%%,*}
    local worker_path=${rest#*,}
    [[ -n "$host" && "$master_path" != "$rest" && -n "$worker_path" ]] ||
      die "worker spec must be HOST,MASTER_BACKEND_PATH,AGENT_ENDPOINT_PATH"
    printf '%s\t%s\t%s\n' "$host" "$master_path" "$worker_path"
    return
  fi

  if [[ "$spec" == \[*\]:* ]]; then
    host=${spec%%\]:*}
    host="${host}]"
    rest=${spec#"$host":}
  else
    host=${spec%%:*}
    rest=${spec#*:}
  fi
  local master_path=${rest%%:*}
  local worker_path=${rest#*:}
  [[ "$host" != "$spec" && "$master_path" != "$rest" && -n "$worker_path" ]] ||
    die "worker spec must be HOST:MASTER_BACKEND_PATH:AGENT_ENDPOINT_PATH"
  printf '%s\t%s\t%s\n' "$host" "$master_path" "$worker_path"
}

host_label() {
  printf '%s' "$1" | tr -c 'A-Za-z0-9_.-' '_'
}

prepare_remote_host() {
  local host=$1
  local repo_on_host=$2
  local log_file="$output_dir/logs/prepare_$(host_label "$host").log"

  {
    printf 'prepare host=%s repo=%s remote_dir=%s\n' "$host" "$repo_on_host" "$remote_dir"
    remote_cmd "$host" "mkdir -p '$remote_dir/input' '$remote_dir/output' '$remote_dir/logs'"
    if ((remote_build)); then
      remote_cmd "$host" "cmake -S '$repo_on_host' -B '$repo_on_host/build' ${remote_hydrarpc_root:+-DHYDRARPC_ROOT='$remote_hydrarpc_root'} && cmake --build '$repo_on_host/build'"
    fi
  } >> "$log_file" 2>&1
}

repo_on_master=${remote_repo:-$repo}
remote_input="$remote_dir/input/combined-input.txt"
master_paths=()
worker_hosts=()
worker_paths=()
worker_arg=""

prepare_ssh_environment() {
  local start end spec parsed host master_path worker_path

  master_paths=()
  worker_hosts=()
  worker_paths=()

  start=$(now_ms)
  prepare_remote_host "$master_host" "$repo_on_master"

  for spec in "${workers[@]}"; do
    parsed=$(parse_worker_spec "$spec")
    host=$(printf '%s' "$parsed" | awk -F '\t' '{print $1}')
    master_path=$(printf '%s' "$parsed" | awk -F '\t' '{print $2}')
    worker_path=$(printf '%s' "$parsed" | awk -F '\t' '{print $3}')
    master_paths+=("$master_path")
    worker_hosts+=("$host")
    worker_paths+=("$worker_path")
    prepare_remote_host "$host" "${remote_repo:-$repo}"
  done

  worker_arg=$(IFS=,; printf '%s' "${master_paths[*]}")
  end=$(now_ms)

  if ((remote_build)); then
    build_ms=$((end - start))
  else
    build_ms=0
  fi
}

run_ssh_once() {
  local phase=$1
  local index=$2
  local local_output=$3
  local remote_output="$remote_dir/output/${phase}_${index}.out"

  remote_copy_to "$combined_input" "$master_host" "$remote_input"

  worker_pids=()
  local i
  for ((i = 0; i < ${#worker_hosts[@]}; i++)); do
    remote_cmd "${worker_hosts[$i]}" "'${remote_repo:-$repo}/build/mr_hydra' agent --backend='$backend' --path='${worker_paths[$i]}' --node-id='$i' --agent-count='${#worker_hosts[@]}' --workers='$workers_per_agent' --global-worker-base='$((i * workers_per_agent))' --store-dir='$remote_dir/store/agent${i}_${phase}_${index}' --shuffle-backend='$backend' --shuffle-connections='$shuffle_connections' --queue-policy='$queue_policy' --payload-max='$payload_max' --queue-capacity='$queue_capacity' --size='$map_size' --quiet > '$remote_dir/logs/${phase}_${index}_worker${i}.log' 2>&1" &
    worker_pids+=("$!")
  done

  start=$(now_ms)
  remote_cmd "$master_host" "'$repo_on_master/build/mr_hydra' master --backend='$backend' --input='$remote_input' --output='$remote_output' --agents='$worker_arg' --payload-max='$payload_max' --queue-capacity='$queue_capacity' --size='$map_size' --connections='$shuffle_connections' --queue-policy='$queue_policy' ${reducers:+--reducers='$reducers'} ${map_chunk:+--map-chunk='$map_chunk'} --quiet > '$remote_dir/logs/${phase}_${index}_master.log' 2>&1" &
  master_pid=$!

  local rc=0
  wait "$master_pid" || rc=$?
  end=$(now_ms)
  for pid in "${worker_pids[@]}"; do
    wait "$pid" || rc=$?
  done

  copy_remote_log "$master_host" \
    "$remote_dir/logs/${phase}_${index}_master.log" \
    "$output_dir/logs/${phase}_${index}_master.remote.log"
  for ((i = 0; i < ${#worker_hosts[@]}; i++)); do
    copy_remote_log "${worker_hosts[$i]}" \
      "$remote_dir/logs/${phase}_${index}_worker${i}.log" \
      "$output_dir/logs/${phase}_${index}_worker${i}.remote.log"
  done

  if ((rc != 0)) || ! remote_file_exists "$master_host" "$remote_output"; then
    printf 'missing remote output: %s:%s\n' "$master_host" "$remote_output" \
      > "$output_dir/logs/${phase}_${index}_missing_output.log"
    rc=1
  else
    remote_copy_from "$master_host" "$remote_output" "$local_output" || rc=$?
  fi

  echo "$((end - start)):$rc"
}

if [[ "$mode" == ssh && ${#workers[@]} -eq 0 ]]; then
  die "--mode=ssh requires at least one --worker"
fi

build_start=$(now_ms)
if [[ "$mode" == local ]]; then
  build_ms=$(build_local)
elif [[ "$mode" == ssh ]]; then
  prepare_ssh_environment
else
  build_ms=0
fi
build_end=$(now_ms)
if [[ "$mode" == local && "$build_ms" == 0 ]]; then
  build_ms=$((build_end - build_start))
fi

run_csv="$output_dir/runs.csv"
warmup_csv="$output_dir/warmups.csv"
printf 'index,run_ms,exit_status\n' > "$run_csv"
printf 'index,run_ms,exit_status\n' > "$warmup_csv"

run_values=()
status=ok
correctness=skipped

for ((i = 0; i < warmup; i++)); do
  raw="$output_dir/warmup_${i}.out"
  if [[ "$mode" == local ]]; then
    result=$(run_local_once warmup "$i" "$raw")
  else
    result=$(run_ssh_once warmup "$i" "$raw")
  fi
  run_ms=${result%%:*}
  rc=${result#*:}

  if [[ "$rc" != 0 ]]; then
    status=failed
    exit_status=$rc
  else
    exit_status=0
  fi
  printf '%s,%s,%s\n' "$i" "$run_ms" "$exit_status" >> "$warmup_csv"
done

for ((i = 0; i < repeat; i++)); do
  raw="$output_dir/repeat_${i}.out"
  if [[ "$mode" == local ]]; then
    result=$(run_local_once repeat "$i" "$raw")
  else
    result=$(run_ssh_once repeat "$i" "$raw")
  fi
  run_ms=${result%%:*}
  rc=${result#*:}

  if [[ "$rc" != 0 ]]; then
    status=failed
    exit_status=$rc
  else
    exit_status=0
  fi
  printf '%s,%s,%s\n' "$i" "$run_ms" "$exit_status" >> "$run_csv"
  run_values+=("$run_ms")
done

if [[ "$verify" != none ]]; then
  correctness=ok
  verify_indices=()
  if [[ "$verify" == all ]]; then
    for ((i = 0; i < repeat; i++)); do
      verify_indices+=("$i")
    done
  else
    verify_indices=("$((repeat - 1))")
  fi

  for i in "${verify_indices[@]}"; do
    raw="$output_dir/repeat_${i}.out"
    norm="$output_dir/repeat_${i}.norm"
    if [[ ! -f "$raw" ]]; then
      status=failed
      correctness=failed
      printf 'output file not produced: %s\n' "$raw" \
        > "$output_dir/logs/repeat_${i}.diff"
      continue
    fi

    normalize_wordcount "$raw" "$norm"
    if ! diff -u "$expected" "$norm" > "$output_dir/logs/repeat_${i}.diff"; then
      status=failed
      correctness=failed
    fi
  done
fi

read -r run_ms_avg run_ms_min run_ms_max run_ms_stddev < <(
  printf '%s\n' "${run_values[@]}" |
    awk 'NR == 1 {min = max = $1} {values[NR] = $1; if ($1 < min) min = $1; if ($1 > max) max = $1; sum += $1} END {if (NR == 0) print "0 0 0 0"; else {mean = sum / NR; for (i = 1; i <= NR; i++) {d = values[i] - mean; ss += d * d} printf "%.3f %d %d %.3f\n", mean, min, max, sqrt(ss / NR)}}'
)

run_ms_median=$(
  printf '%s\n' "${run_values[@]}" |
    LC_ALL=C sort -n |
    awk '{values[NR] = $1} END {if (NR == 0) print "0"; else if (NR % 2 == 1) printf "%.3f", values[(NR + 1) / 2]; else printf "%.3f", (values[NR / 2] + values[NR / 2 + 1]) / 2}'
)

throughput_mib_s=$(awk -v bytes="$input_bytes" -v ms="$run_ms_avg" 'BEGIN { if (ms <= 0) print "0"; else printf "%.3f", bytes / 1048576 / (ms / 1000) }')
agent_count=$([[ "$mode" == local ]] && printf '%s' "$local_workers" || printf '%s' "${#workers[@]}")
worker_count=$((agent_count * workers_per_agent))
details="mode=${mode};backend=${backend};agents=${agent_count};workers=${worker_count};workers_per_agent=${workers_per_agent};reducers=${reducers:-$worker_count};payload_max=${payload_max};map_chunk=${map_chunk:-auto};queue_capacity=${queue_capacity};size=${map_size};shuffle_connections=${shuffle_connections};queue_policy=${queue_policy};verify=${verify}"

cat > "$output_dir/result.env" <<EOF
impl_name=distributed
status=$status
correctness=$correctness
build_ms=$build_ms
run_ms_avg=$run_ms_avg
run_ms_min=$run_ms_min
run_ms_max=$run_ms_max
run_ms_median=$run_ms_median
run_ms_stddev=$run_ms_stddev
throughput_mib_s=$throughput_mib_s
input_bytes=$input_bytes
details='$details'
EOF

cat > "$output_dir/result.json" <<EOF
{
  "impl": "distributed",
  "status": "$status",
  "correctness": "$correctness",
  "build_ms": $build_ms,
  "run_ms_avg": $run_ms_avg,
  "run_ms_min": $run_ms_min,
  "run_ms_max": $run_ms_max,
  "run_ms_median": $run_ms_median,
  "run_ms_stddev": $run_ms_stddev,
  "throughput_mib_s": $throughput_mib_s,
  "mode": "$mode",
  "backend": "$backend",
  "agents": $agent_count,
  "workers": $worker_count,
  "workers_per_agent": $workers_per_agent,
  "reducers": ${reducers:-$worker_count},
  "payload_max": $payload_max,
  "map_chunk": "${map_chunk:-auto}",
  "queue_capacity": $queue_capacity,
  "size": $map_size,
  "shuffle_connections": $shuffle_connections,
  "queue_policy": "$queue_policy",
  "verify": "$verify",
  "warmup": $warmup,
  "repeat": $repeat,
  "input_bytes": $input_bytes
}
EOF

[[ "$status" == ok ]] || exit 1
