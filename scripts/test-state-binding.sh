#!/usr/bin/env bash

set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
CXLFORK_ROOT=${CXLFORK_ROOT:-/home/xtl/cxlfork/cxlfork-repro}
HYDRARPC_ROOT=${HYDRARPC_ROOT:-/home/xtl/HydraRPC}
RESULT_ROOT=${RESULT_ROOT:-/home/xtl/cxlfork/mr-static-results/state-binding-acceptance}
INPUT_DIR=${INPUT_DIR:-/home/xtl/cxlfork/mr-file-sim-smoke/input}
EXPECTED=${EXPECTED:-/home/xtl/cxlfork/mr-file-sim-smoke/expected.txt}
VM0_PORT=${VM0_PORT:-2200}
VM1_PORT=${VM1_PORT:-2201}
SSH_USER=${SSH_USER:-ubuntu}
SSH_KEY=${SSH_KEY:-}
TEST_KEY=${TEST_KEY:-2002}
TEMPLATE_BYTES=${TEMPLATE_BYTES:-4194304}
WORKERS=${WORKERS:-2}
GUEST_BIN=${GUEST_BIN:-/tmp/mr_hydra_state_binding}
SSH_OPTS=(-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null)
if [[ -n "$SSH_KEY" ]]; then
  SSH_OPTS+=(-o IdentitiesOnly=yes -i "$SSH_KEY")
fi

die() {
  printf 'state-binding-test: %s\n' "$*" >&2
  exit 1
}

guest() {
  local port=$1
  shift
  ssh "${SSH_OPTS[@]}" -p "$port" "$SSH_USER"@localhost "$@"
}

field() {
  local line=$1 name=$2
  printf '%s\n' "$line" | awk -v key="$name" '
    {
      for (i = 1; i <= NF; i++) {
        split($i, kv, "=")
        if (kv[1] == key) {
          print kv[2]
          exit
        }
      }
    }'
}

release_test_key() {
  local inspect generation readers
  if ! inspect=$(guest "$VM1_PORT" "sudo '$GUEST_BIN' cxlfork-inspect '$TEST_KEY'" 2>/dev/null); then
    return 0
  fi
  generation=$(field "$inspect" generation)
  readers=$(field "$inspect" readers)
  [[ -n "$generation" && "$readers" == 0 ]] ||
    die "refusing to release key=$TEST_KEY generation=${generation:-unknown} readers=${readers:-unknown}"
  guest "$VM1_PORT" "sudo '$GUEST_BIN' cxlfork-release '$TEST_KEY' '$generation'"
}

publish() {
  local role=$1 scale=$2 mode=${3:-valid}
  local extra=""
  [[ "$mode" == corrupt ]] && extra="--test-corrupt-state"
  guest "$VM0_PORT" \
    "sudo '$GUEST_BIN' template --role='$role' --key='$TEST_KEY' --bytes='$TEMPLATE_BYTES' --value-scale='$scale' $extra"
}

run_case() {
  local name=$1 scale=$2 generation=$3 checksum=$4 expected=$5
  local policy=${6:-mow}
  local out="$RESULT_ROOT/$name"
  local input_bytes
  local ssh_key_args=()
  [[ -n "$SSH_KEY" ]] && ssh_key_args+=(--ssh-key="$SSH_KEY")
  input_bytes=$(find "$INPUT_DIR" -maxdepth 1 -type f -name '*.txt' -print0 |
    xargs -0 wc -c | awk 'END {print $1}')
  rm -rf "$out"
  mkdir -p "$out"
  set +e
  "$ROOT/scripts/run-bench.sh" \
    --mode=ssh --repo="$ROOT" \
    --input-dir="$INPUT_DIR" --expected="$expected" \
    --output-dir="$out" --input-bytes="$input_bytes" \
    --master-host="127.0.0.1:$VM0_PORT" \
    --worker="127.0.0.1:$VM0_PORT,/sys/bus/pci/devices/0000:00:02.0/resource2,/sys/bus/pci/devices/0000:00:02.0/resource2,map,localfork,$WORKERS" \
    --worker="127.0.0.1:$VM1_PORT,/sys/bus/pci/devices/0000:00:03.0/resource2,/sys/bus/pci/devices/0000:00:03.0/resource2,reduce,cxlfork,$WORKERS" \
    --ssh-user="$SSH_USER" --remote-dir="/tmp/mr-binding-$name" \
    "${ssh_key_args[@]}" \
    --no-remote-build --remote-binary="$GUEST_BIN" --sudo \
    --backend=ivshmem --shuffle-backend=ivshmem \
    --size=2147483648 --payload-max=4194304 \
    --schedule=role-static --reducers="$WORKERS" \
    --shuffle-connections="$WORKERS" \
    --template-bytes="$TEMPLATE_BYTES" --template-value-scale="$scale" \
    --reduce-template-key="$TEST_KEY" \
    --reduce-template-generation="$generation" \
    --reduce-template-checksum="$checksum" \
    --cxlfork-policy="$policy" --validation=fast \
    --warmup=0 --repeat=1 --verify=all \
    > "$out/runner.stdout.log" 2> "$out/runner.stderr.log"
  local rc=$?
  set -e
  printf '%s\n' "$rc"
}

assert_success_evidence() {
  local name=$1 expected_state_checksum=$2
  local out="$RESULT_ROOT/$name"
  grep -q '^status=ok$' "$out/result.env" || die "$name did not finish successfully"
  grep -q '^correctness=ok$' "$out/result.env" || die "$name output mismatch"
  awk -F, -v expected="$expected_state_checksum" '
    NR == 1 {next}
    $4 == 2 {
      count++
      if ($8 != 0 || $26 != expected || $27 == 0 || $28 == 0 || $29 != 1 ||
          $30 != 15) {
        bad++
      }
    }
    END {exit !(count > 0 && bad == 0)}
  ' "$out/tasks.csv" || die "$name lacks complete template binding evidence"
}

assert_policy_path() {
  local name=$1 expected_path=$2
  local out="$RESULT_ROOT/$name"
  awk -F, -v expected="$expected_path" '
    NR == 1 {next}
    $4 == 2 {
      count++
      if ($20 != expected || (expected == 5 && ($22 != 1024 || $23 != 0)))
        bad++
    }
    END {exit !(count > 0 && bad == 0)}
  ' "$out/tasks.csv" || die "$name did not use expected path=$expected_path"
}

assert_failure_evidence() {
  local name=$1
  local out="$RESULT_ROOT/$name"
  [[ -f "$out/tasks.csv" ]] || die "$name did not retain task evidence"
  awk -F, '
    NR == 1 {next}
    $4 == 2 && $8 != 0 {failed++}
    END {exit !(failed > 0)}
  ' "$out/tasks.csv" || die "$name was not rejected by a reducer worker"
  local inspect readers
  inspect=$(guest "$VM1_PORT" "sudo '$GUEST_BIN' cxlfork-inspect '$TEST_KEY'")
  readers=$(field "$inspect" readers)
  [[ "$readers" == 0 ]] || die "$name leaked slot readers=$readers"
}

mkdir -p "$RESULT_ROOT"
test -d "$INPUT_DIR" || die "input directory not found: $INPUT_DIR"
test -f "$EXPECTED" || die "expected output not found: $EXPECTED"

if [[ ${ALLOW_DIRTY:-0} != 1 ]]; then
  git -C "$ROOT" diff --quiet || die "MR worktree has unstaged tracked changes"
  git -C "$ROOT" diff --cached --quiet || die "MR worktree has staged changes"
fi

cmake -S "$ROOT" -B "$ROOT/build-static-cxlfork" \
  -DHYDRARPC_ROOT="$HYDRARPC_ROOT" \
  -DMR_ENABLE_CXLFORK=ON \
  -DCXLFORK_UAPI_INCLUDE_DIR="$CXLFORK_ROOT/kernel/build-6.6.32/usr/include"
cmake --build "$ROOT/build-static-cxlfork" -j"$(nproc)"

for port in "$VM0_PORT" "$VM1_PORT"; do
  scp -q -P "$port" "${SSH_OPTS[@]}" \
    "$ROOT/build-static-cxlfork/mr_hydra" \
    "$SSH_USER"@localhost:"$GUEST_BIN"
  guest "$port" "sudo '$GUEST_BIN' cxlfork-caps"
done

release_test_key

normal_pub=$(publish reduce 1)
printf '%s\n' "$normal_pub" | tee "$RESULT_ROOT/publish-normal.log"
normal_generation=$(field "$normal_pub" generation)
normal_page_checksum=$(field "$normal_pub" page_checksum)
normal_state_checksum=$(field "$normal_pub" state_checksum)
[[ $(run_case normal 1 "$normal_generation" "$normal_page_checksum" "$EXPECTED") == 0 ]] ||
  die "normal state-bound CXLfork run failed"
assert_success_evidence normal "$normal_state_checksum"
assert_policy_path normal 5
release_test_key

awk '{printf "%s\t%d\n", $1, $2 * 2}' "$EXPECTED" > "$RESULT_ROOT/expected-scale2.txt"
scale_pub=$(publish reduce 2)
printf '%s\n' "$scale_pub" | tee "$RESULT_ROOT/publish-scale2.log"
scale_generation=$(field "$scale_pub" generation)
scale_page_checksum=$(field "$scale_pub" page_checksum)
scale_state_checksum=$(field "$scale_pub" state_checksum)
[[ $(run_case scale2 2 "$scale_generation" "$scale_page_checksum" "$RESULT_ROOT/expected-scale2.txt") == 0 ]] ||
  die "scale2 state-bound CXLfork run failed"
assert_success_evidence scale2 "$scale_state_checksum"
assert_policy_path scale2 5
release_test_key

policy_pub=$(publish reduce 1)
printf '%s\n' "$policy_pub" | tee "$RESULT_ROOT/publish-policy-matrix.log"
policy_generation=$(field "$policy_pub" generation)
policy_page_checksum=$(field "$policy_pub" page_checksum)
policy_state_checksum=$(field "$policy_pub" state_checksum)
for spec in readonly:5 moa:17 hybrid:33; do
  policy=${spec%%:*}
  expected_path=${spec#*:}
  name="policy-$policy"
  [[ $(run_case "$name" 1 "$policy_generation" "$policy_page_checksum" "$EXPECTED" "$policy") == 0 ]] ||
    die "$policy state-bound CXLfork run failed"
  assert_success_evidence "$name" "$policy_state_checksum"
  assert_policy_path "$name" "$expected_path"
done
release_test_key

corrupt_pub=$(publish reduce 1 corrupt)
printf '%s\n' "$corrupt_pub" | tee "$RESULT_ROOT/publish-corrupt.log"
corrupt_generation=$(field "$corrupt_pub" generation)
corrupt_page_checksum=$(field "$corrupt_pub" page_checksum)
[[ $(run_case corrupt-rejected 1 "$corrupt_generation" "$corrupt_page_checksum" "$EXPECTED") != 0 ]] ||
  die "corrupt state unexpectedly completed"
assert_failure_evidence corrupt-rejected
release_test_key

role_pub=$(publish map 1)
printf '%s\n' "$role_pub" | tee "$RESULT_ROOT/publish-wrong-role.log"
role_generation=$(field "$role_pub" generation)
role_page_checksum=$(field "$role_pub" page_checksum)
[[ $(run_case wrong-role-rejected 1 "$role_generation" "$role_page_checksum" "$EXPECTED") != 0 ]] ||
  die "wrong-role state unexpectedly completed"
assert_failure_evidence wrong-role-rejected
release_test_key

stale_pub=$(publish reduce 1)
printf '%s\n' "$stale_pub" | tee "$RESULT_ROOT/publish-stale-source.log"
stale_generation=$(field "$stale_pub" generation)
stale_page_checksum=$(field "$stale_pub" page_checksum)
release_test_key
current_pub=$(publish reduce 1)
printf '%s\n' "$current_pub" | tee "$RESULT_ROOT/publish-current.log"
[[ $(run_case stale-generation-rejected 1 "$stale_generation" "$stale_page_checksum" "$EXPECTED") != 0 ]] ||
  die "stale generation unexpectedly completed"
assert_failure_evidence stale-generation-rejected
release_test_key

for port in "$VM0_PORT" "$VM1_PORT"; do
  guest "$port" \
    "sudo dmesg | grep -Ei 'BUG:|WARNING:|Oops:|bad pagetable|general protection|kernel panic' | tail -200" \
    > "$RESULT_ROOT/dmesg-$port.log"
  test ! -s "$RESULT_ROOT/dmesg-$port.log" ||
    die "kernel diagnostics found on VM port $port"
done

cat > "$RESULT_ROOT/summary.env" <<EOF
status=ok
normal=ok
scale2_semantic_dependency=ok
readonly_moa_hybrid_binding=ok
corrupt_state_rejected=ok
wrong_role_rejected=ok
stale_generation_rejected=ok
reader_cleanup=ok
dmesg=ok
template_binding_required_flags=15
EOF

printf 'state-binding acceptance: verify=ok results=%s\n' "$RESULT_ROOT"
