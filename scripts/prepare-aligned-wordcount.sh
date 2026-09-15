#!/usr/bin/env bash
set -euo pipefail

# Generate the same deterministic WordCount stream used by the accepted
# single-VM Motivation experiment.  Generation and hashing are outside the
# measured MR interval.
output_dir=""
input_mib=1024
files=512
vocab=65536
line_words=16
jobs=48

die() { printf 'error: %s\n' "$*" >&2; exit 1; }
usage() {
  cat <<'EOF'
usage: prepare-aligned-wordcount.sh --output-dir=DIR [options]

Options:
  --input-mib=N   total generated input in MiB (default: 1024)
  --files=N       number of input files (default: 512)
  --vocab=N       number of wNNNNNN tokens (default: 65536)
  --line-words=N  tokens per line (default: 16)
  --jobs=N        concurrent generators (default: 48)
EOF
}

while (($#)); do
  case "$1" in
    --output-dir=*) output_dir=${1#*=} ;;
    --input-mib=*) input_mib=${1#*=} ;;
    --files=*) files=${1#*=} ;;
    --vocab=*) vocab=${1#*=} ;;
    --line-words=*) line_words=${1#*=} ;;
    --jobs=*) jobs=${1#*=} ;;
    --help|-h) usage; exit 0 ;;
    *) die "unknown argument: $1" ;;
  esac
  shift
done

[[ -n "$output_dir" ]] || die "--output-dir is required"
for value in "$input_mib" "$files" "$vocab" "$line_words" "$jobs"; do
  [[ "$value" =~ ^[1-9][0-9]*$ ]] || die "all numeric options must be positive integers"
done
((vocab <= 1000000)) || die "--vocab must be <= 1000000"

input_dir="$output_dir/input"
expected="$output_dir/expected.txt"
manifest="$output_dir/workload.env"
target_bytes=$((input_mib * 1024 * 1024))
word_bytes=7
line_bytes=$((line_words * word_bytes + line_words))
total_lines=$((target_bytes / line_bytes))
total_tokens=$((total_lines * line_words))
actual_bytes=$((total_lines * line_bytes))
((actual_bytes == target_bytes)) ||
  die "requested byte count is not divisible by generated line size $line_bytes"

rm -rf "$input_dir"
mkdir -p "$input_dir" "$output_dir"
base_lines=$((total_lines / files))
extra_lines=$((total_lines % files))
line_offset=0
pids=()

for ((i = 0; i < files; i++)); do
  lines=$base_lines
  ((i < extra_lines)) && lines=$((lines + 1))
  output=$(printf '%s/part-%05d.txt' "$input_dir" "$i")
  awk -v lines="$lines" -v line_offset="$line_offset" \
      -v vocab="$vocab" -v line_words="$line_words" '
    BEGIN {
      for (line = 0; line < lines; line++) {
        token_start = (line_offset + line) * line_words
        for (word = 0; word < line_words; word++) {
          id = (token_start + word) % vocab
          printf "w%06d%s", id, (word + 1 == line_words ? "\n" : " ")
        }
      }
    }
  ' > "$output" &
  pids+=("$!")
  line_offset=$((line_offset + lines))
  if ((${#pids[@]} >= jobs)); then
    wait "${pids[0]}"
    pids=("${pids[@]:1}")
  fi
done
for pid in "${pids[@]}"; do wait "$pid"; done

awk -v vocab="$vocab" -v total_tokens="$total_tokens" '
  BEGIN {
    base = int(total_tokens / vocab)
    rem = total_tokens % vocab
    for (id = 0; id < vocab; id++) {
      count = base + (id < rem ? 1 : 0)
      printf "w%06d\t%d\n", id, count
    }
  }
' > "$expected"

found_bytes=$(find "$input_dir" -maxdepth 1 -type f -name '*.txt' \
  -printf '%s\n' | awk '{sum += $1} END {print sum + 0}')
found_files=$(find "$input_dir" -maxdepth 1 -type f -name '*.txt' | wc -l)
[[ "$found_bytes" -eq "$actual_bytes" && "$found_files" -eq "$files" ]] ||
  die "generated workload failed size/count verification"

find "$input_dir" -maxdepth 1 -type f -name '*.txt' -print0 |
  LC_ALL=C sort -z | xargs -0 sha256sum > "$output_dir/input-files.sha256"
expected_sha256=$(sha256sum "$expected" | awk '{print $1}')
cat > "$manifest" <<EOF
format=aligned-wordcount-v1
input_bytes=$actual_bytes
files=$files
vocab=$vocab
line_words=$line_words
total_lines=$total_lines
total_tokens=$total_tokens
expected=$expected
expected_sha256=$expected_sha256
generator_commit_note=same-token-stream-as-native-motivation
EOF

printf 'workload ready: dir=%s bytes=%s files=%s expected=%s\n' \
  "$input_dir" "$actual_bytes" "$files" "$expected"
