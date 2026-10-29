#!/usr/bin/env bash
# The steps of the dev workflow's lane, one per call: init, unchanged, up, refusals, receivers, run, leak, fail, down,
# reset. Usage: lane.sh STEP. RUNTIME names the container runtime; STORAGE=true adds --storage-enabled. WHERRY is the
# wherry binary, by default $RUNNER_TEMP/wherry, and WORK the lane's own files, by default $RUNNER_TEMP/dev-lane. up
# and the steps after it read the mock's credential from WHERRY_DEV_MOCK_TOKEN.
set -euo pipefail
step=${1:?usage: lane.sh STEP}
runtime=${RUNTIME:?RUNTIME names docker or podman}
wherry=${WHERRY:-$RUNNER_TEMP/wherry}
work=${WORK:-$RUNNER_TEMP/dev-lane}
root=${XDG_DATA_HOME:-$HOME/.local/share}/wherry/dev/default
config=$root/env/host/config.yaml
mkdir -p "$work"

fail() { echo "FAIL: $*" >&2; exit 1; }

# refused CMD... runs a wherry command that must fail with exactly MESSAGE, the last argument, as its last line.
refused() {
  local message=${*: -1} output
  if output=$("$wherry" "${@:1:$#-1}" 2>&1); then fail "$* succeeded"; fi
  [ "$(tail -n 1 <<<"$output")" = "wherry: $message" ] || fail "${*:1:$#-1}: got '$output', want '$message'"
}

# submit PROMPT [FLAG...] submits a mock run and prints its run ID.
submit() {
  "$wherry" submit --config-file "$config" --agent mock --prompt "$@" | sed -n 's/^run_id=\([^ ]*\) .*/\1/p'
}

case $step in
init)
  flags=(--runtime "$runtime" --agent mock)
  [ "${STORAGE:-false}" = true ] && flags+=(--storage-enabled)
  "$wherry" dev init "${flags[@]}"
  [ -f "$root/cache/manifest.json" ] || fail "no manifest"
  grep -q "\"runtime\": \"$runtime\"" "$root/cache/manifest.json" || fail "the manifest names another runtime"
  ;;
unchanged)
  flags=(--runtime "$runtime" --agent mock)
  [ "${STORAGE:-false}" = true ] && flags+=(--storage-enabled)
  output=$("$wherry" dev init "${flags[@]}" 2>/dev/null)
  [ "$output" = "instance default: unchanged" ] || fail "second init printed '$output'"
  ;;
up)
  : "${WHERRY_DEV_MOCK_TOKEN:?}"
  "$wherry" dev up | tee "$work/up.txt"
  grep -q '^ready: instance default (' "$work/up.txt" || fail "no ready line"
  ;;
refusals)
  refused dev up "instance default is already up"
  refused dev init "instance default is up; run wherry dev down first"
  refused dev reset "instance default is up; run wherry dev down first"
  ;;
receivers)
  "$wherry" receivers --config-file "$config" | tee "$work/receivers.txt"
  grep -q '^receiver=dev-1 pool=dev .* status=live$' "$work/receivers.txt" || fail "dev-1 is not live in pool dev"
  grep -q '^  profile=mock plugin=nomad agent=mock ready=true ' "$work/receivers.txt" || fail "profile mock is not ready"
  ;;
run)
  id=$(submit 'mock: ci' --output echo.txt:required)
  echo "$id" > "$work/run-id"
  "$wherry" watch "$id" --config-file "$config" --timeout 5m --outputs-dir "$work/out" | tee "$work/watch.txt"
  [ "$("$wherry" status "$id" --config-file "$config" | head -n 1)" = TASK_STATE_COMPLETED ] || fail "run $id did not complete"
  [ "$(cat "$work/out/echo.txt")" = "mock: ci" ] || fail "echo.txt does not hold the prompt"
  grep -qx 'MOCK_TOKEN set' "$work/watch.txt" || fail "the mock had no MOCK_TOKEN"
  if [ "${STORAGE:-false}" = true ]; then
    echo "s3 input" > "$work/s3-input.txt"
    id=$(submit 'mock: s3' --s3-input "$work/s3-input.txt" --output echo.txt:required:s3)
    "$wherry" watch "$id" --config-file "$config" --timeout 5m --outputs-dir "$work/out-s3" | tee "$work/watch-s3.txt"
    [ "$(cat "$work/out-s3/echo.txt")" = "mock: s3" ] || fail "the S3 echo.txt does not hold the prompt"
    grep -q '^input s3-input.txt ' "$work/watch-s3.txt" || fail "the mock saw no S3 input"
  fi
  ;;
leak)
  : "${WHERRY_DEV_MOCK_TOKEN:?}"
  export NOMAD_ADDR=http://127.0.0.1:4646
  nomad=$root/cache/nomad
  "$nomad" alloc logs -job wherry-default-receiver receiver > "$work/receiver.log" 2>&1
  "$nomad" alloc logs -stderr -job wherry-default-receiver receiver >> "$work/receiver.log" 2>&1
  curl -fsS "$NOMAD_ADDR/v1/jobs" > "$work/jobs.json"
  for id in $(curl -fsS "$NOMAD_ADDR/v1/jobs" | jq -r '.[].ID'); do
    curl -fsS "$NOMAD_ADDR/v1/job/$id" >> "$work/jobs.json"
  done
  for file in "$root/env/nomad/nomad.log" "$work/jobs.json" "$work/receiver.log" "$work/out/transcript.jsonl"; do
    [ -s "$file" ] || fail "$file is empty"
    if grep -qF "$WHERRY_DEV_MOCK_TOKEN" "$file"; then fail "the mock token is in $file"; fi
  done
  ;;
fail)
  id=$(submit 'mock: fail')
  if "$wherry" watch "$id" --config-file "$config" --timeout 5m --outputs-dir "$work/out-fail" | tee "$work/watch-fail.txt"; then
    fail "watch of a failed run exited 0"
  fi
  grep -qx TASK_STATE_FAILED "$work/watch-fail.txt" || fail "run $id did not end TASK_STATE_FAILED"
  ;;
down)
  "$wherry" dev down
  if ss -ltn | awk '{print $4}' | grep -E ':(4646|4647|4648|9093|18080)$'; then fail "a port is still held"; fi
  [ -z "$("$runtime" ps -a -q --filter label=com.hashicorp.nomad.alloc_id)" ] || fail "a Nomad container is left"
  [ ! -e "$root/env" ] || fail "env/ is left"
  [ -d "$root/cache" ] || fail "cache/ is gone"
  ;;
reset)
  "$wherry" dev reset
  [ ! -e "$root/cache" ] || fail "cache/ is left"
  ;;
*)
  fail "unknown step $step"
  ;;
esac
