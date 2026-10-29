#!/usr/bin/env bash
# Collects the default instance's Nomad log, the receiver's and Kafka's task logs and the jobs' state into DIR after a
# failed step. Under the podman driver `podman logs` is empty, so task output comes from `nomad alloc logs`.
# Usage: collect.sh DIR. RUNTIME names the container runtime.
set +e
out=${1:?usage: collect.sh DIR}
root=${XDG_DATA_HOME:-$HOME/.local/share}/wherry/dev/default
nomad=$root/cache/nomad
export NOMAD_ADDR=http://127.0.0.1:4646
mkdir -p "$out"
cp "$root/env/nomad/nomad.log" "$root/env/nomad/agent.hcl" "$root/cache/manifest.json" "$out/" 2>/dev/null
for job in receiver kafka s3; do
  "$nomad" job status "wherry-default-$job" > "$out/$job-status.txt" 2>&1
  "$nomad" alloc logs -job "wherry-default-$job" "$job" > "$out/$job.stdout.log" 2>&1
  "$nomad" alloc logs -stderr -job "wherry-default-$job" "$job" > "$out/$job.stderr.log" 2>&1
done
cp -r "$root/env/kafka/data/logs" "$out/kafka-server-logs" 2>/dev/null
"${RUNTIME:-docker}" ps -a > "$out/containers.txt" 2>&1
for id in $("${RUNTIME:-docker}" ps -a -q --filter label=com.hashicorp.nomad.alloc_id 2>/dev/null); do
  "${RUNTIME:-docker}" logs "$id" > "$out/container-$id.log" 2>&1
done
exit 0
