#!/usr/bin/env bash
# Starts ceph-aio (a single-node Ceph with RGW) for the S3 tests and writes their environment to DIR/env: RGW on
# 127.0.0.1:18000 over http and a user whose generated keys are in DIR/access and DIR/secret. Usage: up.sh DIR.
# CONTAINER (docker by default) runs it; down: $CONTAINER rm -f wherry-ceph-aio.
set -euo pipefail
dir=${1:?usage: up.sh DIR}
container=${CONTAINER:-docker}
# ceph-aio rebuilds weekly and its dated tags expire, so no digest stays pullable; the major tag follows the rebuilds.
image=quay.io/benjamin_holmes/ceph-aio:v21
mkdir -p "$dir"
dir=$(cd "$dir" && pwd)
umask 077

head -c 10 /dev/urandom | od -An -tx1 | tr -d ' \n' > "$dir/access"
head -c 20 /dev/urandom | od -An -tx1 | tr -d ' \n' > "$dir/secret"

"$container" rm -f wherry-ceph-aio >/dev/null 2>&1 || true
"$container" run -d --name wherry-ceph-aio -p 127.0.0.1:18000:8000 "$image" >/dev/null

# The image's healthcheck reports healthy once RGW answers; Ceph takes about a minute to form.
for _ in $(seq 1 60); do
  [ "$("$container" inspect --format '{{.State.Health.Status}}' wherry-ceph-aio)" = healthy ] && break
  sleep 5
done
[ "$("$container" inspect --format '{{.State.Health.Status}}' wherry-ceph-aio)" = healthy ] || {
  "$container" logs --tail 50 wherry-ceph-aio; exit 1; }
"$container" exec wherry-ceph-aio radosgw-admin user create --uid=wherry-test --display-name="wherry tests" \
  --access-key="$(cat "$dir/access")" --secret-key="$(cat "$dir/secret")" >/dev/null

cat > "$dir/env" <<ENV
export WHERRY_S3_TEST_ENDPOINT=http://127.0.0.1:18000
export WHERRY_S3_TEST_ACCESS_KEY_FILE=$dir/access
export WHERRY_S3_TEST_SECRET_KEY_FILE=$dir/secret
ENV
