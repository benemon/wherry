#!/usr/bin/env bash
# Starts a three-node Kafka cluster for the real-broker tests and writes their environment to DIR/env, the multi-broker
# counterpart of up.sh. Each node is a combined broker and controller (node IDs 1-3, a static three-voter quorum): three
# containers rather than six fit a CI runner, the quorum still survives the loss of one node, and the tests move
# partition leaders by election rather than by stopping nodes. Every node has up.sh's listeners, TLS and SCRAM: a TLS
# listener with SCRAM-SHA-512 and the StandardAuthorizer (super user wherry-admin, operator wherry.operator with the
# operator's ACLs on wherry.admin) and a plaintext listener whose anonymous principal is a super user, published on
# 127.0.0.1:9092/9093, 19092/19093 and 29092/29093 for nodes 1, 2 and 3. The nodes reach each other on a container
# network over a plaintext INTERNAL listener (inter-broker) and the CONTROLLER listener. Topics default to replication
# factor 3 and min.insync.replicas 2, which wherry's topics take since wherry creates them with the broker's default
# replication factor; the internal topics (offsets, transaction state, share-group state) are replicated the same way.
# Usage: up-cluster.sh DIR. CONTAINER (docker by default) runs it; down:
# $CONTAINER rm -f wherry-kafka-1 wherry-kafka-2 wherry-kafka-3 && $CONTAINER network rm wherry-kafka.
set -euo pipefail
dir=${1:?usage: up-cluster.sh DIR}
container=${CONTAINER:-docker}
image=docker.io/apache/kafka:4.3.1@sha256:77e3df9054047a88b520d0cc46e16696d3b22022e1d580aeccd2632df6532837
network=wherry-kafka
nodes=(1 2 3)
mkdir -p "$dir"
dir=$(cd "$dir" && pwd)
umask 077

openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes -days 2 -subj /CN=wherry-test-ca \
  -keyout "$dir/ca.key" -out "$dir/ca.crt" 2>/dev/null
openssl req -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes -subj /CN=localhost \
  -keyout "$dir/server.key" -out "$dir/server.csr" 2>/dev/null
openssl x509 -req -in "$dir/server.csr" -CA "$dir/ca.crt" -CAkey "$dir/ca.key" -CAcreateserial -days 2 \
  -extfile <(printf 'subjectAltName=DNS:localhost,IP:127.0.0.1') -out "$dir/server.crt" 2>/dev/null
openssl pkcs8 -topk8 -nocrypt -in "$dir/server.key" -out "$dir/server.pk8"
cat "$dir/server.crt" "$dir/server.pk8" > "$dir/server.pem"
head -c 24 /dev/urandom | od -An -tx1 | tr -d ' \n' > "$dir/admin-password"
head -c 24 /dev/urandom | od -An -tx1 | tr -d ' \n' > "$dir/operator-password"
chmod 0644 "$dir/server.pem" "$dir/ca.crt"

voters=1@wherry-kafka-1:9094,2@wherry-kafka-2:9094,3@wherry-kafka-3:9094
for node in "${nodes[@]}"; do
  # Node 1 publishes on 9092/9093 like up.sh, node 2 on 19092/19093, node 3 on 29092/29093.
  offset=$(( (node - 1) * 10000 ))
  cat > "$dir/server-$node.properties" <<EOF
node.id=$node
process.roles=broker,controller
controller.quorum.voters=$voters
listeners=PLAIN://0.0.0.0:9092,TLS://0.0.0.0:9093,CONTROLLER://0.0.0.0:9094,INTERNAL://0.0.0.0:9095
advertised.listeners=PLAIN://127.0.0.1:$((9092 + offset)),TLS://127.0.0.1:$((9093 + offset)),INTERNAL://wherry-kafka-$node:9095
listener.security.protocol.map=PLAIN:PLAINTEXT,TLS:SASL_SSL,CONTROLLER:PLAINTEXT,INTERNAL:PLAINTEXT
controller.listener.names=CONTROLLER
inter.broker.listener.name=INTERNAL
sasl.enabled.mechanisms=SCRAM-SHA-512
listener.name.tls.scram-sha-512.sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required;
ssl.keystore.type=PEM
ssl.keystore.location=/certs/server.pem
authorizer.class.name=org.apache.kafka.metadata.authorizer.StandardAuthorizer
super.users=User:wherry-admin;User:ANONYMOUS
auto.create.topics.enable=false
connections.max.reauth.ms=300000
transaction.max.timeout.ms=60000
offsets.topic.replication.factor=3
transaction.state.log.replication.factor=3
transaction.state.log.min.isr=2
share.coordinator.state.topic.replication.factor=3
share.coordinator.state.topic.min.isr=2
default.replication.factor=3
min.insync.replicas=2
log.dirs=/tmp/kraft-logs
EOF
  chmod 0644 "$dir/server-$node.properties"
done

for node in "${nodes[@]}"; do
  "$container" rm -f "wherry-kafka-$node" >/dev/null 2>&1 || true
done
"$container" network rm "$network" >/dev/null 2>&1 || true
"$container" network create "$network" >/dev/null

"$container" pull "$image" >/dev/null
cluster=$("$container" run --rm --entrypoint /opt/kafka/bin/kafka-storage.sh "$image" random-uuid | tail -n 1)
for node in "${nodes[@]}"; do
  offset=$(( (node - 1) * 10000 ))
  # Every node formats with the same cluster ID and SCRAM credentials; the static quorum needs no initial controllers.
  "$container" run -d --name "wherry-kafka-$node" --hostname "wherry-kafka-$node" --network "$network" \
    -p "127.0.0.1:$((9092 + offset)):9092" -p "127.0.0.1:$((9093 + offset)):9093" \
    -v "$dir/server-$node.properties:/cfg/server.properties:ro" -v "$dir/server.pem:/certs/server.pem:ro" \
    -e CLUSTER_ID="$cluster" -e KAFKA_HEAP_OPTS="-Xmx768m -Xms768m" \
    -e ADMIN_PASSWORD="$(cat "$dir/admin-password")" -e OPERATOR_PASSWORD="$(cat "$dir/operator-password")" \
    --entrypoint /bin/sh "$image" -c '
      /opt/kafka/bin/kafka-storage.sh format -t "$CLUSTER_ID" -c /cfg/server.properties \
        --add-scram "SCRAM-SHA-512=[name=wherry-admin,password=$ADMIN_PASSWORD]" \
        --add-scram "SCRAM-SHA-512=[name=wherry.operator,password=$OPERATOR_PASSWORD]" >/dev/null &&
      exec /opt/kafka/bin/kafka-server-start.sh /cfg/server.properties' >/dev/null
done

# Tools inside a container use the INTERNAL listener: the others advertise host ports that only the host can reach.
internal=wherry-kafka-1:9095
ready=
for _ in $(seq 1 90); do
  registered=$("$container" exec wherry-kafka-1 /opt/kafka/bin/kafka-broker-api-versions.sh \
    --bootstrap-server "$internal" 2>/dev/null | grep -c '(id: ' || true)
  if [ "$registered" = "${#nodes[@]}" ]; then
    ready=1
    break
  fi
  sleep 2
done
if [ -z "$ready" ]; then
  for node in "${nodes[@]}"; do
    "$container" logs --tail 50 "wherry-kafka-$node" >&2 || true
  done
  echo "up-cluster.sh: ${#nodes[@]} brokers never registered" >&2
  exit 1
fi
"$container" exec wherry-kafka-1 /opt/kafka/bin/kafka-acls.sh --bootstrap-server "$internal" --add \
  --allow-principal User:wherry.operator --operation Write --operation Read --operation Describe \
  --topic wherry.admin >/dev/null

cat > "$dir/env" <<EOF
export WHERRY_KAFKA_TEST_BROKER=127.0.0.1:9092
export WHERRY_KAFKA_TEST_BROKERS=${#nodes[@]}
export WHERRY_TRUST_BROKER=127.0.0.1:9093
export WHERRY_TRUST_CA=$dir/ca.crt
export WHERRY_TRUST_ADMIN_USER=wherry-admin
export WHERRY_TRUST_ADMIN_PASSWORD_FILE=$dir/admin-password
export WHERRY_TRUST_OPERATOR_PASSWORD_FILE=$dir/operator-password
EOF
