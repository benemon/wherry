#!/usr/bin/env bash
# Starts a single-node Kafka for the real-broker tests and writes their environment to DIR/env: a TLS listener on
# 127.0.0.1:9093 with SCRAM-SHA-512 and the StandardAuthorizer (super user wherry-admin, operator wherry.operator
# with the operator's ACLs on wherry.admin), and a plaintext listener on 127.0.0.1:9092 whose anonymous principal is
# a super user. Usage: up.sh DIR. CONTAINER (docker by default) runs it; down: $CONTAINER rm -f wherry-kafka.
set -euo pipefail
dir=${1:?usage: up.sh DIR}
container=${CONTAINER:-docker}
image=docker.io/apache/kafka:4.3.1@sha256:77e3df9054047a88b520d0cc46e16696d3b22022e1d580aeccd2632df6532837
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

cat > "$dir/server.properties" <<EOF
node.id=1
process.roles=broker,controller
controller.quorum.bootstrap.servers=localhost:9094
listeners=PLAIN://0.0.0.0:9092,TLS://0.0.0.0:9093,CONTROLLER://localhost:9094
advertised.listeners=PLAIN://127.0.0.1:9092,TLS://127.0.0.1:9093
listener.security.protocol.map=PLAIN:PLAINTEXT,TLS:SASL_SSL,CONTROLLER:PLAINTEXT
controller.listener.names=CONTROLLER
inter.broker.listener.name=PLAIN
sasl.enabled.mechanisms=SCRAM-SHA-512
listener.name.tls.scram-sha-512.sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required;
ssl.keystore.type=PEM
ssl.keystore.location=/certs/server.pem
authorizer.class.name=org.apache.kafka.metadata.authorizer.StandardAuthorizer
super.users=User:wherry-admin;User:ANONYMOUS
auto.create.topics.enable=false
connections.max.reauth.ms=300000
transaction.max.timeout.ms=60000
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
share.coordinator.state.topic.replication.factor=1
share.coordinator.state.topic.min.isr=1
default.replication.factor=1
min.insync.replicas=1
log.dirs=/tmp/kraft-logs
EOF
chmod 0644 "$dir/server.properties"

"$container" rm -f wherry-kafka >/dev/null 2>&1 || true
"$container" run -d --name wherry-kafka -p 127.0.0.1:9092:9092 -p 127.0.0.1:9093:9093 \
  -v "$dir/server.properties:/cfg/server.properties:ro" -v "$dir/server.pem:/certs/server.pem:ro" \
  -e ADMIN_PASSWORD="$(cat "$dir/admin-password")" -e OPERATOR_PASSWORD="$(cat "$dir/operator-password")" \
  --entrypoint /bin/sh "$image" -c '
    /opt/kafka/bin/kafka-storage.sh format --standalone -t "$(/opt/kafka/bin/kafka-storage.sh random-uuid)" \
      -c /cfg/server.properties \
      --add-scram "SCRAM-SHA-512=[name=wherry-admin,password=$ADMIN_PASSWORD]" \
      --add-scram "SCRAM-SHA-512=[name=wherry.operator,password=$OPERATOR_PASSWORD]" >/dev/null &&
    exec /opt/kafka/bin/kafka-server-start.sh /cfg/server.properties' >/dev/null

for _ in $(seq 1 60); do
  "$container" exec wherry-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server 127.0.0.1:9092 --list \
    >/dev/null 2>&1 && break
  sleep 2
done
"$container" exec wherry-kafka /opt/kafka/bin/kafka-acls.sh --bootstrap-server 127.0.0.1:9092 --add \
  --allow-principal User:wherry.operator --operation Write --operation Read --operation Describe \
  --topic wherry.admin >/dev/null

cat > "$dir/env" <<EOF
export WHERRY_KAFKA_TEST_BROKER=127.0.0.1:9092
export WHERRY_TRUST_BROKER=127.0.0.1:9093
export WHERRY_TRUST_CA=$dir/ca.crt
export WHERRY_TRUST_ADMIN_USER=wherry-admin
export WHERRY_TRUST_ADMIN_PASSWORD_FILE=$dir/admin-password
export WHERRY_TRUST_OPERATOR_PASSWORD_FILE=$dir/operator-password
EOF
