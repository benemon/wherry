#!/usr/bin/env bash
# The collection's role run, for the ./cmd/wherry/ entry of ci.yml's broker job: install, prepare, probe and run, in
# that order, after the job's checkouts of wherry-plugins at WHERRY_PLUGIN_VERSION into wherry-plugins/ and of this
# revision into wherry-plugins/wherry-src/. examples/site.yml and examples/retire.yml run with examples/inventory.yml,
# which names the wherry release and plugin versions, and an inventory of this run's addresses, images and pins.
# Needs sudo, systemd, docker, Go and jq.
set -euo pipefail
src=$(cd "$(dirname "$0")/../.." && pwd)
plugins=$src/wherry-plugins
work=${RUNNER_TEMP:?}/collection
kafka=$work/kafka
examples=$src/ansible/examples
inventories=(-i "$examples/inventory.yml" -i "$work/inventory.yml")

fail() {
	echo "FAIL: $*" >&2
	exit 1
}

wait_for() {
	local end=$((SECONDS + $1))
	shift
	until "$@"; do
		((SECONDS < end)) || fail "timed out: $*"
		sleep 2
	done
}

install_tools() {
	local archive
	mkdir -p "$work/bin"
	python3 -m venv "$work/venv"
	"$work/venv/bin/pip" install -q ansible-core==2.22.0 'nomadpy[exec]==0.3.0' kubernetes==37.0.3
	"$work/venv/bin/ansible-galaxy" collection install "$src/ansible" -p "$work/collections"

	archive=nomad_2.1.1_linux_amd64.zip
	curl -fsSL -o "$work/$archive" "https://releases.hashicorp.com/nomad/2.1.1/$archive"
	echo "073084f7690fdaea756ff1eff8b669b25c3a6c17778f19ae0c47606973ed3f39  $work/$archive" | sha256sum -c -
	unzip -oq "$work/$archive" -d "$work/bin"

	archive=cni-plugins-linux-amd64-v1.9.1.tgz
	curl -fsSL -o "$work/$archive" "https://github.com/containernetworking/plugins/releases/download/v1.9.1/$archive"
	echo "b98f74a0f8522f0a83867178729c1aa70f2158f90c45a2ca8fa791db1c76b303  $work/$archive" | sha256sum -c -
	sudo install -d -m 0755 /opt/cni/bin
	sudo tar -xzf "$work/$archive" -C /opt/cni/bin

	curl -fsSL -o "$work/bin/kind" "https://github.com/kubernetes-sigs/kind/releases/download/v0.33.0/kind-linux-amd64"
	echo "aee6151561422756b764a4ae28e7f44cda5af5a9eead3cc9985112b1de8d8e0d  $work/bin/kind" | sha256sum -c -
	curl -fsSL -o "$work/bin/cosign" "https://github.com/sigstore/cosign/releases/download/v3.1.4/cosign-linux-amd64"
	echo "424e2c97d0b45fd7a6b608a87f090f35633f4c23ece5f092d48bba11aaf4bbfa  $work/bin/cosign" | sha256sum -c -
	chmod 0755 "$work/bin/kind" "$work/bin/cosign"

	printf '%s\n' "$work/bin" "$work/venv/bin" >>"$GITHUB_PATH"
	echo "ANSIBLE_COLLECTIONS_PATH=$work/collections" >>"$GITHUB_ENV"
}

# The Kafka of the real-broker tests holds their topics and principals, so the role run starts its own from up.sh.
prepare() {
	local address token version wherry_version
	go build -trimpath -o "$work/bin/wherry" ./cmd/wherry
	version=$(wherry version | sed -n 's/^version=//p')
	wherry_version=$(inventory_var wherry_version)
	[[ $WHERRY_PLUGIN_VERSION == "nomad/v$(inventory_var wherry_plugin_versions.nomad)" ]] ||
		fail "WHERRY_PLUGIN_VERSION $WHERRY_PLUGIN_VERSION is not the inventory's Nomad plugin version"
	address=$(ip -4 -o route get 1.1.1.1 | sed -n 's/.* src \([0-9.]*\).*/\1/p')
	[[ -n $address ]] || fail "the runner has no address on a default route"
	echo "ADVERTISED_IP=$address" >>"$GITHUB_ENV"

	docker build -q -f "$plugins/examples/nomad/Containerfile" --build-arg VERSION="$(inventory_var wherry_plugin_versions.nomad)" \
		--build-arg WHERRY_VERSION="$wherry_version" -t wherry-receiver-nomad:ci "$plugins"
	docker build -q -f "$plugins/examples/kubernetes/Containerfile" --build-arg VERSION="$(inventory_var wherry_plugin_versions.kubernetes)" \
		--build-arg WHERRY_VERSION="$wherry_version" -t wherry-receiver-kubernetes:ci "$plugins"
	WHERRY_REF=$(git -C "$plugins/wherry-src" rev-parse HEAD) \
		"$plugins/.github/wherry-ref.sh" "$work/ref-bin" wherry-receiver-nomad:ci wherry-receiver-kubernetes:ci

	ADVERTISED_IP=$address test/kafka/up.sh "$kafka"

	# wherry-plugins' nomad/test/e2e/agent.hcl, with ACLs on.
	cat >"$work/nomad.hcl" <<-'EOF'
		acl {
		  enabled = true
		}
		plugin "docker" {
		  config {
		    gc {
		      image = false
		    }
		  }
		}
		client {
		  options {
		    "fingerprint.denylist" = "env_aws,env_gce,env_azure,env_digitalocean"
		  }
		}
	EOF
	# Templates set the receiver's bundle owner, which needs the agent to run as root.
	sudo systemd-run --unit wherry-collection-nomad "$work/bin/nomad" agent -dev -config "$work/nomad.hcl"
	wait_for 120 curl -fsS -o /dev/null http://127.0.0.1:4646/v1/agent/health
	token=$(nomad acl bootstrap -address=http://127.0.0.1:4646 -json | jq -r .SecretID)
	NOMAD_ADDR=http://127.0.0.1:4646 NOMAD_TOKEN=$token wait_for 120 nomad_client_ready
	echo "::add-mask::$token"
	echo "::add-mask::$(cat "$kafka/admin-password")"

	kind create cluster --name wherry-collection --wait 5m
	kind load docker-image --name wherry-collection wherry-receiver-kubernetes:ci

	docker cp wherry-kafka:/opt/kafka "$work/kafka-dist"
	printf '%s\n' security.protocol=SASL_SSL sasl.mechanism=SCRAM-SHA-512 \
		"sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username=\"wherry-admin\" password=\"$(cat "$kafka/admin-password")\";" \
		ssl.truststore.type=PEM "ssl.truststore.location=$kafka/ca.crt" >"$work/kafka-admin.properties"
	chmod 0600 "$work/kafka-admin.properties"

	cat >"$work/inventory.yml" <<-EOF
		all:
		  vars:
		    wherry_broker: $address:9093
		    wherry_receiver_images:
		      nomad: wherry-receiver-nomad:ci
		      kubernetes: wherry-receiver-kubernetes:ci
		    wherry_kubernetes_overlay_patches: [base/fsgroup.yaml, $src/ansible/tests/kind-storage.yaml]
		  children:
		    controllers:
		      hosts:
		        controller:
		          wherry_version: "$version"
	EOF

	{
		echo "WHERRY_KAFKA_ADMIN_PASSWORD=$(cat "$kafka/admin-password")"
		echo "WHERRY_KAFKA_CA_CERT=$kafka/ca.crt"
		echo "WHERRY_KAFKA_ADMIN_CONFIG=$work/kafka-admin.properties"
		echo "KAFKA_HOME=$work/kafka-dist"
		echo "NOMAD_ADDR=http://127.0.0.1:4646"
		echo "WHERRY_NOMAD_TOKEN=$token"
		echo "NOMAD_TOKEN=$token"
		echo "KUBECONFIG=$HOME/.kube/config"
		echo "WHERRY_NOMAD_PLUGIN_SHA256=$(docker run --rm --entrypoint sha256sum wherry-receiver-nomad:ci /usr/local/libexec/wherry/plugins/wherry-plugin-nomad | cut -d' ' -f1)"
		echo "WHERRY_KUBERNETES_PLUGIN_SHA256=$(docker run --rm --entrypoint sha256sum wherry-receiver-kubernetes:ci /usr/local/libexec/wherry/plugins/wherry-plugin-kubernetes | cut -d' ' -f1)"
		echo "WHERRY_SYSTEMD_PLUGIN_SHA256=$(systemd_pin)"
	} >>"$GITHUB_ENV"
}

# nomad_client_ready succeeds once the dev client's docker driver is healthy and it has fingerprinted the CNI plugins
# bridge mode runs, as wherry-plugins' nomad/test/e2e/01-nomad.sh waits.
nomad_client_ready() {
	nomad node status -self -json 2>/dev/null |
		jq -e '.Status == "ready" and .Drivers.docker.Healthy and (.Attributes | has("plugins.cni.version.bridge") and has("plugins.cni.version.portmap"))' >/dev/null
}

# systemd_pin is the systemd plugin binary's SHA-256 from its release's checksums.txt, once cosign verifies that file
# against the release workflow at that release's tag.
systemd_pin() {
	local dir=$work/systemd-release plugin
	plugin=$(inventory_var wherry_plugin_versions.systemd)
	mkdir -p "$dir"
	gh release download "systemd/v$plugin" -R benemon/wherry-plugins -D "$dir" --clobber \
		-p checksums.txt -p checksums.txt.sigstore.json
	cosign verify-blob "$dir/checksums.txt" --bundle "$dir/checksums.txt.sigstore.json" \
		--certificate-identity-regexp "^https://github[.]com/benemon/wherry-plugins/[.]github/workflows/release[.]yml@refs/tags/systemd/v${plugin//./[.]}\$" \
		--certificate-oidc-issuer https://token.actions.githubusercontent.com >&2
	awk -v name="wherry-plugin-systemd_v${plugin}_linux_amd64" '$2 == name {print $1}' "$dir/checksums.txt" | grep -E '^[0-9a-f]{64}$'
}

# inventory_var prints a variable of examples/inventory.yml's host controller, as written there.
inventory_var() {
	ansible-inventory -i "$examples/inventory.yml" --host controller | jq -er ".$1"
}

# S5: a kind pod's verified TLS handshake with the broker at the runner's address.
probe() {
	kubectl create configmap s5-ca --from-file=ca.crt="$kafka/ca.crt"
	kubectl run s5-probe --restart=Never \
		--image=docker.io/library/alpine:3.24.3@sha256:be2656061b536c1883c4034790e21d0a09c5aaaf51968b89368a3c0371ea2c9a \
		--overrides='{"spec":{"volumes":[{"name":"ca","configMap":{"name":"s5-ca"}}],"containers":[{"name":"s5-probe","image":"docker.io/library/alpine:3.24.3@sha256:be2656061b536c1883c4034790e21d0a09c5aaaf51968b89368a3c0371ea2c9a","command":["sh","-c","apk add -q openssl && openssl s_client -connect '"$ADVERTISED_IP"':9093 -CAfile /ca/ca.crt -verify_return_error -brief </dev/null"],"volumeMounts":[{"name":"ca","mountPath":"/ca"}]}]}}'
	if kubectl wait --for=jsonpath='{.status.phase}'=Succeeded pod/s5-probe --timeout=3m; then
		kubectl logs s5-probe
		echo "S5 pass: a kind pod verified the broker's TLS at $ADVERTISED_IP:9093"
	else
		kubectl logs s5-probe || true
		fail "S5: a kind pod did not reach the broker at $ADVERTISED_IP:9093"
	fi
	kubectl delete pod/s5-probe configmap/s5-ca
}

# unchanged fails unless the PLAY RECAP of log $1 reports changed=0 and failed=0 for every host.
unchanged() {
	grep -E '^[^ ]+ +: ok=' "$1" >/dev/null || fail "$1 has no PLAY RECAP"
	! grep -E '^[^ ]+ +: ok=' "$1" | grep -vE 'changed=0 .*failed=0 ' || fail "$1 reports a change or a failure"
}

# secret_free fails if log $1 holds the Kafka admin password, the Nomad token, the Kafka admin configuration's text, or
# any bundle's password.
secret_free() {
	local bundle secret
	for secret in "$WHERRY_KAFKA_ADMIN_PASSWORD" "$WHERRY_NOMAD_TOKEN"; do
		! grep -F -- "$secret" "$1" >/dev/null || fail "$1 holds a controller secret"
	done
	shopt -s nullglob
	for bundle in ~/.local/state/wherry-example/{operator,backup/operator-*}/bundles/{receivers,hosts/*}/*.bundle; do
		secret=$(sed -n 's/^password: *//p' "$bundle" | tr -d "\"'")
		[[ -z $secret ]] || ! grep -F -- "$secret" "$1" >/dev/null || fail "$1 holds the password of $bundle"
	done
	shopt -u nullglob
}

kafka_admin() {
	docker exec wherry-kafka "/opt/kafka/bin/$1" --bootstrap-server 127.0.0.1:9092 "${@:2}"
}

# seed_foreign adds objects a clear must keep: topic other.topic, its consumer group alice-group with an offset, user
# alice with ACLs on wherry topic $2 and pool group p-build, and grants to wherry principal $1 on alice-group and on the
# cluster, which a clear removes as that principal's ACLs while keeping alice-group.
seed_foreign() {
	kafka_admin kafka-topics.sh --create --topic other.topic --partitions 1 --replication-factor 1
	echo seed | docker exec -i wherry-kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server 127.0.0.1:9092 \
		--topic other.topic
	kafka_admin kafka-console-consumer.sh --topic other.topic --group alice-group --from-beginning --max-messages 1 >/dev/null
	kafka_admin kafka-configs.sh --alter --entity-type users --entity-name alice --add-config 'SCRAM-SHA-512=[password=alice-seed]'
	kafka_admin kafka-acls.sh --add --allow-principal User:alice --operation Read --topic "$2" --group p-build
	kafka_admin kafka-acls.sh --add --allow-principal "User:$1" --operation Read --group alice-group
	kafka_admin kafka-acls.sh --add --allow-principal "User:$1" --operation IdempotentWrite --cluster
}

# kafka_cleared fails unless Kafka answers and holds no wherry. topic and no wherry.rcv. or wherry.tnt. ACL, credential
# or quota, while seed_foreign's objects, the super user and wherry.operator remain.
kafka_cleared() {
	local acls groups topics user users
	topics=$(kafka_admin kafka-topics.sh --list)
	groups=$(kafka_admin kafka-consumer-groups.sh --list)
	acls=$(kafka_admin kafka-acls.sh --list)
	users=$(kafka_admin kafka-configs.sh --describe --entity-type users)
	! grep -q '^wherry[.]' <<<"$topics" || fail "a wherry. topic remains"
	! grep -qE 'User:wherry[.](rcv|tnt)[.]' <<<"$acls" || fail "a wherry.rcv. or wherry.tnt. ACL remains"
	! grep -qE "'wherry[.](rcv|tnt)[.]" <<<"$users" || fail "a wherry.rcv. or wherry.tnt. credential or quota remains"
	grep -qx other.topic <<<"$topics" || fail "other.topic was deleted"
	grep -qx alice-group <<<"$groups" || fail "alice-group was deleted"
	[[ $(grep -c 'principal=User:alice,' <<<"$acls") == 2 ]] || fail "alice's ACLs were removed"
	grep -q 'principal=User:wherry[.]operator,' <<<"$acls" || fail "wherry.operator's ACLs were removed"
	for user in alice wherry-admin wherry.operator; do
		grep -q "user-principal '$user' are SCRAM" <<<"$users" || fail "the credential of $user was deleted"
	done
}

run_roles() {
	local platform retire
	cd "$examples"
	ansible-playbook -vvv "${inventories[@]}" site.yml >"$work/site.log" || {
		tail -n 200 "$work/site.log"
		fail "site.yml"
	}
	secret_free "$work/site.log"
	wherry operator check --dir ~/.local/state/wherry-example/operator
	wherry host receivers --json |
		jq -e '[.[] | select(.status == "live") | .receiver_id] | sort == ["kubernetes-1", "nomad-1", "systemd-1"]'
	for platform in nomad kubernetes systemd; do
		wherry host submit --agent mock --selector "platform=$platform" --prompt "collection $platform" --wait
	done

	ansible-playbook "${inventories[@]}" site.yml | tee "$work/rerun.log"
	unchanged "$work/rerun.log"
	ansible-playbook --check "${inventories[@]}" site.yml | tee "$work/check.log"
	unchanged "$work/check.log"

	seed_foreign wherry.rcv.nomad-1 wherry.tenant.acme
	retire=(retire.yml -e wherry_confirm_absent=true -e wherry_operator_clear_kafka=true
		-e "wherry_kafka_bin_dir=$KAFKA_HOME/bin" -e "wherry_kafka_admin_config=$WHERRY_KAFKA_ADMIN_CONFIG")
	ansible-playbook -vvv "${inventories[@]}" "${retire[@]}" >"$work/retire.log" || {
		tail -n 200 "$work/retire.log"
		fail "retire.yml"
	}
	secret_free "$work/retire.log"
	ansible-playbook "${inventories[@]}" "${retire[@]}" | tee "$work/retire-again.log"
	unchanged "$work/retire-again.log"

	[[ ! -e /etc/systemd/system/wherry-receiver.service && ! -e /etc/wherry && ! -e /usr/local/libexec/wherry ]] ||
		fail "the systemd platform's files remain"
	! kubectl get namespace wherry 2>/dev/null || fail "the Kubernetes namespace wherry remains"
	[[ $(nomad namespace list -json | jq '[.[] | select(.Name == "wherry")] | length') == 0 ]] ||
		fail "the Nomad namespace wherry remains"
	[[ $(nomad acl policy list -json | jq '[.[] | select(.Name | startswith("wherry-receiver"))] | length') == 0 ]] ||
		fail "a receiver ACL policy remains"
	[[ ! -e ~/.config/wherry/bundle.yaml && ! -e ~/.config/wherry/host.key ]] || fail "the host's identity remains"
	kafka_cleared
}

case ${1:-} in
install) install_tools ;;
prepare) prepare ;;
probe) probe ;;
run) run_roles ;;
seed) seed_foreign "$2" "$3" ;;
cleared) kafka_cleared ;;
*) fail "usage: ci.sh install|prepare|probe|run, or seed PRINCIPAL TOPIC and cleared against a running wherry-kafka" ;;
esac
