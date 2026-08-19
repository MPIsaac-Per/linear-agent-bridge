#!/bin/bash
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
SOURCE_INSTALL="$SCRIPT_DIR/install.sh"
SOURCE_PLIST="$SCRIPT_DIR/launchd.plist.template"
TEST_ROOT=$(mktemp -d "${TMPDIR:-/tmp}/linear-agent-bridge-install.XXXXXX")
trap 'rm -rf "$TEST_ROOT"' EXIT

PASS_COUNT=0

fail() {
	echo "FAIL: $*" >&2
	exit 1
}

assert_contains() {
	local haystack=$1
	local needle=$2
	[[ "$haystack" == *"$needle"* ]] || fail "expected output to contain: $needle"
}

assert_not_contains() {
	local haystack=$1
	local needle=$2
	[[ "$haystack" != *"$needle"* ]] || fail "output exposed forbidden text: $needle"
}

file_mode() {
	local mode
	if mode=$(stat -f '%Lp' "$1" 2>/dev/null); then
		printf '%s\n' "$mode"
	elif mode=$(stat -c '%a' "$1" 2>/dev/null); then
		printf '%s\n' "$mode"
	else
		return 1
	fi
}

make_fixture() {
	local name=$1
	local fixture="$TEST_ROOT/$name"
	mkdir -p "$fixture/repo/deploy" "$fixture/repo/dist" "$fixture/home" "$fixture/bin"
	cp "$SOURCE_INSTALL" "$fixture/repo/deploy/install.sh"
	cp "$SOURCE_PLIST" "$fixture/repo/deploy/launchd.plist.template"
	chmod +x "$fixture/repo/deploy/install.sh"
	echo "old-dist" > "$fixture/repo/dist/version.txt"

	cat > "$fixture/bin/npm" <<'EOF'
#!/bin/bash
set -euo pipefail
echo "new-dist" > dist/version.txt
EOF
	cat > "$fixture/bin/node" <<'EOF'
#!/bin/bash
set -euo pipefail
for key in LINEAR_CLIENT_ID LINEAR_CLIENT_SECRET LINEAR_WEBHOOK_SECRET LINEAR_ACCESS_TOKEN INGRESS_RECOVERY_KEY; do
	if [ -z "${!key:-}" ]; then
		echo "Missing required environment variable: $key" >&2
		exit 1
	fi
done
if [[ ! "$INGRESS_RECOVERY_KEY" =~ ^[A-Za-z0-9_-]{43}$ ]]; then
	echo "Invalid INGRESS_RECOVERY_KEY: expected canonical 32-byte base64url" >&2
	exit 1
fi
if [ -n "${INGRESS_RECOVERY_PREVIOUS_KEYS:-}" ]; then
	IFS=',' read -r -a previous_keys <<< "$INGRESS_RECOVERY_PREVIOUS_KEYS"
	if [ "${#previous_keys[@]}" -gt 4 ]; then
		echo "Invalid INGRESS_RECOVERY_PREVIOUS_KEYS" >&2
		exit 1
	fi
	seen=",$INGRESS_RECOVERY_KEY,"
	for previous_key in "${previous_keys[@]}"; do
		if [[ ! "$previous_key" =~ ^[A-Za-z0-9_-]{43}$ ]] || [[ "$seen" == *",$previous_key,"* ]]; then
			echo "Invalid INGRESS_RECOVERY_PREVIOUS_KEYS" >&2
			exit 1
		fi
		seen="$seen$previous_key,"
	done
fi
printf '%s' "${PORT:-3979}"
EOF
	cat > "$fixture/bin/launchctl" <<'EOF'
#!/bin/bash
set -euo pipefail
printf '%s\n' "$*" >> "$INSTALL_TEST_LAUNCHCTL_LOG"
EOF
	cat > "$fixture/bin/curl" <<'EOF'
#!/bin/bash
set -euo pipefail
count=0
if [ -f "$INSTALL_TEST_CURL_COUNT" ]; then
	count=$(cat "$INSTALL_TEST_CURL_COUNT")
fi
count=$((count + 1))
printf '%s\n' "$count" > "$INSTALL_TEST_CURL_COUNT"
case "${INSTALL_TEST_HEALTH_MODE:-healthy}" in
	healthy)
		printf '%s\n' 'ok'
		;;
	delayed)
		if [ "$count" -ge 3 ]; then
			printf '%s\n' 'ok'
		else
			exit 22
		fi
		;;
	prehealthy_then_down)
		if [ "$count" -eq 1 ]; then
			printf '%s\n' 'ok'
		else
			exit 22
		fi
		;;
	*)
		exit 22
		;;
esac
EOF
	cat > "$fixture/bin/sleep" <<'EOF'
#!/bin/bash
exit 0
EOF
	cat > "$fixture/bin/stat" <<'EOF'
#!/bin/bash
set -euo pipefail
if [ "$#" -ne 3 ] || [ "$1" != "-f" ]; then
	echo "fixture stat supports only the macOS stat -f interface" >&2
	exit 64
fi
if value=$(/usr/bin/stat -f "$2" "$3" 2>/dev/null); then
	printf '%s\n' "$value"
elif [ "$2" = '%u' ]; then
	/usr/bin/stat -c '%u' "$3"
elif [ "$2" = '%Lp' ]; then
	/usr/bin/stat -c '%a' "$3"
else
	echo "unsupported fixture stat format" >&2
	exit 64
fi
EOF
	chmod +x \
		"$fixture/bin/npm" \
		"$fixture/bin/node" \
		"$fixture/bin/launchctl" \
		"$fixture/bin/curl" \
		"$fixture/bin/sleep" \
		"$fixture/bin/stat"
	printf '%s\n' "$fixture"
}

write_valid_env() {
	local path=$1
	cat > "$path" <<'EOF'
LINEAR_CLIENT_ID=client
LINEAR_CLIENT_SECRET=client-secret-value
LINEAR_WEBHOOK_SECRET=webhook-secret-value
LINEAR_ACCESS_TOKEN=pending
INGRESS_RECOVERY_KEY=AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA
INGRESS_RECOVERY_PREVIOUS_KEYS=AQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQE
PORT=3979
RUNTIME=claude
EOF
}

run_installer() {
	local fixture=$1
	shift
	(
		cd "$fixture/repo"
		HOME="$fixture/home" \
		PATH="$fixture/bin:/usr/bin:/bin" \
		INSTALL_TEST_LAUNCHCTL_LOG="$fixture/launchctl.log" \
		INSTALL_TEST_CURL_COUNT="$fixture/curl.count" \
		SKIP_FUNNEL=1 \
		"$@" \
		./deploy/install.sh
	)
}

test_invalid_key_fails_before_service_mutation() {
	local fixture output status
	fixture=$(make_fixture invalid-key)
	write_valid_env "$fixture/repo/.env"
	sed -i.bak 's/^INGRESS_RECOVERY_KEY=.*/INGRESS_RECOVERY_KEY=too-short/' "$fixture/repo/.env"
	rm "$fixture/repo/.env.bak"
	chmod 600 "$fixture/repo/.env"

	set +e
	output=$(run_installer "$fixture" env INSTALL_TEST_HEALTH_MODE=healthy 2>&1)
	status=$?
	set -e

	[ "$status" -ne 0 ] || fail "invalid recovery key install succeeded"
	assert_contains "$output" "INGRESS_RECOVERY_KEY"
	[ ! -s "$fixture/launchctl.log" ] || fail "invalid config mutated launchd"
	[ "$(cat "$fixture/repo/dist/version.txt")" = "old-dist" ] || fail "failed preflight did not restore the prior build"
}

test_missing_config_fails_before_service_mutation() {
	local fixture output status
	fixture=$(make_fixture missing-config)
	write_valid_env "$fixture/repo/.env"
	sed -i.bak '/^LINEAR_CLIENT_SECRET=/d' "$fixture/repo/.env"
	rm "$fixture/repo/.env.bak"
	chmod 600 "$fixture/repo/.env"

	set +e
	output=$(run_installer "$fixture" env LINEAR_CLIENT_SECRET=ambient-secret INSTALL_TEST_HEALTH_MODE=healthy 2>&1)
	status=$?
	set -e

	[ "$status" -ne 0 ] || fail "missing required config install succeeded"
	assert_contains "$output" "LINEAR_CLIENT_SECRET"
	[ ! -s "$fixture/launchctl.log" ] || fail "missing config mutated launchd"
}

test_invalid_previous_key_fails_before_service_mutation() {
	local fixture output status
	fixture=$(make_fixture invalid-previous-key)
	write_valid_env "$fixture/repo/.env"
	sed -i.bak 's/^INGRESS_RECOVERY_PREVIOUS_KEYS=.*/INGRESS_RECOVERY_PREVIOUS_KEYS=too-short/' "$fixture/repo/.env"
	rm "$fixture/repo/.env.bak"
	chmod 600 "$fixture/repo/.env"

	set +e
	output=$(run_installer "$fixture" env INSTALL_TEST_HEALTH_MODE=healthy 2>&1)
	status=$?
	set -e

	[ "$status" -ne 0 ] || fail "invalid previous recovery key install succeeded"
	assert_contains "$output" "INGRESS_RECOVERY_PREVIOUS_KEYS"
	[ ! -s "$fixture/launchctl.log" ] || fail "invalid previous key mutated launchd"
}

test_repairs_env_permissions_without_exposing_secrets() {
	local fixture output command_log
	fixture=$(make_fixture safe-config)
	write_valid_env "$fixture/repo/.env"
	chmod 644 "$fixture/repo/.env"

	output=$(run_installer "$fixture" env INSTALL_TEST_HEALTH_MODE=healthy 2>&1)
	command_log=$(cat "$fixture/launchctl.log")

	[ "$(file_mode "$fixture/repo/.env")" = "600" ] || fail ".env permissions were not repaired"
	assert_not_contains "$output" "client-secret-value"
	assert_not_contains "$output" "webhook-secret-value"
	assert_not_contains "$output" "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
	assert_not_contains "$command_log" "client-secret-value"
	assert_contains "$output" "Health check:"
}

test_success_waits_for_bounded_health_check() {
	local fixture output
	fixture=$(make_fixture delayed-health)
	write_valid_env "$fixture/repo/.env"
	sed -i.bak 's/^INGRESS_RECOVERY_PREVIOUS_KEYS=.*/INGRESS_RECOVERY_PREVIOUS_KEYS=/' "$fixture/repo/.env"
	rm "$fixture/repo/.env.bak"
	chmod 600 "$fixture/repo/.env"

	output=$(run_installer "$fixture" env INSTALL_TEST_HEALTH_MODE=delayed 2>&1)

	[ "$(cat "$fixture/curl.count")" -eq 3 ] || fail "installer did not stop polling after health succeeded"
	assert_contains "$output" 'Health check: ok'
}

test_failed_restart_rolls_back_working_service() {
	local fixture output status command_log poll_count
	fixture=$(make_fixture rollback)
	write_valid_env "$fixture/repo/.env"
	chmod 600 "$fixture/repo/.env"
	mkdir -p "$fixture/home/Library/LaunchAgents"
	echo "old-plist" > "$fixture/home/Library/LaunchAgents/com.linear-agent-bridge.plist"

	set +e
	output=$(run_installer "$fixture" env INSTALL_TEST_HEALTH_MODE=prehealthy_then_down 2>&1)
	status=$?
	set -e
	command_log=$(cat "$fixture/launchctl.log")
	poll_count=$(cat "$fixture/curl.count")

	[ "$status" -ne 0 ] || fail "unhealthy install exited successfully"
	[ "$poll_count" -le 12 ] || fail "health polling was not bounded"
	[ "$(cat "$fixture/repo/dist/version.txt")" = "old-dist" ] || fail "rollback did not restore the prior build"
	[ "$(cat "$fixture/home/Library/LaunchAgents/com.linear-agent-bridge.plist")" = "old-plist" ] || fail "rollback did not restore the prior plist"
	assert_contains "$command_log" "bootout"
	[ "$(grep -c '^bootstrap ' "$fixture/launchctl.log")" -eq 2 ] || fail "rollback did not bootstrap the prior service"
	assert_contains "$output" "was healthy before install"
}

run_test() {
	local name=$1
	"$name"
	PASS_COUNT=$((PASS_COUNT + 1))
	echo "ok $PASS_COUNT - ${name#test_}"
}

run_test test_invalid_key_fails_before_service_mutation
run_test test_missing_config_fails_before_service_mutation
run_test test_invalid_previous_key_fails_before_service_mutation
run_test test_repairs_env_permissions_without_exposing_secrets
run_test test_success_waits_for_bounded_health_check
run_test test_failed_restart_rolls_back_working_service

echo "$PASS_COUNT installer tests passed"
