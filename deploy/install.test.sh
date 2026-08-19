#!/bin/bash
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
SOURCE_INSTALL="$SCRIPT_DIR/install.sh"
SOURCE_PLIST="$SCRIPT_DIR/launchd.plist.template"
SOURCE_UNIT="$SCRIPT_DIR/systemd.service.template"
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
	[[ "$haystack" == *"$needle"* ]] || fail "expected output to contain: $needle
Actual output was:
$haystack"
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
	local platform=${2:-macos}
	local fixture="$TEST_ROOT/$name-$platform"
	mkdir -p "$fixture/repo/deploy" "$fixture/repo/dist" "$fixture/home" "$fixture/bin" "$fixture/units"
	cp "$SOURCE_INSTALL" "$fixture/repo/deploy/install.sh"
	cp "$SOURCE_PLIST" "$fixture/repo/deploy/launchd.plist.template"
	cp "$SOURCE_UNIT" "$fixture/repo/deploy/systemd.service.template"
	chmod +x "$fixture/repo/deploy/install.sh"
	printf '%s' "$platform" > "$fixture/platform"
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
printf '%s\n' "$*" >> "$INSTALL_TEST_SERVICE_LOG"
EOF
	cat > "$fixture/bin/systemctl" <<'EOF'
#!/bin/bash
set -euo pipefail
printf '%s\n' "$*" >> "$INSTALL_TEST_SERVICE_LOG"
EOF
	if [ "$platform" = linux ]; then
	# The Linux path manages a system account and a unit, so it insists on root.
	# The fixture supplies that rather than the installer offering a bypass:
	# a privilege check with a test-only escape hatch is not a privilege check.
	cat > "$fixture/bin/id" <<'EOF'
#!/bin/bash
set -euo pipefail
case "${1:-}" in
	-u)
		if [ -n "${2:-}" ]; then exit 1; fi
		printf '0\n'
		;;
	-un)
		printf 'root\n'
		;;
	*)
		exit 1
		;;
esac
EOF
	cat > "$fixture/bin/getent" <<'EOF'
#!/bin/bash
exit 2
EOF
	cat > "$fixture/bin/groupadd" <<'EOF'
#!/bin/bash
printf 'groupadd %s\n' "$*" >> "$INSTALL_TEST_SERVICE_LOG"
EOF
	cat > "$fixture/bin/useradd" <<'EOF'
#!/bin/bash
printf 'useradd %s\n' "$*" >> "$INSTALL_TEST_SERVICE_LOG"
EOF
	cat > "$fixture/bin/chgrp" <<'EOF'
#!/bin/bash
exit 0
EOF
	fi
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
if [ "$#" -ne 3 ]; then
	echo "fixture stat expects a format flag, a format and a path" >&2
	exit 64
fi
case "$1" in
	-c | -f) ;;
	*)
		echo "fixture stat supports only -c and -f" >&2
		exit 64
		;;
esac
if value=$(/usr/bin/stat "$1" "$2" "$3" 2>/dev/null); then
	printf '%s\n' "$value"
	exit 0
fi
# Translate between the two dialects so either host answers either question.
case "$1$2" in
	'-c%u' | '-f%u') /usr/bin/stat -c '%u' "$3" 2>/dev/null || /usr/bin/stat -f '%u' "$3" ;;
	'-c%a' | '-f%Lp') /usr/bin/stat -c '%a' "$3" 2>/dev/null || /usr/bin/stat -f '%Lp' "$3" ;;
	*)
		echo "unsupported fixture stat format" >&2
		exit 64
		;;
esac
EOF
	chmod +x "$fixture"/bin/*
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
	local platform
	platform=$(cat "$fixture/platform")
	(
		cd "$fixture/repo"
		HOME="$fixture/home" \
		PATH="$fixture/bin:/usr/bin:/bin" \
		INSTALL_TEST_SERVICE_LOG="$fixture/service.log" \
		INSTALL_TEST_CURL_COUNT="$fixture/curl.count" \
		INSTALL_PLATFORM="$platform" \
		SYSTEMD_UNIT_DIR="$fixture/units" \
		SKIP_FUNNEL=1 \
		"$@" \
		./deploy/install.sh
	)
}

# The service definition the platform's manager reads.
service_definition_path() {
	local fixture=$1
	if [ "$(cat "$fixture/platform")" = macos ]; then
		printf '%s' "$fixture/home/Library/LaunchAgents/com.linear-agent-bridge.plist"
	else
		printf '%s' "$fixture/units/linear-agent-bridge.service"
	fi
}

# .env stays owner-only on macOS. On Linux the service account has to read it,
# so the boundary moves to the service group.
expected_env_mode() {
	if [ "$(cat "$1/platform")" = macos ]; then printf '600'; else printf '640'; fi
}

test_invalid_key_fails_before_service_mutation() {
	local platform=$1
	local fixture output status
	fixture=$(make_fixture invalid-key "$platform")
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
	[ ! -s "$fixture/service.log" ] || fail "invalid config mutated the service manager"
	[ "$(cat "$fixture/repo/dist/version.txt")" = "old-dist" ] || fail "failed preflight did not restore the prior build"
}

test_missing_config_fails_before_service_mutation() {
	local platform=$1
	local fixture output status
	fixture=$(make_fixture missing-config "$platform")
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
	[ ! -s "$fixture/service.log" ] || fail "missing config mutated the service manager"
}

test_invalid_previous_key_fails_before_service_mutation() {
	local platform=$1
	local fixture output status
	fixture=$(make_fixture invalid-previous-key "$platform")
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
	[ ! -s "$fixture/service.log" ] || fail "invalid previous key mutated the service manager"
}

test_repairs_env_permissions_without_exposing_secrets() {
	local platform=$1
	local fixture output command_log
	fixture=$(make_fixture safe-config "$platform")
	write_valid_env "$fixture/repo/.env"
	chmod 644 "$fixture/repo/.env"

	output=$(run_installer "$fixture" env INSTALL_TEST_HEALTH_MODE=healthy 2>&1)
	command_log=$(cat "$fixture/service.log")

	[ "$(file_mode "$fixture/repo/.env")" = "$(expected_env_mode "$fixture")" ] || fail ".env permissions were not repaired"
	assert_not_contains "$output" "client-secret-value"
	assert_not_contains "$output" "webhook-secret-value"
	assert_not_contains "$output" "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
	assert_not_contains "$command_log" "client-secret-value"
	assert_contains "$output" "Health check:"
}

test_success_waits_for_bounded_health_check() {
	local platform=$1
	local fixture output
	fixture=$(make_fixture delayed-health "$platform")
	write_valid_env "$fixture/repo/.env"
	sed -i.bak 's/^INGRESS_RECOVERY_PREVIOUS_KEYS=.*/INGRESS_RECOVERY_PREVIOUS_KEYS=/' "$fixture/repo/.env"
	rm "$fixture/repo/.env.bak"
	chmod 600 "$fixture/repo/.env"

	output=$(run_installer "$fixture" env INSTALL_TEST_HEALTH_MODE=delayed 2>&1)

	[ "$(cat "$fixture/curl.count")" -eq 3 ] || fail "installer did not stop polling after health succeeded"
	assert_contains "$output" 'Health check: ok'
}

test_failed_restart_rolls_back_working_service() {
	local platform=$1
	local fixture output status command_log poll_count
	fixture=$(make_fixture rollback "$platform")
	write_valid_env "$fixture/repo/.env"
	chmod 600 "$fixture/repo/.env"
	local definition
	definition=$(service_definition_path "$fixture")
	mkdir -p "$(dirname "$definition")"
	echo "old-definition" > "$definition"

	set +e
	output=$(run_installer "$fixture" env INSTALL_TEST_HEALTH_MODE=prehealthy_then_down 2>&1)
	status=$?
	set -e
	command_log=$(cat "$fixture/service.log")
	poll_count=$(cat "$fixture/curl.count")

	[ "$status" -ne 0 ] || fail "unhealthy install exited successfully"
	[ "$poll_count" -le 12 ] || fail "health polling was not bounded"
	[ "$(cat "$fixture/repo/dist/version.txt")" = "old-dist" ] || fail "rollback did not restore the prior build"
	[ "$(cat "$definition")" = "old-definition" ] || fail "rollback did not restore the prior service definition"
	if [ "$platform" = macos ]; then
		assert_contains "$command_log" "bootout"
		[ "$(grep -c '^bootstrap ' "$fixture/service.log")" -eq 2 ] || fail "rollback did not bootstrap the prior service"
	else
		assert_contains "$command_log" "stop"
		# Once for the failed candidate, once restoring the previous definition.
		[ "$(grep -c '^restart ' "$fixture/service.log")" -eq 2 ] || fail "rollback did not restart the prior service"
	fi
	assert_contains "$output" "was healthy before install"
}

run_test() {
	local name=$1
	local platform=$2
	"$name" "$platform"
	PASS_COUNT=$((PASS_COUNT + 1))
	echo "ok $PASS_COUNT - ${name#test_} [$platform]"
}

# The same cases against both fakes. Config preflight, health polling and
# rollback are the transaction, and the transaction is what must not drift.
for install_platform in macos linux; do
	run_test test_invalid_key_fails_before_service_mutation "$install_platform"
	run_test test_missing_config_fails_before_service_mutation "$install_platform"
	run_test test_invalid_previous_key_fails_before_service_mutation "$install_platform"
	run_test test_repairs_env_permissions_without_exposing_secrets "$install_platform"
	run_test test_success_waits_for_bounded_health_check "$install_platform"
	run_test test_failed_restart_rolls_back_working_service "$install_platform"
done

echo "$PASS_COUNT installer tests passed"
