#!/bin/bash
# Install the bridge as an always-on service and, unless told otherwise, expose
# it with tailscale funnel. One installer, branching only where the service
# manager differs: launchd on macOS, systemd on Linux. The transaction around
# it (config preflight, build preservation, bounded health polling, rollback,
# Funnel preflight) is identical on both, which is the part that must not drift.
set -euo pipefail
cd "$(dirname "$0")/.."

REPO_DIR=$(pwd)
LABEL="com.linear-agent-bridge"
LEGACY_LABEL="com.linear-claude-bridge"
UNIT_NAME="linear-agent-bridge"

# INSTALL_PLATFORM exists so the test suite can drive both paths from either
# host. Operators never set it.
PLATFORM=${INSTALL_PLATFORM:-$(uname -s)}
case "$PLATFORM" in
	Darwin | macos) PLATFORM=macos ;;
	Linux | linux) PLATFORM=linux ;;
	*)
		echo "Unsupported platform: $PLATFORM (expected Darwin or Linux)" >&2
		exit 1
		;;
esac

SERVICE_USER=${SERVICE_USER:-linear-agent-bridge}
SERVICE_GROUP=${SERVICE_GROUP:-$SERVICE_USER}
SYSTEMCTL=${SYSTEMCTL_BIN:-systemctl}

if [ "$PLATFORM" = macos ]; then
	SERVICE_DEF=~/Library/LaunchAgents/$LABEL.plist
	LEGACY_SERVICE_DEF=~/Library/LaunchAgents/$LEGACY_LABEL.plist
	SERVICE_TEMPLATE="deploy/launchd.plist.template"
	LOG_HINT="~/Library/Logs/linear-agent-bridge.log"
	# macOS runs the service as the invoking user. A dedicated account there
	# would mean a system LaunchDaemon, sudo on every install, and a second
	# Claude Code login for that account with no Keychain access. See MPI-1461.
	PRIMARY_ID=$LABEL
	LEGACY_ID=$LEGACY_LABEL
	RUN_AS_DESCRIPTION="the invoking user ($(id -un))"
else
	SYSTEMD_UNIT_DIR=${SYSTEMD_UNIT_DIR:-/etc/systemd/system}
	SERVICE_DEF=$SYSTEMD_UNIT_DIR/$UNIT_NAME.service
	LEGACY_SERVICE_DEF=$SYSTEMD_UNIT_DIR/$LEGACY_LABEL.service
	SERVICE_TEMPLATE="deploy/systemd.service.template"
	LOG_HINT="journalctl -u $UNIT_NAME"
	PRIMARY_ID=$UNIT_NAME
	LEGACY_ID="$LEGACY_LABEL"
	RUN_AS_DESCRIPTION="the dedicated account $SERVICE_USER"
fi
INSTALL_TEMP=$(mktemp -d "${TMPDIR:-/tmp}/linear-agent-bridge-install.XXXXXX")
DIST_BACKED_UP=0
DIST_WAS_PRESENT=0
ROLLBACK_NEEDED=0
PLIST_WAS_PRESENT=0
LEGACY_PLIST_WAS_PRESENT=0
PREVIOUS_HEALTHY=0
PORT=3979

service_stop() {
	if [ "$PLATFORM" = macos ]; then
		launchctl bootout "gui/$(id -u)/$1" 2>/dev/null || true
	else
		"$SYSTEMCTL" stop "$1" 2>/dev/null || true
	fi
}

service_load() {
	if [ "$PLATFORM" = macos ]; then
		launchctl bootstrap "gui/$(id -u)" "$2"
		launchctl kickstart -k "gui/$(id -u)/$1"
	else
		"$SYSTEMCTL" daemon-reload
		"$SYSTEMCTL" enable "$1"
		"$SYSTEMCTL" restart "$1"
	fi
}

service_load_quietly() {
	if [ "$PLATFORM" = macos ]; then
		launchctl bootstrap "gui/$(id -u)" "$2" 2>/dev/null || true
		launchctl kickstart -k "gui/$(id -u)/$1" 2>/dev/null || true
	else
		"$SYSTEMCTL" daemon-reload 2>/dev/null || true
		"$SYSTEMCTL" enable "$1" 2>/dev/null || true
		"$SYSTEMCTL" restart "$1" 2>/dev/null || true
	fi
}

restore_dist() {
	if [ "$DIST_BACKED_UP" -ne 1 ]; then
		return
	fi
	rm -rf "$REPO_DIR/dist"
	if [ "$DIST_WAS_PRESENT" -eq 1 ]; then
		cp -R "$INSTALL_TEMP/dist" "$REPO_DIR/dist"
	fi
	DIST_BACKED_UP=0
}

rollback_service() {
	set +e
	service_stop "$PRIMARY_ID"
	service_stop "$LEGACY_ID"
	restore_dist
	if [ "$PLIST_WAS_PRESENT" -eq 1 ]; then
		cp "$INSTALL_TEMP/current.definition" "$SERVICE_DEF"
		service_load_quietly "$PRIMARY_ID" "$SERVICE_DEF"
	else
		rm -f "$SERVICE_DEF"
		if [ "$PLATFORM" != macos ]; then
			"$SYSTEMCTL" daemon-reload 2>/dev/null || true
		fi
	fi
	if [ "$LEGACY_PLIST_WAS_PRESENT" -eq 1 ]; then
		cp "$INSTALL_TEMP/legacy.definition" "$LEGACY_SERVICE_DEF"
		service_load_quietly "$LEGACY_ID" "$LEGACY_SERVICE_DEF"
	else
		rm -f "$LEGACY_SERVICE_DEF"
	fi
	if curl -fsS --connect-timeout 1 --max-time 1 \
		"http://127.0.0.1:$PORT/healthz" >/dev/null 2>&1; then
		echo "Install failed; the previous build and service configuration were restored and are healthy." >&2
	elif [ "$PREVIOUS_HEALTHY" -eq 1 ]; then
		echo "Install failed; the previous build and service configuration were restored, but the service was healthy before install and is now unconfirmed. Check $LOG_HINT." >&2
	else
		echo "Install failed; the previous build and service configuration were restored, but health is unconfirmed. Check $LOG_HINT." >&2
	fi
	set -e
}

cleanup() {
	local status=$?
	trap - EXIT
	if [ "$status" -ne 0 ] && [ "$ROLLBACK_NEEDED" -eq 1 ]; then
		rollback_service
	elif [ "$status" -ne 0 ]; then
		restore_dist
	fi
	rm -rf "$INSTALL_TEMP"
	exit "$status"
}
trap cleanup EXIT

# 0. Linux only: a dedicated service account and a unit in /etc/systemd/system
# both need privilege. Fail here with something an operator can act on rather
# than part-way through with a permission error.
if [ "$PLATFORM" = linux ]; then
	if [ "$(id -u)" -ne 0 ]; then
		echo "Linux installs manage a system account and a systemd unit; rerun with sudo" >&2
		exit 1
	fi
fi

# 1. Refuse to install without an owner-controlled config file. Repair a
# readable-by-others mode before loading any secret from it.
if [ ! -f .env ] || [ -L .env ]; then
	echo "Missing or unsafe .env; copy .env.example to a regular file and fill it in (see README)" >&2
	exit 1
fi
# GNU form first: on Linux `stat -f` means --file-system, succeeds, and returns
# an unrelated value instead of failing through to the BSD form.
env_stat() {
	stat -c "$1" .env 2>/dev/null || stat -f "$2" .env 2>/dev/null || true
}
if [ "$PLATFORM" = macos ]; then
	ENV_OWNER=$(env_stat '%u' '%u')
	if [ "$ENV_OWNER" != "$(id -u)" ]; then
		echo ".env must be owned by the user installing the service" >&2
		exit 1
	fi
fi
REQUIRED_ENV_MODE=600
ENV_MODE=$(env_stat '%a' '%Lp')
if [ "$ENV_MODE" != "$REQUIRED_ENV_MODE" ]; then
	chmod "0$REQUIRED_ENV_MODE" .env
	ENV_MODE=$(env_stat '%a' '%Lp')
	if [ "$ENV_MODE" != "$REQUIRED_ENV_MODE" ]; then
		echo "Could not set .env permissions to 0$REQUIRED_ENV_MODE" >&2
		exit 1
	fi
	echo "Set .env permissions to 0$REQUIRED_ENV_MODE."
fi

# 2. Preserve the last runnable build, then build the candidate. On macOS this
# also compiles dist/native/process_identity with Xcode Command Line Tools.
if [ -d dist ]; then
	cp -R dist "$INSTALL_TEMP/dist"
	DIST_WAS_PRESENT=1
fi
DIST_BACKED_UP=1
if ! npm run build; then
	restore_dist
	echo "Build failed; the previous build was restored." >&2
	exit 1
fi

# 3. Load the environment without echoing it, then call the application's own
# config validator. Only the validated numeric port crosses the subshell.
if ! PORT=$(env -i HOME="$HOME" PATH="$PATH" /bin/bash -c '
	set -euo pipefail
	set -a
	if ! source ./.env >/dev/null 2>&1; then
		echo "Could not load .env" >&2
		exit 1
	fi
	set +a
	node --input-type=module -e "import(\"./dist/config.js\").then(({ loadConfig }) => process.stdout.write(String(loadConfig().port)))"
'); then
	restore_dist
	echo "Configuration preflight failed; launchd was not changed." >&2
	exit 1
fi

# 3b. Linux only, and only now that the configuration is known good: create the
# dedicated account and widen .env to its group. The service account has to read
# the file, so owner-only is not an option, but nothing is created and no secret
# is widened for an install that was going to fail anyway.
if [ "$PLATFORM" = linux ]; then
	if ! getent group "$SERVICE_GROUP" >/dev/null 2>&1; then
		groupadd --system "$SERVICE_GROUP"
		echo "Created service group $SERVICE_GROUP."
	fi
	if ! id -u "$SERVICE_USER" >/dev/null 2>&1; then
		useradd --system --gid "$SERVICE_GROUP" --shell /usr/sbin/nologin \
			--home-dir /var/lib/linear-agent-bridge --create-home "$SERVICE_USER"
		echo "Created service account $SERVICE_USER."
	fi
	chgrp "$SERVICE_GROUP" .env
	chmod 640 .env
	ENV_MODE=$(env_stat '%a' '%Lp')
	if [ "$ENV_MODE" != "640" ]; then
		echo "Could not set .env permissions to 0640" >&2
		exit 1
	fi
	echo "Set .env to 0640, readable by $SERVICE_GROUP."
fi

# 4. Render and back up the service definition before the first mutation.
if [ "$PLATFORM" = macos ]; then
	mkdir -p ~/Library/Logs ~/Library/LaunchAgents
	sed -e "s|__REPO_DIR__|$REPO_DIR|g" -e "s|__HOME__|$HOME|g" \
		"$SERVICE_TEMPLATE" > "$INSTALL_TEMP/new.definition"
else
	mkdir -p "$(dirname "$SERVICE_DEF")"
	sed -e "s|__REPO_DIR__|$REPO_DIR|g" \
		-e "s|__SERVICE_USER__|$SERVICE_USER|g" \
		-e "s|__SERVICE_GROUP__|$SERVICE_GROUP|g" \
		"$SERVICE_TEMPLATE" > "$INSTALL_TEMP/new.definition"
	# The account runs out of this tree and reads the build, so it needs to
	# traverse and read it. It never needs to write here; agent output is
	# configured separately (see MPI-1460).
	chgrp -R "$SERVICE_GROUP" "$REPO_DIR/dist" 2>/dev/null || true
	chmod -R g+rX "$REPO_DIR/dist" 2>/dev/null || true
fi
if [ -f "$SERVICE_DEF" ]; then
	cp "$SERVICE_DEF" "$INSTALL_TEMP/current.definition"
	PLIST_WAS_PRESENT=1
fi
if [ -f "$LEGACY_SERVICE_DEF" ]; then
	cp "$LEGACY_SERVICE_DEF" "$INSTALL_TEMP/legacy.definition"
	LEGACY_PLIST_WAS_PRESENT=1
fi

# Record whether this port was healthy for the rollback report. A failed probe
# does not prevent install because this may be the first installation.
if curl -fsS --connect-timeout 1 --max-time 1 \
	"http://127.0.0.1:$PORT/healthz" >/dev/null 2>&1; then
	PREVIOUS_HEALTHY=1
fi

# 5. (Re)load the service. Remove the pre-rename launchd job so upgrades
# do not leave two bridge processes consuming the same webhook and stores.
ROLLBACK_NEEDED=1
service_stop "$LEGACY_ID"
service_stop "$PRIMARY_ID"
cp "$INSTALL_TEMP/new.definition" "$SERVICE_DEF"
rm -f "$LEGACY_SERVICE_DEF"
service_load "$PRIMARY_ID" "$SERVICE_DEF"
echo "Service runs as $RUN_AS_DESCRIPTION."

# 6. Poll health for a bounded window against the loopback listener the Funnel
# will target. An unhealthy candidate exits nonzero; the EXIT trap restores the
# previous build and launchd configuration.
HEALTH_URL="http://127.0.0.1:$PORT/healthz"
HEALTH_RESULT=""
for attempt in 1 2 3 4 5; do
	if HEALTH_RESULT=$(curl -q -fsS --connect-timeout 1 --max-time 1 \
		"$HEALTH_URL" 2>/dev/null); then
		break
	fi
	HEALTH_RESULT=""
	if [ "$attempt" -lt 5 ]; then
		sleep 1
	fi
done
if [ -z "$HEALTH_RESULT" ]; then
	echo "Local health check failed after 5 attempts: $HEALTH_URL" >&2
	exit 1
fi
if [ "$HEALTH_RESULT" != "ok" ]; then
	echo "Local health check returned an unexpected response: $HEALTH_URL" >&2
	exit 1
fi
echo "Health check: $HEALTH_RESULT"
ROLLBACK_NEEDED=0
DIST_BACKED_UP=0

# 7. Expose this exact loopback listener directly with Tailscale Funnel unless
#    the operator explicitly requested a local-only install.
if [ "${SKIP_FUNNEL:-0}" = "1" ]; then
	echo "SKIP_FUNNEL=1: service installed without public ingress"
else
	# PATH first on both platforms. macOS additionally keeps the CLI inside the
	# app bundle, which is the only place a bare `tailscale` will not be found.
	if [ -n "${TAILSCALE_BIN:-}" ]; then
		TAILSCALE=$TAILSCALE_BIN
	elif TAILSCALE=$(command -v tailscale 2>/dev/null); then
		:
	elif [ "$PLATFORM" = macos ]; then
		TAILSCALE="/Applications/Tailscale.app/Contents/MacOS/Tailscale"
	else
		TAILSCALE="tailscale"
	fi
	if [ ! -x "$TAILSCALE" ]; then
		echo "tailscale not found; refusing to complete without public ingress (set SKIP_FUNNEL=1 for an explicit local-only install)" >&2
		exit 1
	fi
	FUNNEL_TARGET="http://127.0.0.1:$PORT"
	FUNNEL_CLEANUP_REQUIRED=0
	FUNNEL_STATUS_FILE=$(mktemp "${TMPDIR:-/tmp}/linear-agent-funnel.XXXXXX")
	cleanup_funnel_setup() {
		cleanup_status=$?
		trap - EXIT HUP INT TERM
		rm -f -- "$FUNNEL_STATUS_FILE"
		rm -rf -- "$INSTALL_TEMP"
		if [ "$FUNNEL_CLEANUP_REQUIRED" = "1" ]; then
			if ! "$TAILSCALE" funnel --https=443 off >/dev/null 2>&1; then
				echo "Funnel cleanup failed; run: '$TAILSCALE' funnel --https=443 off" >&2
			fi
		fi
		exit "$cleanup_status"
	}
	trap cleanup_funnel_setup EXIT
	trap 'exit 129' HUP
	trap 'exit 130' INT
	trap 'exit 143' TERM

	# Refuse to overwrite an unrelated public route. Only an empty public
	# Funnel state may be changed; the exact target is an idempotent success.
	"$TAILSCALE" funnel status --json > "$FUNNEL_STATUS_FILE"
	if ! FUNNEL_PREFLIGHT=$(node deploy/parse-funnel-status.mjs preflight "$FUNNEL_TARGET" "$FUNNEL_STATUS_FILE"); then
		echo "Refusing to replace unrelated or ambiguous public Funnel state" >&2
		exit 1
	fi

	case "$FUNNEL_PREFLIGHT" in
		empty)
			# Arm teardown before mutation. Preflight proved there is no unrelated
			# public Funnel route, so `off` can only remove this attempted setup.
			FUNNEL_CLEANUP_REQUIRED=1
			"$TAILSCALE" funnel --bg "$FUNNEL_TARGET"
			"$TAILSCALE" funnel status --json > "$FUNNEL_STATUS_FILE"
			if ! WEBHOOK_URL=$(node deploy/parse-funnel-status.mjs verify "$FUNNEL_TARGET" "$FUNNEL_STATUS_FILE"); then
				echo "Funnel status did not contain exactly one public route to $FUNNEL_TARGET" >&2
				exit 1
			fi
			FUNNEL_CLEANUP_REQUIRED=0
			;;
		existing\ *)
			WEBHOOK_URL=${FUNNEL_PREFLIGHT#existing }
			echo "Funnel already targets $FUNNEL_TARGET; leaving it unchanged"
			;;
		*)
			echo "Invalid Funnel preflight result" >&2
			exit 1
			;;
	esac
	echo "Webhook URL: $WEBHOOK_URL"
	echo "Verify with: WEBHOOK_URL='$WEBHOOK_URL' LINEAR_WEBHOOK_SECRET='<from .env>' ./deploy/verify-ingress.sh"
fi
