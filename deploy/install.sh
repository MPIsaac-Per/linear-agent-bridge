#!/bin/bash
# macOS launchd install: builds the bridge, installs it as an always-on
# user agent, and (optionally) exposes it with tailscale funnel.
set -euo pipefail
cd "$(dirname "$0")/.."

REPO_DIR=$(pwd)
LABEL="com.linear-agent-bridge"
LEGACY_LABEL="com.linear-claude-bridge"
PLIST=~/Library/LaunchAgents/$LABEL.plist
LEGACY_PLIST=~/Library/LaunchAgents/$LEGACY_LABEL.plist
INSTALL_TEMP=$(mktemp -d "${TMPDIR:-/tmp}/linear-agent-bridge-install.XXXXXX")
DIST_BACKED_UP=0
DIST_WAS_PRESENT=0
ROLLBACK_NEEDED=0
PLIST_WAS_PRESENT=0
LEGACY_PLIST_WAS_PRESENT=0
PREVIOUS_HEALTHY=0
PORT=3979

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
	local domain="gui/$(id -u)"
	set +e
	launchctl bootout "$domain/$LABEL" 2>/dev/null || true
	launchctl bootout "$domain/$LEGACY_LABEL" 2>/dev/null || true
	restore_dist
	if [ "$PLIST_WAS_PRESENT" -eq 1 ]; then
		cp "$INSTALL_TEMP/current.plist" "$PLIST"
		launchctl bootstrap "$domain" "$PLIST" 2>/dev/null || true
		launchctl kickstart -k "$domain/$LABEL" 2>/dev/null || true
	else
		rm -f "$PLIST"
	fi
	if [ "$LEGACY_PLIST_WAS_PRESENT" -eq 1 ]; then
		cp "$INSTALL_TEMP/legacy.plist" "$LEGACY_PLIST"
		launchctl bootstrap "$domain" "$LEGACY_PLIST" 2>/dev/null || true
		launchctl kickstart -k "$domain/$LEGACY_LABEL" 2>/dev/null || true
	else
		rm -f "$LEGACY_PLIST"
	fi
	if curl -fsS --connect-timeout 1 --max-time 1 \
		"http://127.0.0.1:$PORT/healthz" >/dev/null 2>&1; then
		echo "Install failed; the previous build and launchd configuration were restored and are healthy." >&2
	elif [ "$PREVIOUS_HEALTHY" -eq 1 ]; then
		echo "Install failed; the previous build and launchd configuration were restored, but the service was healthy before install and is now unconfirmed. Check ~/Library/Logs/linear-agent-bridge.log." >&2
	else
		echo "Install failed; the previous build and launchd configuration were restored, but health is unconfirmed. Check ~/Library/Logs/linear-agent-bridge.log." >&2
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

# 1. Refuse to install without an owner-controlled config file. Repair a
# readable-by-others mode before loading any secret from it.
if [ ! -f .env ] || [ -L .env ]; then
	echo "Missing or unsafe .env; copy .env.example to a regular file and fill it in (see README)" >&2
	exit 1
fi
ENV_OWNER=$(stat -f '%u' .env 2>/dev/null || stat -c '%u' .env 2>/dev/null || true)
if [ "$ENV_OWNER" != "$(id -u)" ]; then
	echo ".env must be owned by the user installing the service" >&2
	exit 1
fi
ENV_MODE=$(stat -f '%Lp' .env 2>/dev/null || stat -c '%a' .env 2>/dev/null || true)
if [ "$ENV_MODE" != "600" ]; then
	chmod 600 .env
	ENV_MODE=$(stat -f '%Lp' .env 2>/dev/null || stat -c '%a' .env 2>/dev/null || true)
	if [ "$ENV_MODE" != "600" ]; then
		echo "Could not set .env permissions to 0600" >&2
		exit 1
	fi
	echo "Set .env permissions to 0600."
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

# 4. Render and back up launchd files before the first service mutation.
mkdir -p ~/Library/Logs ~/Library/LaunchAgents
sed -e "s|__REPO_DIR__|$REPO_DIR|g" -e "s|__HOME__|$HOME|g" \
	deploy/launchd.plist.template > "$INSTALL_TEMP/new.plist"
if [ -f "$PLIST" ]; then
	cp "$PLIST" "$INSTALL_TEMP/current.plist"
	PLIST_WAS_PRESENT=1
fi
if [ -f "$LEGACY_PLIST" ]; then
	cp "$LEGACY_PLIST" "$INSTALL_TEMP/legacy.plist"
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
launchctl bootout "gui/$(id -u)/$LEGACY_LABEL" 2>/dev/null || true
launchctl bootout "gui/$(id -u)/$LABEL" 2>/dev/null || true
cp "$INSTALL_TEMP/new.plist" "$PLIST"
rm -f "$LEGACY_PLIST"
launchctl bootstrap "gui/$(id -u)" "$PLIST"
launchctl kickstart -k "gui/$(id -u)/$LABEL"

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
	# macOS keeps the CLI inside the app bundle.
	TAILSCALE=${TAILSCALE_BIN:-$(command -v tailscale || echo "/Applications/Tailscale.app/Contents/MacOS/Tailscale")}
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
