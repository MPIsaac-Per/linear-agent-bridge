#!/bin/bash
# macOS launchd install: builds the bridge, installs it as an always-on
# user agent, and (optionally) exposes it with tailscale funnel.
set -euo pipefail
cd "$(dirname "$0")/.."

REPO_DIR=$(pwd)
LABEL="com.linear-claude-bridge"
PLIST=~/Library/LaunchAgents/$LABEL.plist

# 1. Refuse to install without config
if [ ! -f .env ]; then
	echo "Missing .env — copy .env.example and fill it in first (see README)"
	exit 1
fi

# 2. Build
npm run build

# 3. Render the plist template for this checkout
mkdir -p ~/Library/Logs ~/Library/LaunchAgents
sed -e "s|__REPO_DIR__|$REPO_DIR|g" -e "s|__HOME__|$HOME|g" \
	deploy/launchd.plist.template > "$PLIST"

# 4. (Re)load the service
launchctl bootout "gui/$(id -u)/$LABEL" 2>/dev/null || true
launchctl bootstrap "gui/$(id -u)" "$PLIST"
launchctl kickstart -k "gui/$(id -u)/$LABEL"

# 5. Health check
sleep 2
PORT=$(grep -E "^PORT=" .env | cut -d= -f2)
PORT=${PORT:-3979}
HEALTH_RESULT=$(curl -fsS "localhost:$PORT/healthz" 2>/dev/null || echo "unreachable")
echo "Health check: $HEALTH_RESULT"

# 6. Optional: expose publicly with tailscale funnel (macOS keeps the CLI
#    inside the app bundle). Skip silently if tailscale isn't present.
TAILSCALE=$(command -v tailscale || echo "/Applications/Tailscale.app/Contents/MacOS/Tailscale")
if [ -x "$TAILSCALE" ]; then
	"$TAILSCALE" funnel --bg "$PORT" || echo "Funnel failed; front the service with any public HTTPS ingress (see README)"
	echo ""
	FUNNEL_STATUS=$("$TAILSCALE" funnel status 2>/dev/null || echo "")
	FUNNEL_URL=$(echo "$FUNNEL_STATUS" | grep -oE 'https://[^/ ]+' | head -1 || echo "")
	if [ -n "$FUNNEL_URL" ]; then
		echo "Webhook URL: $FUNNEL_URL/webhook"
	else
		echo "Run '$TAILSCALE funnel status' and use https://<host>/webhook as the webhook URL"
	fi
else
	echo "tailscale not found; front the service with any public HTTPS ingress (see README)"
fi
