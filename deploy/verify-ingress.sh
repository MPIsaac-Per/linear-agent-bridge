#!/bin/bash
# Verify the public ingress without dispatching an AgentSession turn.
set -euo pipefail

: "${WEBHOOK_URL:?WEBHOOK_URL is required}"
: "${LINEAR_WEBHOOK_SECRET:?LINEAR_WEBHOOK_SECRET is required}"

WEBHOOK_INPUT=$WEBHOOK_URL
WEBHOOK_SECRET=$LINEAR_WEBHOOK_SECRET
NODE_BIN=${VERIFY_INGRESS_NODE_BIN:-$(command -v node)}
CURL_BIN=${VERIFY_INGRESS_CURL_BIN:-$(command -v curl)}
unset WEBHOOK_URL LINEAR_WEBHOOK_SECRET
unset VERIFY_INGRESS_NODE_BIN VERIFY_INGRESS_CURL_BIN

umask 077
VERIFY_TEMP_DIR=$(mktemp -d "${TMPDIR:-/tmp}/linear-agent-ingress.XXXXXX")
cleanup() {
  rm -rf -- "$VERIFY_TEMP_DIR"
}
trap cleanup EXIT
trap 'exit 130' HUP INT TERM
SECRET_FILE="$VERIFY_TEMP_DIR/secret"
HMAC_HELPER="$VERIFY_TEMP_DIR/hmac-helper.sh"
printf '%s' "$WEBHOOK_SECRET" > "$SECRET_FILE"
unset WEBHOOK_SECRET
cat > "$HMAC_HELPER" <<'BASH'
#!/bin/bash
set -eu
node_bin=$1
secret_file=$2
body_file=$3
LINEAR_WEBHOOK_SECRET=$(< "$secret_file")
export LINEAR_WEBHOOK_SECRET
unset node_bin secret_file PWD OLDPWD SHLVL _
exec "$1" -e '
  const { createHmac } = require("node:crypto");
  const { readFileSync } = require("node:fs");
  process.stdout.write(
    createHmac("sha256", process.env.LINEAR_WEBHOOK_SECRET)
      .update(readFileSync(process.argv[1]))
      .digest("hex"),
  );
' "$body_file"
BASH

URLS=$(
  /usr/bin/env -i WEBHOOK_URL="$WEBHOOK_INPUT" "$NODE_BIN" <<'NODE'
const raw = process.env.WEBHOOK_URL;
try {
  if (
    raw !== raw.trim() ||
    /[\u0000-\u0020\u007f?#]/.test(raw)
  ) {
    throw new Error("whitespace or control character");
  }
  const webhook = new URL(raw);
  if (
    webhook.protocol !== "https:" ||
    webhook.username !== "" ||
    webhook.password !== "" ||
    webhook.search !== "" ||
    webhook.hash !== "" ||
    webhook.pathname.includes("//") ||
    !webhook.pathname.endsWith("/webhook")
  ) {
    throw new Error("unsafe URL");
  }
  const health = new URL(webhook.toString());
  health.pathname = health.pathname.slice(0, -"/webhook".length) + "/healthz";
  process.stdout.write(health.toString() + "\n" + webhook.toString());
} catch {
  process.stderr.write("verify-ingress: invalid WEBHOOK_URL\n");
  process.exit(2);
}
NODE
)
HEALTH_URL=$(printf '%s\n' "$URLS" | sed -n '1p')
VALIDATED_WEBHOOK_URL=$(printf '%s\n' "$URLS" | sed -n '2p')

BODY=$(/usr/bin/env -i "$NODE_BIN" <<'NODE'
process.stdout.write(JSON.stringify({
  type: "IngressVerificationEvent",
  action: "verify",
  webhookTimestamp: Date.now(),
}));
NODE
)
BODY_FILE="$VERIFY_TEMP_DIR/body.json"
HEADER_CONFIG="$VERIFY_TEMP_DIR/headers.curlrc"
printf '%s' "$BODY" > "$BODY_FILE"

if ! SIGNATURE=$(/usr/bin/env -i /bin/bash "$HMAC_HELPER" \
  "$NODE_BIN" "$SECRET_FILE" "$BODY_FILE"); then
  printf '%s\n' "verify-ingress: signature generation failed" >&2
  exit 1
fi
printf 'header = "linear-signature: %s"\n' "$SIGNATURE" > "$HEADER_CONFIG"
printf 'header = "content-type: application/json"\n' >> "$HEADER_CONFIG"
unset SIGNATURE BODY

probe_get() {
  probe_label=$1
  probe_url=$2
  probe_result=""
  probe_exit=0
  probe_result=$(/usr/bin/env -i "$CURL_BIN" -q \
    --silent \
    --request GET \
    --connect-timeout 5 \
    --max-time 15 \
    --max-redirs 0 \
    --proto '=https' \
    --output /dev/null \
    --write-out '%{http_code} %{time_total}' \
    "$probe_url" 2>/dev/null) || probe_exit=$?
  set -- $probe_result
  probe_status=${1:-000}
  probe_elapsed=${2:-0.000}
  printf '%s url=%s http_status=%s elapsed_seconds=%s\n' \
    "$probe_label" "$probe_url" "$probe_status" "$probe_elapsed"
  if [ "$probe_exit" -ne 0 ] || [ "$probe_status" != "200" ]; then
    return 1
  fi
}

probe_post() {
  probe_label=$1
  probe_url=$2
  probe_result=""
  probe_exit=0
  probe_result=$(/usr/bin/env -i "$CURL_BIN" -q \
    --silent \
    --request POST \
    --connect-timeout 5 \
    --max-time 15 \
    --max-redirs 0 \
    --proto '=https' \
    --output /dev/null \
    --write-out '%{http_code} %{time_total}' \
    --config "$HEADER_CONFIG" \
    --data-binary "@$BODY_FILE" \
    "$probe_url" 2>/dev/null) || probe_exit=$?
  set -- $probe_result
  probe_status=${1:-000}
  probe_elapsed=${2:-0.000}
  printf '%s url=%s http_status=%s elapsed_seconds=%s\n' \
    "$probe_label" "$probe_url" "$probe_status" "$probe_elapsed"
  if [ "$probe_exit" -ne 0 ] || [ "$probe_status" != "200" ]; then
    return 1
  fi
}

probe_get healthz "$HEALTH_URL"
probe_post webhook "$VALIDATED_WEBHOOK_URL"
