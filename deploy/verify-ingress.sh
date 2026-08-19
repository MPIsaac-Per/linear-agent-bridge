#!/bin/bash
# Verify the public ingress without dispatching an AgentSession turn.
#
# Every probe is forced onto an address returned by a public resolver. Run from
# a host on the same overlay network as the service, the system resolver hands
# back the node's private address, the node terminates TLS locally with a valid
# certificate, and all three probes pass while public ingress is down. The
# script was loudest about success exactly when it was least able to check, so
# it now resolves the name itself instead of trusting the ambient resolver.
set -euo pipefail

: "${WEBHOOK_URL:?WEBHOOK_URL is required}"
: "${LINEAR_WEBHOOK_SECRET:?LINEAR_WEBHOOK_SECRET is required}"

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
RESOLVER_SCRIPT="$SCRIPT_DIR/resolve-public-addresses.mjs"

WEBHOOK_INPUT=$WEBHOOK_URL
WEBHOOK_SECRET=$LINEAR_WEBHOOK_SECRET
NODE_BIN=${VERIFY_INGRESS_NODE_BIN:-$(command -v node)}
CURL_BIN=${VERIFY_INGRESS_CURL_BIN:-$(command -v curl)}
# Two defaults so a single provider outage or one filtered resolver does not
# turn into a false failure.
RESOLVERS=${VERIFY_INGRESS_RESOLVERS:-1.1.1.1,8.8.8.8}
unset WEBHOOK_URL LINEAR_WEBHOOK_SECRET
unset VERIFY_INGRESS_NODE_BIN VERIFY_INGRESS_CURL_BIN VERIFY_INGRESS_RESOLVERS

umask 077
VERIFY_TEMP_DIR=$(mktemp -d "${TMPDIR:-/tmp}/linear-agent-ingress.XXXXXX")
cleanup() {
  rm -rf -- "$VERIFY_TEMP_DIR"
}
trap cleanup EXIT
trap 'exit 130' HUP INT TERM
SECRET_FILE="$VERIFY_TEMP_DIR/secret"
printf '%s' "$WEBHOOK_SECRET" > "$SECRET_FILE"
unset WEBHOOK_SECRET

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
  process.stdout.write(
    health.toString() +
      "\n" +
      webhook.toString() +
      "\n" +
      webhook.hostname +
      "\n" +
      (webhook.port === "" ? "443" : webhook.port),
  );
} catch {
  process.stderr.write("verify-ingress: invalid WEBHOOK_URL\n");
  process.exit(2);
}
NODE
)
HEALTH_URL=$(printf '%s\n' "$URLS" | sed -n '1p')
VALIDATED_WEBHOOK_URL=$(printf '%s\n' "$URLS" | sed -n '2p')
WEBHOOK_HOST=$(printf '%s\n' "$URLS" | sed -n '3p')
WEBHOOK_PORT=$(printf '%s\n' "$URLS" | sed -n '4p')

RESOLUTION_EXIT=0
RESOLUTION=$(
  /usr/bin/env -i \
    VERIFY_HOST="$WEBHOOK_HOST" \
    VERIFY_RESOLVERS="$RESOLVERS" \
    "$NODE_BIN" "$RESOLVER_SCRIPT"
) || RESOLUTION_EXIT=$?

PUBLIC_ADDRESSES=$(printf '%s\n' "$RESOLUTION" | sed -n 's/^public //p')
SYSTEM_ADDRESSES=$(printf '%s\n' "$RESOLUTION" | sed -n 's/^system //p')
RESOLVER_FAILURES=$(printf '%s\n' "$RESOLUTION" | sed -n 's/^resolver_failed /resolver_unreachable /p')

# A resolver that disagrees or fails is a warning. Only failing probes fail.
if [ -n "$RESOLVER_FAILURES" ]; then
  printf '%s\n' "$RESOLVER_FAILURES"
fi

if [ "$RESOLUTION_EXIT" -eq 3 ] || [ -z "$PUBLIC_ADDRESSES" ]; then
  printf '%s\n' \
    "verify-ingress: the public path was NOT tested. No configured resolver returned an address for $WEBHOOK_HOST." \
    "verify-ingress: falling back to the system resolver would prove nothing here, so this is a failure." >&2
  exit 1
fi
if [ "$RESOLUTION_EXIT" -ne 0 ]; then
  printf '%s\n' "verify-ingress: resolution failed for $WEBHOOK_HOST" >&2
  exit 1
fi

# The gap between what this host resolves and what the public internet resolves
# is the entire defect, so it leads the report rather than hiding inside it.
SORTED_PUBLIC=$(printf '%s\n' "$PUBLIC_ADDRESSES" | sort)
SORTED_SYSTEM=$(printf '%s\n' "$SYSTEM_ADDRESSES" | sort)
if [ "$SORTED_PUBLIC" != "$SORTED_SYSTEM" ]; then
  printf '%s\n' "split_horizon_dns host=$WEBHOOK_HOST"
  printf '  system_resolver=%s\n' "$(printf '%s' "$SORTED_SYSTEM" | tr '\n' ' ')"
  printf '  public_resolver=%s\n' "$(printf '%s' "$SORTED_PUBLIC" | tr '\n' ' ')"
  printf '%s\n' "  probes below use the public addresses only"
fi

BODY=$(/usr/bin/env -i "$NODE_BIN" <<'NODE'
process.stdout.write(JSON.stringify({
  type: "IngressVerificationEvent",
  action: "verify",
  webhookTimestamp: Date.now(),
}));
NODE
)
BODY_FILE="$VERIFY_TEMP_DIR/body.json"
CONTENT_TYPE_CONFIG="$VERIFY_TEMP_DIR/content-type.curlrc"
SIGNED_HEADER_CONFIG="$VERIFY_TEMP_DIR/signed-headers.curlrc"
printf '%s' "$BODY" > "$BODY_FILE"

if ! SIGNATURE=$(/usr/bin/env -i "$NODE_BIN" -e '
  const { createHmac } = require("node:crypto");
  const { readFileSync } = require("node:fs");
  process.stdout.write(
    createHmac("sha256", readFileSync(process.argv[1]))
      .update(readFileSync(process.argv[2]))
      .digest("hex"),
  );
' "$SECRET_FILE" "$BODY_FILE"); then
  printf '%s\n' "verify-ingress: signature generation failed" >&2
  exit 1
fi
printf 'header = "content-type: application/json"\n' > "$CONTENT_TYPE_CONFIG"
printf 'header = "linear-signature: %s"\n' "$SIGNATURE" > "$SIGNED_HEADER_CONFIG"
printf 'header = "content-type: application/json"\n' >> "$SIGNED_HEADER_CONFIG"
unset SIGNATURE BODY

# curl wants a bracketed literal for IPv6 in --resolve.
resolve_argument() {
  case $1 in
    *:*) printf '%s:%s:[%s]' "$WEBHOOK_HOST" "$WEBHOOK_PORT" "$1" ;;
    *) printf '%s:%s:%s' "$WEBHOOK_HOST" "$WEBHOOK_PORT" "$1" ;;
  esac
}

probe_get() {
  probe_label=$1
  probe_url=$2
  probe_address=$3
  probe_result=""
  probe_exit=0
  probe_result=$(/usr/bin/env -i "$CURL_BIN" -q \
    --silent \
    --request GET \
    --connect-timeout 5 \
    --max-time 15 \
    --max-redirs 0 \
    --proto '=https' \
    --resolve "$(resolve_argument "$probe_address")" \
    --output /dev/null \
    --write-out '%{http_code} %{time_total}' \
    "$probe_url" 2>/dev/null) || probe_exit=$?
  set -- $probe_result
  probe_status=${1:-000}
  probe_elapsed=${2:-0.000}
  printf '%s url=%s address=%s http_status=%s elapsed_seconds=%s\n' \
    "$probe_label" "$probe_url" "$probe_address" "$probe_status" "$probe_elapsed"
  if [ "$probe_exit" -ne 0 ] || [ "$probe_status" != "200" ]; then
    return 1
  fi
}

probe_post() {
  probe_label=$1
  probe_url=$2
  probe_address=$3
  expected_status=$4
  header_config=$5
  probe_result=""
  probe_exit=0
  probe_result=$(/usr/bin/env -i "$CURL_BIN" -q \
    --silent \
    --request POST \
    --connect-timeout 5 \
    --max-time 15 \
    --max-redirs 0 \
    --proto '=https' \
    --resolve "$(resolve_argument "$probe_address")" \
    --output /dev/null \
    --write-out '%{http_code} %{time_total}' \
    --config "$header_config" \
    --data-binary "@$BODY_FILE" \
    "$probe_url" 2>/dev/null) || probe_exit=$?
  set -- $probe_result
  probe_status=${1:-000}
  probe_elapsed=${2:-0.000}
  printf '%s url=%s address=%s http_status=%s elapsed_seconds=%s\n' \
    "$probe_label" "$probe_url" "$probe_address" "$probe_status" "$probe_elapsed"
  if [ "$probe_exit" -ne 0 ] || [ "$probe_status" != "$expected_status" ]; then
    return 1
  fi
}

# Every public address is probed, both families. A host answering on one family
# and not the other is broken for whichever clients use the other, so the run
# continues past a failure to report the whole picture and fails at the end.
FAILED_ADDRESSES=""
while IFS= read -r probe_target; do
  [ -n "$probe_target" ] || continue
  # Ordering is a control, not a formality: the unsigned 401 proves the route
  # enforces the authentication boundary, so a failure there must stop the
  # signed probe from ever being sent to this address.
  target_failed=0
  probe_get healthz "$HEALTH_URL" "$probe_target" \
    && probe_post authentication_control "$VALIDATED_WEBHOOK_URL" "$probe_target" 401 "$CONTENT_TYPE_CONFIG" \
    && probe_post webhook "$VALIDATED_WEBHOOK_URL" "$probe_target" 200 "$SIGNED_HEADER_CONFIG" \
    || target_failed=1
  if [ "$target_failed" -ne 0 ]; then
    FAILED_ADDRESSES="$FAILED_ADDRESSES $probe_target"
  fi
done <<PUBLIC_ADDRESS_LIST
$PUBLIC_ADDRESSES
PUBLIC_ADDRESS_LIST

if [ -n "$FAILED_ADDRESSES" ]; then
  printf '%s\n' "verify-ingress: the public path failed at:$FAILED_ADDRESSES" >&2
  exit 1
fi
