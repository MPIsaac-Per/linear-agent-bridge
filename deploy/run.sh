#!/bin/bash
set -euo pipefail
cd "$(dirname "$0")/.."
set -a; source ./.env; set +a
exec /usr/bin/env node dist/index.js
