#!/usr/bin/env bash
set -euo pipefail

if ! command -v gitleaks >/dev/null 2>&1; then
  echo "gitleaks not found. Install: brew install gitleaks" >&2
  exit 2
fi

gitleaks detect --source . --no-git --redact --report-format json --report-path .gitleaks-report-local.json

echo "Secret scan OK (.gitleaks-report-local.json)"
