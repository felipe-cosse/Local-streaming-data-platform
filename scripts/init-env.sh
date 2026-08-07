#!/usr/bin/env bash
set -euo pipefail

readonly PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
readonly ENV_FILE="${ENV_FILE:-${PROJECT_ROOT}/.env}"
readonly ENV_EXAMPLE_FILE="${ENV_EXAMPLE_FILE:-${PROJECT_ROOT}/.env.example}"

if [[ ! -f "${ENV_FILE}" ]]; then
  cp "${ENV_EXAMPLE_FILE}" "${ENV_FILE}"
fi

current_key="$(sed -n 's/^AIRFLOW_FERNET_KEY=//p' "${ENV_FILE}" | tail -n 1)"
if [[ -n "${current_key}" ]]; then
  if ! python3 -c 'import base64, sys; parts = sys.argv[1].split(","); assert all(len(base64.urlsafe_b64decode(value)) == 32 for value in parts)' "${current_key}"; then
    echo "ERROR: AIRFLOW_FERNET_KEY in ${ENV_FILE} is not a valid Fernet key." >&2
    exit 1
  fi
  chmod 600 "${ENV_FILE}"
  echo "Local environment already has a valid Airflow Fernet key."
  exit 0
fi

generated_key="$(python3 -c 'import base64, secrets; print(base64.urlsafe_b64encode(secrets.token_bytes(32)).decode())')"
temporary_file="$(mktemp "${ENV_FILE}.tmp.XXXXXX")"
trap 'rm -f "${temporary_file}"' EXIT

awk -v key="${generated_key}" '
  BEGIN { replaced = 0 }
  /^AIRFLOW_FERNET_KEY=/ && !replaced {
    print "AIRFLOW_FERNET_KEY=" key
    replaced = 1
    next
  }
  { print }
  END {
    if (!replaced) {
      print ""
      print "AIRFLOW_FERNET_KEY=" key
    }
  }
' "${ENV_FILE}" > "${temporary_file}"

chmod 600 "${temporary_file}"
mv "${temporary_file}" "${ENV_FILE}"
trap - EXIT
echo "Generated a unique Airflow Fernet key in ${ENV_FILE}."
