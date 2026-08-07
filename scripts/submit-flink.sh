#!/usr/bin/env bash
set -euo pipefail

FLINK_URL="${FLINK_URL:-http://flink-jobmanager:8081}"

for attempt in $(seq 1 60); do
  if curl -fsS "${FLINK_URL}/overview" >/dev/null; then
    break
  fi
  if [[ "${attempt}" == "60" ]]; then
    echo "Flink JobManager did not become ready" >&2
    exit 1
  fi
  sleep 2
done

running_jobs="$(curl -fsS "${FLINK_URL}/jobs/overview" || true)"

for sql_file in /opt/platform/sql/*.sql; do
  pipeline_name="$(sed -n "s/SET 'pipeline.name' = '\([^']*\)';/\1/p" "${sql_file}")"
  if [[ -n "${pipeline_name}" ]] && \
    grep -Eq "\"name\":\"${pipeline_name}\"[^}]*\"state\":\"(CREATED|INITIALIZING|RUNNING|RESTARTING)\"" <<<"${running_jobs}"; then
    echo "Skipping already submitted pipeline: ${pipeline_name}"
    continue
  fi

  rendered="/tmp/$(basename "${sql_file}")"
  sed \
    -e "s|__AWS_ACCESS_KEY_ID__|${AWS_ACCESS_KEY_ID}|g" \
    -e "s|__AWS_SECRET_ACCESS_KEY__|${AWS_SECRET_ACCESS_KEY}|g" \
    "${sql_file}" >"${rendered}"

  echo "Submitting ${pipeline_name:-${sql_file}}"
  output="/tmp/$(basename "${sql_file}").output"
  set +e
  /opt/flink/bin/sql-client.sh -f "${rendered}" 2>&1 | tee "${output}"
  sql_status="${PIPESTATUS[0]}"
  set -e
  if [[ "${sql_status}" -ne 0 ]] || grep -Fq '[ERROR]' "${output}"; then
    echo "Flink rejected ${pipeline_name:-${sql_file}}" >&2
    exit 1
  fi
done
