#!/usr/bin/env bash
set -Eeuo pipefail

cd -- "$(dirname -- "${BASH_SOURCE[0]}")"
umask 077

die() {
  printf 'ERROR: %s\n' "$*" >&2
  exit 1
}

set_env() {
  local key=$1 value=$2 temp
  temp=$(mktemp .env.XXXXXX)
  awk -v key="$key" -v value="$value" '
    BEGIN { found = 0 }
    index($0, key "=") == 1 { print key "=" value; found = 1; next }
    { print }
    END { if (!found) print key "=" value }
  ' .env >"$temp"
  mv -f -- "$temp" .env
}

read_secret() {
  local prompt=$1 target=$2 value
  if [[ -s "$target" ]]; then
    printf 'Reuse existing %s\n' "$target"
    return
  fi
  read -r -s -p "$prompt" value
  printf '\n'
  [[ -n "$value" ]] || die "Secret cannot be empty"
  printf '%s' "$value" >"$target"
}

command -v docker >/dev/null 2>&1 || die "docker is not installed"
docker compose version >/dev/null 2>&1 || die "docker compose v2 is required"
[[ -t 0 ]] || die "deploy.sh requires an interactive terminal"

if [[ ! -f .env ]]; then
  cp -- .env.example .env
fi

container_uid=${CONTAINER_UID:-0}
container_gid=${CONTAINER_GID:-0}
set_env HOST_UID "$container_uid"
set_env HOST_GID "$container_gid"

mkdir -p -- data secrets certs
touch -- secrets/es_api_key secrets/es_password secrets/smtp_password
for secret_file in secrets/es_api_key secrets/es_password secrets/smtp_password; do
  if ! grep -q '[^[:space:]]' "$secret_file"; then
    : >"$secret_file"
  fi
done
if [[ $(id -u) -eq 0 ]]; then
  chown -R "$container_uid:$container_gid" data secrets certs .env
fi
chmod 0700 data secrets certs
chmod 0600 .env secrets/es_api_key secrets/es_password secrets/smtp_password

if [[ ! -s secrets/es_api_key && ! -s secrets/es_password ]]; then
  printf '\nElasticsearch authentication:\n'
  printf '  1) API Key (recommended)\n'
  printf '  2) Basic username/password\n'
  printf '  3) No credentials (reverse proxy handles auth)\n'
  read -r -p 'Choose [1-3]: ' auth_choice
  case "$auth_choice" in
    1)
      read_secret 'Elasticsearch API Key (encoded value): ' secrets/es_api_key
      : >secrets/es_password
      set_env ES_USERNAME ""
      ;;
    2)
      read -r -p 'Elasticsearch username: ' es_username
      [[ -n "$es_username" ]] || die "Elasticsearch username cannot be empty"
      set_env ES_USERNAME "$es_username"
      read_secret 'Elasticsearch password: ' secrets/es_password
      : >secrets/es_api_key
      ;;
    3)
      : >secrets/es_api_key
      : >secrets/es_password
      set_env ES_USERNAME ""
      ;;
    *)
      die "Invalid authentication choice"
      ;;
  esac
fi

read_secret 'smtp.126.com authorization code: ' secrets/smtp_password
chown "$container_uid:$container_gid" secrets/es_api_key secrets/es_password secrets/smtp_password 2>/dev/null || true
chmod 0600 secrets/es_api_key secrets/es_password secrets/smtp_password

printf '\nBuilding container image...\n'
docker compose build --pull cold-archive

printf '\nRunning mandatory dry-run (including SMTP delivery test)...\n'
set +e
dry_output=$(docker compose run --rm -e ARCHIVE_EXECUTE=false cold-archive --once --dry-run 2>&1)
dry_status=$?
set -e
printf '%s\n' "$dry_output"
if [[ $dry_status -ne 0 && $dry_status -ne 2 ]]; then
  die "Dry-run failed with exit code $dry_status; live mode was not enabled"
fi

cluster_uuid=$(printf '%s\n' "$dry_output" \
  | sed -n 's/^Cluster UUID:[[:space:]]*//p' \
  | tail -n 1 \
  | tr -d '\r')
[[ -n "$cluster_uuid" && "$cluster_uuid" != "-" ]] \
  || die "Could not extract Cluster UUID from dry-run output"
set_env ES_EXPECTED_CLUSTER_UUID "$cluster_uuid"

printf '\nDry-run passed. Cluster UUID pinned to %s.\n' "$cluster_uuid"
printf 'Review every candidate shown above.\n'
read -r -p 'Type YES to enable snapshot-and-delete mode: ' confirmation
if [[ "$confirmation" == "YES" ]]; then
  set_env ARCHIVE_EXECUTE true
  mode=LIVE
else
  set_env ARCHIVE_EXECUTE false
  mode=DRY-RUN
fi

docker compose up -d --remove-orphans cold-archive
if [[ $(id -u) -eq 0 ]]; then
  chown "$container_uid:$container_gid" .env
fi
docker compose ps
printf '\nDeployment complete. Scheduler mode: %s.\n' "$mode"
printf 'Configuration is mounted from: %s/config.compose.json\n' "$PWD"
printf 'Logs: docker compose logs -f cold-archive\n'
printf 'The first scheduled run will be the next 02:00 Asia/Shanghai.\n'
