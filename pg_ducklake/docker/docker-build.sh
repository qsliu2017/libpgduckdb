#!/usr/bin/env bash
# Local build of the pg_ducklake image. The bake file sets the build context
# to the repo root, so this script works from any CWD.
set -euo pipefail

BAKE_FILE="$(cd "$(dirname "$0")" && pwd)/docker-bake.hcl"

POSTGRES_VERSION="${POSTGRES_VERSION:-18}"
REPO="${REPO:-pgducklake/pgducklake}"
TARGET="${TARGET:-pg_ducklake_${POSTGRES_VERSION}}"
PUSH="${PUSH:-0}"
PLATFORM="${PLATFORM:-}"

if [[ "${PUSH}" == "1" ]]; then
  OUTPUT_FLAG="--push"
  PLATFORM_SET=()
else
  OUTPUT_FLAG="--load"
  if [[ -z "${PLATFORM}" ]]; then
    case "$(uname -m)" in
      x86_64|amd64) PLATFORM="linux/amd64" ;;
      arm64|aarch64) PLATFORM="linux/arm64" ;;
      *) PLATFORM="linux/amd64" ;;
    esac
  fi
  PLATFORM_SET=(--set "*.platform=${PLATFORM}")
fi

exec docker buildx bake \
  --file "${BAKE_FILE}" \
  "${TARGET}" \
  --set "*.args.POSTGRES_VERSION=${POSTGRES_VERSION}" \
  --set "*.tags=${REPO}:${POSTGRES_VERSION}-local" \
  "${PLATFORM_SET[@]}" \
  "${OUTPUT_FLAG}"
