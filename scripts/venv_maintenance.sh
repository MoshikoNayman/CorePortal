#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VENV_DIR="${ROOT_DIR}/.venv"

usage() {
  cat <<'EOF'
CorePortal preprod venv maintenance

Usage:
  ./scripts/venv_maintenance.sh [--status|--prune|--rebuild]

Options:
  --status   Show .venv size and cache count
  --prune    Remove __pycache__ and *.pyc from .venv and purge pip cache
  --rebuild  Recreate .venv from requirements.txt
EOF
}

status() {
  if [[ ! -d "${VENV_DIR}" ]]; then
    echo ".venv not found at ${VENV_DIR}"
    exit 0
  fi

  echo "[status] .venv size"
  du -sh "${VENV_DIR}"
  echo
  echo "[status] cache entries"
  find "${VENV_DIR}" -type d -name '__pycache__' | wc -l | awk '{print "__pycache__ dirs:", $1}'
  find "${VENV_DIR}" -type f -name '*.pyc' | wc -l | awk '{print "pyc files:", $1}'
}

prune() {
  if [[ -d "${VENV_DIR}" ]]; then
    find "${VENV_DIR}" -type d -name '__pycache__' -prune -exec rm -rf {} +
    find "${VENV_DIR}" -type f -name '*.pyc' -delete
  fi

  if [[ -d "${HOME}/.cache/pip" ]]; then
    rm -rf "${HOME}/.cache/pip"
  fi

  echo "[prune] complete"
}

rebuild() {
  rm -rf "${VENV_DIR}"
  python3 -m venv "${VENV_DIR}"
  "${VENV_DIR}/bin/pip" install --upgrade pip
  "${VENV_DIR}/bin/pip" install -r "${ROOT_DIR}/requirements.txt"
  echo "[rebuild] complete"
}

case "${1:---status}" in
  --status)
    status
    ;;
  --prune)
    prune
    status
    ;;
  --rebuild)
    prune
    rebuild
    status
    ;;
  -h|--help)
    usage
    ;;
  *)
    usage
    exit 1
    ;;
esac
