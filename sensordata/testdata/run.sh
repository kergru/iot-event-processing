#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VENV_DIR="${ROOT_DIR}/.venv"

if [ ! -x "${VENV_DIR}/bin/python" ]; then
  "${ROOT_DIR}/setup.sh"
fi

exec "${VENV_DIR}/bin/python" "${ROOT_DIR}/sensor_simulator.py"
