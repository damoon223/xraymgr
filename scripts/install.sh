#!/usr/bin/env bash
set -euo pipefail

REPO_URL="https://github.com/damoon223/xraymgr.git"
INSTALL_DIR="/opt/xraymgr"
VENV_DIR="${INSTALL_DIR}/.venv"
BRANCH="main"

if [[ "${EUID}" -ne 0 ]]; then
  echo "ERROR: run as root (example: sudo -i)"; exit 1
fi

if ! command -v apt-get >/dev/null 2>&1; then
  echo "ERROR: this installer currently supports Debian/Ubuntu (apt-get not found)."; exit 1
fi

export DEBIAN_FRONTEND=noninteractive

# 1) Base OS deps
apt-get update -y
apt-get install -y --no-install-recommends \
  git ca-certificates \
  python3 python3-venv python3-pip \
  build-essential python3-dev

# 2) Clone repo
if [[ -d "${INSTALL_DIR}/.git" ]]; then
  git -C "${INSTALL_DIR}" fetch --all --prune
  git -C "${INSTALL_DIR}" reset --hard "origin/${BRANCH}"
else
  rm -rf "${INSTALL_DIR}"
  git clone --depth 1 --branch "${BRANCH}" "${REPO_URL}" "${INSTALL_DIR}"
fi

# 3) Create venv in the same folder + install requirements
python3 -m venv "${VENV_DIR}"
# shellcheck disable=SC1091
source "${VENV_DIR}/bin/activate"

python -m pip install --upgrade pip setuptools wheel
if [[ ! -f "${INSTALL_DIR}/requirements.txt" ]]; then
  echo "ERROR: requirements.txt not found at ${INSTALL_DIR}/requirements.txt"; exit 1
fi
python -m pip install -r "${INSTALL_DIR}/requirements.txt"

# 4) Optional: verify node (jsbridge needs a node CLI; if you added nodejs-wheel to requirements it should exist) :contentReference[oaicite:1]{index=1}
if command -v node >/dev/null 2>&1; then
  node -v
else
  echo "WARN: 'node' not found in PATH. If jsbridge is required, install Node (OS package) or add a Node wheel package (e.g., nodejs-wheel) to requirements."
fi

echo "OK: installed at ${INSTALL_DIR} with venv ${VENV_DIR}"
