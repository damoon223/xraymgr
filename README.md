# xraymgr

Minimal manager + web dashboard for collecting/importing configs and maintaining an SQLite database.

## Requirements

- Linux server (installer script targets **Debian/Ubuntu** via `apt-get`)
- Python **3.9+**
- Git
- **Node.js** runtime available as `node` (needed for the JS bridge used in URL → outbound conversion)

> Note: If you installed `nodejs-wheel` via `requirements.txt`, `node` should be available inside the Python venv.

## Install (fresh server, recommended)

This installs into: `/opt/xraymgr/`  
It clones the repo, creates a venv in the same folder, and installs `requirements.txt`.

```bash
# Install script (runs as root and uses apt-get)
sudo mkdir -p /opt

# Run installer directly from GitHub
curl -fsSL https://raw.githubusercontent.com/damoon223/xraymgr/main/scripts/install.sh | sudo bash
