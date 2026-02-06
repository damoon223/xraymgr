````markdown
# xraymgr

A small FastAPI-based dashboard + jobs for collecting/importing proxy configs and maintaining an SQLite database.

## Quick install (fresh Debian/Ubuntu server)

Installs into: `/opt/xraymgr/`  
Creates a venv at: `/opt/xraymgr/.venv`  
Installs Python deps from `requirements.txt`.

```bash
sudo mkdir -p /opt
curl -fsSL https://raw.githubusercontent.com/damoon223/xraymgr/main/scripts/install.sh | sudo bash
````

After install:

```bash
source /opt/xraymgr/.venv/bin/activate
python --version
pip --version
node -v
```

> `node` must be available for the JS bridge used by URL → outbound conversion.

## Manual install

```bash
sudo mkdir -p /opt
sudo git clone --depth 1 https://github.com/damoon223/xraymgr.git /opt/xraymgr

cd /opt/xraymgr
python3 -m venv .venv
source .venv/bin/activate

python -m pip install --upgrade pip setuptools wheel
python -m pip install -r requirements.txt

node -v
```

## Run the dashboard

From the repo root:

```bash
cd /opt/xraymgr
source .venv/bin/activate

uvicorn xraymgr.web:app --app-dir app --host 0.0.0.0 --port 8000
```

Open:

* `http://<SERVER_IP>:8000/`

## Required data file

Collector sources file path:

* `/opt/xraymgr/data/sources/proxy_sources.txt`

Create it if missing:

```bash
mkdir -p /opt/xraymgr/data/sources
touch /opt/xraymgr/data/sources/proxy_sources.txt
```

## Troubleshooting

### “No links open” / bridge can’t convert

```bash
# Node must exist
node -v

# outbound.js must exist somewhere under webbundle
find /opt/xraymgr/app/xraymgr/webbundle -iname 'outbound.js' -print
```

```
::contentReference[oaicite:0]{index=0}
```
