#!/usr/bin/env bash
# Installs the simulation fuzzer, the management UI and a self-updater on an
# Ubuntu box as root. Idempotent; provision.sh runs it over SSH.
#
#   wfe-dst.service     wfe dst run on every core, status on 127.0.0.1:7600
#   wfe-ui.service      wfe ui on the Tailscale address, port 7500
#   wfe-update.timer    every 5 minutes: pull main, rebuild, restart both
set -euo pipefail

repo=git@github.com:0x63616c/workflow-engine.git
home=/home/wfe
src=$home/workflow-engine
bin=$src/target/sim/wfe

export DEBIAN_FRONTEND=noninteractive
apt-get update -q
apt-get install -yq build-essential cmake pkg-config git curl ca-certificates

id wfe >/dev/null 2>&1 || useradd --create-home --shell /bin/bash wfe
install -d -o wfe -g wfe "$home/findings"

as_wfe() { sudo -u wfe -H bash -lc "$1"; }
as_wfe 'command -v rustup >/dev/null || curl -fsSL https://sh.rustup.rs | sh -s -- -y --profile minimal --default-toolchain none'
as_wfe 'mkdir -p ~/.ssh && grep -q github.com ~/.ssh/known_hosts 2>/dev/null || ssh-keyscan github.com >> ~/.ssh/known_hosts'
as_wfe "test -d $src/.git || git clone $repo $src"

cat >/usr/local/bin/wfe-update <<EOF
#!/usr/bin/env bash
# Pulls main; when it moved (or nothing is built yet), rebuilds and restarts.
# A failed build leaves the running binary alone.
set -euo pipefail
cd $src
sudo -u wfe -H git fetch -q origin main
if [[ -x $bin && \$(sudo -u wfe git rev-parse HEAD) == \$(sudo -u wfe git rev-parse origin/main) ]]; then
  exit 0
fi
sudo -u wfe -H git reset -q --hard origin/main
sudo -u wfe -H bash -lc 'cd $src && rustup toolchain install && cargo build -q --locked --profile sim -p wfe-cli'
systemctl restart wfe-dst wfe-ui
EOF
chmod 755 /usr/local/bin/wfe-update

cat >/etc/systemd/system/wfe-dst.service <<EOF
[Unit]
Description=WFE simulation fuzzer
After=network-online.target

[Service]
User=wfe
WorkingDirectory=$src
Environment=WFE_FINDINGS=$home/findings/dst.jsonl
ExecStart=$bin dst run --ticks 12000 --serve 127.0.0.1:7600 --progress 0
Nice=10
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
EOF

cat >/etc/systemd/system/wfe-ui.service <<EOF
[Unit]
Description=WFE management UI (Tailscale only)
After=tailscaled.service wfe-dst.service
Wants=tailscaled.service

[Service]
User=wfe
Environment=WFE_FINDINGS=$home/findings/dst.jsonl
ExecStart=/bin/sh -c 'exec $bin ui --listen "\$(tailscale ip -4 | head -n1):7500" --dst http://127.0.0.1:7600'
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
EOF

cat >/etc/systemd/system/wfe-update.service <<EOF
[Unit]
Description=Pull, rebuild and restart WFE when main moves

[Service]
Type=oneshot
ExecStart=/usr/local/bin/wfe-update
EOF

cat >/etc/systemd/system/wfe-update.timer <<EOF
[Unit]
Description=Check for new WFE commits every 5 minutes

[Timer]
OnBootSec=1min
OnUnitInactiveSec=5min

[Install]
WantedBy=timers.target
EOF

systemctl daemon-reload
/usr/local/bin/wfe-update
systemctl enable --now wfe-dst wfe-ui wfe-update.timer
