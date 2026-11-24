#!/usr/bin/env bash
# Creates the always-on simulation box on Hetzner Cloud and joins it to Tailscale.
# Run from a Mac or Linux machine with hcloud, gh and op signed in:
#
#   deploy/dst-box/provision.sh [name] [server-type] [location]
#
# Safe to rerun: existing pieces are reused and setup.sh is idempotent.
set -euo pipefail

name=${1:-wfe-dst}
type=${2:-cpx42}
location=${3:-hel1}
repo=0x63616c/workflow-engine
key="$HOME/.ssh/${name}_ed25519"
tailscale_oauth="op://homelab/Homelab Tailscale OAuth"
here=$(cd "$(dirname "$0")" && pwd)

if [[ ! -f $key ]]; then
  ssh-keygen -t ed25519 -N "" -C "$name" -f "$key"
fi
hcloud ssh-key describe "$name" >/dev/null 2>&1 || hcloud ssh-key create --name "$name" --public-key-from-file "$key.pub"

if ! hcloud firewall describe "$name" >/dev/null 2>&1; then
  hcloud firewall create --name "$name"
  hcloud firewall add-rule "$name" --direction in --protocol tcp --port 22 \
    --source-ips 0.0.0.0/0 --source-ips ::/0 --description ssh
  hcloud firewall add-rule "$name" --direction in --protocol udp --port 41641 \
    --source-ips 0.0.0.0/0 --source-ips ::/0 --description tailscale-direct
fi

if ! hcloud server describe "$name" >/dev/null 2>&1; then
  hcloud server create --name "$name" --type "$type" --location "$location" \
    --image ubuntu-24.04 --ssh-key "$name" --firewall "$name"
fi
ip=$(hcloud server ip "$name")
box() { ssh -i "$key" -o StrictHostKeyChecking=accept-new -o UserKnownHostsFile="$HOME/.ssh/known_hosts" "root@$ip" "$@"; }

until box true 2>/dev/null; do sleep 5; done

auth_key() {
  local id secret token
  id=$(op read "$tailscale_oauth/client-id")
  secret=$(op read "$tailscale_oauth/client-secret")
  token=$(curl -fsS -d "client_id=$id" -d "client_secret=$secret" https://api.tailscale.com/api/v2/oauth/token |
    python3 -c 'import json,sys; print(json.load(sys.stdin)["access_token"])')
  curl -fsS -H "Authorization: Bearer $token" -H 'Content-Type: application/json' \
    -d '{"capabilities":{"devices":{"create":{"reusable":false,"ephemeral":false,"preauthorized":true,"tags":["tag:homelab"]}}},"expirySeconds":900}' \
    https://api.tailscale.com/api/v2/tailnet/-/keys |
    python3 -c 'import json,sys; print(json.load(sys.stdin)["key"])'
}

box 'command -v tailscale >/dev/null || curl -fsSL https://tailscale.com/install.sh | sh'
if ! box 'tailscale status >/dev/null 2>&1'; then
  auth_key | box "tailscale up --hostname '$name' --auth-key \"\$(cat)\""
fi

box 'id wfe >/dev/null 2>&1 || useradd --create-home --shell /bin/bash wfe'
box 'sudo -u wfe bash -c "test -f ~/.ssh/id_ed25519 || ssh-keygen -q -t ed25519 -N \"\" -C wfe-dst -f ~/.ssh/id_ed25519"'
if ! gh repo deploy-key list --repo "$repo" | grep -q "$name"; then
  box 'cat ~wfe/.ssh/id_ed25519.pub' | gh repo deploy-key add - --repo "$repo" --title "$name"
fi

box 'bash -s' <"$here/setup.sh"
echo "UI: http://$(box 'tailscale ip -4'):7500 (or http://$name:7500 with MagicDNS)"
