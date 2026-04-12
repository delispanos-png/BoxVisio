#!/usr/bin/env bash
set -euo pipefail

ALLOW_SSH_FROM="${1:-194.154.34.84}"
HTTPS_MSS="${2:-1360}"

sudo ufw --force reset
sudo ufw default deny incoming
sudo ufw default allow outgoing

sudo ufw allow 80/tcp
sudo ufw allow 443/tcp
sudo ufw allow from "$ALLOW_SSH_FROM" to any port 22 proto tcp

# Defensive explicit denies for data services
sudo ufw deny 5432/tcp
sudo ufw deny 6379/tcp

sudo ufw --force enable

# Mitigate intermittent TLS resets on paths with lower MTU by clamping
# MSS on outbound HTTPS SYN/SYN-ACK packets.
sudo iptables -t mangle -C POSTROUTING -p tcp --sport 443 --tcp-flags SYN,RST SYN -j TCPMSS --set-mss "$HTTPS_MSS" 2>/dev/null \
  || sudo iptables -t mangle -A POSTROUTING -p tcp --sport 443 --tcp-flags SYN,RST SYN -j TCPMSS --set-mss "$HTTPS_MSS"

# Persist the MSS clamp across reboot/UFW reloads.
if ! sudo grep -q "cloudon-mss-fix begin" /etc/ufw/before.rules; then
  sudo tee -a /etc/ufw/before.rules >/dev/null <<EOF

# cloudon-mss-fix begin
*mangle
:POSTROUTING ACCEPT [0:0]
-A POSTROUTING -p tcp --sport 443 --tcp-flags SYN,RST SYN -j TCPMSS --set-mss ${HTTPS_MSS}
COMMIT
# cloudon-mss-fix end
EOF
  sudo ufw reload
fi

sudo ufw status numbered
