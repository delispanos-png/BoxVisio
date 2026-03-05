#!/usr/bin/env bash
set -euo pipefail

ALLOW_SSH_FROM="${1:-194.154.34.84}"

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
sudo ufw status numbered
