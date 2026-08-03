#!/bin/bash
# Example userdata for a proxy instance backed by EFS (Elastic Throughput).
# Assumes:
#   - amazon-efs-utils is pre-installed in the AMI (AL2023 includes it via dnf)
#   - The binary has been uploaded to s3://YOUR-BUCKET/s3-proxy
#   - The instance profile grants s3:GetObject on that prefix
#   - EFS file system ID and config path are filled in below
set -euo pipefail
exec > /tmp/userdata.log 2>&1

EFS_ID="fs-EXAMPLE"
CONFIG_PATH="/mnt/efs/config/config.yaml"
BINARY_S3="s3://YOUR-BUCKET/s3-proxy"
REGION="us-west-2"

# --- Install EFS helper if not present ---
command -v mount.efs >/dev/null 2>&1 || dnf install -y amazon-efs-utils

# --- Mount EFS ---
# Both Elastic Throughput AND the efs mount helper are required for the
# 1,500 MiBps per-client cap. Without either, the cap is 500 MiBps.
# lookupcache=pos is REQUIRED for multi-instance cache coordination.
# Security: 'tls' enables encryption in transit. EFS also encrypts at rest
# automatically when the file system is created with encryption enabled (default
# in the console; specify --encrypted with the CLI).
mkdir -p /mnt/efs
echo "${EFS_ID}.efs.${REGION}.amazonaws.com:/ /mnt/efs efs defaults,_netdev,tls,lookupcache=pos 0 0" >> /etc/fstab
mount /mnt/efs
df -h /mnt/efs | grep -q efs || { echo "FATAL: EFS mount failed"; exit 1; }

# --- Fetch binary from S3 ---
aws s3 cp "${BINARY_S3}" /usr/local/bin/s3-proxy --region "${REGION}"
chmod +x /usr/local/bin/s3-proxy

# --- Install systemd unit ---
cat > /etc/systemd/system/s3-proxy.service << 'UNIT'
[Unit]
Description=S3 Proxy Service
After=network.target

[Service]
Type=simple
ExecStart=/usr/local/bin/s3-proxy -c /mnt/efs/config/config.yaml
Restart=on-failure
RestartSec=5
User=root
NoNewPrivileges=true
ProtectSystem=strict
ProtectHome=true
PrivateTmp=true
PrivateDevices=true
ProtectKernelTunables=true
ProtectKernelModules=true
ProtectControlGroups=true
RestrictAddressFamilies=AF_INET AF_INET6 AF_UNIX
ReadWritePaths=/mnt/efs
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
UNIT

# --- Start ---
systemctl daemon-reload
systemctl enable s3-proxy
systemctl start s3-proxy

# --- Verify ---
sleep 3
curl -sf http://localhost:8080/health || { echo "WARN: health check failed after start"; }
echo "Bootstrap complete: $(/usr/local/bin/s3-proxy --version)"
