#!/bin/bash
# Example EC2 user data for an instance that runs the proxy with its cache on
# EFS (Elastic Throughput).
# Assumes:
#   - amazon-efs-utils is installed, or dnf can install it (Amazon Linux 2023 packages it)
#   - The binary has been uploaded to s3://YOUR-BUCKET/s3-proxy
#   - The instance profile grants s3:GetObject on that object
#   - The EFS file system ID and the configuration path are filled in below
set -euo pipefail
exec > /tmp/userdata.log 2>&1

EFS_ID="fs-EXAMPLE"
CONFIG_PATH="/mnt/efs/config/config.yaml"
BINARY_S3="s3://YOUR-BUCKET/s3-proxy"
REGION="us-west-2"

# --- Install the EFS mount helper if it is missing ---
command -v mount.efs >/dev/null 2>&1 || dnf install -y amazon-efs-utils

# --- Mount EFS ---
# The 1,500 MiBps per-client throughput cap needs both Elastic Throughput and
# the EFS mount helper. Without either, the cap is 500 MiBps.
# lookupcache=pos is required when more than one proxy shares the cache volume.
# This mount has no nconnect option on purpose: the EFS client provides the
# connection parallelism itself. The FSx mount in userdata-fsxz.sh does need
# nconnect, so do not copy mount options between the two. See docs/SHARED_STORAGE.md.
# Security: the tls option encrypts traffic to EFS in transit. Encryption at
# rest is set when the file system is created: the console enables it by
# default, and the AWS CLI needs --encrypted.
mkdir -p /mnt/efs
echo "${EFS_ID}.efs.${REGION}.amazonaws.com:/ /mnt/efs efs defaults,_netdev,tls,lookupcache=pos 0 0" >> /etc/fstab
mount /mnt/efs
df -h /mnt/efs | grep -q efs || { echo "FATAL: EFS mount failed"; exit 1; }

# --- Fetch the binary from S3 ---
aws s3 cp "${BINARY_S3}" /usr/local/bin/s3-proxy --region "${REGION}"
chmod +x /usr/local/bin/s3-proxy

# --- Install the systemd unit ---
# The heredoc is unquoted so that ${CONFIG_PATH} expands into ExecStart.
cat > /etc/systemd/system/s3-proxy.service << UNIT
[Unit]
Description=S3 Proxy Service
After=network.target

[Service]
Type=simple
ExecStart=/usr/local/bin/s3-proxy -c ${CONFIG_PATH}
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
StateDirectory=s3-proxy
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
