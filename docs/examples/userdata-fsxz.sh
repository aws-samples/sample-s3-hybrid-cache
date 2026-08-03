#!/bin/bash
# Example userdata for a proxy instance with cache on FSx for OpenZFS
# and configuration/logs on EFS (the recommended hybrid layout).
# Assumes:
#   - amazon-efs-utils is pre-installed in the AMI (AL2023 includes it via dnf)
#   - The binary has been uploaded to s3://YOUR-BUCKET/s3-proxy
#   - The instance profile grants s3:GetObject on that prefix
#   - FSx and EFS file system IDs are filled in below
#   - Same VPC and subnet as both file systems
set -euo pipefail
exec > /tmp/userdata.log 2>&1

FSX_DNS="fs-EXAMPLE.fsx.us-west-2.amazonaws.com"
EFS_ID="fs-EXAMPLE"
BINARY_S3="s3://YOUR-BUCKET/s3-proxy"
REGION="us-west-2"

# --- Install EFS helper if not present ---
command -v mount.efs >/dev/null 2>&1 || dnf install -y amazon-efs-utils

# --- Mount EFS (config and logs) ---
# Both Elastic Throughput AND the efs mount helper are required for the
# 1,500 MiBps per-client cap. lookupcache=pos is REQUIRED for multi-instance
# cache coordination — even on the config/log volume, since cache_rules.json
# lives here and is read by all instances.
# Security: 'tls' enables encryption in transit. EFS also encrypts at rest
# automatically when the file system is created with encryption enabled (default
# in the console; specify --encrypted with the CLI).
mkdir -p /mnt/efs
echo "${EFS_ID}.efs.${REGION}.amazonaws.com:/ /mnt/efs efs defaults,_netdev,tls,lookupcache=pos 0 0" >> /etc/fstab
mount /mnt/efs
df -h /mnt/efs | grep -q efs || { echo "FATAL: EFS mount failed"; exit 1; }

# --- Mount FSx for OpenZFS (cache) ---
# lookupcache=pos is REQUIRED for multi-instance cache coordination.
# Do NOT add noresvport — FSx rejects it (EFS requires it; FSx does not).
# Security: FSx for OpenZFS encrypts data at rest (KMS) and in transit
# automatically for supported EC2 instance types — no mount flag needed.
mkdir -p /mnt/fsx
echo "${FSX_DNS}:/fsx/ /mnt/fsx nfs4 nfsvers=4.1,rsize=1048576,wsize=1048576,hard,timeo=600,retrans=2,lookupcache=pos,_netdev 0 0" >> /etc/fstab
mount /mnt/fsx
df -h /mnt/fsx | grep -q fsx || { echo "FATAL: FSx mount failed"; exit 1; }

# --- Fetch binary from S3 ---
aws s3 cp "${BINARY_S3}" /usr/local/bin/s3-proxy --region "${REGION}"
chmod +x /usr/local/bin/s3-proxy

# --- Install systemd unit ---
# Config on EFS, cache on FSx — ReadWritePaths must include both.
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
ReadWritePaths=/mnt/fsx /mnt/efs
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
