#!/bin/bash
# Example EC2 user data for an instance that runs the proxy with its cache on
# FSx for OpenZFS and its configuration and logs on EFS (the recommended
# hybrid layout).
# Assumes:
#   - amazon-efs-utils is installed, or dnf can install it (Amazon Linux 2023 packages it)
#   - The binary has been uploaded to s3://YOUR-BUCKET/s3-proxy
#   - The instance profile grants s3:GetObject on that object
#   - The FSx and EFS file system IDs are filled in below
#   - The instance is in the same VPC and subnet as both file systems
set -euo pipefail
exec > /tmp/userdata.log 2>&1

FSX_DNS="fs-EXAMPLE.fsx.us-west-2.amazonaws.com"
EFS_ID="fs-EXAMPLE"
BINARY_S3="s3://YOUR-BUCKET/s3-proxy"
REGION="us-west-2"

# --- Install the EFS mount helper if it is missing ---
command -v mount.efs >/dev/null 2>&1 || dnf install -y amazon-efs-utils

# --- Mount EFS (configuration and logs) ---
# The 1,500 MiBps per-client throughput cap needs both Elastic Throughput and
# the EFS mount helper. lookupcache=pos matches the cache mount below, so a
# file another host creates here is visible at once. cache_rules.json goes in
# cache_dir on the FSx mount below, because the proxy reads it from cache_dir.
# Security: the tls option encrypts traffic to EFS in transit. Encryption at
# rest is set when the file system is created: the console enables it by
# default, and the AWS CLI needs --encrypted.
mkdir -p /mnt/efs
echo "${EFS_ID}.efs.${REGION}.amazonaws.com:/ /mnt/efs efs defaults,_netdev,tls,lookupcache=pos 0 0" >> /etc/fstab
mount /mnt/efs
df -h /mnt/efs | grep -q efs || { echo "FATAL: EFS mount failed"; exit 1; }

# --- Mount FSx for OpenZFS (cache) ---
# lookupcache=pos is required when more than one proxy shares the cache volume.
# nconnect=16 is required for throughput. Without it the mount uses one TCP
# connection, which hits the EC2 5 Gbps single-flow limit and caps disk cache
# reads near 625 MB/s per proxy, whatever throughput the file system has
# provisioned. EFS needs no equivalent option, because its client provides the
# same parallelism itself. See docs/SHARED_STORAGE.md.
# Do not add noresvport here: FSx rejects it, although EFS mounts use it.
# Security: FSx for OpenZFS encrypts data at rest with AWS KMS, and encrypts it
# in transit for supported EC2 instance types with no mount option.
mkdir -p /mnt/fsx
echo "${FSX_DNS}:/fsx/ /mnt/fsx nfs4 nfsvers=4.1,rsize=1048576,wsize=1048576,hard,timeo=600,retrans=2,lookupcache=pos,nconnect=16,_netdev 0 0" >> /etc/fstab
mount /mnt/fsx
df -h /mnt/fsx | grep -q fsx || { echo "FATAL: FSx mount failed"; exit 1; }

# --- Fetch the binary from S3 ---
aws s3 cp "${BINARY_S3}" /usr/local/bin/s3-proxy --region "${REGION}"
chmod +x /usr/local/bin/s3-proxy

# --- Install the systemd unit ---
# ReadWritePaths must include both mounts: the configuration and logs are on
# EFS, and the cache is on FSx.
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
