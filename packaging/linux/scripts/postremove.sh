#!/bin/sh
# deb: $1 = purge deletes config and data; rpm never deletes data.
set -eu
if [ "${1:-}" = purge ]; then
	rm -rf /etc/storage-analyzer /var/lib/storage-analyzer
fi
if [ -d /run/systemd/system ]; then
	systemctl daemon-reload || true
fi
