#!/bin/sh
# Remove Storage Analyzer installed by install.sh. --purge also deletes config and data.
set -eu
if [ "$(id -u)" -ne 0 ]; then
	echo "uninstall.sh must be run as root" >&2
	exit 1
fi
purge=
[ "${1:-}" = --purge ] && purge=1

if [ -d /run/systemd/system ]; then
	systemctl disable --now storage-analyzer || true
fi
rm -f /usr/local/bin/storage-analyzer /etc/systemd/system/storage-analyzer.service
if [ -d /run/systemd/system ]; then
	systemctl daemon-reload
fi
if [ -n "$purge" ]; then
	rm -rf /etc/storage-analyzer /var/lib/storage-analyzer
fi
