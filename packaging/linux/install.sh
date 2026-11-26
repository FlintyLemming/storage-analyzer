#!/bin/sh
# Install (or upgrade) Storage Analyzer from the extracted tarball. Run as root.
set -eu
if [ "$(id -u)" -ne 0 ]; then
	echo "install.sh must be run as root" >&2
	exit 1
fi
cd "$(dirname "$0")"

if ! getent group storage-analyzer >/dev/null 2>&1; then
	groupadd --system storage-analyzer
fi
if ! getent passwd storage-analyzer >/dev/null 2>&1; then
	nologin=/sbin/nologin
	[ -x /usr/sbin/nologin ] && nologin=/usr/sbin/nologin
	useradd --system --no-create-home --home-dir /var/lib/storage-analyzer \
		--shell "$nologin" --gid storage-analyzer storage-analyzer
fi

install -d -m 0755 /usr/local/bin /etc/systemd/system /etc/storage-analyzer
install -m 0755 storage-analyzer /usr/local/bin/storage-analyzer
sed 's|/usr/bin/|/usr/local/bin/|' storage-analyzer.service > /etc/systemd/system/storage-analyzer.service
chmod 0644 /etc/systemd/system/storage-analyzer.service
if [ ! -e /etc/storage-analyzer/config ]; then
	install -m 0644 config /etc/storage-analyzer/config
fi
if [ -f /etc/storage-analyzer/password ]; then
	chgrp storage-analyzer /etc/storage-analyzer/password
	chmod 0640 /etc/storage-analyzer/password
fi

if [ -d /run/systemd/system ]; then
	systemctl daemon-reload || true
	if systemctl is-active --quiet storage-analyzer; then
		systemctl try-restart storage-analyzer
		echo "Upgraded and restarted storage-analyzer."
		exit 0
	fi
fi
if [ ! -f /etc/storage-analyzer/password ]; then
	echo "Set the web password:  sudo storage-analyzer passwd"
fi
echo "Start the service:     sudo systemctl enable --now storage-analyzer"
