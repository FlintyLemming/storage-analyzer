#!/bin/sh
# deb: $1 = configure, $2 = previous version (empty on first install)
# rpm: $1 = 1 (first install) or 2 (upgrade)
set -eu

if ! getent group storage-analyzer >/dev/null 2>&1; then
	groupadd --system storage-analyzer
fi
if ! getent passwd storage-analyzer >/dev/null 2>&1; then
	nologin=/sbin/nologin
	[ -x /usr/sbin/nologin ] && nologin=/usr/sbin/nologin
	useradd --system --no-create-home --home-dir /var/lib/storage-analyzer \
		--shell "$nologin" --gid storage-analyzer storage-analyzer
fi

if [ -f /etc/storage-analyzer/password ]; then
	chgrp storage-analyzer /etc/storage-analyzer/password
	chmod 0640 /etc/storage-analyzer/password
fi

first=
if [ "${1:-}" = configure ] && [ -z "${2:-}" ]; then first=1; fi
if [ "${1:-}" = 1 ]; then first=1; fi

if [ -d /run/systemd/system ]; then
	systemctl daemon-reload || true
	if [ -z "$first" ] && systemctl is-active --quiet storage-analyzer; then
		systemctl try-restart storage-analyzer
	fi
fi

if [ -n "$first" ]; then
	echo "Set the web password:  sudo storage-analyzer passwd"
	echo "Start the service:     sudo systemctl enable --now storage-analyzer"
fi
