#!/bin/sh
# deb: $1 = remove | upgrade | ...; rpm: $1 = 0 on real removal
set -eu
if [ "${1:-}" = remove ] || [ "${1:-}" = 0 ]; then
	if [ -d /run/systemd/system ]; then
		systemctl disable --now storage-analyzer || true
	fi
fi
