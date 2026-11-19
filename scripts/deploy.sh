#!/bin/sh
# Install the built QPKG on the test NAS (needs passwordless sudo there).
# qpkg_cli refuses unsigned packages, so the self-extracting package is run
# directly as root, which is what App Center's manual install does after the
# "allow apps without a valid signature" check.
set -eu
cd "$(dirname "$0")/.."
HOST=${HOST:-hf-qts}
ARCH=${ARCH:-x86_64} # x86_64 | arm_64 | arm-x41, must match the NAS
PKG=$(ls -t build/StorageAnalyzer_*_"$ARCH".qpkg | head -1)
NAME=$(basename "$PKG")
STAGE=/share/CACHEDEV2_DATA/.sa-deploy
ssh "$HOST" "mkdir -p $STAGE"
scp -q "$PKG" "$HOST:$STAGE/$NAME"
ssh "$HOST" "cd $STAGE && sudo sh ./$NAME 2>&1 | tail -5; rm -rf $STAGE; \
  echo; for k in Version Enable Install_Path Shell WebUI Desktop Win_Width Win_Height; do printf '%s = ' \$k; /sbin/getcfg StorageAnalyzer \$k -f /etc/config/qpkg.conf; done"
