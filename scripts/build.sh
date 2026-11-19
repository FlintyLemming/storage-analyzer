#!/bin/sh
# Build build/StorageAnalyzer_<ver>_<arch>.qpkg for every supported QTS
# architecture: x86_64, arm_64 (aarch64) and arm-x41 (32-bit ARMv7,
# Annapurna Labs Alpine).
set -eu
cd "$(dirname "$0")/.."
ROOT=$(pwd)
VER=$(sed -n 's/^QPKG_VER="\(.*\)"/\1/p' packaging/qpkg/qpkg.cfg)
STAGE="$ROOT/build/qpkg"

# QDK architecture, GOARCH, GOARM
TARGETS="x86_64 amd64 -
arm_64 arm64 -
arm-x41 arm 7"

echo "==> go test"
go test ./... >/dev/null

echo "==> linux tests (all architectures)"
./scripts/test-linux.sh

rm -rf "$STAGE"
mkdir -p "$STAGE/shared/web"
QB_ARGS=
echo "$TARGETS" | while read -r qarch goarch goarm; do
	echo "==> cross-compile linux/$goarch for $qarch"
	mkdir -p "$STAGE/$qarch/bin"
	[ "$goarm" = "-" ] && goarm=
	GOOS=linux GOARCH=$goarch GOARM=$goarm CGO_ENABLED=0 \
		go build -trimpath -ldflags="-s -w" -o "$STAGE/$qarch/bin/storage-analyzer" ./cmd/storage-analyzer
	chmod 755 "$STAGE/$qarch/bin/storage-analyzer"
done
for qarch in $(echo "$TARGETS" | cut -d' ' -f1); do
	QB_ARGS="$QB_ARGS --build-arch $qarch"
done

echo "==> stage package"
cp packaging/qpkg/qpkg.cfg packaging/qpkg/package_routines "$STAGE/"
cp -R packaging/qpkg/config "$STAGE/config"
cp packaging/qpkg/shared/StorageAnalyzer.sh "$STAGE/shared/"
cp packaging/qpkg/shared/web/api.cgi "$STAGE/shared/web/"
cp web/index.html web/i18n.js web/app.js web/style.css "$STAGE/shared/web/"
mkdir -p "$STAGE/icons"
go run ./tools/genicons "$STAGE/icons"
chmod 755 "$STAGE/shared/StorageAnalyzer.sh" "$STAGE/shared/web/api.cgi"
chmod 644 "$STAGE"/shared/web/*.html "$STAGE"/shared/web/*.js "$STAGE"/shared/web/*.css

echo "==> qbuild (docker)"
docker build -q -t storage-analyzer-qdk packaging >/dev/null
# shellcheck disable=SC2086 # QB_ARGS is a word list
docker run --rm -v "$STAGE:/work" storage-analyzer-qdk qbuild $QB_ARGS --build-dir /work/out
for qarch in $(echo "$TARGETS" | cut -d' ' -f1); do
	pkg="$STAGE/out/StorageAnalyzer_${VER}_${qarch}.qpkg"
	[ -f "$pkg" ] || { echo "missing $(basename "$pkg")" >&2; exit 1; }
	cp "$pkg" "$ROOT/build/"
	echo "==> $(ls -la "$ROOT/build/$(basename "$pkg")")"
done
