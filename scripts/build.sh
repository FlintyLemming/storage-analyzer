#!/bin/sh
# Build build/StorageAnalyzer_<ver>_x86_64.qpkg
set -eu
cd "$(dirname "$0")/.."
ROOT=$(pwd)
VER=$(sed -n 's/^QPKG_VER="\(.*\)"/\1/p' packaging/qpkg/qpkg.cfg)
STAGE="$ROOT/build/qpkg"

echo "==> go test"
go test ./... >/dev/null

echo "==> cross-compile linux/amd64"
rm -rf "$STAGE"
mkdir -p "$STAGE/x86_64/bin" "$STAGE/shared/web"
GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o "$STAGE/x86_64/bin/storage-analyzer" ./cmd/storage-analyzer

echo "==> stage package"
cp packaging/qpkg/qpkg.cfg packaging/qpkg/package_routines "$STAGE/"
cp -R packaging/qpkg/config "$STAGE/config"
cp packaging/qpkg/shared/StorageAnalyzer.sh "$STAGE/shared/"
cp packaging/qpkg/shared/web/api.cgi "$STAGE/shared/web/"
cp web/index.html web/app.js web/style.css "$STAGE/shared/web/"
mkdir -p "$STAGE/icons"
go run ./tools/genicons "$STAGE/icons"
chmod 755 "$STAGE/shared/StorageAnalyzer.sh" "$STAGE/shared/web/api.cgi" "$STAGE/x86_64/bin/storage-analyzer"
chmod 644 "$STAGE"/shared/web/*.html "$STAGE"/shared/web/*.js "$STAGE"/shared/web/*.css

echo "==> qbuild (docker)"
docker build -q -t storage-analyzer-qdk packaging >/dev/null
docker run --rm -v "$STAGE:/work" storage-analyzer-qdk qbuild --build-arch x86_64 --build-dir /work/out
OUT=$(ls "$STAGE"/out/*.qpkg | head -1)
cp "$OUT" "$ROOT/build/"
echo "==> $(ls -la "$ROOT/build/$(basename "$OUT")")"
