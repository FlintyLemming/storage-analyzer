#!/bin/sh
# Build build/fnos/storage-analyzer_<ver>_<platform>.fpk for fnOS
# architectures: x86 (amd64) and arm (arm64).
#
# Usage:
#   ./scripts/build-fnos.sh            # build x86 and arm
#   ./scripts/build-fnos.sh x86        # build x86 only
#   SKIP_TESTS=1 ./scripts/build-fnos.sh
set -eu
cd "$(dirname "$0")/.."
ROOT=$(pwd)
VER=$(tr -d '[:space:]' < VERSION)
OUT="$ROOT/build/fnos"
FNPACK_VERSION=${FNPACK_VERSION:-1.3.3}

ensure_fnpack() {
	if [ -n "${FNPACK:-}" ] && [ -x "$FNPACK" ]; then
		return
	fi
	if command -v fnpack >/dev/null 2>&1; then
		FNPACK=$(command -v fnpack)
		return
	fi
	os=$(uname -s | tr '[:upper:]' '[:lower:]')
	arch=$(uname -m)
	case "$os" in
		linux|darwin) ;;
		mingw*|msys*|cygwin*) os=windows ;;
		*) echo "unsupported host OS for fnpack download: $os" >&2; exit 1 ;;
	esac
	case "$arch" in
		x86_64|amd64) arch=amd64 ;;
		aarch64|arm64)
			if [ "$os" = "linux" ]; then
				arch=arm
			else
				arch=arm64
			fi
			;;
		*) echo "unsupported host arch for fnpack download: $arch" >&2; exit 1 ;;
	esac
	mkdir -p "$ROOT/build/tools"
	FNPACK="$ROOT/build/tools/fnpack-${FNPACK_VERSION}-${os}-${arch}"
	[ "$os" = "windows" ] && FNPACK="${FNPACK}.exe"
	if [ ! -x "$FNPACK" ]; then
		url="https://static2.fnnas.com/fnpack/fnpack-${FNPACK_VERSION}-${os}-${arch}"
		echo "==> download $url"
		curl -fsSL --retry 3 --retry-delay 2 "$url" -o "$FNPACK"
		chmod 755 "$FNPACK"
	fi
}

if [ "${SKIP_TESTS:-0}" != "1" ]; then
	echo "==> go test -tags fnos"
	go test -tags fnos ./... >/dev/null
fi

ensure_fnpack
echo "==> using fnpack: $FNPACK"

TARGETS="x86 amd64
arm arm64"
if [ "$#" -gt 0 ]; then
	FILTER=" $* "
else
	FILTER=""
fi

mkdir -p "$OUT"
rm -rf "$OUT/stage" "$OUT/tmp"

echo "$TARGETS" | while read -r farch goarch; do
	if [ -n "$FILTER" ]; then
		case "$FILTER" in
			*" $farch "*) ;;
			*) continue ;;
		esac
	fi

	stage="$OUT/stage/$farch"
	tmpout="$OUT/tmp/$farch"
	rm -rf "$stage" "$tmpout"
	mkdir -p "$stage/app/bin" "$stage/app/ui/images" "$stage/cmd" "$stage/config" "$stage/wizard" "$tmpout"

	echo "==> cross-compile linux/$goarch (-tags fnos) for fnOS $farch"
	GOOS=linux GOARCH=$goarch CGO_ENABLED=0 \
		go build -tags fnos -trimpath -ldflags "-s -w -X main.version=$VER" \
		-o "$stage/app/bin/storage-analyzer" ./cmd/storage-analyzer
	chmod 755 "$stage/app/bin/storage-analyzer"

	echo "==> stage fnOS package ($farch)"
	sed -e "s/@VERSION@/$VER/g" -e "s/@PLATFORM@/$farch/g" packaging/fnos/manifest | tr -d '\r' > "$stage/manifest"
	tr -d '\r' < packaging/fnos/config/privilege > "$stage/config/privilege"
	tr -d '\r' < packaging/fnos/config/resource > "$stage/config/resource"
	tr -d '\r' < packaging/fnos/app/ui/config > "$stage/app/ui/config"
	tr -d '\r' < packaging/fnos/wizard/config > "$stage/wizard/config"
	tr -d '\r' < LICENSE > "$stage/LICENSE"

	for s in packaging/fnos/cmd/*; do
		name=$(basename "$s")
		tr -d '\r' < "$s" > "$stage/cmd/$name"
		chmod 755 "$stage/cmd/$name"
	done

	go run ./tools/genicons -fnos "$stage"

	echo "==> fnpack build ($farch)"
	if [ -n "${FNPACK_KEY:-}" ] && [ -n "${FNPACK_CERT:-}" ]; then
		(cd "$tmpout" && "$FNPACK" build --directory "$stage" --key "$FNPACK_KEY" --cert "$FNPACK_CERT")
	elif "$FNPACK" build --help 2>&1 | grep -q -- '--nosign'; then
		(cd "$tmpout" && "$FNPACK" build --directory "$stage" --nosign)
	else
		(cd "$tmpout" && "$FNPACK" build --directory "$stage")
	fi
	fpk=$(find "$tmpout" -maxdepth 1 -name '*.fpk' | head -n 1)
	[ -n "$fpk" ] && [ -f "$fpk" ] || { echo "fnpack produced no .fpk in $tmpout" >&2; exit 1; }

	dest="$OUT/storage-analyzer_${VER}_fnos_${farch}.fpk"
	mv -f "$fpk" "$dest"
	echo "==> $(ls -la "$dest")"
done

rm -rf "$OUT/stage" "$OUT/tmp"
ls -la "$OUT"
