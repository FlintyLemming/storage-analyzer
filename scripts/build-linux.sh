#!/bin/sh
# Build the generic Linux release artifacts into build/linux/:
#   storage-analyzer_<ver>_linux_{amd64,arm64,armv7}.tar.gz
#   storage-analyzer_<ver>_{amd64,arm64,armhf}.deb
#   storage-analyzer-<ver>-1.{x86_64,aarch64,armv7hl}.rpm
# nfpm runs from its Docker image (goreleaser/nfpm) so no Go module download
# of nfpm is needed.
set -eu
cd "$(dirname "$0")/.."
ROOT=$(pwd)
VER=$(tr -d '[:space:]' < VERSION)
NFPM_IMAGE=${NFPM_IMAGE:-goreleaser/nfpm:v2.49.0}
OUT="$ROOT/build/linux"

# GOARCH GOARM tar-arch nfpm-arch deb-arch rpm-arch
TARGETS="amd64 - amd64 amd64 amd64 x86_64
arm64 - arm64 arm64 arm64 aarch64
arm 7 armv7 arm7 armhf armv7hl"

echo "==> go test"
go test ./... >/dev/null

rm -rf "$OUT"
mkdir -p "$OUT"
echo "$TARGETS" | while read -r goarch goarm tarch narch darch rarch; do
	[ "$goarm" = "-" ] && goarm=
	name="storage-analyzer_${VER}_linux_${tarch}"
	dir="$OUT/stage/$name"
	mkdir -p "$dir"
	echo "==> build linux/$goarch"
	GOOS=linux GOARCH=$goarch GOARM=$goarm CGO_ENABLED=0 \
		go build -trimpath -ldflags "-s -w -X main.version=$VER" -o "$dir/storage-analyzer" ./cmd/storage-analyzer
	chmod 755 "$dir/storage-analyzer"
	cp packaging/linux/storage-analyzer.service packaging/linux/config \
		packaging/linux/install.sh packaging/linux/uninstall.sh \
		packaging/linux/README.md LICENSE "$dir/"
	chmod 755 "$dir/install.sh" "$dir/uninstall.sh"
	COPYFILE_DISABLE=1 tar --uid 0 --gid 0 --numeric-owner -C "$OUT/stage" -czf "$OUT/$name.tar.gz" "$name"

	for pkgr in deb rpm; do
		case $pkgr in
		deb) file="storage-analyzer_${VER}_${darch}.deb" ;;
		rpm) file="storage-analyzer-${VER}-1.${rarch}.rpm" ;;
		esac
		echo "==> $file"
		# nfpm does not expand env vars in contents.src, so the binary goes
		# to a fixed path.
		mkdir -p "$OUT/nfpm-bin"
		cp "$dir/storage-analyzer" "$OUT/nfpm-bin/storage-analyzer"
		docker run --rm -i --user "$(id -u):$(id -g)" -v "$ROOT:/src" -w /src \
			-e VERSION="$VER" -e ARCH="$narch" \
			"$NFPM_IMAGE" package --config packaging/linux/nfpm.yaml \
			--packager $pkgr --target "build/linux/$file" </dev/null
	done
done
rm -rf "$OUT/stage" "$OUT/nfpm-bin"
ls -la "$OUT"
