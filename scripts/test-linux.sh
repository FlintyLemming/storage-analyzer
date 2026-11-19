#!/bin/sh
# Run the Go test suite as Linux binaries for every QPKG architecture, inside
# a container. Architectures other than the host's run under Docker Desktop's
# emulation. The *_linux.go code is only exercised here, not by `go test` on
# macOS.
#
#   scripts/test-linux.sh              amd64 arm64 arm (arm = GOARM 7)
#   scripts/test-linux.sh arm          only the listed GOARCH values
set -eu
cd "$(dirname "$0")/.."
ROOT=$(pwd)
IMAGE=${IMAGE:-ubuntu:24.04}
ARCHES=${*:-amd64 arm64 arm}
OUT="$ROOT/build/test-linux"
PKGS=$(GOOS=linux go list -f '{{if or .TestGoFiles .XTestGoFiles}}{{.ImportPath}}{{end}}' ./...)
MOD=$(go list -m)
HOSTARCH=$(docker version --format '{{.Server.Arch}}')

for arch in $ARCHES; do
	echo "==> linux/$arch: build tests"
	rm -rf "$OUT/$arch"
	mkdir -p "$OUT/$arch"
	run="$OUT/$arch/run.sh"
	echo 'set -u; fail=0' > "$run"
	for pkg in $PKGS; do
		rel=${pkg#"$MOD"/}
		name=$(echo "$rel" | tr / _)
		GOOS=linux GOARCH=$arch GOARM=7 CGO_ENABLED=0 go test -c -o "$OUT/$arch/$name.test" "./$rel"
		# Run from the package directory so testdata/ resolves.
		echo "cd /src/$rel && /t/$name.test -test.count=1 >/t/$name.log 2>&1 && echo 'ok   $rel' || { echo 'FAIL $rel'; tail -20 /t/$name.log; fail=1; }" >> "$run"
	done
	echo 'exit $fail' >> "$run"
	emu=
	[ "$arch" = "$HOSTARCH" ] || emu=1
	echo "==> linux/$arch: run${emu:+ (emulated)}"
	docker run --rm -e SA_EMULATED="$emu" -v "$ROOT:/src:ro" -v "$OUT/$arch:/t" "$IMAGE" sh /t/run.sh || {
		echo "linux/$arch tests FAILED" >&2
		exit 1
	}
done
rm -rf "$OUT"
