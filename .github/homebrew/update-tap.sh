#!/bin/sh
# Writes the tap's wherry formula for TAG from dist/checksums.txt into the tap checkout at TAP.
set -eu
TAG=$1 TAP=$2
sum() { awk -v f="wherry_${TAG}_$1.tar.gz" '$2 == f {print $1}' dist/checksums.txt; }
for p in darwin_arm64 darwin_amd64 linux_arm64 linux_amd64; do [ -n "$(sum $p)" ] || { echo "no checksum for $p"; exit 1; }; done
mkdir -p "$TAP/Formula"
sed -e "s/@TAG@/$TAG/g" -e "s/@DARWIN_ARM64@/$(sum darwin_arm64)/" -e "s/@DARWIN_AMD64@/$(sum darwin_amd64)/" \
  -e "s/@LINUX_ARM64@/$(sum linux_arm64)/" -e "s/@LINUX_AMD64@/$(sum linux_amd64)/" \
  .github/homebrew/wherry.rb > "$TAP/Formula/wherry.rb"
