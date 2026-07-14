#!/usr/bin/env bash
# Copy the per-platform native libraries downloaded as GitHub Actions artifacts
# into the layout the `include-native-artifacts` Maven profile expects
# (core/target/native-libs/io/questdb/client/bin/<platform>/), and fail if any
# expected library is missing or empty.
set -euo pipefail

downloaded="core/target/downloaded-native-artifacts"
staged="core/target/native-libs/io/questdb/client/bin"

# platform -> library filename
# darwin-x86-64 is intentionally not shipped: no CI runs the test suite on x64
# macOS, so the release does not build or bundle a binary it cannot test.
declare -A libs=(
  [darwin-aarch64]=libquestdb.dylib
  [linux-aarch64]=libquestdb.so
  [linux-x86-64]=libquestdb.so
  [windows-x86-64]=libquestdb.dll
)

for platform in "${!libs[@]}"; do
  lib="${libs[$platform]}"
  src="${downloaded}/native-${platform}/${lib}"
  dst_dir="${staged}/${platform}"

  if [[ ! -s "${src}" ]]; then
    echo "::error::Missing or empty native artifact: ${src}"
    exit 1
  fi

  mkdir -p "${dst_dir}"
  cp "${src}" "${dst_dir}/${lib}"
  echo "Staged ${platform}/${lib}"
done
