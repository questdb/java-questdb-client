#!/usr/bin/env bash
# Assert the glibc runtime floor of a Linux native library.
#
# Usage: check-glibc-floor.sh <path-to-libquestdb.so> <max-glibc-version>
#   e.g. check-glibc-floor.sh core/.../linux-x86-64/libquestdb.so 2.14
#        check-glibc-floor.sh core/.../linux-aarch64/libquestdb.so 2.17
#
# The dynamic linker resolves .gnu.version_r at load time, so the HIGHEST
# GLIBC_x.y version node the library imports is its hard load floor: a host
# whose glibc is older than that node fails System.loadLibrary/dlopen with
# `version 'GLIBC_x.y' not found`. This script extracts every versioned import
# and fails if the highest one exceeds the allowed floor.
#
# Why the floors are what they are:
#   * linux-x86-64 -> 2.14. The oldest node we intentionally keep is
#     memcpy@GLIBC_2.14; clock_gettime is pinned back to GLIBC_2.2.5 by
#     src/main/c/share/glibc_compat.h, and stat/fstat resolve to the inline
#     __xstat/__fxstat@GLIBC_2.2.5 wrappers when built in a low-glibc container.
#     A build on a modern host (glibc >= 2.33) instead emits stat@GLIBC_2.33 /
#     fstat@GLIBC_2.33 and trips this guard -- that is exactly the regression it
#     exists to catch.
#   * linux-aarch64 -> 2.17. glibc gained aarch64 support in 2.17, so 2.17 is
#     the lowest floor physically achievable on that architecture.
#
# Portable to bash 3.2 (no mapfile / no negative array indices) so it can be run
# locally on macOS as well as in the glibc build containers.
set -euo pipefail

lib="${1:?usage: check-glibc-floor.sh <lib.so> <max-glibc-version>}"
floor="${2:?usage: check-glibc-floor.sh <lib.so> <max-glibc-version>}"

if [ ! -f "$lib" ]; then
  echo "::error::check-glibc-floor: library not found: $lib"
  exit 1
fi

# All distinct versioned GLIBC nodes (e.g. 2.14, 2.2.5), sorted ascending.
# objdump prints them as (GLIBC_x.y) or GLIBC_x.y depending on the toolchain;
# the -o regex captures the token regardless of surrounding parentheses.
# GLIBC_PRIVATE has no digit after the underscore, so it is naturally excluded.
versions="$(
  objdump -T "$lib" \
    | grep -oE 'GLIBC_[0-9]+(\.[0-9]+)+' \
    | sed 's/^GLIBC_//' \
    | sort -Vu
)"

if [ -z "$versions" ]; then
  echo "::error::check-glibc-floor: no versioned GLIBC symbols found in $lib (unexpected)."
  exit 1
fi

highest="$(printf '%s\n' "$versions" | tail -n1)"

echo "GLIBC version nodes required by $lib:"
printf '%s\n' "$versions" | sed 's/^/  GLIBC_/'
echo "Highest required: GLIBC_${highest} (allowed floor: GLIBC_${floor})"

# leq A B -> succeeds when version A <= version B: sorting {A, B} with -V puts B
# last, or they are equal.
leq() {
  [ "$1" = "$2" ] && return 0
  [ "$(printf '%s\n%s\n' "$1" "$2" | sort -V | tail -n1)" = "$2" ]
}

if leq "$highest" "$floor"; then
  echo "OK: $lib floor is GLIBC_${highest} (<= GLIBC_${floor})."
  exit 0
fi

echo "::error::GLIBC floor regression in $lib: requires GLIBC_${highest}, above the GLIBC_${floor} floor."
echo "::error::This library will fail to load on hosts with glibc < ${highest}."
echo "Offending nodes above the floor and the symbols that pull them in:"
printf '%s\n' "$versions" | while IFS= read -r v; do
  if ! leq "$v" "$floor"; then
    echo "  GLIBC_${v}:"
    objdump -T "$lib" | grep -E "GLIBC_${v//./\\.}([^0-9]|\$)" | awk '{print "    " $NF}' | sort -u
  fi
done
exit 1
