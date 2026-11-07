#!/usr/bin/env bash
# Build LIST_TRANSPORT_SEGMENTS.parquet from the per-municipality LIST zips.
# Requires GDAL >= 3.11 with Parquet + OpenFileGDB drivers.
# Usage: scripts/build.sh [workdir] [output]
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
work="${1:-work}"
out="${2:-LIST_TRANSPORT_SEGMENTS.parquet}"
base="https://listdata.thelist.tas.gov.au/opendata/data"
mkdir -p "$work"
export GDAL_HTTP_MAX_RETRY="${GDAL_HTTP_MAX_RETRY:-5}" GDAL_HTTP_RETRY_DELAY="${GDAL_HTTP_RETRY_DELAY:-10}"

parts=()
while read -r m; do
  [ -z "$m" ] && continue
  lc="$(printf '%s' "$m" | tr '[:upper:]' '[:lower:]')"
  zip="$work/${m}.zip"
  part="$work/${m}.parquet"
  echo "== $m" >&2
  rm -f "$zip"
  gdal vsi copy "/vsicurl/$base/LIST_TRANSPORT_SEGMENTS_${m}.zip" "$zip"
  # add a municipality column
  gdal vector sql --overwrite --of Parquet \
    --sql "SELECT *, '${m}' AS municipality FROM list_transport_segments_${lc}" \
    "/vsizip/${zip}/list_transport_segments_${lc}.gdb" "$part"
  rm -f "$zip"
  parts+=("$part")
done < "$here/municipalities.txt"

rm -f "$out"
gdal vector concat --mode single --of Parquet \
  --lco COMPRESSION=ZSTD --lco GEOMETRY_NAME=geometry \
  "${parts[@]}" "$out"
rm -f "${parts[@]}"

# sanity check: every municipality present, nothing empty
layer="$(basename "$out" .parquet)"
expected="$(grep -c . "$here/municipalities.txt")"
got="$(gdal vector info --features --dialect SQLITE \
  --sql "SELECT count(DISTINCT municipality) AS n FROM \"$layer\"" "$out" | awk -F' = ' '/n \(Integer\)/ {print $2}')"
echo "municipalities: $got / $expected" >&2
[ "$got" = "$expected" ] || { echo "municipality count mismatch" >&2; exit 1; }
gdal vector info "$out" | grep -E 'Feature Count|Geometry' >&2
