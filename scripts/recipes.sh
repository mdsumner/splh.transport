#!/usr/bin/env bash
# Write two text "recipes" that describe the statewide layer directly over the
# 29 remote LIST zips -- no download, no cache:
#   recipes/LIST_TRANSPORT_SEGMENTS.gdalg.json  GDAL streamed-algorithm (pipeline) file
#   recipes/LIST_TRANSPORT_SEGMENTS.vrt         classic OGR VRT union layer
# The GDALG is written by GDAL itself (needs GDAL >= 3.11 to write and read);
# the VRT is templated here because GDAL has no vector VRT writer.
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
out="$here/../recipes"
base="https://listdata.thelist.tas.gov.au/opendata/data"
mkdir -p "$out"

src() { local lc; lc="$(printf '%s' "$1" | tr '[:upper:]' '[:lower:]')"
  printf '/vsizip//vsicurl/%s/LIST_TRANSPORT_SEGMENTS_%s.zip/list_transport_segments_%s.gdb' "$base" "$1" "$lc"; }

mapfile -t munips < <(grep . "$here/municipalities.txt")
inputs=(); for m in "${munips[@]}"; do inputs+=("$(src "$m")"); done

# layer names are list_transport_segments_<name>; substr(...,25) strips the prefix
gdal vector pipeline \
  ! concat --mode single --output-layer LIST_TRANSPORT_SEGMENTS \
      --source-layer-field-name src_layer --source-layer-field-content '{LAYER_NAME}' \
      "${inputs[@]}" \
  ! sql --dialect SQLITE --output-layer LIST_TRANSPORT_SEGMENTS \
      "SELECT *, upper(substr(src_layer, 25)) AS municipality FROM LIST_TRANSPORT_SEGMENTS" \
  ! select --exclude src_layer \
  ! write "$out/LIST_TRANSPORT_SEGMENTS.gdalg.json" --overwrite

{
  echo '<OGRVRTDataSource>'
  echo '  <OGRVRTUnionLayer name="LIST_TRANSPORT_SEGMENTS">'
  echo '    <SourceLayerFieldName>municipality</SourceLayerFieldName>'
  for m in "${munips[@]}"; do
    lc="$(printf '%s' "$m" | tr '[:upper:]' '[:lower:]')"
    echo "    <OGRVRTLayer name=\"$m\">"
    echo "      <SrcDataSource>$(src "$m")</SrcDataSource>"
    echo "      <SrcLayer>list_transport_segments_$lc</SrcLayer>"
    echo '    </OGRVRTLayer>'
  done
  echo '  </OGRVRTUnionLayer>'
  echo '</OGRVRTDataSource>'
} > "$out/LIST_TRANSPORT_SEGMENTS.vrt"

ls -l "$out" >&2
