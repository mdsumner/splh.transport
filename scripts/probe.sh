#!/usr/bin/env bash
# HEAD every LIST transport-segment zip and write a manifest (TSV) to stdout:
#   municipality  last_modified  etag  content_length
# Exits non-zero if any URL fails, so a partial manifest is never trusted.
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
base="https://listdata.thelist.tas.gov.au/opendata/data"

printf 'municipality\tlast_modified\tetag\tcontent_length\n'
while read -r m; do
  [ -z "$m" ] && continue
  hdr="$(curl -fsSI --retry 5 --retry-all-errors "$base/LIST_TRANSPORT_SEGMENTS_${m}.zip" | tr -d '\r')"
  get() { printf '%s\n' "$hdr" | awk -v k="$1" 'tolower($1)==tolower(k)":" {sub(/^[^:]*: */,""); print; exit}'; }
  lm="$(get Last-Modified)"; et="$(get ETag)"; cl="$(get Content-Length)"
  if [ -z "$lm$et" ]; then echo "no Last-Modified/ETag for $m" >&2; exit 1; fi
  printf '%s\t%s\t%s\t%s\n' "$m" "$lm" "$et" "$cl"
done < "$here/municipalities.txt"
