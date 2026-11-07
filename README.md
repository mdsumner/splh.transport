# splh.transport

A cached, single-file copy of the [LIST](https://www.thelist.tas.gov.au/) transport segments
for all 29 Tasmanian municipalities, as GeoParquet (ZSTD), with a `municipality` column added.

Download: <https://github.com/mdsumner/splh.transport/releases/download/latest/LIST_TRANSPORT_SEGMENTS.parquet>

```
ogrinfo -so /vsicurl/https://github.com/mdsumner/splh.transport/releases/download/latest/LIST_TRANSPORT_SEGMENTS.parquet LIST_TRANSPORT_SEGMENTS
```

## How it updates

`.github/workflows/refresh.yml` runs weekly (and on demand via *Run workflow*):

1. `scripts/probe.sh` HEADs every upstream zip and records `Last-Modified`, `ETag` and size in `manifest.tsv`.
2. If that differs from the `manifest.tsv` on the `latest` release (or *force* is ticked),
   `scripts/build.sh` rebuilds the parquet inside a pinned GDAL container.
3. The parquet and the new manifest are uploaded to the `latest` release.

The `manifest.tsv` release asset records the upstream state the current parquet was built from.

Run locally (GDAL >= 3.11 with Parquet):

```
scripts/build.sh work LIST_TRANSPORT_SEGMENTS.parquet
```
