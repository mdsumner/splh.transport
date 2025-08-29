#munips <- c("CLARENCE", "GLENORCHY", "HOBART", "KINGBOROUGH")
munips <- c("HUON_VALLEY", "KING_ISLAND", "SOUTHERN_MIDLANDS", "LAUNCESTON",
  "GEORGE_TOWN", "MEANDER_VALLEY", "TASMAN", "KENTISH", "KINGBOROUGH",
  "CENTRAL_HIGHLANDS", "DORSET", "WARATAH_WYNYARD", "DEVONPORT",
  "GLAMORGAN_SPRING_BAY", "CLARENCE", "BREAK_O_DAY", "CIRCULAR_HEAD",
  "HOBART", "GLENORCHY", "NORTHERN_MIDLANDS", "DERWENT_VALLEY",
  "BURNIE", "SORELL", "CENTRAL_COAST", "WEST_COAST", "WEST_TAMAR",
  "FLINDERS", "LATROBE", "BRIGHTON")
dsn_all <- sprintf("/vsizip/{/vsicurl/https://listdata.thelist.tas.gov.au/opendata/data/LIST_TRANSPORT_SEGMENTS_%s.zip}/list_transport_segments_%s.gdb",
        munips, tolower(munips))

library(gdalraster)
#gdal_usage("vector concat")
delete_after <- c()
for (i in seq_along(munips)) {
  args <- list(input = dsn_all[i], output = outf <- sprintf("%s.parquet", munips[i]), format = "Parquet",
               layer = sprintf("list_transport_segments_%s", tolower(munips[i])),
               `output-layer` = "list_transport_segments")
  alg <- new(gdalraster::GDALAlg, "vector convert", args)

  alg$run()

  alg$release()
  delete_after <- c(delete_after, outf)
}


## now merge em
args <- list(input = delete_after,
             output = outf <- "LIST_TRANSPORT_SEGMENTS.parquet", format = "Parquet", mode = "single",
             `creation-option` = c("COMPRESSION=ZSTD"))
alg <- new(gdalraster::GDALAlg, "vector concat", args)

alg$run()

alg$release()
unlink(delete_after)

## now upload to github
#piggyback::pb_upload("LIST_TRANSPORT_SEGMENTS.parquet")
