munips <- c("CLARENCE", "GLENORCHY", "HOBART", "KINGBOROUGH")

dsn_all <- sprintf("/vsizip//vsicurl/https://listdata.thelist.tas.gov.au/opendata/data/LIST_TRANSPORT_SEGMENTS_%s.zip",
        munips)

library(gdalraster)
#gdal_usage("vector concat")
delete_after <- c()
for (i in seq_along(munips)) {
  args <- list(input = dsn_all, output = outf <- sprintf("%s.parquet", munips[i]), format = "Parquet",
               layer = sprintf("list_transport_segments_%s", tolower(munips[i])),
               `output-layer` = "list_transport_segments", mode = "single")
  alg <- new(gdalraster::GDALAlg, "vector concat", args)

  alg$run()

  alg$release()
  delete_after <- c(delete_after, outf)
}


## now merge em
args <- list(input = delete_after,
             output = outf <- "LIST_TRANSPORT_SEGMENTS.parquet", format = "Parquet", mode = "single")
alg <- new(gdalraster::GDALAlg, "vector concat", args)

alg$run()

alg$release()
unlink(delete_after)
