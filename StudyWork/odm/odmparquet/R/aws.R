#justin.thomson#@viome.com

library(arrow)

writeParquetToS3 = function(odmobj, bucketName="viome-studies") {

  print(paste0(Sys.time(), ": writeParquestToS3 started"))
  basePath = paste0("s3://", bucketName, "/", odmobj$studyname, "/ODMparquet/")
  # iterate through list of objects and write each to s3 even if they are empty
  for (i in 1:length(odmobj$odmdfnames)) {
    elem = odmobj$odmdfnames[i]
    writePath = path=paste0(basePath, elem, "/")

    ds = odmobj[[elem]]
    if (is.null(ds)) {
      print(paste0("Element ", elem, " doesn't exist in input object and can't be written to parquet.Skipped."))
    } else {
      print(paste0(Sys.time(), ": writing element ", elem, " to ", writePath))
      write_dataset(dataset=ds,path = writePath, format='parquet')
    }
  }
  print(paste0(Sys.time(), ": writeParquestToS3 complete"))

}

#load environment context if github-hidden file exists
initializeAWSFromFile = function() {
  if (file.exists("/users/justin/git/justin-viome/set_env.R")) {
    source("/users/justin/git/justin-viome/set_env.R")
  }
}
