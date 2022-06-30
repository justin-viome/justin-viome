# justin.thomson#@viome.com

library(arrow)
library(aws.s3)

writeParquetToStudiesS3 = function(odmobj) {
  basePath = paste0("s3://viome-studies/ODMparquet/")
  writeODMParquetToS3(odmobj=odmobj, writePath=basePath)
}


writeODMParquetToS3 = function(odmobj, writePath) {
  print(paste0(Sys.time(), ": writeODMParquetToS3 started"))
  # iterate through list of objects and write each to s3 even if they are empty
  for (i in 1:length(odmobj$odmdfnames)) {
    elem = odmobj$odmdfnames[i]
    ds = odmobj[[elem]]

    if (is.null(ds)) {
      print(paste0("Element ", elem, " doesn't exist in input object and can't be written to parquet. Skipped."))
    } else if (nrow(ds)==0) {
      print(paste0("Element ", elem, " has no data in input ODM file and won't be written to parquet. Skipped."))
    } else {
      print(paste0(Sys.time(), ": writing element ", elem, " to ", writePath))
      writePath = path=paste0(basePath, elem, "/")
      write_dataset(dataset = ds, path = writePath, format = "parquet",
                    basename_template = paste0(odmobj$studyname, "-part-{i}.parquet"),
                    use_dictionary = TRUE, write_statistics = TRUE)
    }
  }
  print(paste0(Sys.time(), ": writeODMParquetToS3 complete"))
}

# assumes aws creds are in system env
runStudyCrawler = function(crawlerName='jt-odmparquet') {

  print(paste0("starting crawler ", crawlerName))
  commandStr = paste0("aws glue start-crawler --name ", crawlerName, " --region ", Sys.getenv("AWS_REGION"))
  system(command=commandStr)
}

#load environment context if github-hidden file exists
initializeAWSFromFile = function() {
  if (file.exists("/users/justin/git/justin-viome/set_env.R")) {
    source("/users/justin/git/justin-viome/set_env.R")
  }
}
