# justin.thomson#@viome.com

library(arrow)
library(aws.s3)

s_AWS_CONFIG_LOCATION = 's3://viome-studies/config/set_env_ingestion.R'

writeParquetToStudiesS3 = function(odmobj) {
  basePath = paste0("s3://viome-studies/ODMparquet/")
  writeODMParquetToS3(odmobj=odmobj, basePath=basePath)
}


writeODMParquetToS3 = function(odmobj, basePath) {
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
      writePath = paste0(basePath, elem, "/")
      print(paste0(Sys.time(), ": writing parquet file(s) for ", elem, " to ", writePath))
      write_dataset(dataset = ds, path = writePath, format = "parquet",
                    basename_template = paste0(odmobj$studyname, "-part-{i}.parquet"),
                    use_dictionary = TRUE, write_statistics = TRUE)
    }
  }
  print(paste0(Sys.time(), ": writeODMParquetToS3 complete"))
}

# assumes aws access key and secret access key are in system env
runStudyCrawler = function(crawlerName='jt-odmparquet') {

  print(paste0("starting crawler ", crawlerName))
  envRegion = Sys.getenv("AWS_REGION")
  if (nchar(envRegion)==0) {
    envRegion='us-east-1'
  }
  commandStr = paste0("aws glue start-crawler --name ", crawlerName, " --region ", envRegion)
  system(command=commandStr)
}

# load environment context if github-hidden file exists locally
# otherwise load from standard s3 location
initializeAWSFromFile = function() {
  if (file.exists("/users/justin/git/justin-viome/set_env.R")) {
    print("setting environment variables from local file")
    source("/users/justin/git/justin-viome/set_env.R")
  } else if (aws.s3::object_exists(s_AWS_CONFIG_LOCATION)) {
    print(paste0("setting environment variables from s3 file at: ", s_AWS_CONFIG_LOCATION))
    aws.s3::s3read_using(base::source, object=s_AWS_CONFIG_LOCATION)
  } else {
    print(paste("no environment config files found locally or at ", s_AWS_CONFIG_LOCATION))
  }
}

# get standard location of latest xml data for given study
# note this returns the object location within a bucket
getStandardFolderStudyODMLocation = function(studyname) {
  locName=paste0(studyname, "/ODM/", studyname, "_odm.xml")

  locName
}

# get full path of standard latest odm location for given study
getStandardS3StudyODMLocation = function (studyname) {
  loc = paste0("s3://viome-studies/", getStandardFolderStudyODMLocation(studyname))

  loc
}

