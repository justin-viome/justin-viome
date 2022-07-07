# justin.thomson@viome.com
# intended usage: Rscript generateParquetFromODM.R
# assumes S3_ODM_LOCATION and S3_ODM_STUDY_NAME are set appropriately in environment

library(odmparquet)
library(xml2)
library(aws.s3)
library(arrow, warn.conflicts = F)
library(R6)
library(XML)


#!/usr/bin/env Rscript

initializeAWSFromFile()
s3Location = Sys.getenv("S3_ODM_LOCATION")
s3StudyName = Sys.getenv("S3_ODM_STUDY_NAME")

if (is.null(s3Location)) {
  stop(paste0("S3 Location needs to be defined in the 'S3_ODM_LOCATION' environment variable. Exiting."))
}
if (nchar(s3Location) < 6 || !startsWith(x=s3Location, prefix="s3://")) {
  stop(paste0("S3 Location in the 'S3_ODM_LOCATION' environment variable is invalid (Currently '",
              s3Location, "'). Exiting."))
}

if (is.null(s3StudyName) || nchar(s3StudyName) ==0) {
  stop("Valid Study Name needs to be defined in the 'S3_ODM_STUDY_NAME' environment variable. Exiting.")
}
odmp=odmObj$new(studyname = s3StudyName, odmFileLocation = s3Location)
odmp$parseODM()
writeParquetToStudiesS3(odmobj = odmp)
