# justin.thomson@viome.com

library(odmparquet)
library(xml2)
library(aws.s3)
library(arrow)
library(R6)


#!/usr/bin/env Rscript

# this file allows parquet generation to be done from the command line using an s3location as input
# args = commandArgs(trailingOnly=TRUE)
#
# if (length(args) != 2) {
#   stop("Study name and the s3 location of the ODM xml file must be supplied in that order.", call.=FALSE)
# } else {
#   initializeAWSFromFile()
#   studyName = args[1]
#   s3Location = args[2]
# }
initializeAWSFromFile()
s3Location = Sys.getenv("S3_ODM_LOCATION")
s3StudyName = Sys.getenv("S3_ODM_STUDY_NAME")

if (is.null(s3Location) || length(s3Location) < 6) {
 print("Valid S3 Location needs to be defined in the 'S3_ODM_LOCATION' environment variable. Exiting.")
}

if (is.null(s3StudyName) || length(s3StudyName) ==0) {
  print("Valid Study Name needs to be defined in the 'S3_ODM_STUDY_NAME' environment variable. Exiting.")
}
odmp=odmObj$new(studyname = s3StudyName, xmlFile = s3Location)
odmp$parseODM()
writeParquetToStudiesS3(odmobj = odmp)
