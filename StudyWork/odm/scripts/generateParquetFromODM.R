# justin.thomson@viome.com
# intended usage: Rscript generateParquetFromODM.R
# assumes S3_ODM_STUDY_NAME is set appropriately in environment

library(odmparquet)
library(xml2)
library(aws.s3)
library(arrow, warn.conflicts = F)
library(R6)
library(XML)


#!/usr/bin/env Rscript

initializeAWSFromFile()
s3StudyName = Sys.getenv("S3_ODM_STUDY_NAME")

if (is.null(s3StudyName) || nchar(s3StudyName) ==0) {
  stop("Valid Study Name needs to be defined in the 'S3_ODM_STUDY_NAME' environment variable. Exiting.")
}

fetchNParseRedcapStudy(s3StudyName)
