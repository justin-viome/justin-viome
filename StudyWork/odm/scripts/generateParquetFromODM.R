# justin.thomson@viome.com

library(odmparquet)
library(xml2)
library(aws.s3)
library(arrow)
library(R6)


#!/usr/bin/env Rscript

# this file allows parquet generation to be done from the command line using an s3location as input
args = commandArgs(trailingOnly=TRUE)

if (length(args) != 2) {
  stop("Study name and the s3 location of the ODM xml file must be supplied in that order.", call.=FALSE)
} else {
  initializeAWSFromFile()
  studyName = args[1]
  s3Location = args[2]
}

odmp=odmObj$new(studyname = studyName, xmlFile = s3Location)
odmp$parseODM()
writeParquetToStudiesS3(odmobj = odmp)
