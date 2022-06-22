# justin.thomson@viome.com
# poc for odm data lake
# parse odm file and create parquet tables in s3
# one table should be created for each important odm element

# create predefined dataframe structures that encompass all of ODM
# populate them with data parsed from an ODM file
# each data frame will have a pk and a fk to its parent, except the odm data frame
# pk and fkids are simply row numbers
# each dataframe will denormalize the viomestudyname for downstream consumption
# lower case used for names due to parquet case sensitivity
# maintain referential integrity of data through the addition of PK and FK values for each table
# generate parquet for each resulting dataframe and store in s3

# todo?: could sort each data frame before storage to improve parquet reads (use of parquet metadata)

# considerations
# some xml elements are used in many places in ODM, with many different parent element types.
# for this, where possible, my convention is to add them as attributes to their parents. E.g translatedtext.
# convention: store xml element body values in columns called  "value"

# can also check for any variables with no values (all NA in R) and remove those columns in the output for efficiency

library(xml2)
library(arrow)


# create global var for studyname to avoid passing through to all calls
s_viomestudyname="v128.234"

########## methods

initializeAWS = function() {
  #load environment context if github-hidden file exists
  if (file.exists("/users/justin/git/justin-viome/set_env.R")) {
    source("/users/justin/git/justin-viome/set_env.R")
  }
}

# read odm file from s3
readODMFromS3= function(s3bucket="viome-studies", s3FileLocation) {
  initializeAWS()
  out=s3read_using(FUN=read_xml, bucket = bucket, object = s3FileLocation)

  #TODO: handle errors such as invalid file format, or xml file that isn't ODM
  out
}

# generate list of dataframes for input ODM file
# takes in xml file read via xml2::read_xml
ODMToDataFrames = function(xmlData) {
  xmlRoot <- xml_root(xmlData)
  print(paste0(Sys.time(), ": calling visitNode from the top"))

  odm = odmObj$new(studyname='v128.234test')

  visitNode(parentID=NULL, node=xmlRoot)

}
