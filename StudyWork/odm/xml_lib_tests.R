# Justin Thomson justin.thomson@viome.com
library(arrow)
library(aws.s3)
library(xml2)
library(xml2relational)
#library(ODM2R)
library(XML)
library(flatxml)
library(xmlconvert)


#load environment context if github-hidden file exists
if (file.exists("/users/justin/git/justin-viome/set_env.R")) {
  source("/users/justin/git/justin-viome/set_env.R")
}


fetchXmlFromAWS = function(bucket = "viome-studies", study,
                           file) {
  
  fileLoc=paste0(study, "/ODM/", file)
  b1=s3read_using(FUN=read_xml, bucket = bucket, object = fileLoc)
  dfout = toRelational(b1)
  
  dfout
}

writeStudyParquet = function(xmldf, bucket = "viome-studes", study) {
  dirLoc=paste0("s3://", bucket,study, "/ODMparquet")
  print(paste0("writing to: ", dirLoc))
  write_parquet(x=xmldf,sink =dirLoc, use_dictionary = T, write_statistics = T)
  
}

testOdm2R = function() {
  
  testFile = "/Users/justin/Downloads/V128_Pilot_odm_export_20220601012550.xml"
  # x=ODM2R(ODMfile=testFile)
  # output: massive R file
}

testOdm2Office = function() {
  testFile = "/Users/justin/Downloads/V128_Pilot_odm_export_20220601012550.xml"
  ODM2office(ODMfile=testFile)
}

testXml2Relational = function() {
  
  studyname="v128.234"
  fileName="V128_Pilot_odm_export_20220601012550.xml"
  df=fetchXmlFromAWS(study=studyname, file=fileName)
  writeStudyParquet(xmldf=df, study=studyname)
}

testFlatXml = function() {
  testFile = "/Users/justin/Downloads/V128_Pilot_odm_export_20220601012550.xml"
  flatx=fxml_importXMLFlat(testFile) # didn't finish after 3 min
}

testXmlConvert = function() {
  testFile = "/Users/justin/Downloads/V128_Pilot_odm_export_20220601012550.xml"
  
  # fails with subscript out of bounds
  convx = xmlconvert::xml_to_df(file=testFile, records.tags = "ItemData")
}

testXmlSchemaValidate = function() {
  
  testFile = "/Users/justin/Downloads/V128_Pilot_odm_export_20220601012550.xml"
  testSchema="/Users/justin/Downloads/odm1_3_2/ODM1-3-2-foundation.xsd"
  doc <- read_xml(testFile, package = "xml2")
  schema <- read_xml(testSchema, package = "xml2")
  
  
  # result: failure.
  # "Element '{http://www.cdisc.org/ns/odm/v1.3}CodeList': This element is not expected. Expected is ( {http://www.cdisc.org/ns/odm/v1.3}MethodDef )."
  xml_validate(doc, schema)
  
}

testOdmCompare = function() {
  
  testFile1 = "/Users/justin/Downloads/V128_Pilot_odm_export_20220601012550.xml"
  testFile2 = "/Users/justin/Downloads/V128_Pilot_odm_export_20220601012550.xml"
  
  #compareodm library removed from CRAN due to unreachable maintainer
}

# Find node name: xxx
# Check if data frame called xxx exists
# If it does not
# create data frame called xxx
# create PK column called pk_tablename 
# if parent is not null
# create FK_parentid column 
# 
# 
# For each attribute aaa:
#   If column with name aaa doesn’t exist in table xxx
# add it
# Add row to table xxx with ID for pk_tablename and values for columns 
