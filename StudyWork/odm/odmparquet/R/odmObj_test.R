# justin.thomson@viome.com

library(assertthat)
library(xml2)
library(arrow)

testParseSmallFile = function () {
  smallrealFile = "/Users/justin/Downloads/V128_reduced.xml"
  initializeAWSFromFile()
  x = odmObj$new(studyname = 'testn', odmFileLocation = smallrealFile)

  x$parseODM()
}

testParseNCrawlCastorAndRedcap = function() {
  initializeAWSFromFile()
  castor_v128xml = "/Users/justin/Downloads/V128_Pilot_odm_export_20220601012550.xml"
  castorobj = odmObj$new(studyname = 'v128.234', xmlFile = castor_v128xml)
  castorobj$parseODM()
  writeParquetToStudiesS3(castorobj)

  redcap_v302xml = "/Users/justin/Downloads/V302_2022-06-28_1048.REDCap.xml"
  redcapobj = odmObj$new(studyname = 'v302', xmlFile = redcap_v302xml)
  redcapobj$parseODM()
  writeParquetToStudiesS3(redcapobj)

  runStudyCrawler()
}
# testParseNCrawlFullStudy = function() {
#   v128xml = "/Users/justin/Downloads/V128_Pilot_odm_export_20220601012550.xml"
#   xml = read_xml(v128xml)
#   initializeAWSFromFile()
#   x = odmObj$new(studyname = 'v128.234', xmlDoc = xml)
#
#   x$parseODM()
#   writeParquetToS3(x)
#
#   runStudyCrawler(crawlerName = "jt-odmparquet")
# }
#
# testParseFullStudyRedcap = function() {
#
#   v302xml = "/Users/justin/Downloads/V302_2022-06-28_1048.REDCap.xml"
#   initializeAWSFromFile()
#   x = odmObj$new(studyname = 'v302', odmFileLocation = xml)
#
#   x$parseODM()
#   writeParquetToS3(x)
# }
getGrandChildValue = function () {
  testfile='/Users/justin/Downloads/itemdeftest.xml'
  xml = read_xml(testfile)
  root = xml_root(xml)
  gc=xml_text(xml_child(x=(xml_child(x=root, search='Question')), search='TranslatedText'))
}

# read odm file from s3
testParseXmlFromS3= function() {
  s3FileLocation='s3://viome-studies/v128.234/ODM/V128_Pilot_odm_export_20220601012550.xml'
  initializeAWSFromFile()
  x = odmObj$new(studyname = 'v128test', odmFileLocation = s3FileLocation)

  x$parseODM()
}

testParseFromEnvironmentVariables = function() {
  initializeAWSFromFile()
  x = odmObj$new()
  x$parseODM()
}

testJoinData = function() {
  v128xml = "/Users/justin/Downloads/V128_Pilot_odm_export_20220601012550.xml"
  initializeAWSFromFile()
  x = odmObj$new(studyname = 'v128.234', odmFileLocation = xml)

  x$parseODM()

  #check that pks and fks match up as intended
}

testParseNWritev242 = function() {
  redcap_v242xml = "/Users/justin/Downloads/v242.xml"
  redcapobj = odmObj$new(studyname = 'v242', odmFileLocation = redcap_v242xml)
  redcapobj$parseODM()
  writeParquetToStudiesS3(redcapobj)

  runStudyCrawler()
}
