# justin.thomson@viome.com


library(assertthat)
library(xml2)
library(arrow)

simpleTest = function() {
  smallrealFile = "/Users/justin/Downloads/V128_reduced.xml"
  xml = read_xml(smallrealFile)
  x = odmObj$new(studyname = 'testn', xmlDoc = xml)

  assert_that(x$studyname=='testn')

  nodename='itemdef'
  t1= x$subjectdata
}


testParseSmallFile = function () {
  smallrealFile = "/Users/justin/Downloads/V128_reduced.xml"
  xml = read_xml(smallrealFile)
  initializeAWSFromFile()
  x = odmObj$new(studyname = 'testn', xmlDoc = xml)

  x$parseODM()
  writeParquetToS3(x)
}

testParseFullStudy = function() {
  v128xml = "/Users/justin/Downloads/V128_Pilot_odm_export_20220601012550.xml"
  xml = read_xml(v128xml)
  initializeAWSFromFile()
  x = odmObj$new(studyname = 'v128.234', xmlDoc = xml)

  x$parseODM()
  writeParquetToS3(x)
}

getGrandChildValue = function () {
  testfile='/Users/justin/Downloads/itemdeftest.xml'
  xml = read_xml(testfile)
  root = xml_root(xml)
  gc=xml_text(xml_child(x=(xml_child(x=root, search='Question')), search='TranslatedText'))
}

# read odm file from s3
readODMFromS3= function(s3bucket="viome-studies", s3FileLocation) {
  initializeAWSFromFile()
  out=s3read_using(FUN=read_xml, bucket = bucket, object = s3FileLocation)

  #TODO: handle errors such as invalid file format, or xml file that isn't ODM
  out
}



