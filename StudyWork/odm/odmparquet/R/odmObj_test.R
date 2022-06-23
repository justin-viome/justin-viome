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


testParse = function () {
  smallrealFile = "/Users/justin/Downloads/V128_reduced.xml"
  xml = read_xml(smallrealFile)
  initializeAWS()
  x = odmObj$new(studyname = 'testn', xmlDoc = xml)

  x$parseODM()
  x$writeParquetToS3()
}

testParseFullStudy = function() {
  v128xml = "/Users/justin/Downloads/V128_Pilot_odm_export_20220601012550.xml"
  xml = read_xml(v128xml)
  initializeAWS()
  x = odmObj$new(studyname = 'v128.234', xmlDoc = xml)

  x$parseODM()
  x$writeParquetToS3()
}
