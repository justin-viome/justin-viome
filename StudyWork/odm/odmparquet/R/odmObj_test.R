# justin.thomson@viome.com


library(assertthat)
library(xml2)

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
}
