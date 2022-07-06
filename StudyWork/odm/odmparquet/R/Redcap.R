# justin.thomson@viome.com

library(XML)

# export records including survey fields
uri <- "https://redcap.vanderbilt.edu/api/"

# test redcapr library
# return completely flattened view of fields across forms
# v197out=redcap_read_oneshot(redcap_uri = uri, token = mytoken)
# could get list of forms, then call this for each form, saving each .csv file separately

# direct calls to REDCap API to get ODM

# remove ItemDataBase64Binary information (signature pictures) from output
cleanStudyData = function(xml)
{
  d <- xmlRoot(xml)
  nsDefs <- xmlNamespaceDefinitions(xml)
  ns <- structure(sapply(nsDefs, function(x) x$uri), names = names(nsDefs))
  names(ns)[1] <- "x"
  removeNodes(xpathSApply(xml, "//x:ItemDataBase64Binary", namespaces=ns))

  xml
}

testFetch302 = function() {
  mytoken=Sys.getenv("REDCAP_TOKEN_302")
  outLoc = "/users/justin/Downloads/v302_test.xml"
  fetchREDCapData(uri = uri, token = mytoken, fileToWrite = outLoc)
}


fetchRedcapODM = function(studyname = "v302", redcapStudyUserToken=Sys.getenv("REDCAP_TOKEN_302")) {
  token=redcapStudyUserToken
  url <- "https://redcap.vanderbilt.edu/api/"
  formData <- list("token"=token,
                   content='project_xml',
                   format='odm',
                   returnMetadataOnly='false',
                   exportFiles='false',
                   exportSurveyFields='false',
                   exportDataAccessGroups='false',
                   returnFormat='odm'
  )
  response <- httr::POST(uri, body = formData, encode = "form")
  if (response$status_code != 200) {
    print(paste0('Bad response received from REDCap: ', response$status_code))
  } else
  {
    print('Successful 200 response received from REDCap')
    xml <- xmlInternalTreeParse(httr::content(response,type="text", encoding = "UTF-8"))
    cleanXml = cleanStudyData(xml)
  }
}

writeXmlToDisk = function(studyName, xmlData, fileLocation) {
  if (file.exists(fileLocation)) {
    file.remove(fileLocation)
  }
  print(paste0("writing output odm xml for ", studyname, " to ", fileLocation))
  saveXML(xmlData, fileLocation)
}

writeRedcapODMXmlToS3 = function (studyName, xmlData, s3Bucket, s3Location) {
  print(paste0("writing output odm xml for ", studyname, " to ", s3Location))
  aws.s3::save_object(object = xmlData, bucket = s3Bucket, file = s3Location, overwrite = T)
}

fetchRedcapODMAndSaveLocally = function(studyname, redcapStudyUserToken) {
  fileToWrite=paste0("/users/justin/Downloads/", studyname, "_odm.xml")
  odm=fetchRedcapODM(studyname = studyname, redcapStudyUserToken = redcapStudyUserToken)
  writeXmlToDisk(studyName = studyname, xmlData = odm, fileLocation = fileToWrite)
}

fetchRedcapODMAndSaveToS3 = function(studyname) {
  locName=paste0(studyname, "/", studyname, "/", studyname, "_odm.xml")

  # try and get token using inputed studyname sans leading v
  envKey = paste0("REDCAP_TOKEN_", gsub(pattern='v', replacement = '', x=studyname))
  redcapStudyUserToken = Sys.getenv(paste0("REDCAP_TOKEN_", ))

  odm=fetchRedcapODM(studyname = studyname, redcapStudyUserToken = redcapStudyUserToken)
  writeRedcapODMXmlToS3(studyName = studyname, xmlData = odm, s3Bucket='viome-studies', s3Location=locName)
}


