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

writeXmlToDisk = function(studyname, xmlData, fileLocation) {
  if (file.exists(fileLocation)) {
    file.remove(fileLocation)
  }
  print(paste0("writing output odm xml for ", studyname, " to ", fileLocation))
  saveXML(xmlData, fileLocation)
}

writeRedcapODMXmlToS3 = function (studyname, xmlData, s3Bucket, s3Location) {
  print(paste0("writing output odm xml for ", studyname, " to ", s3Bucket, "/", s3Location))
  odmStr=XML::toString.XMLNode(xmlData)
  s3write_using(x = xmlData, FUN = saveXML, bucket = s3Bucket, object = s3Location)
}

fetchRedcapODMAndSaveLocally = function(studyname, redcapStudyUserToken) {
  fileToWrite=paste0("/users/justin/Downloads/", studyname, "_odm.xml")
  odm=fetchRedcapODM(studyname = studyname, redcapStudyUserToken = redcapStudyUserToken)
  writeXmlToDisk(studyname = studyname, xmlData = odm, fileLocation = fileToWrite)
}

fetchRedcapODMAndSaveToS3 = function(studyname) {
  locName=getStandardFolderStudyODMLocation(studyname)

  # TODO: fix issue.  Look at studyname='v197'. Not all responses from Redcap work.  Skip non live?
  envKey = paste0("REDCAP_TOKEN_", gsub(pattern='v', replacement = '', x=studyname))
  redcapStudyUserToken = Sys.getenv(envKey)
  if (is.null(redcapStudyUserToken) || nchar(redcapStudyUserToken)==0) {
    stop(paste0("Redcap token not found in memory for key ", envKey))
  } else {
    print(paste0("fetching ODM for study ", studyname))
    odm=fetchRedcapODM(studyname = studyname, redcapStudyUserToken = redcapStudyUserToken)
    writeRedcapODMXmlToS3(studyname = studyname, xmlData = odm, s3Bucket='viome-studies', s3Location=locName)
  }
}

# fetches ODM, writes ODM xml to s3, transforms odm, and writes parquet to s3
# if successful, generates parquet for study
fetchNParseRedcapStudy = function(studyname) {
  if (nchar(studyname)==0) {
    stop("studyname must be provided")
  } else if (substr(studyname, 1, 1) != 'v') {
    stop("first character of study name must be 'v' per Viome convention")
  }

  fetchRedcapODMAndSaveToS3(studyname)
  odmp = transformODM(studyname)
  writeParquetToStudiesS3(odmobj = odmp)
}

transformODM = function(studyname) {
  odmp=odmObj$new(studyname = studyname, odmFileLocation = getStandardS3StudyODMLocation(studyname))
  odmp$parseODM()

  odmp
}

# fetch redcap study data to s3, run parquet generation, and update the data catalog
updateStudyDataLake = function() {
  fetchNParseRedcapStudy('v242')
  fetchNParseRedcapStudy('v168')
  fetchNParseRedcapStudy('v302')
  runStudyCrawler()
}

