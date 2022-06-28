# justin.thomson@viome.com

# justin.thomson@viome.com

library(XML)
library(REDCapR)

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

fetchREDCapData = function(uri, token, fileToWrite)
{
  formData <- list("token"=token,
                   content='record',
                   action='export',
                   format='odm',
                   type='flat',
                   csvDelimiter='',
                   rawOrLabel='label',
                   rawOrLabelHeaders='label',
                   exportCheckboxLabel='false',
                   exportSurveyFields='true', #metadata around surveys
                   exportDataAccessGroups='false',
                   returnFormat='xml' # error format
  )
  response <- httr::POST(uri, body = formData, encode = "form")
  if (response$status_code != 200) {
    print(paste0('Bad response received from REDCap: ', response$status_code))
  } else
  {
    print('Successful 200 response received from REDCap')
    xml <- xmlInternalTreeParse(httr::content(response,type="text", encoding = "UTF-8"))
    cleanXml = cleanStudyData(xml)

    if (file.exists(fileToWrite)) {
      file.remove(fileToWrite)
    }
    saveXML(cleanXml, fileToWrite)
  }

}

# note that api access only returns clinical data
# recap in Project Settings->Other Functionality allows full downloads of ODM for "project backup"
testFetch302 = function() {
  mytoken=Sys.getenv("REDCAP_TOKEN_302")
  outLoc = "/users/justin/Downloads/v302_test.xml"
  fetchREDCapData(uri = uri, token = mytoken, fileToWrite = outLoc)
}




