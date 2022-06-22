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


########## global vars 

# for now, just store critical tables
# metadata:
#odm, metadataversion, protocol, studyeventref, studyeventdef, formref, studyeventdef, formdef, itemgroupref, itemgroupdef, itemref, itemdef, question, codelistref,
#codelist, codelistitem
#clinical data:
#clinicaldata,subjectdata, studyeventdata, formdata, itemgroupdata, itemdata

odm = setNames(data.frame(matrix(ncol = 16, nrow = 0)), c("pk_odm", "description", "filetype", "granularity", "archival", "fileoid", "creationdatetime", "priorfileoid",
                                                          "asofdatetime", "odmversion", "originator", "sourcesystem","sourcesystemversion",
                                                          "id", "xmlns","viomestudyname"))
study = setNames(data.frame(matrix(ncol = 4, nrow = 0)), c("pk_study", "oid", "fk_odm", "viomestudyname"))
#globalVariables = setNames(data.frame(matrix(ncol = 6, nrow = 0)), c("pk_globalvariables", "studyname", "studydescription", "protocolname", "fk_study", "viomestudyname"))
metadataversion = setNames(data.frame(matrix(ncol = 10, nrow = 0)), c("pk_metadataversion", "oid", "version", "fk_study", "viomestudyname"))
protocol= setNames(data.frame(matrix(ncol = 3, nrow = 0)), c("pk_protocol","fk_metadataversion", "viomestudyname"))
studyeventref = setNames(data.frame(matrix(ncol = 6, nrow = 0)), c("pk_studyeventref", "studyeventoid", "ordernumber", "mandatory", "fk_protocol", "viomestudyname"))

studyeventdef= setNames(data.frame(matrix(ncol = 7, nrow = 0)), c("pk_studyeventdef", "oid", "name", "repeating", "type", "fk_metadataversion", "viomestudyname"))
formref = setNames(data.frame(matrix(ncol = 5, nrow = 0)), c("pk_formref", "formoid", "mandatory", "fk_studyeventdef", "viomestudyname"))

formdef = setNames(data.frame(matrix(ncol = 6, nrow = 0)), c("pk_formdef", "oid", "name", "repeating", "fk_metadataversion", "viomestudyname"))
itemgroupref= setNames(data.frame(matrix(ncol = 7, nrow = 0)), c("pk_itemgroupref", "itemgroupoid", "ordernumber", "mandatory", "collectionexceptionconditionoid", "fk_formdef", "viomestudyname"))

itemgroupdef = setNames(data.frame(matrix(ncol = 13, nrow = 0)), c("pk_itemgroupdef", "oid", "name", "repeating", "isreferencedata", "sasdatasetname", "domain", "origin", "role", "purpose", "comment","fk_metadataversion", "viomestudyname"))
itemref = setNames(data.frame(matrix(ncol = 11, nrow = 0)), c("pk_itemref","itemoid","ordernumber","mandatory","keysequence","methodoid","role","rolecodelistoid","collectionexceptionconditionoid","fk_itemgroupdef", "viomestudyname"))

itemdef = setNames(data.frame(matrix(ncol = 12, nrow = 0)), c("pk_itemdef","oid","name","datatype","length","significantdigits","sasfieldname","sdsvarname","origin","comment","fk_metadataversion", "viomestudyname"))
question = setNames(data.frame(matrix(ncol = 5, nrow = 0)), c("pk_question", "value", "xml:lang", "fk_itemdef", "viomestudyname"))
externalquestion = setNames(data.frame(matrix(ncol = 6, nrow = 0)), c("pk_externalquestion", "dictionary", "version", "code", "fk_itemdef", "viomestudyname"))
codelistref = setNames(data.frame(matrix(ncol = 4, nrow = 0)), c("pk_codelistref", "codelistoid", "fk_itemdef", "viomestudyname"))
#rangecheck = setNames(data.frame(matrix(ncol = 3, nrow = 0)), c("pk_rangecheck", "comparator", "softhard", "fk_itemdef", "viomestudyname"))

#methoddef = setNames(data.frame(matrix(ncol = 5, nrow = 0)), c("pk_methoddef","oid", "name", "fk_metadataversion", "viomestudyname"))
# basicDefinitions = setNames(data.frame(matrix(ncol = 3, nrow = 0)), c("pk_basicdefinitions","fk_study", "viomestudyname"))
# measurementUnit = studyDescription = setNames(data.frame(matrix(ncol = 5, nrow = 0)), c("pk_measurementunit", "oid", "name","fk_basicdefinitions", "viomestudyname"))
# symbol = setNames(data.frame(matrix(ncol = 10, nrow = 0)), c("pk_symbol","fk_measurementunit", "viomestudyname"))
# translatedtext= setNames(data.frame(matrix(ncol = 10, nrow = 0)), c("pk_translatedtext", "xml:lang", "text","fk_", "viomestudyname"))

codelist = setNames(data.frame(matrix(ncol = 6, nrow = 0)), c("pk_codelist","oid","name","datatype", "fk_metadataversion", "viomestudyname"))

# should store value
codelistitem = setNames(data.frame(matrix(ncol = 6, nrow = 0)), c("pk_codelistitem","codedvalue", "rank", "ordernumber", "fk_codelist", "viomestudyname"))

clinicaldata = setNames(data.frame(matrix(ncol = 5, nrow = 0)), c("pk_clinicaldata","fk_odm","studyoid","metadataversionoid", "viomestudyname"))
subjectdata = setNames(data.frame(matrix(ncol = 4, nrow = 0)), c("pk_subjectdata","fk_clinicaldata","subjectkey", "viomestudyname"))
studyeventdata = setNames(data.frame(matrix(ncol = 5, nrow = 0)), c("pk_studyeventdata","fk_subjectdata","studyeventoid","studyeventrepeatkey", "viomestudyname"))
formdata = setNames(data.frame(matrix(ncol = 5, nrow = 0)), c("pk_formdata","fk_studyeventdata","formoid", "formrepeatkey", "viomestudyname"))
itemgroupdata = setNames(data.frame(matrix(ncol = 5, nrow = 0)), c("pk_itemgroupdata","fk_formdata","itemgroupoid","itemgrouprepeatkey", "viomestudyname"))
itemdata = setNames(data.frame(matrix(ncol = 5, nrow = 0)), c("pk_itemdata","fk_itemgroupdata","itemoid","value", "viomestudyname"))

# hacky. The order of dfList and dfListNames have to be the same  
# could create two lists one for metadata, one smaller one for data
dfList = list(odm, study, metadataversion, protocol, studyeventref, studyeventdef, formref, formdef, itemgroupref, itemgroupdef, itemdef, question, externalquestion, 
              codelistref, codelist, codelistitem, clinicaldata, subjectdata, studyeventdata, formdata, itemgroupdata, itemdata)

# using separate variable for dataframe names due to r quirks. dataframe name is lost when list(dataframe) is called.
#names(dfList)=
dfListNames=c('odm', 'study', 'metadataversion', 'protocol', 'studyeventref', 'studyeventdef', 'formref', 'formdef', 'itemgroupref', 'itemgroupdef', 'itemdef', 
  'question', 'externalquestion', 'codelistref', 'codelist', 'codelistitem', 'clinicaldata', 'subjectdata', 'studyeventdata', 'formdata', 'itemgroupdata', 'itemdata')

# create global var for studyname to avoid passing through to all calls 
s_viomestudyname="v128.234"

########## methods

initializeAWS = function() {
  #load environment context if github-hidden file exists
  if (file.exists("/users/justin/git/justin-viome/set_env.R")) {
    source("/users/justin/git/justin-viome/set_env.R")
  }
}

# gets dataframe that corresponds to given node 
# dataframe returned is not named due to r weirdness
getDataFrame = function(dfs = dfList, nodeName) {
  if(nodeName %in% dfListNames) {
    # use double brackets to get dataframe at correct location
    return(dfs[[which(dfListNames==nodeName)]]) 
  }
  else return(NULL)
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
  dfs=visitNode(parentID=NULL, node=xmlRoot)
  
  dfs
}

# Visit node(parent, node)
# Start at root ODM(NULL, “ODM”)
# Find node name: xxx
# 
# Log nodes not in defined set
# Add row to table xxx with ID for pk_xxx, ID for fk_parentname and values for columns
# Log attributes (columns not in defined set)
# 
# Call VisitNode(...) for children

# uses global variable for dfList for now
visitNode = function(parentID, node) {
  elemName = tolower(xml_name(node))
  print(paste0("elemname processing: ", elemName))
  nodedf = getDataFrame(nodeName=elemName)
  # null returned when element is intentionally skipped
  if (!is.null(nodedf)) {
    newparentid=setAttributesForNode(parentID, nodename=elemName, node, nodedf)
    chdr <- xml_children(node)
    for(i in 1:length(chdr)) {
      childnode= chdr[i]
      visitNode(parentID=newparentid,node=childnode)
    }
  } else {
    print(paste0("Node dataframe not found for element: ", elemName))
  }
}

# set pk, set attributes, set fk
setAttributesForNode = function(parentID, nodename, node, nodedf) {
  
  # add row to df
  newparentid = nrow(nodedf)+1
  
  #add row with nas then replace with values from input node
  nodedf[newparentid, c(paste0("pk_", nodename))] <- newparentid
  
  attrs = xml_attrs(node)
  for (i in 1:length(attrs)) {
    attrname=tolower(names(attrs[i]))
    if (attrname %in% names(nodedf)) {
      nodedf[newparentid, attrname]=attrs[i]
    } else {
      print(paste0("Attribute ", attrname, " not found in dataframe ", nodename))
    }
  }
  
  collen=ncol(nodedf)
  
  #set fk. For now, fk is always the second-to-last column in a given data frame 
  if (!is.null(parentID)) {
    nodedf[newparentid, collen-1]=parentID
  }
  
  nodedf[newparentid, collen] = s_viomestudyname
  
  # return newparentid for next call
  newparentid
}

testX = function () {
  nodedf=odm
  nodename='odm'
  parentID=NULL
  
  testFile=  odmfile= "/Users/justin/Downloads/V128_Pilot_odm_export_20220601012550.xml"
  smallrealFile = "/Users/justin/Downloads/V128_reduced.xml"
  
  initializeAWS()
  x <- xml2::read_xml(smallrealFile)
  
  elemName=tolower(xml_name(x))
  
  t = ODMToDataFrames(x)
}
