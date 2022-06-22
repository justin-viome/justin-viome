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

# add fk column second from the end for consistent setting logic


# use datatables to allow for in-place changes.Dataframes do not.
odm = setNames(data.frame(matrix(ncol = 17, nrow = 0)), c("pk_odm", "Description", "FileType", "Granularity", "Archival", "FileOID", "CreationDateTime", "PriorFileOID",
                                                          "AsOfDateTime", "ODMVersion", "Originator", "SourceSystem","SourceSystemVersion",
                                                          "ID", "xmlns", "fk_none", "viomestudyname"))
study = setNames(data.frame(matrix(ncol = 4, nrow = 0)), c("pk_study", "OID", "fk_odm", "viomestudyname"))
#globalVariables = setNames(data.frame(matrix(ncol = 6, nrow = 0)), c("pk_globalvariables", "studyname", "studyDescription", "protocolname", "fk_study", "viomestudyname"))
metadataversion = setNames(data.frame(matrix(ncol = 5, nrow = 0)), c("pk_metadataversion", "OID", "Version", "fk_study", "viomestudyname"))
protocol= setNames(data.frame(matrix(ncol = 3, nrow = 0)), c("pk_protocol","fk_metadataversion", "viomestudyname"))
studyeventref = setNames(data.frame(matrix(ncol = 6, nrow = 0)), c("pk_studyeventref", "StudyEventOID", "OrderNumber", "Mandatory", "fk_protocol", "viomestudyname"))

studyeventdef= setNames(data.frame(matrix(ncol = 7, nrow = 0)), c("pk_studyeventdef", "OID", "Name", "Repeating", "Type", "fk_metadataversion", "viomestudyname"))
formref = setNames(data.frame(matrix(ncol = 5, nrow = 0)), c("pk_formref", "FormOID", "Mandatory", "fk_studyeventdef", "viomestudyname"))

formdef = setNames(data.frame(matrix(ncol = 6, nrow = 0)), c("pk_formdef", "OID", "Name", "Repeating", "fk_metadataversion", "viomestudyname"))
itemgroupref= setNames(data.frame(matrix(ncol = 7, nrow = 0)), c("pk_itemgroupref", "itemgroupOID", "OrderNumber", "Mandatory", "CollectionExceptionConditionOID", "fk_formdef", "viomestudyname"))

itemgroupdef = setNames(data.frame(matrix(ncol = 11, nrow = 0)), c("pk_itemgroupdef", "OID", "Name", "Repeating", "Domain", "Origin", "Role", "Purpose", "Comment","fk_metadataversion", "viomestudyname"))
itemref = setNames(data.frame(matrix(ncol = 11, nrow = 0)), c("pk_itemref","ItemOID","OrderNumber","Mandatory","KeySequence","MethodOID","Role","RoleCodeListOID","CollectionExceptionConditionOID","fk_itemgroupdef", "viomestudyname"))

itemdef = setNames(data.frame(matrix(ncol = 10, nrow = 0)), c("pk_itemdef","OID","Name","DataType","Length","SignificantDigits","Origin","Comment","fk_metadataversion", "viomestudyname"))

# todo: fetch english value from translatedtext
question = setNames(data.frame(matrix(ncol = 4, nrow = 0)), c("pk_question", "Value", "fk_itemdef", "viomestudyname"))
externalquestion = setNames(data.frame(matrix(ncol = 6, nrow = 0)), c("pk_externalquestion", "Dictionary", "Version", "Code", "fk_itemdef", "viomestudyname"))
codelistref = setNames(data.frame(matrix(ncol = 4, nrow = 0)), c("pk_codelistref", "CodeListOID", "fk_itemdef", "viomestudyname"))
#rangecheck = setNames(data.frame(matrix(ncol = 3, nrow = 0)), c("pk_rangecheck", "Comparator", "SoftHard", "fk_itemdef", "viomestudyname"))

# methoddef = setNames(data.frame(matrix(ncol = 5, nrow = 0)), c("pk_methoddef","OID", "Name", "fk_metadataversion", "viomestudyname"))
# basicDefinitions = setNames(data.frame(matrix(ncol = 3, nrow = 0)), c("pk_basicdefinitions","fk_study", "viomestudyname"))
# measurementUnit = studyDescription = setNames(data.frame(matrix(ncol = 5, nrow = 0)), c("pk_measurementunit", "OID", "Name","fk_basicdefinitions", "viomestudyname"))
# symbol = setNames(data.frame(matrix(ncol = 10, nrow = 0)), c("pk_symbol","fk_measurementunit", "viomestudyname"))
# translatedtext= setNames(data.frame(matrix(ncol = 10, nrow = 0)), c("pk_translatedtext", "xml:lang", "text","fk_", "viomestudyname"))

codelist = setNames(data.frame(matrix(ncol = 6, nrow = 0)), c("pk_codelist","OID","Name","DataType", "fk_metadataversion", "viomestudyname"))

# todo: should store value
codelistitem = setNames(data.frame(matrix(ncol = 6, nrow = 0)), c("pk_codelistitem","CodedValue", "Rank", "OrderNumber", "fk_codelist", "viomestudyname"))

clinicaldata = setNames(data.frame(matrix(ncol = 5, nrow = 0)), c("pk_clinicaldata","StudyOID","MetadataVersionOID", "fk_odm", "viomestudyname"))
subjectdata = setNames(data.frame(matrix(ncol = 4, nrow = 0)), c("pk_subjectdata","SubjectKey","fk_clinicaldata", "viomestudyname"))
studyeventdata = setNames(data.frame(matrix(ncol = 5, nrow = 0)), c("pk_studyeventdata","StudyEventOID","StudyEventRepeatKey","fk_subjectdata", "viomestudyname"))
formdata = setNames(data.frame(matrix(ncol = 5, nrow = 0)), c("pk_formdata","FormOID", "FormRepeatKey","fk_studyeventdata","viomestudyname"))
itemgroupdata = setNames(data.frame(matrix(ncol = 5, nrow = 0)), c("pk_itemgroupdata","ItemGroupOID","itemgrouprepeatkey","fk_formdata", "viomestudyname"))
itemdata = setNames(data.frame(matrix(ncol = 5, nrow = 0)), c("pk_itemdata","ItemOID","Value","fk_itemgroupdata", "viomestudyname"))

# hacky. The order of dfList and dfListNames have to be the same  
# could create two lists one for metadata, one smaller one for data
dfList = list(odm, study, metadataversion, protocol, studyeventref, studyeventdef, formref, formdef, itemgroupref, itemgroupdef, itemdef, question, externalquestion, 
              codelistref, codelist, codelistitem, clinicaldata, subjectdata, studyeventdata, formdata, itemgroupdata, itemdata)

# using separate variable for dataframe names due to r quirks. dataframe name is lost when list(dataframe) is called.
names(dfList)=c('odm', 'study', 'metadataversion', 'protocol', 'studyeventref', 'studyeventdef', 'formref', 'formdef', 'itemgroupref', 'itemgroupdef', 'itemdef', 
              'question', 'externalquestion', 'codelistref', 'codelist', 'codelistitem', 'clinicaldata', 'subjectdata', 'studyeventdata', 'formdata', 'itemgroupdata', 'itemdata')

# create separate environment to manage data frames 
# TODO: use r6 instead for cleaner approach 
odmenv <- new.env(parent = baseenv())
list2env(dfList2, envir = odmenv)

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
  if (length(elemName)==0) {
    debugx = 3 #debugging
  }
  print(paste0("elemname processing: ", elemName))
  nodedf = getDataFrame(nodeName=elemName)
  # null returned when element is intentionally skipped
  if (!is.null(nodedf)) {
    nodedf=setAttributesForNode(parentID=parentID, nodename=elemName, node=node, nodedf=nodedf)
    newparentid=nrow(nodedf)
    chdr <- xml_children(node)
    if(length(chdr) > 0) {
      for(i in 1:length(chdr)) {
        childnode= chdr[i]
        visitNode(parentID=newparentid,node=childnode)
      }
    }
    
    # assign local dataframe to global variable at end of run
    glb = get(x=elemName) 
    glb <<- nodedf
    
  } else {
    print(paste0("Node dataframe not found for element: ", elemName))
  }
}

# set pk, set attributes, set fk
setAttributesForNode = function(parentID, nodename, node, nodedf) {
  
  #add row with nas then replace with values from input node
  newparentid = nrow(nodedf)+1
  nodedf[newparentid, c(paste0("pk_", nodename))] <- newparentid
  
  #only attempt to set values we care about via the dataframe definitions
  attrs = xml_attrs(node)
  colNames = colnames(nodedf)
  col.len = length(colNames)
  
  # skip first column, PK, 2nd-to-last last column, FK,a nd final column, viomestudyname
  for (i in 2:(col.len-2)) {
    col = colNames[i]
    nodedf[newparentid, col]= xml_attr(x=node, attr = col)
  }
  
  #set fk. For now, fk is always the second-to-last column in a given data frame 
  if (!is.null(parentID)) {
    nodedf[newparentid, col.len-1]=parentID
  }
  
  # set studyname for all rows 
  nodedf[newparentid, col.len] = s_viomestudyname
  
  # return dataframe
  nodedf
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
  
  ODMToDataFrames(x)
}

t=testX()