# justin.thomson@viome.com

# R6 class to serve as basis for odm element data
# Separate environment is needed

library(R6)

# for now, just store critical tables
# metadata:
#odm, metadataversion, protocol, studyeventref, studyeventdef, formref, studyeventdef, formdef, itemgroupref, itemgroupdef, itemref, itemdef, question, codelistref,
#codelist, codelistitem
#clinical data:
#clinicaldata,subjectdata, studyeventdata, formdata, itemgroupdata, itemdata

# add fk column second from the end for consistent setting logic
odmObj <- R6Class("obmObj",
  public = list(
    # use datatables to allow for in-place changes.Dataframes do not.
    odm = setNames(data.frame(matrix(ncol = 17, nrow = 0)), c("pk_odm", "Description", "FileType", "Granularity", "Archival", "FileOID", "CreationDateTime", "PriorFileOID",
                                                              "AsOfDateTime", "ODMVersion", "Originator", "SourceSystem","SourceSystemVersion",
                                                              "ID", "xmlns", "fk_none", "viomestudyname")),
    study = setNames(data.frame(matrix(ncol = 4, nrow = 0)), c("pk_study", "OID", "fk_odm", "viomestudyname")),
    #globalVariables = setNames(data.frame(matrix(ncol = 6, nrow = 0)), c("pk_globalvariables", "studyname", "studyDescription", "protocolname", "fk_study", "viomestudyname")),
    metadataversion = setNames(data.frame(matrix(ncol = 5, nrow = 0)), c("pk_metadataversion", "OID", "Version", "fk_study", "viomestudyname")),
    protocol= setNames(data.frame(matrix(ncol = 3, nrow = 0)), c("pk_protocol","fk_metadataversion", "viomestudyname")),
    studyeventref = setNames(data.frame(matrix(ncol = 6, nrow = 0)), c("pk_studyeventref", "StudyEventOID", "OrderNumber", "Mandatory", "fk_protocol", "viomestudyname")),

    studyeventdef= setNames(data.frame(matrix(ncol = 7, nrow = 0)), c("pk_studyeventdef", "OID", "Name", "Repeating", "Type", "fk_metadataversion", "viomestudyname")),
    formref = setNames(data.frame(matrix(ncol = 5, nrow = 0)), c("pk_formref", "FormOID", "Mandatory", "fk_studyeventdef", "viomestudyname")),

    formdef = setNames(data.frame(matrix(ncol = 6, nrow = 0)), c("pk_formdef", "OID", "Name", "Repeating", "fk_metadataversion", "viomestudyname")),
    itemgroupref= setNames(data.frame(matrix(ncol = 7, nrow = 0)), c("pk_itemgroupref", "itemgroupOID", "OrderNumber", "Mandatory", "CollectionExceptionConditionOID", "fk_formdef", "viomestudyname")),

    itemgroupdef = setNames(data.frame(matrix(ncol = 11, nrow = 0)), c("pk_itemgroupdef", "OID", "Name", "Repeating", "Domain", "Origin", "Role", "Purpose", "Comment","fk_metadataversion", "viomestudyname")),
    itemref = setNames(data.frame(matrix(ncol = 11, nrow = 0)), c("pk_itemref","ItemOID","OrderNumber","Mandatory","KeySequence","MethodOID","Role","RoleCodeListOID","CollectionExceptionConditionOID","fk_itemgroupdef", "viomestudyname")),

    itemdef = setNames(data.frame(matrix(ncol = 10, nrow = 0)), c("pk_itemdef","OID","Name","DataType","Length","SignificantDigits","Origin","Comment","fk_metadataversion", "viomestudyname")),

    # todo: fetch english value from translatedtext
    question = setNames(data.frame(matrix(ncol = 4, nrow = 0)), c("pk_question", "Value", "fk_itemdef", "viomestudyname")),
    externalquestion = setNames(data.frame(matrix(ncol = 6, nrow = 0)), c("pk_externalquestion", "Dictionary", "Version", "Code", "fk_itemdef", "viomestudyname")),
    codelistref = setNames(data.frame(matrix(ncol = 4, nrow = 0)), c("pk_codelistref", "CodeListOID", "fk_itemdef", "viomestudyname")),
    #rangecheck = setNames(data.frame(matrix(ncol = 3, nrow = 0)), c("pk_rangecheck", "Comparator", "SoftHard", "fk_itemdef", "viomestudyname")),

    # methoddef = setNames(data.frame(matrix(ncol = 5, nrow = 0)), c("pk_methoddef","OID", "Name", "fk_metadataversion", "viomestudyname")),
    # basicDefinitions = setNames(data.frame(matrix(ncol = 3, nrow = 0)), c("pk_basicdefinitions","fk_study", "viomestudyname")),
    # measurementUnit = studyDescription = setNames(data.frame(matrix(ncol = 5, nrow = 0)), c("pk_measurementunit", "OID", "Name","fk_basicdefinitions", "viomestudyname")),
    # symbol = setNames(data.frame(matrix(ncol = 10, nrow = 0)), c("pk_symbol","fk_measurementunit", "viomestudyname")),
    # translatedtext= setNames(data.frame(matrix(ncol = 10, nrow = 0)), c("pk_translatedtext", "xml:lang", "text","fk_", "viomestudyname")),

    codelist = setNames(data.frame(matrix(ncol = 6, nrow = 0)), c("pk_codelist","OID","Name","DataType", "fk_metadataversion", "viomestudyname")),

    # todo: should store value
    codelistitem = setNames(data.frame(matrix(ncol = 6, nrow = 0)), c("pk_codelistitem","CodedValue", "Rank", "OrderNumber", "fk_codelist", "viomestudyname")),

    clinicaldata = setNames(data.frame(matrix(ncol = 5, nrow = 0)), c("pk_clinicaldata","StudyOID","MetadataVersionOID", "fk_odm", "viomestudyname")),
    subjectdata = setNames(data.frame(matrix(ncol = 4, nrow = 0)), c("pk_subjectdata","SubjectKey","fk_clinicaldata", "viomestudyname")),
    studyeventdata = setNames(data.frame(matrix(ncol = 5, nrow = 0)), c("pk_studyeventdata","StudyEventOID","StudyEventRepeatKey","fk_subjectdata", "viomestudyname")),
    formdata = setNames(data.frame(matrix(ncol = 5, nrow = 0)), c("pk_formdata","FormOID", "FormRepeatKey","fk_studyeventdata","viomestudyname")),
    itemgroupdata = setNames(data.frame(matrix(ncol = 5, nrow = 0)), c("pk_itemgroupdata","ItemGroupOID","itemgrouprepeatkey","fk_formdata", "viomestudyname")),
    itemdata = setNames(data.frame(matrix(ncol = 5, nrow = 0)), c("pk_itemdata","ItemOID","Value","fk_itemgroupdata", "viomestudyname")),

    odmdfnames = c('odm', 'study', 'metadataversion', 'protocol', 'studyeventref', 'studyeventdef', 'formref', 'formdef', 'itemgroupref',
                   'itemgroupdef', 'itemdef', 'question', 'externalquestion', 'codelistref', 'codelist', 'codelistitem', 'clinicaldata', 'subjectdata',
                   'studyeventdata', 'formdata', 'itemgroupdata', 'itemdata'),

    studyname = '',
    xmlDoc = NULL,
    xmlRoot = NULL,

    initialize = function(studyname, xmlDoc) {
      stopifnot(!is.null(studyname))
      stopifnot(nchar(studyname) > 0)
      self$studyname=studyname

      stopifnot(!is.null(xmlDoc))
      self$xmlDoc = xmlDoc
      self$xmlRoot = xml_root(self$xmlDoc)
    },

    # set pk, set attributes, set fk
    setAttributesForNode = function(parentID, nodename, node) {

      #add row with nas then replace with values from input node
      newparentid = nrow(self[[nodename]])+1
      self[[nodename]][newparentid, c(paste0("pk_", nodename))] <- newparentid

      #only attempt to set values we care about via the dataframe definitions
      attrs = xml_attrs(node)
      colNames = colnames(self[[nodename]])
      col.len = length(colNames)

      # skip first column, PK, 2nd-to-last last column, FK,a nd final column, viomestudyname
      for (i in 2:(col.len-2)) {
        col = colNames[i]
        self[[nodename]][newparentid, col]= xml_attr(x=node, attr = col)
      }

      #set fk. For now, fk is always the second-to-last column in a given data frame
      if (!is.null(parentID)) {
        self[[nodename]][newparentid, col.len-1]=parentID
      }

      # set studyname for all rows
      self[[nodename]][newparentid, col.len] = s_viomestudyname

    },

    visitNode = function(parentID, node) {
      elemName = tolower(xml_name(node))
      if (length(elemName)==0) {
        browse()
        debugx = 3 #debugging
      }
      print(paste0("elemname processing: ", elemName))

      # null returned when element is intentionally skipped
      if (!is.null(self[[elemName]])) {
        self$setAttributesForNode(parentID=parentID, nodename=elemName, node=node)
        newparentid=nrow(self[[elemName]])
        chdr <- xml_children(node)
        if(length(chdr) > 0) {
          for(i in 1:length(chdr)) {
            childnode= chdr[i]
            self$visitNode(parentID=newparentid,node=childnode)
          }
        }

      } else {
        print(paste0("Node dataframe not found for element: ", elemName))
      }
    },

    parseODM = function() {
      self$visitNode(NULL, self$xmlRoot)
    },

    writeParquetToS3 = function(s3folderpath, bucketName="viome-studies") {

      # iterate through list of objects and write each to s3 even if they are empty

      outFileName = paste0("s3://", bucketName, s3folderpath, dfName)
      print(paste0(Sys.time(), ": writing dataframe ", dfName, " to ", outFileName))
      write_dataset(dataset=df, path=outFileName, format='parquet')

      # aws glue did not recognize any data rows from this call
      #write_parquet(x=df, sink=outFileName, )
      }
  )
)
