# justin.thomson@viome.com

# R6 class to serve as basis for odm element data
# Separate environment is needed

library(R6)
library(arrow)
library(xml2)

# for now, just store critical tables
# metadata:
#odm, metadataversion, protocol, studyeventref, studyeventdef, formref, studyeventdef, formdef, itemgroupref, itemgroupdef, itemref, itemdef, codelistref,
#codelist, codelistitem
#clinical data:
#clinicaldata,subjectdata, studyeventdata, formdata, itemgroupdata, itemdata


odmObj <- R6Class("obmObj",
  public = list(
    #### ODM Data Frames ####
    # predefine data frames for all capturable elements. ---
    # add fk column second from the end for consistent setting logic

    # odm element is the top-level element and includes "data_updated_date" and a "parquet_write"date" fields.
    odm = setNames(data.frame(matrix(ncol = 19, nrow = 0)), c("pk_odm", "Description", "FileType", "Granularity", "Archival", "FileOID", "CreationDateTime", "PriorFileOID",
                                                              "AsOfDateTime", "ODMVersion", "Originator", "SourceSystem","SourceSystemVersion",
                                                              "ID", "xmlns", "data_updated_date", "parquet_write_date", "fk_none", "viomestudyname")),
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

    # special handling added for Label
    itemdef = setNames(data.frame(matrix(ncol = 11, nrow = 0)), c("pk_itemdef","OID","Name","Label","DataType","Length","SignificantDigits","Origin","Comment","fk_metadataversion", "viomestudyname")),

    # todo: fetch english value from translatedtext
    #question = setNames(data.frame(matrix(ncol = 4, nrow = 0)), c("pk_question", "Value", "fk_itemdef", "viomestudyname")),
    externalquestion = setNames(data.frame(matrix(ncol = 6, nrow = 0)), c("pk_externalquestion", "Dictionary", "Version", "Code", "fk_itemdef", "viomestudyname")),
    codelistref = setNames(data.frame(matrix(ncol = 4, nrow = 0)), c("pk_codelistref", "CodeListOID", "fk_itemdef", "viomestudyname")),
    #rangecheck = setNames(data.frame(matrix(ncol = 3, nrow = 0)), c("pk_rangecheck", "Comparator", "SoftHard", "fk_itemdef", "viomestudyname")),

    # methoddef = setNames(data.frame(matrix(ncol = 5, nrow = 0)), c("pk_methoddef","OID", "Name", "fk_metadataversion", "viomestudyname")),
    # basicDefinitions = setNames(data.frame(matrix(ncol = 3, nrow = 0)), c("pk_basicdefinitions","fk_study", "viomestudyname")),
    # measurementUnit = studyDescription = setNames(data.frame(matrix(ncol = 5, nrow = 0)), c("pk_measurementunit", "OID", "Name","fk_basicdefinitions", "viomestudyname")),
    # symbol = setNames(data.frame(matrix(ncol = 10, nrow = 0)), c("pk_symbol","fk_measurementunit", "viomestudyname")),
    # translatedtext= setNames(data.frame(matrix(ncol = 10, nrow = 0)), c("pk_translatedtext", "xml:lang", "text","fk_", "viomestudyname")),

    #special handling added for description
    codelist = setNames(data.frame(matrix(ncol = 7, nrow = 0)), c("pk_codelist","OID","Name", "Description", "DataType", "fk_metadataversion", "viomestudyname")),

    #special handling added for description
    codelistitem = setNames(data.frame(matrix(ncol = 7, nrow = 0)), c("pk_codelistitem","CodedValue", "Description", "Rank", "OrderNumber", "fk_codelist", "viomestudyname")),

    clinicaldata = setNames(data.frame(matrix(ncol = 5, nrow = 0)), c("pk_clinicaldata","StudyOID","MetadataVersionOID", "fk_odm", "viomestudyname")),
    subjectdata = setNames(data.frame(matrix(ncol = 4, nrow = 0)), c("pk_subjectdata","SubjectKey","fk_clinicaldata", "viomestudyname")),
    studyeventdata = setNames(data.frame(matrix(ncol = 5, nrow = 0)), c("pk_studyeventdata","StudyEventOID","StudyEventRepeatKey","fk_subjectdata", "viomestudyname")),
    formdata = setNames(data.frame(matrix(ncol = 5, nrow = 0)), c("pk_formdata","FormOID", "FormRepeatKey","fk_studyeventdata","viomestudyname")),
    itemgroupdata = setNames(data.frame(matrix(ncol = 5, nrow = 0)), c("pk_itemgroupdata","ItemGroupOID","itemgrouprepeatkey","fk_formdata", "viomestudyname")),
    itemdata = setNames(data.frame(matrix(ncol = 6, nrow = 0)), c("pk_itemdata","ItemOID","Value", "SubjectKey", "fk_itemgroupdata", "viomestudyname")),

    odmdfnames = c('odm', 'study', 'metadataversion', 'protocol', 'studyeventref', 'studyeventdef', 'formref', 'formdef', 'itemgroupref',
                   'itemgroupdef', 'itemdef', 'externalquestion', 'codelistref', 'codelist', 'codelistitem', 'clinicaldata', 'subjectdata',
                   'studyeventdata', 'formdata', 'itemgroupdata', 'itemdata'),

    #### Other Class Variables ####
    studyname = '',
    subjectkey = '', # store the subject key when parsing subjectdata so child itemdata elements can store it for faster joins
    data_updated_date=Sys.Date(),
    odmFileLocation = NULL,
    xmlDoc = NULL,
    xmlRoot = NULL,

    #### Class Methods ####
    # initialize can take either a local file or an s3 file location as input
    initialize = function(studyname=Sys.getenv("S3_ODM_STUDY_NAME"), odmFileLocation=Sys.getenv("S3_ODM_LOCATION")) {
      stopifnot(!is.null(studyname))
      stopifnot(nchar(studyname) > 0)
      self$studyname=studyname

      self$odmFileLocation=odmFileLocation

      if (startsWith(odmFileLocation, "s3://")) {
        xmlDoc=aws.s3::s3read_using(read_xml, object=odmFileLocation)
      } else {
        xmlDoc = read_xml(odmFileLocation)
      }
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

      # skip first column, PK, 2nd-to-last last column, FK, and final column, viomestudyname
      for (i in 2:(col.len-2)) {
        col = colNames[i]
        self[[nodename]][newparentid, col]= xml_attr(x=node, attr = col)
      }

      #set fk. For now, fk is always the second-to-last column in a given data frame
      if (!is.null(parentID)) {
        self[[nodename]][newparentid, col.len-1]=parentID
      }

      # set studyname for all rows
      self[[nodename]][newparentid, col.len] = self$studyname

      # special case handling
      if (nodename=='odm') {
        tdy = Sys.Date()
        self[[nodename]][newparentid, 'data_updated_date']=self$data_updated_date
        self[[nodename]][newparentid, 'parquet_write_date']=tdy
      } else if (nodename=='subjectdata') {
        self$subjectkey=xml_attr(x=node, attr='SubjectKey')
      } else if (nodename=='itemdata') {
        # set subjectkey at itemdata level
        self[[nodename]][newparentid, 'SubjectKey']=self$subjectkey
      } else if (nodename=='itemdef') {
        # set itemdef label to be question value under description
        # note: first value chosen if multiple translated text values exist for different xml:langs
        xc=xml_children(node)
        lbl = xml_text(xml_child(xc[which(xml_name(xc)=='Question')]))
        if (!is.na(lbl)) {
          self[[nodename]][newparentid, 'Label']=lbl
        }
      } else if (nodename =='codelist') {
          # note: first value chosen if multiple translated text values exist for different xml:langs
          xc=xml_children(node)
          dn=xc[which(xml_name(xc)=='Description')]
          if (length(dn) > 0) {
            descr = xml_text(xml_child(dn))
            if (!is.na(descr)) {
              self[[nodename]][newparentid, 'Description']=descr
            }
          }
        } else if (nodename == 'codelistitem') {
          # note: first value chosen if multiple translated text values exist for different xml:langs
          xc=xml_children(node)
          dn=xc[which(xml_name(xc)=='Decode')]
          if (length(dn) > 0) {
            descr = xml_text(xml_child(dn))
            if (!is.na(descr)) {
              self[[nodename]][newparentid, 'Description']=descr
            }
          }
        }

    },

    visitNode = function(parentID, node) {
      elemName = tolower(xml_name(node))

      # null returned when element is intentionally skipped
      if (!is.null(self[[elemName]])) {
        self$setAttributesForNode(parentID=parentID, nodename=elemName, node=node)
        newparentid=nrow(self[[elemName]])
        chdr <- xml_children(node)
        if(length(chdr) > 0) {
          for(i in 1:length(chdr)) {
            childnode= chdr[i]
            # could bypass skipped elements here rather than through additional call
            self$visitNode(parentID=newparentid,node=childnode)
          }
        }

      } else {
        #log this in the future when logging introduced
        #print(paste0("Node dataframe not found for element: ", elemName))
      }
    },

    parseODM = function() {
      print(paste0(Sys.time(), ": beginning parseODM using file: ", self$odmFileLocation))
      self$visitNode(NULL, self$xmlRoot)
      print(paste0(Sys.time(), ": parseODM complete"))
      print(paste0(Sys.time(), " Metadata Summary: ", nrow(self$formdef), " forms with ", nrow(self$itemdef), " fields"))
      print(paste0(Sys.time(), " Clinical Data Summary: ", nrow(self$subjectdata), " subjects with ", nrow(self$itemdata), " datapoints"))
    }

  )
)
