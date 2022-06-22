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

    studyname='',

    initialize = function(studyname) {
      stopifnot(!is.null(studyname))
      stopifnot(nchar(studyname) > 0)
      self$studyname=studyname
    }
  )
)
