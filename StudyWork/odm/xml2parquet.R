library(xml2)

get.df <- function(l, table.name) {
  if(table.name %in% names(l)) {
    return(which(names(l)==table.name))
  }
  else return(NULL)
}

# check.all: Unique ID across all tables or only in relation to current table?
# table.name: Table for which ID is generated
# TODO: this could be updated to use GUIDs or ints for IDs 
create.id <- function(l, table.name, check.all = TRUE, prefix.primary = "ID_", keys.dim = 6) {
  id <- NULL
  df.index <- get.df(l, table.name)
  if(df.index) {
    repeat {
      id <- round(stats::runif(1, 1, 10^keys.dim-1),0)
      if(check.all == TRUE) to.check <- names(l)
      else to.check <- table.name
      found <- FALSE
      for(i in 1:NROW(to.check)){
        if(id %in% l[[which(names(l)==to.check[i])]][, paste0(prefix.primary, to.check[i])]) found <- TRUE
      }
      if(found == FALSE) break
    }
  }
  return(id)
}

parseXMLNode <- function(parent, envir, first = FALSE, prefix.primary, prefix.foreign, keys.unique, keys.dim) {
  if(first == TRUE) {
    xml2relational <- new.env(parent = baseenv())
    rlang::env_bind(xml2relational, ldf=list())
    parseXMLNode(parent, xml2relational, FALSE, prefix.primary, prefix.foreign, keys.unique, keys.dim)
  }
  else {
    obj.name <- xml2::xml_name(parent)
    chdr <- xml2::xml_children(parent)
    # Does parent have children, i.e. is parent an object?
    if(length(chdr) > 0) {
      elem <- get.df(envir$ldf, obj.name)
      # Is there no dataframe for the parent?
      if(is.null(elem)) {
        # Create new dataframe
        df = data.frame()
        envir$ldf[[length(envir$ldf)+1]] <- df
        names(envir$ldf)[length(envir$ldf)] <- obj.name
        # Create record in dataframe
        id.name <- paste0(prefix.primary, obj.name)
        envir$ldf[[length(envir$ldf)]][1,id.name] <- 0
        id.value <- create.id(envir$ldf, obj.name, keys.unique, prefix.primary)
        envir$ldf[[length(envir$ldf)]][1,id.name] <- id.value
        
        # iterate through attributes and add new column to dataframe if necessary
        parseAttributesForNode(parent, df)
        attrs = xmlAttrs(parent)
      } else {
        
      }
    }
  }
}

parseAttributesForNode = function(node, df) {

      attrs=xml_attrs(node)
      for (i in 1:length(attrs)) {
        #add column names to data frame for missing attributes 
        if(!names(attrs[i]) %in% colnames(df)) {
          df
        }
      }
}

toRelational <- function(file, prefix.primary = "ID_", prefix.foreign = "FKID_", keys.unique = TRUE, keys.dim = 6) {
  x <- xml2::read_xml(file)
  p <- xml2::xml_root(x)
  return(parseXMLNode(p, NULL, TRUE, prefix.primary, prefix.foreign, keys.unique, keys.dim)$ldf)
}

testing = function() {
  largeFile="/Users/justin/Downloads/V128_Pilot_odm_export_20220601012550.xml"
  smallFile = "/Users/justin/Downloads/attribtest1.xml"
  
  #set up vars for main parseNode code 
  file=smallFile
  x <- xml2::read_xml(file)
  p <- xml2::xml_root(x)
  parent = p
  prefix.primary = "ID_"
  prefix.foreign = "FKID_"
  xml2relational <- new.env(parent = baseenv())
  rlang::env_bind(xml2relational, ldf=list())
  envir=xml2relational
  keys.unique = TRUE
  keys.dim = 7
  
  
}










