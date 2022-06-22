
library(xml2)
library(arrow)


get.df <- function(l, table.name) {
  if(table.name %in% names(l)) {
    return(which(names(l)==table.name))
  }
  else return(NULL)
}


# check.all: Unique ID across all tables or only in relation to current table?
# table.name: Table for which ID is generated
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


serial.df <- function(l, elem.df, df.name, record, prefix.primary, prefix.foreign) {
  serial <- c()
  elem.df <- data.frame(lapply(elem.df, as.character), stringsAsFactors = FALSE)
  for(i in 1:NCOL(elem.df)) {
    if(stringr::str_sub(names(elem.df)[i], 1, nchar(prefix.foreign)) == prefix.foreign) {
      table.name <- stringr::str_replace(names(elem.df)[i], prefix.foreign, "")
      df.sub <- l[[get.df(l, table.name)]]
      if(is.null(df.sub)) {
        return(NA)
      }
      else {
        if(!is.na(elem.df[record, i])) {
          serial <- append(serial, tidyr::replace_na(
            serial.df(l, df.sub, table.name, which(df.sub[, paste0(prefix.primary, table.name)] == elem.df[record, i]), prefix.primary, prefix.foreign),"0"))
        }
      }
    }
    else {
      if(stringr::str_sub(names(elem.df)[i], 1, nchar(prefix.primary)) != prefix.primary) {
        serial <- append(serial, tidyr::replace_na(elem.df[record, i], "0"))
        names(serial)[NROW(serial)] <- paste0(df.name, "@", names(elem.df)[i])
      }
    }
  }
  if(NROW(serial) > 0) {
    return(tidyr::replace_na(serial[order(names(serial))],"0"))
  }
  else return(NA)
}


serial.xml <- function(obj) {
  serial <- c()
  chdr <- xml2::xml_children(obj)
  if(length(chdr) > 0) {
    for(i in 1:length(chdr)) {
      if(length(xml2::xml_children(chdr[i])) > 0) serial <- append(serial, serial.xml(chdr[i]))
      else {
        ctn <- as.character(xml2::xml_contents(chdr[i]))
        
        # if(identical(ctn, character(0))) ctn <- NA
        # serial <- append(serial, tidyr::replace_na(ctn, "0"))
        # fix bug: Error in vec_assign(data, missing, replace, x_arg = "data", value_arg = "replace") : 
        # Can't convert `replace` <character> to match type of `data` <logical>.
        
        if(identical(ctn, character(0))) ctn <- "0"
        serial <- append(serial, ctn)
        
        names(serial)[NROW(serial)] <- paste0(xml2::xml_name(obj), "@", xml2::xml_name(chdr[i]))
      }
    }
    return(serial[order(names(serial))])
  }
  #return(serial[order(names(serial))])
}


find.object <- function(l, obj, prefix.primary, prefix.foreign) {
  ex <- NULL
  elem <- get.df(l, xml2::xml_name(obj))
  if(is.null(elem)) ex <- NULL
  else {
    elem.df <- l[[elem]]
    for(i in 1:NROW(elem.df)) {
      res.df <- serial.df(l, elem.df, xml2::xml_name(obj), i, prefix.primary, prefix.foreign)
      res.xml <- serial.xml(obj)
      if(NROW(res.df) == NROW(res.xml)) {
        if(sum(tidyr::replace_na(res.df, "0") == tidyr::replace_na(res.xml, "0")) - NROW(res.df) == 0) {
          ex <- elem.df[i,paste0(prefix.primary,xml2::xml_name(obj))]
          break
        }
      }
    }
  }
  return(ex)
}


parseXMLNode <- function(parent, envir, first = FALSE, prefix.primary, prefix.foreign, keys.unique, keys.dim) {
  if(first == TRUE) {
    xml2relational <- new.env(parent = baseenv())
    rlang::env_bind(xml2relational, ldf=list())
    parseXMLNode(parent, xml2relational, FALSE, prefix.primary, prefix.foreign, keys.unique, keys.dim)
  }
  else {
    obj.name <- xml2::xml_name(parent)
    if (obj.name=='ItemData') {
      return 
    }
    chdr <- xml2::xml_children(parent)
    # Does parent have children, i.e. is parent an object?
    if(TRUE) { #if(length(chdr) > 0) {
      elem <- get.df(envir$ldf, obj.name)
      # Is there no dataframe for the parent?
      if(is.null(elem)) {
        # Create new dataframe
        print(paste0(Sys.time(), ": adding table for element type: ", obj.name))
        envir$ldf[[length(envir$ldf)+1]] <- data.frame()
        names(envir$ldf)[length(envir$ldf)] <- obj.name
        
        # Create record in dataframe
        id.name <- paste0(prefix.primary, obj.name)
        envir$ldf[[length(envir$ldf)]][1,id.name] <- 0
        id.value <- create.id(envir$ldf, obj.name, keys.unique, prefix.primary)
        envir$ldf[[length(envir$ldf)]][1,id.name] <- id.value
        
        xa = xml_attrs(parent)
        if (is.null(names(xa))) {
          ddf = t(as.data.frame(xa))
          for (a in 1:length(colnames(ddf))) {
            envir$ldf[[length(envir$ldf)]][1, colnames(ddf)[a]] = ddf[a]
          }
        } else {
          for (j in 1:length(xa)) {
            envir$ldf[[length(envir$ldf)]][1, names(xml_attrs(parent))[j]] = xml_attrs(parent)[j]
          }
        }
        
        if(length(chdr) > 0) {
          for(i in 1:length(chdr)) {
            if(length(xml2::xml_children(chdr[i])) > 0) envir$ldf[[get.df(envir$ldf, obj.name)]][1, paste0(prefix.foreign, xml2::xml_name(chdr[i]))] <- parseXMLNode(chdr[i], envir, FALSE, prefix.primary, prefix.foreign, keys.unique, keys.dim)$value
            else envir$ldf[[get.df(envir$ldf, obj.name)]][1, xml2::xml_name(chdr[i])] <- parseXMLNode(chdr[i], envir, FALSE, prefix.primary, prefix.foreign, keys.unique, keys.dim)$value
          }
        }
        return(list(ldf=envir$ldf, value=id.value))
      }
      # dataframe for the object exists already
      else {
        res <- find.object(envir$ldf, parent, prefix.primary, prefix.foreign)
        #elem <- get.df(envir$ldf, obj.name)
        # Is parent not yet captured in dataframe?
        if(is.null(res)) {
          id.name <- paste0(prefix.primary, obj.name)
          id.value <- create.id(envir$ldf, obj.name, TRUE, prefix.primary, keys.dim)
          new.index <- NROW(envir$ldf[[elem]]) + 1
          envir$ldf[[elem]][new.index,id.name] <- id.value
          
          xa = xml_attrs(parent)
          
          # hack for strange r issue where names(xml_attrs(parent)) returns NULL
          # TODO: need to fix bug where all properties are not being set because of wrong loop j 
          if (is.null(names(xa))) {
            ddf = t(as.data.frame(xa))
            for (a in 1:length(colnames(ddf))) {
              envir$ldf[[length(envir$ldf)]][new.index, colnames(ddf)[a]] = ddf[a]
            }
            # debug
            if (obj.name=='ItemDef') {
              notvary = NULL
            }
          } else {
            for (j in 1:length(xa)) {
              envir$ldf[[length(envir$ldf)]][new.index, names(xa)[j]] = xa[j]
            }
          }
          
          if(length(chdr) > 0) {
            for(i in 1:length(chdr)) {
              if(length(xml2::xml_children(chdr[i])) > 0) envir$ldf[[get.df(envir$ldf, obj.name)]][new.index, paste0(prefix.foreign, xml2::xml_name(chdr[i]))] <- parseXMLNode(chdr[i], envir, FALSE, prefix.primary, prefix.foreign, keys.unique, keys.dim)$value
              else envir$ldf[[get.df(envir$ldf, obj.name)]][new.index, xml2::xml_name(chdr[i])] <- parseXMLNode(chdr[i], envir, FALSE, prefix.primary, prefix.foreign, keys.unique, keys.dim)$value
            }
          }
          return(list(ldf=envir$ldf, value=id.value))
        }
        # Return ID of existing parent entry in dataframe
        else return(list(ldf=envir$ldf, value=res))
      }
    }
    # parent is not an object
    else {
      res <- as.character(xml2::xml_contents(parent))
      
      xa = xml_attrs(parent)
      
      # TODO: add new index logic for leaf nodes with attributes
      
      id.name <- paste0(prefix.primary, obj.name)
      id.value <- create.id(envir$ldf, obj.name, TRUE, prefix.primary, keys.dim)
      new.index <- NROW(envir$ldf[[elem]]) + 1
      envir$ldf[[elem]][new.index,id.name] <- id.value
      
      if (is.null(names(xa))) {
        ddf = t(as.data.frame(xa))
        for (a in 1:length(colnames(ddf))) {
          envir$ldf[[length(envir$ldf)]][new.index, colnames(ddf)[a]] = ddf[a]
        }
      } else {
        for (j in 1:length(xa)) {
          envir$ldf[[length(envir$ldf)]][new.index, names(xml_attrs(parent))[j]] = xml_attrs(parent)[j]
        }
      }
      if(length(res) > 0) return(list(ldf=envir$ldf, value=res))
      else return(list(ldf=envir$ldf, value=NA))
    }
  }
}

toRelational <- function(file, prefix.primary = "ID_", prefix.foreign = "FKID_", keys.unique = TRUE, keys.dim = 6) {
  x <- xml2::read_xml(file)
  p <- xml2::xml_root(x)
  print(paste0(Sys.time(), ": calling parseXMLNode from the top"))
  return(parseXMLNode(p, NULL, TRUE, prefix.primary, prefix.foreign, keys.unique, keys.dim)$ldf)
}


check.datetimeformats <- function(vec, funcs, return.convertfunc = FALSE, tz = "UTC") {
  conv <- list()
  for(i in 1:length(funcs)) {
    conv[[length(conv)+1]] <- suppressWarnings(funcs[[i]](vec, tz=tz))
  }
  if(return.convertfunc) {
    if(max(unlist(lapply(conv, function(x){sum(!is.na(x))}))) == NROW(vec[!is.na(vec)])) {
      return(funcs[unlist(lapply(conv, function(x){sum(!is.na(x))})) == NROW(vec[!is.na(vec)])][[1]])
    }
    else {
      return(NULL)
    }
  }
  else {
    return(max(unlist(lapply(conv, function(x){sum(!is.na(x))}))))
  }
}


convertible.datetime <- function(vec, return.convertfunc = FALSE, tz = "UTC") {
  res <- ""
  vec <- as.character(vec)
  has.time <- sum(stringr::str_detect(vec, ":"), na.rm=TRUE) == NROW(vec[!is.na(vec)])
  funcs <- list(lubridate::ymd_hms, lubridate::ymd_hm, lubridate::ymd_h, lubridate::dmy_hms, lubridate::dmy_hm, lubridate::dmy_h, lubridate::mdy_hms, lubridate::mdy_hm, lubridate::mdy_h)
  if(has.time & check.datetimeformats(vec, funcs, FALSE, tz) == NROW(vec[!is.na(vec)])) {
    if(return.convertfunc) res <- check.datetimeformats(vec, funcs, TRUE, tz)
    else res <- "DateTime"
  }
  else {
    funcs <- list(lubridate::ymd, lubridate::dmy, lubridate::mdy)
    if(check.datetimeformats(vec, funcs, FALSE, tz) == NROW(vec[!is.na(vec)])) {
      if(return.convertfunc) res <- check.datetimeformats(vec, funcs, TRUE, tz)
      else res <- "Date"
    }
    else {
      funcs <- list(lubridate::hms, lubridate::hm, lubridate::ms)
      if(has.time & check.datetimeformats(vec, funcs, FALSE, tz) == NROW(vec[!is.na(vec)])) {
        if(return.convertfunc) res <- check.datetimeformats(vec, funcs, TRUE, tz)
        else res <- "Time"
      }
    }
  }
  return(res)
}


convertible.num <- function(vec) {
  vec[vec == ""] <- NA
  return(sum(is.na(vec)) == sum(is.na(suppressWarnings(as.numeric(vec)))))
}


convertible.double <- function(vec) {
  vec <- vec[!is.na(vec)]
  return(!(sum((as.numeric(vec) %% 1 == 0)) == NROW(vec)))
}


convertible.enum <- function(vec, max.ratio = 0.25) {
  vec <- vec[!is.na(vec)]
  if(NROW(vec) > 0) return(NROW(unique(vec))/NROW(vec) <= max.ratio)
  else return(FALSE)
}


is.nullable <- function(vec) {
  return(sum(is.na(vec)) > 0)
}


infer.datatype <- function(vec, bib, sql.style, tz = "UTC") {
  convert.dt <- convertible.datetime(vec, FALSE, tz)
  if(convert.dt != "") {
    return(as.character(bib[bib$Style == sql.style, convert.dt]))
  }
  else {
    if(convertible.num(vec)) {
      # numeric
      if(!convertible.double(vec)) {
        # integer
        vec <- vec[!is.na(vec)]
        max.limit <- bib[bib$Style == sql.style, "Int.MaxSize"]
        if(max(vec) <= max.limit-1 & min(vec) >= (-1)*max.limit) return(as.character(bib[bib$Style == sql.style, "Int"]))
        else return(as.character(bib[bib$Style == sql.style, "BigInt"]))
      }
      else {
        # floating point number
        vec.char <- as.character(vec)
        dec.pos <- stringr::str_locate(vec.char, "\\.")[,2]
        dec.pos.before <- dec.pos - 1
        dec.pos.before[is.na(dec.pos.before)] <- stringr::str_length(vec.char[is.na(dec.pos.before)])
        vec.char.before <- stringr::str_sub(vec.char, 1, dec.pos.before)
        s <- max(stringr::str_length(stringr::str_sub(vec.char, dec.pos + 1, stringr::str_length(vec.char))), na.rm=TRUE)
        p <- max(nchar(vec.char.before)) + s
        return(paste0(bib[bib$Style == sql.style, "Decimal"], "(", p, ",", s, ")"))
      }
    }
    else {
      max.size <- max(nchar(as.character(vec)), na.rm = TRUE)
      if(max.size > bib[bib$Style == sql.style, "VarChar.MaxSize"]) return (as.character(bib[bib$Style == sql.style, "Text"]))
      else return(paste0(bib[bib$Style == sql.style, "VarChar"], "(",max.size, ")"))
    }
  }
}

getCreateSQL <- function(ldf, sql.style = "MySQL", tables = NULL, prefix.primary = "ID_", prefix.foreign = "FKID_", line.break ="\n", datatype.func = NULL, one.statement = FALSE) {
  sql.stylebib <- data.frame(list(
    Style = c("MySQL", "TransactSQL", "Oracle"),
    NormalField = c("%FIELDNAME% %DATATYPE%","%FIELDNAME% %DATATYPE%","%FIELDNAME% %DATATYPE%"),
    NormalFieldNotNull = c("%FIELDNAME% %DATATYPE% NOT NULL", "%FIELDNAME% %DATATYPE% NOT NULL", "%FIELDNAME% %DATATYPE% NOT NULL"),
    PrimaryKey = c("PRIMARY KEY (%FIELDNAME%)", "%FIELDNAME% %DATATYPE% PRIMARY KEY", "%FIELDNAME% %DATATYPE% PRIMARY KEY"),
    ForeignKey = c("FOREIGN KEY (%FIELDNAME%) REFERENCES %REFTABLE%(%REFPRIMARYKEY%)", "%FIELDNAME% %DATATYPE% REFERENCES %REFTABLE%(%REFPRIMARYKEY%)", "%FIELDNAME% %DATATYPE% REFERENCES %REFTABLE%(%REFPRIMARYKEY%)"),
    PrimaryKeyDefSeparate = c(TRUE, FALSE, FALSE),
    ForeignKeyDefSeparate = c(TRUE, FALSE, FALSE),
    Int = c("INT", "int", "NUMBER"),
    Int.MaxSize = c(2147483648, 2147483648, 1),
    BigInt = c("BIGINT", "bigint", "NUMBER"),
    Decimal = c("DECIMAL", "decimal", "NUMBER"),
    VarChar = c("VARCHAR", "varchar", "VARCHAR2"),
    VarChar.MaxSize = c(65535, 8000, 4000),
    Text = c("TEXT", "varchar(max)", "LONG"),
    Date = c("DATE", "date", "DATE"),
    DateTime = c("TIMESTAMP", "datetime2", "TIMESTAMP"),
    Time = c("TIME", "TIME", "VARCHAR2(20)")
  ), stringsAsFactors = FALSE)
  
  if(is.data.frame(sql.style)) {
    sql.stylebib <- rbind(sql.stylebib, sql.style)
    sql.style = sql.stylebib[1,1]
  }
  else {
    if(!sql.style %in% sql.stylebib[,1]) stop(paste0("'", sql.style, "' is not a valid SQL flavor. Valid flavors are ",
                                                     paste0("'", sql.stylebib[,1], "'", collapse = ",")), ".\n")
  }
  
  if(is.null(tables)) tabs <- 1:length(ldf)
  else {
    tabs <- c()
    for(i in 1:NROW(tables)) {
      if(tables[i] %in% names(ldf)) {
        tabs <- append(tabs, get.df(ldf, tables[i]))
      }
      else warning(paste0("Table '", tables[i], "' does not exist in your data model. Valid tables names are ",
                          paste0("'", names(ldf), "'", collapse = ",")), ".\n")
    }
  }
  
  sql.code <- c()
  for(i in 1:NROW(tabs)) {
    df <- ldf[[get.df(ldf, names(ldf)[tabs[i]])]]
    df <- data.frame(lapply(df, as.character), stringsAsFactors = FALSE)
    sql.code[i] <- paste0("CREATE TABLE ", names(ldf)[tabs[i]], " (", line.break)
    for(f in 1:NCOL(df)) {
      if(f != 1) sql.code[i] <- paste0(sql.code[i], ", ")
      if(is.null(datatype.func)) datatype <- infer.datatype(df[,f], sql.stylebib, sql.style)
      else datatype = datatype.func(df[,f])
      field <- names(df)[f]
      reftable <- ""
      if(field==paste0(prefix.primary, names(ldf)[tabs[i]])) {
        # primary
        sql.code[i] <- paste0(sql.code[i], sql.stylebib[sql.stylebib$Style==sql.style, "PrimaryKey"])
        if(sql.stylebib[sql.stylebib$Style==sql.style, "PrimaryKeyDefSeparate"])
          sql.code[i] <- paste0(sql.code[i], line.break, ", ", sql.stylebib[sql.stylebib$Style==sql.style, "NormalField"])
      }
      else {
        if(stringr::str_sub(field, 1, nchar(prefix.foreign)) == prefix.foreign) {
          # foreign
          reftable <- stringr::str_replace_all(field, prefix.foreign, "")
          sql.code[i] <- paste0(sql.code[i], sql.stylebib[sql.stylebib$Style==sql.style, "ForeignKey"])
          if(sql.stylebib[sql.stylebib$Style==sql.style, "ForeignKeyDefSeparate"])
            sql.code[i] <- paste0(sql.code[i], line.break, ", ", sql.stylebib[sql.stylebib$Style==sql.style, "NormalField"])
        }
        else {
          # normal
          if(is.nullable(df[,f])) {
            # nullable
            sql.code[i] <- paste0(sql.code[i], sql.stylebib[sql.stylebib$Style==sql.style, "NormalField"])
          }
          else {
            # not nullable
            sql.code[i] <- paste0(sql.code[i], sql.stylebib[sql.stylebib$Style==sql.style, "NormalFieldNotNull"])
          }
        }
      }
      sql.code[i] <- stringr::str_replace_all(sql.code[i], "%FIELDNAME%", field)
      sql.code[i] <- stringr::str_replace_all(sql.code[i], "%DATATYPE%", datatype)
      sql.code[i] <- stringr::str_replace_all(sql.code[i], "%REFTABLE%", reftable)
      sql.code[i] <- stringr::str_replace_all(sql.code[i], "%REFPRIMARYKEY%", paste0(prefix.primary, reftable))
      sql.code[i] <- paste0(sql.code[i], line.break)
    }
    sql.code[i] <- paste0(sql.code[i], ");")
  }
  
  if(one.statement) sql.code <- paste0(sql.code, collapse = line.break)
  return(sql.code)
}


getInsertSQL <- function(ldf, table.name, line.break = "\n", one.statement = FALSE, tz = "UTC") {
  if(!table.name %in% names(ldf)) stop(paste0("Table '", table.name, "' does not exist in your data model. Valid tables names are ",
                                              paste0("'", names(ldf), "'", collapse = ",")), ".\n")
  tab <- ldf[[get.df(ldf, table.name)]]
  col.delimiter <- c()
  cols <- c()
  res <- c()
  for(f in 1:NCOL(tab)) {
    if(convertible.datetime(tab[,f], return.convertfunc = FALSE, tz=tz) != "") tab[,f] <- as.character(convertible.datetime(tab[,f], return.convertfunc = TRUE, tz=tz)(tab[,f]))
    
    if(!convertible.num(tab[,f])) col.delimiter[f] <- "'"
    else col.delimiter[f] <- ""
    cols <- append(cols, names(tab)[f])
  }
  cols <- paste0(names(tab), collapse = ", ")
  
  for(i in 1:NROW(tab)) {
    vals <- c()
    for(f in 1:NCOL(tab)) {
      if(!is.na(tab[i,f])) vals <- paste0(vals, ", ", col.delimiter[f], tab[i,f], col.delimiter[f])
      else vals <- paste0(vals, ", NULL")
    }
    vals <- stringr::str_sub(vals, 3, stringr::str_length(vals))
    res <- append(res, paste0("INSERT INTO ", table.name, "(", cols, ") VALUES (", vals, ");"))
  }
  if(one.statement) res <- paste0(res, collapse = line.break)
  return(res)
}



savetofiles <- function(ldf, dir, sep = ",", dec = ".") {
  if(dir != "") {
    if(!dir.exists(dir)) stop(paste0("Directory '", dir, "' does not exist."))
  }
  for(i in 1:length(ldf)) {
    tab <- ldf[[i]]
    for(f in 1:NCOL(tab)) {
      if(convertible.num(tab[,f])) tab[,f] <- as.numeric(tab[,f])
      else tab[,f] <- as.character(tab[,f])
    }
    utils::write.table(tab, file=fs::path(dir, paste0(names(ldf)[i], ".csv")), dec = dec, sep = sep)
  }
}

writeParquetToS3 = function(s3folderpath, xml2relout) {
  for (i in 1:length(xml2relout)) {
    df = as.data.frame(xml2relout[i])
    dfName = names(xml2relout)[i]
    outFileName = paste0(s3folderpath, dfName,"/")
    print(paste0(Sys.time(), ": writing dataframe ", dfName, " to ", outFileName))
    write_dataset(dataset=df, path=outFileName, format='parquet')
      
    # aws glue did not recognize any data rows from this call 
    #write_parquet(x=df, sink=outFileName, )
  }
}

initializeAWS = function() {
  #load environment context if github-hidden file exists
  if (file.exists("/users/justin/git/justin-viome/set_env.R")) {
    source("/users/justin/git/justin-viome/set_env.R")
  }
}
testParseNWrite = function() {
  initializeAWS()
  
  source("/Users/justin/git/justin-viome/StudyWork/odm/xml2relationaljt.R")
  odmfile= "/Users/justin/Downloads/V128_Pilot_odm_export_20220601012550.xml"
  smallrealFile = "/Users/justin/Downloads/V128_reduced.xml"
  
  
  t = toRelational(file = smallrealFile)
  
  s3testfolder = "s3://justin-viome/parquettest/v128/"
  writeParquetToS3(s3folderpath=s3testfolder, xml2relout= t)
}

testReadParquet = function() {
  s3testread = "s3://justin-viome/parquettest/v128/ItemData"
  
  dfin = read_parquet(file=s3testread)
  
}

testSampleDataset = function() {
  id1=t$ItemDef
  
  idsef=t$StudyEventDef
  
  
}



