# justin.thomson@viome.com
# castor edc playaround

library(devtools)
install_github("castoredc/castoRedc")


library(castoRedc)

castor_api <- CastorData$new(key = Sys.getenv("CASTOR_KEY"), 
                             secret = Sys.getenv("CASTOR_SECRET"), 
                             base_url = "https://data.castoredc.com")