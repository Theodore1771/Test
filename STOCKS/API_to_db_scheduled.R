getwd()
setwd("/home/rstudio/STOCKS APP/STOCKS")
getwd()


library(RPostgres)    ####_#####################################################
library(DBI)          ####_#####################################################
library(httr2)        ####_#####################################################
library(tidyverse)    ####_#####################################################
library(lubridate)    ####_#####################################################



## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ##
#### EXTRACT DATA I ############################################################
#### __table I population ######################################################
req <- request("https://alpha-vantage.p.rapidapi.com") %>%
  req_url_path("query") %>%
  req_url_query("interval" = "15min",
                "function" = "TIME_SERIES_INTRADAY",
                "symbol" = "MSFT",
                "datatype" = "json",
                "output_size" = "compact") %>%
  req_headers('X-RapidAPI-Key' = cred_api_alpha_vantage,
              'X-RapidAPI-Host' = 'alpha-vantage.p.rapidapi.com') 
resp <- req %>% 
  req_perform() 
dat <- resp %>%
  resp_body_json()


#### __timestamp transformation ################################################
timestamp <- lubridate::ymd_hms(names(dat$`Time Series (15min)`), tz = "US/Eastern")
timestamp <- format(timestamp, tz = "UTC")


#### __data.frame preparation ##################################################
df <- tibble(symbol_fk = 1, 
             timestamp_utc = timestamp,
             open = NA, high = NA, low = NA, close = NA, volume = NA)


#### __data.frame data transformation ##########################################
for (i in 1:nrow(df)) {
  df[i,-c(1,2)] <- as.data.frame(dat$`Time Series (15min)`[[i]])
}


#### __Retrieve most recent data points ########################################
latest_tmstmp <- psql_select(cred = cred_psql_docker, 
                             query_string = 
                               "select timestamp_utc 
                                from quotes.prices
                                where symbol_fk = 1
                                order by timestamp_utc desc
                                limit 1;")


#### __Newest data points & loading data #######################################
df <- df[df$timestamp_utc > latest_tmstmp[[1]],]

print(paste0(round(Sys.time()), ": Updating Microsoft prices")) 

psql_append_df(cred = cred_psql_docker,
               schema_name = "quotes",
               tab_name = "prices",
               df = df)




## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ##
#### EXTRACT DATA II ###########################################################
#### __table II population #####################################################
req <- request("https://alpha-vantage.p.rapidapi.com") %>%
  req_url_path("query") %>%
  req_url_query("interval" = "15min",
                "function" = "TIME_SERIES_INTRADAY",
                "symbol" = "TSLA",
                "datatype" = "json",
                "output_size" = "compact") %>%
  req_headers('X-RapidAPI-Key' = cred_api_alpha_vantage,
              'X-RapidAPI-Host' = 'alpha-vantage.p.rapidapi.com') 
resp <- req %>% 
  req_perform() 
dat <- resp %>%
  resp_body_json()


#### __timestamp transformation ################################################
timestamp <- lubridate::ymd_hms(names(dat$`Time Series (15min)`), tz = "US/Eastern")
timestamp <- format(timestamp, tz = "UTC")


#### __data.frame preparation ##################################################
df <- tibble(symbol_fk = 2, 
             timestamp_utc = timestamp,
             open = NA, high = NA, low = NA, close = NA, volume = NA)


#### __data.frame data transformation ##########################################
for (i in 1:nrow(df)) {
  df[i,-c(1,2)] <- as.data.frame(dat$`Time Series (15min)`[[i]])
}


#### __Retrieve most recent data points ########################################
latest_tmstmp <- psql_select(cred = cred_psql_docker, 
                             query_string = 
                               "select timestamp_utc 
                                from quotes.prices
                                where symbol_fk = 2
                                order by timestamp_utc desc
                                limit 1;")


#### __Newest data points & loading data #######################################
df <- df[df$timestamp_utc > latest_tmstmp[[1]],]

print(paste0(round(Sys.time()), ": Updating Tesla prices")) 

psql_append_df(cred = cred_psql_docker,
               schema_name = "quotes",
               tab_name = "prices",
               df = df)




## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ##
#### EXTRACT DATA III ##########################################################
#### __table III population ####################################################
req <- request("https://alpha-vantage.p.rapidapi.com") %>%
  req_url_path("query") %>%
  req_url_query("interval" = "15min",
                "function" = "TIME_SERIES_INTRADAY",
                "symbol" = "MDT",
                "datatype" = "json",
                "output_size" = "compact") %>%
  req_headers('X-RapidAPI-Key' = cred_api_alpha_vantage,
              'X-RapidAPI-Host' = 'alpha-vantage.p.rapidapi.com') 
resp <- req %>% 
  req_perform() 
dat <- resp %>%
  resp_body_json()


#### __timestamp transformation ################################################
timestamp <- lubridate::ymd_hms(names(dat$`Time Series (15min)`), tz = "US/Eastern")
timestamp <- format(timestamp, tz = "UTC")


#### __data.frame preparation ##################################################
df <- tibble(symbol_fk = 3, 
             timestamp_utc = timestamp,
             open = NA, high = NA, low = NA, close = NA, volume = NA)


#### __data.frame data transformation ##########################################
for (i in 1:nrow(df)) {
  df[i,-c(1,2)] <- as.data.frame(dat$`Time Series (15min)`[[i]])
}


#### __Retrieve most recent data points ########################################
latest_tmstmp <- psql_select(cred = cred_psql_docker, 
                             query_string = 
                               "select timestamp_utc 
                                from quotes.prices
                                where symbol_fk = 3
                                order by timestamp_utc desc
                                limit 1;")


#### __Newest data points & loading data #######################################
df <- df[df$timestamp_utc > latest_tmstmp[[1]],]

print(paste0(round(Sys.time()), ": Updating Medtronic prices")) 

psql_append_df(cred = cred_psql_docker,
               schema_name = "quotes",
               tab_name = "prices",
               df = df)




## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ##
#### EXTRACT DATA IV ###########################################################
#### __table IV population #####################################################
req <- request("https://alpha-vantage.p.rapidapi.com") %>%
  req_url_path("query") %>%
  req_url_query("interval" = "15min",
                "function" = "TIME_SERIES_INTRADAY",
                "symbol" = "PLTR",
                "datatype" = "json",
                "output_size" = "compact") %>%
  req_headers('X-RapidAPI-Key' = cred_api_alpha_vantage,
              'X-RapidAPI-Host' = 'alpha-vantage.p.rapidapi.com') 
resp <- req %>% 
  req_perform() 
dat <- resp %>%
  resp_body_json()


#### __timestamp transformation ################################################
timestamp <- lubridate::ymd_hms(names(dat$`Time Series (15min)`), tz = "US/Eastern")
timestamp <- format(timestamp, tz = "UTC")


#### __data.frame preparation ##################################################
df <- tibble(symbol_fk = 4, 
             timestamp_utc = timestamp,
             open = NA, high = NA, low = NA, close = NA, volume = NA)


#### __data.frame data transformation ##########################################
for (i in 1:nrow(df)) {
  df[i,-c(1,2)] <- as.data.frame(dat$`Time Series (15min)`[[i]])
}


#### __Retrieve most recent data points ########################################
latest_tmstmp <- psql_select(cred = cred_psql_docker, 
                             query_string = 
                               "select timestamp_utc 
                                from quotes.prices
                                where symbol_fk = 4
                                order by timestamp_utc desc
                                limit 1;")


#### __Newest data points & loading data #######################################
df <- df[df$timestamp_utc > latest_tmstmp[[1]],]

print(paste0(round(Sys.time()), ": Updating Palantir Technologies prices")) 

psql_append_df(cred = cred_psql_docker,
               schema_name = "quotes",
               tab_name = "prices",
               df = df)



