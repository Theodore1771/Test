getwd()
setwd("/home/rstudio/STOCKS APP/STOCKS")
getwd()



library(RPostgres)    ####_#####################################################
library(DBI)          ####_#####################################################
library(httr2)        ####_#####################################################
library(tidyverse)    ####_#####################################################
## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ##



## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ##
#### SOURCES SECTION WHICH REFERS TO .credentials.R & psql_queries.R ###########

#### __CREDENTIALS SOURCE ######################################################
source(".credentials.R")

#### __PSQL QUERIES SOURCE #####################################################
source("PSQL_queries.R")
## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ##



## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ##
#### PSQL SCHEMA - TABLE - VALUES CREATION THROUGH DOCKER/R/POSTGRES ###########

#### __SCHEMA CREATION #########################################################
psql_manipulate(cred = cred_psql_docker,                    #' __Change name__#
                query_string = "CREATE SCHEMA quotes;")


#### __TABLE I CREATION ########################################################
psql_manipulate(cred = cred_psql_docker,            #' __INPUT ACTUAL VALUES__#
                query_string =
                   "CREATE TABLE quotes.symbols (  
                    symbol_sk SERIAL PRIMARY KEY,
                    symbol VARCHAR (255),
                    name VARCHAR (255),
                    type VARCHAR (255),
                    region VARCHAR (255),
                    market_open_local_time VARCHAR (255),
                    market_close_local_time VARCHAR (255),
                    timezone VARCHAR (255),
                    currency VARCHAR (255));")


## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ##
#### __TABLE I VALUE INSERTION #################################################

psql_manipulate(cred = cred_psql_docker,
                query_string =
                   "INSERT INTO QUOTES.SYMBOLS
                      VALUES  (DEFAULT, 'MSFT', 'MICROSOFT CORPORATION', 'EQUITY', 'UNITED STATES', '09:30', '16:00', 'UTC-04', 'CURRENCY'),
                              (DEFAULT, 'TSLA', 'TESLA INC', 'EQUITY', 'UNITED STATES', '09:30', '16:00', 'UTC-04', 'CURRENCY'),
                              (DEFAULT, 'MDT', 'MEDTRONIC PLC', 'EQUITY', 'UNITED STATES', '09:30', '16:00', 'UTC-04', 'CURRENCY'),
                              (DEFAULT, 'PLTR', 'PALANTIR TECHNOLOGIES INC', 'EQUITY', 'UNITED STATES', '09:30', '16:00', 'UTC-04', 'CURRENCY')
                    ;")

#### ____MAKE SURE THE TABLE HAS BEEN CORRECTLY INSERTED #######################
psql_select(cred = cred_psql_docker,
            query_string = "SELECT * FROM QUOTES.SYMBOLS;")


## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ##



#### __TABLE II CREATION #######################################################
psql_manipulate(cred = cred_psql_docker,
                query_string = 
                   "create table quotes.prices (
                      price_sk serial primary key,
                      symbol_fk integer,
                      timestamp_utc timestamp(0) without time zone ,
                      open numeric(30,4),
                      high numeric(30,4),
                      low numeric(30,4),
                      close numeric(30,4),
                      volume numeric(30,4),
                      constraint fk_symbol foreign key (symbol_fk)
                      references quotes.symbols(symbol_sk))
                    ;")
##' _This table is used to integrate the_ incoming _values from the API_
## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ##




## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ##
#### PREPARE THE TABLES FOR EACH VALUE IN SQL TABLE AND API ####################

#### __Data API POPULATION IN SQL I ############################################
#### ____TABLE I POPULATION ####################################################
req <- request("https://alpha-vantage.p.rapidapi.com") %>%
  req_url_path("query") %>%
  req_url_query("interval" = "15min",
                "function" = "TIME_SERIES_INTRADAY",
                "symbol" = "MSFT",
                "datatype" = "json",
                "output_size" = "full") %>%
  req_headers('X-RapidAPI-Key' = cred_api_alpha_vantage,
              'X-RapidAPI-Host' = 'alpha-vantage.p.rapidapi.com')
resp <- req %>%
  req_perform()
dat <- resp %>%
  resp_body_json()


#### ____TIMESTAMP TRANSFORMATION ##############################################
timestamp <- lubridate::ymd_hms(names(dat$'Time Series (15min)'), tz = "US/Eastern")
timestamp <- format(timestamp, tz = "UTC")


#### ____DATA.FRAME PREPARATION ################################################
df <- tibble(timestamp_utc = timestamp,
             symbol_fk = 1,
             open = NA, high = NA, low = NA, close = NA, volume = NA)


#### ____DATA.FRAME DATA TRANSFORMATION ########################################
for (i in 1:nrow(df)) {
  df[i, -c(1,2)] <- as.data.frame(dat$'Time Series (15min)'[[i]])
}


#### ____LOAD DATA INTO TABLE ##################################################
psql_append_df(cred = cred_psql_docker,
               schema_name = "quotes",
               tab_name = "prices",
               df = df)

psql_select(cred = cred_psql_docker,
            query_string = "SELECT * FROM QUOTES.PRICES")
## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ##



## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ##
#### __Data API POPULATION IN SQL II ###########################################
#### ____TABLE II POPULATION ###################################################
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


#### ____TIMESTAMP TRANSFORMATION ##############################################
timestamp <- lubridate::ymd_hms(names(dat$'Time Series (15min)'), tz = "US/Eastern")
timestamp <- format(timestamp, tz = "UTC")


#### ____DATA.FRAME PREPARATION ################################################
df <- tibble(timestamp_utc = timestamp,
             symbol_fk = 2,
             open = NA, high = NA, low = NA, close = NA, volume = NA)


#### ____DATA.FRAME DATA TRANSFORMATION ########################################
for (i in 1:nrow(df)) {
  df[i, -c(1,2)] <- as.data.frame(dat$'Time Series (15min)'[[i]])
}


#### ____LOAD DATA INTO TABLE ##################################################
psql_append_df(cred = cred_psql_docker,
               schema_name = "quotes",
               tab_name = "prices",
               df = df)

psql_select(cred = cred_psql_docker,
            query_string = "SELECT * FROM QUOTES.PRICES")
## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ##



## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ##
#### __Data API POPULATION IN SQL III ##########################################
#### ____TABLE III POPULATION ##################################################
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


#### ____TIMESTAMP TRANSFORMATION ##############################################
timestamp <- lubridate::ymd_hms(names(dat$'Time Series (15min)'), tz = "US/Eastern")
timestamp <- format(timestamp, tz = "UTC")


#### ____DATA.FRAME PREPARATION ################################################
df <- tibble(timestamp_utc = timestamp,
             symbol_fk = 3,
             open = NA, high = NA, low = NA, close = NA, volume = NA)


#### ____DATA.FRAME DATA TRANSFORMATION ########################################
for (i in 1:nrow(df)) {
  df[i, -c(1,2)] <- as.data.frame(dat$'Time Series (15min)'[[i]])
}


#### ____LOAD DATA INTO TABLE ##################################################
psql_append_df(cred = cred_psql_docker,
               schema_name = "quotes",
               tab_name = "prices",
               df = df)

psql_select(cred = cred_psql_docker,
            query_string = "select * from quotes.prices")
## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ##



## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ##
#### __Data API POPULATION IN SQL IV ###########################################
#### ____TABLE IV POPULATION ###################################################
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


#### ____TIMESTAMP TRANSFORMATION ##############################################
timestamp <- lubridate::ymd_hms(names(dat$'Time Series (15min)'), tz = "US/Eastern")
timestamp <- format(timestamp, tz = "UTC")


#### ____DATA.FRAME PREPARATION ################################################
df <- tibble(timestamp_utc = timestamp,
             symbol_fk = 4,
             open = NA, high = NA, low = NA, close = NA, volume = NA)


#### ____DATA.FRAME DATA TRANSFORMATION ########################################
for (i in 1:nrow(df)) {
  df[i, -c(1,2)] <- as.data.frame(dat$'Time Series (15min)'[[i]])
}


#### ____LOAD DATA INTO TABLE ##################################################
psql_append_df(cred = cred_psql_docker,
               schema_name = "quotes",
               tab_name = "prices",
               df = df)

psql_select(cred = cred_psql_docker,
            query_string = "select * from quotes.prices")
## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ##




## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ##
# ********************                                   ********************* #
# **********************                               *********************** #
####                    ***  SRIPTS TO BE SCHEDULED ***                     ####
# **********************                               *********************** #
# ********************                                   ********************* #
## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ##

#' *SHOULD DO IN A SEPARATE .R FILE TO ADD TO A cronR SCHEDULE...*


library(RPostgres)
library(DBI)
library(httr2)
library(tidyverse)
library(lubridate)

#### __FOR DATA I SCHEDULING ###################################################
# Extracting the data I
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

# Data Transformation
timestamp <- lubridate::ymd_hms(names(dat$'Time Series (15min)'), tz = "US/Eastern")
timestamp <- format(timestamp, tz = "UTC")


# Data.frame Preparation
df <- tibble(symbol_fk = 1,
             timestamp_utc = timestamp,
             open = NA, high = NA, low = NA, close = NA, volume = NA)


# Data.frame Transformation
for (i in 1:nrow(df)) {
  df[i, -c(1,2)] <- as.data.frame(dat$'Time Series (15min)'[[i]])
}


# Get most recent datapoint from database
latest_tmstmp <- psql_select(cred = cred_psql_docker,
                             query_string = 
                               "select timestamp_utc
                                from quotes.prices
                                where symbol_fk = 1
                                order by timestamp_utc desc
                                limit 1;")

# Only new datapoint should be loaded to database
df <- df[df$timestamp_utc > latest_tmstmp[[1]],]

# Load price data
print(paste0(round(Sys.time()), ": Updating Microsoft prices"))

psql_append_df(cred = cred_psql_docker,
               schema_name = "quotes",
               tab_name = "prices",
               df = df)


#### __FOR DATA II SCHEDULING ##################################################
# Extracting the data I
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

# Data Transformation
timestamp <- lubridate::ymd_hms(names(dat$'Time Series (15min)'), tz = "US/Eastern")
timestamp <- format(timestamp, tz = "UTC")


# Data.frame Preparation
df <- tibble(symbol_fk = 2,
             timestamp_utc = timestamp,
             open = NA, high = NA, low = NA, close = NA, volume = NA)


# Data.frame Transformation
for (i in 1:nrow(df)) {
  df[i, -c(1,2)] <- as.data.frame(dat$'Time Series (15min)'[[i]])
}


# Get most recent datapoint from database
latest_tmstmp <- psql_select(cred = cred_psql_docker,
                             query_string = 
                               "select timestamp_utc
                                from quotes.prices
                                where symbol_fk = 2
                                order by timestamp_utc desc
                                limit 1;")

# Only new datapoint should be loaded to database
df <- df[df$timestamp_utc > latest_tmstmp[[1]],]

# Load price data
print(paste0(round(Sys.time()), ": Updating Tesla prices"))

psql_append_df(cred = cred_psql_docker,
               schema_name = "quotes",
               tab_name = "prices",
               df = df)


#### __FOR DATA III SCHEDULING #################################################
# Extracting the data I
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

# Data Transformation
timestamp <- lubridate::ymd_hms(names(dat$'Time Series (15min)'), tz = "US/Eastern")
timestamp <- format(timestamp, tz = "UTC")


# Data.frame Preparation
df <- tibble(symbol_fk = 3,
             timestamp_utc = timestamp,
             open = NA, high = NA, low = NA, close = NA, volume = NA)


# Data.frame Transformation
for (i in 1:nrow(df)) {
  df[i, -c(1,2)] <- as.data.frame(dat$'Time Series (15min)'[[i]])
}


# Get most recent datapoint from database
latest_tmstmp <- psql_select(cred = cred_psql_docker,
                             query_string = 
                               "select timestamp_utc
                                from quotes.prices
                                where symbol_fk = 3
                                order by timestamp_utc desc
                                limit 1;")

# Only new datapoint should be loaded to database
df <- df[df$timestamp_utc > latest_tmstmp[[1]],]

# Load price data
print(paste0(round(Sys.time()), ": Updating Medtronic prices"))

psql_append_df(cred = cred_psql_docker,
               schema_name = "quotes",
               tab_name = "prices",
               df = df)


#### __FOR DATA IV SCHEDULING ##################################################
# Extracting the data I
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

# Data Transformation
timestamp <- lubridate::ymd_hms(names(dat$'Time Series (15min)'), tz = "US/Eastern")
timestamp <- format(timestamp, tz = "UTC")


# Data.frame Preparation
df <- tibble(symbol_fk = 4,
             timestamp_utc = timestamp,
             open = NA, high = NA, low = NA, close = NA, volume = NA)


# Data.frame Transformation
for (i in 1:nrow(df)) {
  df[i, -c(1,2)] <- as.data.frame(dat$'Time Series (15min)'[[i]])
}


# Get most recent datapoint from database
latest_tmstmp <- psql_select(cred = cred_psql_docker,
                             query_string = 
                               "select timestamp_utc
                                from quotes.prices
                                where symbol_fk = 4
                                order by timestamp_utc desc
                                limit 1;")

# Only new datapoint should be loaded to database
df <- df[df$timestamp_utc > latest_tmstmp[[1]],]

# Load price data
print(paste0(round(Sys.time()), ": Updating Palantir prices"))

psql_append_df(cred = cred_psql_docker,
               schema_name = "quotes",
               tab_name = "prices",
               df = df)
## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ##




## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ##
#### TEST THE SCRIPTS TO BE SCHEDULED ##########################################

# Delete some observations from the table to see if it auto-updated
psql_manipulate(cred = cred_psql_docker,
                query_string = 
                  "delete 
                   from quotes.prices
                   where
                   price_sk = (Select price_sk
                               from quotes.prices
                               where symbol_fk = 1
                               order by timestamp_utc desc
                               limit 1);")

# Check most recent prices
psql_select(cred = cred_psql_docker,
            query_string = 
              "select *
               from quotes. prices
               where symbol_fk = 1
               order by timestamp_utc desc
               limit 5;")

# Run the scheduled function 1 time
source("/home/rstudio/STOCKS APP/STOCKS/API_to_db_scheduled.R")
# Check that things have been reinserted
psql_select(cred = cred_psql_docker,
            query_string = "select *
                            from quotes.prices
                            where symbol_fk = 1
                            order by timestamp_utc desc
                            limit 5;")
## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ##




## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ##
#### SETTING THE SCHEDULING ####################################################

library(cronR)

# Add Cron job for fetching price data 
cmd <- cron_rscript(rscript = "increment_app_1.R")

# Must run every 15 minutes
cron_add(cmd, frequency = '0,15,30,45 * * * *', id = 'jobStocks')

#remove previous job3 with cron_rm(id = 'job3)
cron_rm(id = "jobStocks")

# Check the schedule
cron_ls()

#cron_clear()
cron_clear()    #' *ONLY* if you want to delete the 


## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ##
#'  ............................................................................
#'  ............................................................................
#'  
#'  *Please refer to the* .credentials.R; API_to_db_scheduled.R  *files* .......
#'  ............................................................................
#'  ............................................................................
## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ##