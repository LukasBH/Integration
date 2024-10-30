library(RPostgres)
library(DBI)
library(tidyverse)
library(httr2)
library(lubridate)
# Investigate which symbols we can search for -----------------------------
req <- request("https://alpha-vantage.p.rapidapi.com") %>%
  req_url_path("query") %>%
  req_url_query("keywords" = "Microsoft",
                "function" = "SYMBOL_SEARCH",
                "datatype" = "json") %>%
  req_headers('X-RapidAPI-Key' = 'b1974985e6msh21f518f4fea290bp189d01jsnbcf11a089d57',
              'X-RapidAPI-Host' = 'alpha-vantage.p.rapidapi.com') 
resp <- req %>% 
  req_perform() 
symbols <- resp %>%
  resp_body_json()
symbols$bestMatches[[1]]
symbols$bestMatches[[2]]

# Extract and Transform  ----------------------------------------------------
# Extract data from Alpha Vantage
req <- request("https://alpha-vantage.p.rapidapi.com") %>%
  req_url_path("query") %>%
  req_url_query("interval" = "1min",
                "function" = "TIME_SERIES_INTRADAY",
                "symbol" = "MSFT",
                "datatype" = "json",
                "output_size" = "compact") %>%
  req_headers('X-RapidAPI-Key' = 'b1974985e6msh21f518f4fea290bp189d01jsnbcf11a089d57',
              'X-RapidAPI-Host' = 'alpha-vantage.p.rapidapi.com') 
resp <- req %>% 
  req_perform() 
dat <- resp %>%
  resp_body_json()

# TRANSFORM timestamp to UTC time
timestamp <- lubridate::ymd_hms(names(dat$`Time Series (1min)`), tz = "US/Eastern")
timestamp <- format(timestamp, tz = "UTC")
# Prepare data.frame to hold results
df <- tibble(timestamp = timestamp,
                 open = NA, high = NA, low = NA, close = NA, volume = NA)
# TRANSFORM data into a data.frame
for (i in 1:nrow(df)) {
  df[i,-1] <- as.data.frame(dat$`Time Series (1min)`[[i]])
}

# Create table in Postgres -------------------------------------------------
# Put the credentials in this script
# Never push credentials to git!! --> use .gitignore on .credentials.R
source("credentials.R")
# Function to send queries to Postgres
source("psql_queries.R")
# Create a new schema in Postgres on docker
psql_manipulate(cred = cred_psql_docker, 
                query_string = "CREATE SCHEMA intg2;")
# Create a table in the new schema 
psql_manipulate(cred = cred_psql_docker, 
                query_string = 
                  "create table intg2.prices (
	id serial primary key,
	timestamp timestamp(0) without time zone ,
	open numeric(30,4),
	high numeric(30,4),
	low numeric(30,4),
	close numeric(30,4),
	volume numeric(30,4));")

# LOAD price data ---------------------------------------------------------------
psql_append_df(cred = cred_psql_docker,
               schema_name = "intg2",
               tab_name = "prices",
               df = df)

# Check results ---------------------------------------------------------------
# Check that we can fetch the data again
psql_select(cred = cred_psql_docker, 
            query_string = 
              "select * from intg2.prices")
# If you wish, your can delete the schema (all the price data) from Postgres 
psql_manipulate(cred = cred_psql_docker, 
                query_string = "drop SCHEMA intg2 cascade;")


# Exercises ---------------------------------------------------------------
####Exercise 4 Go to rapidapi.com and subscribe to the ”Alpha Vantage” API and get your unique API key####
    #0f09d584fcmsh894b66cebeba7ebp18e400jsn75b6f7252297

####Exercise 5 Using the ”Alpha Vantage” API:####
  #Search for another stock (preferably given in "UTC-04” timezone – which corresponds to the"US/Eastern” timezone), 
  #find its symbol, and fetch its intraday price data with an interval of 60 minutes. 
req2 <- request("https://alpha-vantage.p.rapidapi.com") %>%
  req_url_path("query") %>%
  req_url_query("keywords" = "Novo Nordisk",
                "function" = "SYMBOL_SEARCH",
                "datatype" = "json") %>%
  req_headers('X-RapidAPI-Key' = '0f09d584fcmsh894b66cebeba7ebp18e400jsn75b6f7252297',
              'X-RapidAPI-Host' = 'alpha-vantage.p.rapidapi.com') 
resp2 <- req2 %>% 
  req_perform() 
symbols2 <- resp2 %>%
  resp_body_json()
#only 1 match as shown in the environment list in r to the right
symbols2$bestMatches[[1]]

# Extract data from Alpha Vantage on novo nordisk
req2 <- request("https://alpha-vantage.p.rapidapi.com") %>%
  req_url_path("query") %>%
  req_url_query("interval" = "60min",
                "function" = "TIME_SERIES_INTRADAY",
                "symbol" = "NVO",
                "datatype" = "json",
                "output_size" = "compact") %>%
  req_headers('X-RapidAPI-Key' = '0f09d584fcmsh894b66cebeba7ebp18e400jsn75b6f7252297',
              'X-RapidAPI-Host' = 'alpha-vantage.p.rapidapi.com') 

resp2 <- req2 %>% 
  req_perform() 
dat2 <- resp2 %>%
  resp_body_json()

####Exercise 6. Transform the fetched data into an R data.frame where time is given in UTC.####
  #Create a PostgreSQL table with the columns: ”id”, ”timestamp”, ”close”, and ”volume”, and load the corresponding price data into this table.

# TRANSFORM timestamp to UTC time
timestamp2 <- lubridate::ymd_hms(names(dat2$`Time Series (60min)`), tz = "US/Eastern")
timestamp2 <- format(timestamp2, tz = "UTC")
# Prepare data.frame to hold results
df2 <- tibble(timestamp = timestamp2,
             close = NA, volume = NA)
# TRANSFORM data into a data.frame
for (i in 1:nrow(df2)) {
  df2[i,-1] <- as.data.frame(dat2$`Time Series (60min)`[[i]][c(4,5)])
}

#Create table for novo prices
psql_manipulate(cred = cred_psql_docker,
                query_string = 
                  "create table intg2.novo_prices (
                    id serial primary key,
                    timestamp timestamp(0) without time zone ,
                    close numeric(30,4),
                    volume numeric(30,4));")
#load data into novo_prices
psql_append_df(cred = cred_psql_docker,
               schema_name = "intg2",
               tab_name = "novo_prices",
               df = df2)
# to reset the novo_prices table and restart auto-increment of primary key, if append wrong first
psql_manipulate(cred = cred_psql_docker,
                query_string = 
                  "truncate table intg2.novo_prices restart identity;")
psql_select(cred = cred_psql_docker,
            query_string = 
              "select * from intg2.novo_prices;")
