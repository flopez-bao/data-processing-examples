rm(list = ls())
library(aws.s3)
library(readxl)
library(paws)
library(jsonlite)
library(readr)
library(tidyr)
library(dplyr)

# connect ---------------------------------------

#Set the profile and region here
Sys.setenv(SECRET_NAME = Sys.getenv("SECRET_NAME"),
           AWS_DEFAULT_REGION = "us-east-1",
           AWS_REGION = "us-east-1")

svc <- secretsmanager()

#Put the name of the secret which contains the aws key info
see <- svc$get_secret_value(
  SecretId = Sys.getenv("SECRET_NAME")
)

see <- fromJSON(see$SecretString)

#Fill in the strings
Sys.setenv(AWS_ACCESS_KEY_ID = see$aws_access_key,
           AWS_SECRET_ACCESS_KEY = see$aws_secret_access_key,
           #AWS_PROFILE = Sys.getenv("AWS_PROFILE"),
           AWS_DEFAULT_REGION = "us-east-1",
           AWS_REGION = "us-east-1")

#Delete the secret info as its now an env variable
rm(see)
rm(svc)

# read from the main read bucket -----------------------------------------------
my_bucket <- Sys.getenv("TEST_BUCKET")

# Lists all of bucket contents, fill in your bucket
choices <- aws.s3::get_bucket(bucket = my_bucket)

# get just path names
choices <- lapply(choices, "[[", 1)

# get just file names
cleaned_choices <- lapply(choices, function(x) gsub(".*\\/", "", x))

# make dataframe of file names and path names
choices <- do.call(rbind, Map(data.frame, file_names = cleaned_choices,
                              path_names = choices, stringsAsFactors = FALSE))

# filter just files that end in txt or xlsx or csv
choices <- choices[grepl("txt$|xlsx$|csv$|xlsx$", choices$file_names), ]
print(choices)
# reset row names
rownames(choices) <- NULL

# filter out a list of the files you need --------------------------------------

# lets say recent psnu data for 10 countries (this grepl would need to be altered for different data sets)
choices_recent_countries <- subset(choices, grepl("MER_Structured_Datasets/Current_Frozen/PSNU_Recent/txt/", path_names))[1:5,]

# basic lapply process
my_data <- 
  lapply(choices_recent_countries$path_names, function(the_file) {
    
  print(the_file)
  
  # read the data
  data <- aws.s3::s3read_using(FUN = readr::read_delim, "|", escape_double = FALSE,
                               trim_ws = TRUE, col_types = readr::cols(.default = readr::col_character()
                               ), 
                               bucket = my_bucket,
                               object = the_file)
  
  # do something, in this case we will keep only HTS data and aggregagte count
  # some operation
  res <- data %>% filter(indicator == "HTS_TST") %>% 
    group_by(country, snu1) %>%
    tally()
  
  
}) %>% bind_rows()


head(my_data)



