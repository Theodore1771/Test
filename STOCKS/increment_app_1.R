# Get the current datetime
date_time <- format(Sys.time(), digits = 0) 
# Check if "increment_one.rds" exists
if(file.exists("/home/rstudio/STOCKS APP/STOCKS/increment_app_1.rds")){
  # If "increment_one.rds exists then we read it into memory
  increment_app_1 <- readRDS(file = "/home/rstudio/STOCKS APP/STOCKS/increment_app_1.rds")
  # We add one to the R object
  increment_app_1 <- increment_app_1 + 1
  # The R object is saved to the disk
  saveRDS(increment_app_1, file = "/home/rstudio/STOCKS APP/STOCKS/increment_app_1.rds")  
  # We print the datetime and the value of increment_one.
  # This will be captured by the cronR logger and written to the .log file
  print(paste0(date_time, ": Value of increment_app_1.rds is ", increment_app_1))
}else{
  # If "increment_one.rds" does not exist we begin by 1
  increment_app_1 <- 1
  # The R object is saved to the disk
  saveRDS(increment_app_1, file = "/home/rstudio/STOCKS APP/STOCKS/increment_app_1.rds")  
  # We print the datetime and the value of increment_one.
  # This will be captured by the cronR logger and written to the .log file
  print(paste0(date_time, ": Value of increment_app_1.rds is ", increment_app_1))
}
