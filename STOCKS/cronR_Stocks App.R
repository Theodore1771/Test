getwd()
setwd("/home/rstudio/STOCKS APP/STOCKS")
getwd()


library(cronR)

# Add Cron job for fetching price data 

cmd <- cron_rscript(rscript = "increment_app_1.R")
# Must run every 15 minutes

cron_add(cmd, frequency = '1 * * * *', id = 'jobStocks')
#remove previous job3 with cron_rm(id = 'job3)
cron_rm(id = "jobStocks")

# Check the schedule
cron_ls()

#cron_clear()
cron_clear()    #' *ONLY* if you want to delete the cron jobs


