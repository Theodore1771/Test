getwd()
setwd("/home/rstudio/STOCKS APP/STOCKS")
getwd()


library(shiny)
library(ggplot2)
library(DBI)
library(RPostgres)
source(".credentials.R")
source("PSQL_queries.R")


# Define UI
ui <- fluidPage(
  titlePanel("Stock Price Visualization"),
  actionButton("refreshButton", "Refresh Data"),
  fluidRow(
    column(6, plotOutput("teslaPlot")),
    column(6, plotOutput("microsoftPlot")),
    column(6, plotOutput("medtronicPlot")),
    column(6, plotOutput("palantirPlot"))
  )
)



# Define Server
server <- function(input, output) {
  
  # Define price_dat as a reactiveVal
  price_dat <- reactiveVal()
  
  # Function to fetch data
  fetchData <- function() {
    
    psql_select(cred = cred_psql_docker, 
                query_string = 
                  "SELECT s.name, p.timestamp_utc, p.close
                        FROM (SELECT symbol_fk, timestamp_utc, close 
                              FROM quotes.prices
                              WHERE (symbol_fk = 1) OR (symbol_fk = 2) OR (symbol_fk = 3) OR (symbol_fk = 4)
                              ORDER BY timestamp_utc DESC
                              LIMIT 200) as p
                        LEFT JOIN quotes.symbols as s
                        ON p.symbol_fk = s.symbol_sk;")
  }
  
  # Initialize data
  price_dat(fetchData())
  
  # Observe the action button
  observeEvent(input$refreshButton, {
    price_dat(fetchData())
  })
  
  # Plot for Tesla
  output$teslaPlot <- renderPlot({
    tesla_dat <- subset(price_dat(), name == "TESLA INC")
    ggplot(tesla_dat, aes(x = timestamp_utc, y = close, color = close)) +
      geom_line() +
      labs(title = "Tesla Closing Stock Prices", x = "Timestamp", y = "Close Price") +
      theme_minimal() +
      scale_color_gradient(low = "red", high = "green") +
      theme(plot.background = element_rect(fill = "white"))
  })
  
  # Plot for Microsoft
  output$microsoftPlot <- renderPlot({
    microsoft_dat <- subset(price_dat(), name == "MICROSOFT CORPORATION")
    ggplot(microsoft_dat, aes(x = timestamp_utc, y = close, color = close)) +
      geom_line() +
      labs(title = "Microsoft Closing Stock Prices", x = "Timestamp", y = "Close Price") +
      theme_minimal() +
      scale_color_gradient(low = "red", high = "green") +
      theme(plot.background = element_rect(fill = "white"))
  })
  
  # PLot for Medtronic PLC
  output$medtronicPlot <- renderPlot({
    medtronic_dat <- subset(price_dat(), name == "MEDTRONIC PLC")
    ggplot(medtronic_dat, aes(x = timestamp_utc, y = close, color = close)) +
      geom_line() +
      labs(title = "Medtronic Closing Stock Prices", x = "Timestamp", y = "Closing Price") +
      theme_minimal() +
      scale_color_gradient(low = "red", high = "green") +
      theme(plot.background = element_rect(fill = "white"))
  })
  
  # Plot for Palantir Technologies Inc.
  output$palantirPlot <- renderPlot({
    palantir_dat <- subset(price_dat(), name == "PALANTIR TECHNOLOGIES INC")
    ggplot(palantir_dat, aes(x = timestamp_utc, y = close, color = close)) +
      geom_line() +
      labs(title = "Palantir Tech. Closing Stock Prices", x = "Timestamp", y = "Closing Prices") + 
      theme_minimal() +
      scale_color_gradient(low = "red", high = "green") +
      theme(plot.background = element_rect(fill = "white"))
  })
}



# Run the application 
shinyApp(ui = ui, server = server)


################################################################################
#### RUN THE APP FROM DIRECTORY-ORIENTED COMMAND ###############################

runApp("STOCKSapp.R", display.mode = "showcase") # TOO LARGE A FUNCTION

