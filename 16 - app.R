library(shiny)
library(ggplot2)
library(DBI)
library(RPostgres)
source("credentials.R")
source("psql_queries.R")

# Define UI
ui <- fluidPage(
  titlePanel("Stock Price Visualization"),
  actionButton("refreshButton", "Refresh Data"),
  fluidRow(
    column(6, plotOutput("teslaPlot")),
    column(6, plotOutput("microsoftPlot"))
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
                              WHERE (symbol_fk = 1) OR (symbol_fk = 2)
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
    tesla_dat <- subset(price_dat(), name == "Tesla Inc")
    ggplot(tesla_dat, aes(x = timestamp_utc, y = close)) +
      geom_line() +
      labs(title = "Tesla Closing Stock Prices", x = "Timestamp", y = "Close Price") +
      theme_minimal()
  })
  
  # Plot for Microsoft
  output$microsoftPlot <- renderPlot({
    microsoft_dat <- subset(price_dat(), name == "Microsoft Corporation")
    ggplot(microsoft_dat, aes(x = timestamp_utc, y = close)) +
      geom_line() +
      labs(title = "Microsoft Closing Stock Prices", x = "Timestamp", y = "Close Price") +
      theme_minimal()
  })
}



# Run the application 
shinyApp(ui = ui, server = server)
