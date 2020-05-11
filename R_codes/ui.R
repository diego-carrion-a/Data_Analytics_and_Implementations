# File: ui.R 
library(shiny)
titles <- c("Roger_Federer","Kobe_Bryant","Wimbledon","Tiger_Woods","Michael_Jordan","Arnold_Palmer",
            "Jack_Nicklaus","Michael_Schumacher","Phil_Mickelson","David_Beckham","Floyd_Mayweather_Jr.",
            "Cristiano_Ronaldo","Lionel_Messi","Manny_Pacquiao","Tennis","Football","Basketball",
            "Golf","Formula_One")
# Define UI for application 
shinyUI(fluidPage(
  # Application title (Panel 1)
  titlePanel("Word Count Maker"), 
  # Widget (Panel 2) 
  sidebarLayout(
    sidebarPanel(h3("Search panel"),
                 # Where to search 
                 selectInput("select",
                             label = h5("Choose from the following Wiki Pages on"),
                             choices = titles,
                             selected = titles, multiple = TRUE),
                 # Start Search
                 submitButton("Results")
    ),
    # Display Panel (Panel 3)
    mainPanel(                   
      h1("Word Cloud top 50 frequent words",align = "center"),
      plotOutput("distPlot"),
      h2("List of top 50 words with frequency",align = "center"),
      tableOutput("distDF")
    )
  )
))
