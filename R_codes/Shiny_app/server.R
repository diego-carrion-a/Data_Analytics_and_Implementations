# Example: Shiny app that search Wikipedia web pages
# File: server.R 
library(shiny)
library(tm)
library(stringi)
library(wordcloud)
# library(proxy)
source("WikiSearch.R")

shinyServer(function(input, output) {
  output$distPlot <- renderPlot({ 
    
  # Progress Bar while executing function
    withProgress({
      setProgress(message = "Mining Wikipedia ...")
      result <- SearchWiki(input$select)
    })
    
    wordcloud(names(result), result, colors=brewer.pal(6, "Dark2"))
  })
  
  output$distDF <- renderTable({ 
    
    withProgress({
      result2<- SearchWiki(input$select)
      })
    resul2<-as.data.frame(as.table(result2))
    names(result2)[1]<-'Word'
    resul2 <- as.data.frame(resul2)
    })
  
})
