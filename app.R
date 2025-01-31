library(shiny)
library(DT)
library(ggplot2)
library(lubridate)
library(plotly)
library(stringi)
library(stringr)
library(formattable)
library(shinymanager)
library(DBI)
library(pool)
library(dplyr)
library(httr)
library(jsonlite)
library(numbers)
library(DT)

#

credentials <- data.frame(
  user = c("jason", "nigel", "francisco", "ben", "alex", "jim", "thomas", "joe", "mos", "mike", "fandi", "jace", "jane", 'paul', 'kenny', 'sumit', 'vardhan', 'josh', 'anna', 'emily', 'ttominaga', 'natalie',
           'acamacho@masterworks.com', 'vchavez@masterworks.com', 'izazueta@masterworks.com'),
  password = c("jasonmw", "nigelmw", "franciscomw", "benmw","alexmw","jimmw","thomasmw", "joemw", "mosmw", "mikemw", "fandimw", "jacemw", "janemw", 'paulmw', 'kennymw', 'sumitmw', 'vardhanmw', 'joshmw', 'annamw', 'emilymw', 'Mw123$', 'nataliemw',
               '$Test1234$', '$Test1234$', '$Test1234$'),
  stringsAsFactors = FALSE
)






# pre_november_investor_ledger <- read.csv("pre_november_investor_ledger.csv")

pre_november_investor_ledger <- read.csv("pre_november_investor_ledger_new.csv")
pre_november_investor_ledger$encoded_order_id <- NA
pre_november_investor_ledger$order_code <- NA



# pre_july_bank_data <- read.csv("pre_july_bank_data.csv")

pre_july_bank_data <- read.csv("pre_july_bank_data_new.csv")


#

#  DO NOT RUN

# closed_offerings <- read.csv("/Users/Jason/Downloads/closed_offerings_20221003.csv")
# closed_offerings$bank_account <- toupper(closed_offerings$offering)
# closed_offerings$remove <- 1
#
#
# pre_july_bank_data_new <- left_join(pre_july_bank_data, closed_offerings, by = c("Account" = "bank_account"))
#
#
# pre_july_bank_data_old <- pre_july_bank_data_new[which(!is.na(pre_july_bank_data_new$remove)),]
# pre_july_bank_data_new <- pre_july_bank_data_new[which(is.na(pre_july_bank_data_new$remove)),]
#
# pre_july_bank_data_new <- pre_july_bank_data_new[, which(colnames(pre_july_bank_data_new) %in% colnames(pre_july_bank_data))]
# pre_july_bank_data_old <- pre_july_bank_data_old[, which(colnames(pre_july_bank_data_old) %in% colnames(pre_july_bank_data))]
#
# write.csv(pre_july_bank_data_new, "pre_july_bank_data_new.csv", row.names = FALSE)
# write.csv(pre_july_bank_data_old, "pre_july_bank_data_old.csv", row.names = FALSE)
#
#
#
# pre_november_investor_ledger_new <- left_join(pre_november_investor_ledger, closed_offerings, by = c("Offering" = "offering"))
#
#
# pre_november_investor_ledger_old <- pre_november_investor_ledger_new[which(!is.na(pre_november_investor_ledger_new$remove)),]
# pre_november_investor_ledger_new <- pre_november_investor_ledger_new[which(is.na(pre_november_investor_ledger_new$remove)),]
#
# pre_november_investor_ledger_new <- pre_november_investor_ledger_new[, which(colnames(pre_november_investor_ledger_new) %in% colnames(pre_november_investor_ledger))]
# pre_november_investor_ledger_old <- pre_november_investor_ledger_old[, which(colnames(pre_november_investor_ledger_old) %in% colnames(pre_november_investor_ledger))]
#
# write.csv(pre_november_investor_ledger_new, "pre_november_investor_ledger_new.csv", row.names = FALSE)
# write.csv(pre_november_investor_ledger_old, "pre_november_investor_ledger_old.csv", row.names = FALSE)
#
# nrow(pre_july_bank_data) == nrow(pre_july_bank_data_new) + nrow(pre_july_bank_data_old)
# nrow(pre_november_investor_ledger) == nrow(pre_november_investor_ledger_new) + nrow(pre_november_investor_ledger_old)

#

offerings_and_IDs <- GET("https://redash.masterworks.io/api/queries/518/results.json?api_key=lzyJFw3QgeKsE2g67SznSTAoPuTR4oQzQqGCySR5")
offerings_and_IDs <- jsonlite::fromJSON(rawToChar(offerings_and_IDs$content))
offerings_and_IDs <- as.data.frame(offerings_and_IDs$query_result$data$rows)
offerings_list <- unique(offerings_and_IDs$legal_name)

#

ui <- secure_app(
# ui <- fluidPage(
  fluidPage(
    title = 'Reconciliation',
    hr(),
    titlePanel(
      h1('T-REC: The Masterworks Reconciliation Dashboard', align = "center")
    ),
    h2(tags$small(tags$i('Adam Smith Awards 2022 Judges’ Choice Winner')), align = "center"),
    hr(),
    tabsetPanel(type = 'tabs',
                tabPanel('Reconciliation',
                        hr(),
                        fluidRow(column(6, selectInput('offering', "Select an Offering:", c("All", sort(offerings_list), "MW CAYMAN SEGREGATED PORTFOLIOS"), selected = "All"))
                                 # column(6), actionButton("simulate", "Simulate!")
                                 ),
                        hr(),
                        tabsetPanel(type = 'tabs',
                            tabPanel('Investor Orders',
                                     hr(),
                                     fluidRow(column(12, DT::dataTableOutput('investorLedger'))),
                                     hr(),
                                     fluidRow(column(6, downloadButton('downloadInvestorLedger', 'Download Investor Ledger')))
                                     # hr(),
                                     # fluidRow(column(6, verbatimTextOutput('sumOS')))
                                    ),
                            tabPanel('Bank Transactions',
                                     hr(),
                                     fluidRow(column(12, checkboxInput('includeVirtualAccounts', "Include Virtual Accounts?", value = FALSE))),
                                     hr(),
                                     fluidRow(column(12, DT::dataTableOutput('bankTransactions'))),
                                     hr(),
                                     fluidRow(column(6, downloadButton('downloadBankTransactions', 'Download  Bank Transactions')))
                                    ),
                            tabPanel('TabaPay Reports',
                                     hr(),
                                     fluidRow(column(12, DT::dataTableOutput('tabapayReports'))),
                                     hr(),
                                     fluidRow(column(6, downloadButton('downloadTabapayReports', 'Download Tabapay Report')))

                                    ),
                            # tabPanel('Exceptions',
                            #          hr(),
                            #          fluidRow(column(12, DT::dataTableOutput('exceptions')))
                            #         ),
                            tabPanel('Closing',
                                     fluidRow(hr(),
                                              column(6, downloadButton('performClosing', 'Perform Closing')))
                                  )
                                )
                          ),
                tabPanel('Closed Deals and Clearing Lookup',
                         fluidRow(hr(),
                                  column(6, tagList("", a("T-RIP: Closed Deals Reconciliation Dashboard", href="https://mwsd.shinyapps.io/MWTRIP/")))),
                         fluidRow(hr(),
                                  column(6, tagList("", a("Memo or Clearing Note Lookup", href="https://redash.masterworks.io/queries/1895?p_Memo%20or%20Clearing%20Note=From%3A%20Thomas%20Greer"))))




                )
      #           tabPanel('Offering Summary',
      #                    hr(),
      #                    code("Bank Data is updated daily for FRB and hourly for GS. Investor Ledger data is updated hourly. Only unhandled exceptions are summarized."),
      #                    hr(),
      #                    fluidRow(column(12, selectInput('offeringsForSummary', "Select one or more Offerings:", c("All", sort(offerings_list)), selected = c('Masterworks 054',
      #                                                                                                                                                         'Masterworks 058',
      #                                                                                                                                                         'Masterworks 059',
      #                                                                                                                                                         'Masterworks 061',
      #                                                                                                                                                         'Masterworks 064',
      #                                                                                                                                                         'Masterworks 065',
      #                                                                                                                                                         'Masterworks 067',
      #                                                                                                                                                         'Masterworks 068',
      #                                                                                                                                                         'Masterworks 069',
      #                                                                                                                                                         'Masterworks 070',
      #                                                                                                                                                         'Masterworks 072',
      #                                                                                                                                                         'Masterworks 073',
      #                                                                                                                                                         'Masterworks 074',
      #                                                                                                                                                         'Masterworks 076',
      #                                                                                                                                                         'Masterworks 078',
      #                                                                                                                                                         'Masterworks 079',
      #                                                                                                                                                         'Masterworks 080',
      #                                                                                                                                                         'Masterworks 081',
      #                                                                                                                                                         'Masterworks 083',
      #                                                                                                                                                         'Masterworks 084',
      #                                                                                                                                                         'Masterworks 086',
      #                                                                                                                                                         'Masterworks 087'), multiple = TRUE, width = "33%"))),
      #                    hr()
      #                    # fluidRow(column(12, DT::dataTableOutput('offeringsSummary')))
      #
      #             ),
      #           tabPanel('Payment Types Summary',
      #                    # hr(),
      #                    # code("Bank Data is updated daily for FRB and hourly for GS. Investor Ledger data is updated hourly. Only unhandled exceptions are summarized."),
      #                    hr(),
      #                    fluidRow(column(12, selectInput('offeringsForPaymentsSummary', "Select one or more Offerings:", c("All", sort(offerings_list)), selected = c('Masterworks 054',
      #                                                                                                                                                         'Masterworks 058',
      #                                                                                                                                                         'Masterworks 059',
      #                                                                                                                                                         'Masterworks 061',
      #                                                                                                                                                         'Masterworks 064',
      #                                                                                                                                                         'Masterworks 065',
      #                                                                                                                                                         'Masterworks 067',
      #                                                                                                                                                         'Masterworks 068',
      #                                                                                                                                                         'Masterworks 069',
      #                                                                                                                                                         'Masterworks 070',
      #                                                                                                                                                         'Masterworks 072',
      #                                                                                                                                                         'Masterworks 073',
      #                                                                                                                                                         'Masterworks 074',
      #                                                                                                                                                         'Masterworks 076',
      #                                                                                                                                                         'Masterworks 078',
      #                                                                                                                                                         'Masterworks 079',
      #                                                                                                                                                         'Masterworks 080',
      #                                                                                                                                                         'Masterworks 081',
      #                                                                                                                                                         'Masterworks 083',
      #                                                                                                                                                         'Masterworks 084',
      #                                                                                                                                                         'Masterworks 086',
      #                                                                                                                                                         'Masterworks 087'), multiple = TRUE, width = "33%"))),
      #                    hr()
      #                    # fluidRow(column(12, DT::dataTableOutput('paymentTypesSummary')))
      #
      #           )
      # )
    # ,
    # hr()
    )
  )
)


#server start
  server <- function(input, output, session) {

  result_auth <- secure_server(check_credentials = check_credentials(credentials))

  set_labels(
    language = "en",
    "Please authenticate" = "First, authentication.",
    "Username:" = "Username",
    "Password:" = "Password"
  )


  # investor ledger

  full_investor_ledger <- GET("https://redash.masterworks.io/api/queries/534/results.json?api_key=T2VoNjrtbWgJPOTtnGn5bnRw6jqDCLKIwf7gBjdV")
  full_investor_ledger <- jsonlite::fromJSON(rawToChar(full_investor_ledger$content))
  full_investor_ledger <- as.data.frame(full_investor_ledger$query_result$data$rows)
  colnames(pre_november_investor_ledger) <- colnames(full_investor_ledger)
  full_investor_ledger <- rbind(full_investor_ledger, pre_november_investor_ledger)
  full_investor_ledger$`Payment Method`[which(is.na(full_investor_ledger$`Payment Method`))] <- "unclassified"
  full_investor_ledger$`Order Creation Date` <- as.Date(full_investor_ledger$`Order Creation Date`)
  full_investor_ledger$`Payment Method` <- as.factor(full_investor_ledger$`Payment Method`)
  full_investor_ledger$`Order Status` <- as.factor(full_investor_ledger$`Order Status`)
  full_investor_ledger$`In Bank` <- as.factor(full_investor_ledger$`In Bank`)
  #

  # tabapay

  all_tabapay_transactions <- GET("https://redash.masterworks.io/api/queries/533/results.json?api_key=edRY9pBiSiIEdoNwyrd0PnPBjaMaw0QJn0HuB2fW")
  all_tabapay_transactions <- jsonlite::fromJSON(rawToChar(all_tabapay_transactions$content))
  all_tabapay_transactions <- as.data.frame(all_tabapay_transactions$query_result$data$rows)
  all_tabapay_transactions <- left_join(all_tabapay_transactions, offerings_and_IDs, by = c("First 8 Offering ID Digits" = "first_8_digits"))
  all_tabapay_transactions <- all_tabapay_transactions %>% dplyr::select("Transaction ID", "Investor Name", "Gross Transaction Amount", "Convinience Fee", "Expected Deposit Amount", "Expected Deposit Date", "legal_name")
  #

  # bank transactions
  all_bank_transactions <- GET("https://redash.masterworks.io/api/queries/535/results.json?api_key=vQXcldctcgKUBpG649smDU8dYsuY4C1xUd94u5BX")
  all_bank_transactions <- jsonlite::fromJSON(rawToChar(all_bank_transactions$content))
  all_bank_transactions <- as.data.frame(all_bank_transactions$query_result$data$rows)
  colnames(pre_july_bank_data) <- colnames(all_bank_transactions)
  all_bank_transactions <- rbind(all_bank_transactions, pre_july_bank_data)
  all_bank_transactions$`Payment Type`[which(!is.na(all_bank_transactions$tag))] <- all_bank_transactions$tag[which(!is.na(all_bank_transactions$tag))]
  all_bank_transactions$`Payment Type`[which(all_bank_transactions$`Payment Type` == "Alto")] <- toupper(all_bank_transactions$`Payment Type`[which(all_bank_transactions$`Payment Type` == "Alto")])
  all_bank_transactions$`Payment Type`[which(is.na(all_bank_transactions$`Payment Type`))] <- "unclassified"
  all_bank_transactions$`Payment Type` <- as.factor(all_bank_transactions$`Payment Type`)
  all_bank_transactions$Expected <- as.factor(all_bank_transactions$Expected)
  all_bank_transactions <- all_bank_transactions[which(all_bank_transactions$references_isVirtual %in% c(0,1)),]
  #all_bank_transactions <- all_bank_transactions[-which(all_bank_transactions$`Transaction ID` %in% c("frb_18983", "gs_242985")),]
  #


  # exceptions
  # all_exceptions <- GET("https://redash.masterworks.io/api/queries/593/results.json?api_key=cZwiZ1oFds2cDhVYFlQBSowetPDdr7UYTrdbk6ZW")
  # all_exceptions <- jsonlite::fromJSON(rawToChar(all_exceptions$content))
  # all_exceptions <- as.data.frame(all_exceptions$query_result$data$rows)

  #

  output$investorLedger <- DT::renderDataTable({

    if (input$offering == 'All'){
      investor_ledger <- full_investor_ledger
    } else {
      investor_ledger <- full_investor_ledger %>% dplyr::filter(Offering == input$offering)
    }

    investor_ledger <- investor_ledger %>%dplyr::select(-first8_offering_id)

    investor_ledger$`Order ID` <- as.character(investor_ledger$`Order ID`)

    investor_ledger$`Investor Name` <- gsub("  "," ",investor_ledger$`Investor Name`)

    investor_ledger <- investor_ledger[rev(order(investor_ledger$`Order Creation Date`)),]
    investor_ledger <- datatable(investor_ledger, rownames = FALSE, options = list(paging = TRUE, pageLength = 25), escape = FALSE, filter = "top")

    investor_ledger$`Clearing Note` <- trimws(investor_ledger$`Clearing Note`)

    investor_ledger$`Memo or Clearing Note` <- trimws(investor_ledger$`Memo or Clearing Note`)
    formatCurrency(investor_ledger, 8, currency = "$", interval = 3, mark = ",",
                   digits = 0, dec.mark = getOption("OutDec"), before = TRUE)

  })

  output$bankTransactions <- DT::renderDataTable({

    if (input$offering == 'All'){
      bank_transactions <- all_bank_transactions
    } else {
      bank_transactions <- all_bank_transactions %>% dplyr::filter(tolower(Account) == tolower(input$offering))
    }

    if (input$includeVirtualAccounts == FALSE){
      bank_transactions <- bank_transactions[which(bank_transactions$references_isVirtual == 0),]
    }
    bank_transactions <- bank_transactions[,-which(colnames(bank_transactions) == 'references_isVirtual')]

    bank_transactions <- bank_transactions[rev(order(bank_transactions$Date)),]

    bank_transactions <- bank_transactions[order(bank_transactions$Date),]

    bank_transactions$Balance <- cumsum(bank_transactions$`Transaction Amount`)

    bank_transactions <- bank_transactions[order(rev(bank_transactions$Date), rev(bank_transactions$Balance)),]

    bank_transactions <- datatable(bank_transactions, rownames = FALSE, options = list(paging = TRUE, pageLength = 25), escape = FALSE, filter = "top")
    formatCurrency(bank_transactions, c(7, 10), currency = "$", interval = 3, mark = ",", digits = 2, dec.mark = getOption("OutDec"), before = TRUE)

  })

  output$tabapayReports <- DT::renderDataTable({

    if (input$offering == 'All'){
      tabapay_transactions <- all_tabapay_transactions
    } else {
      tabapay_transactions <- all_tabapay_transactions %>% dplyr::filter(legal_name == input$offering)
    }

    tabapay_transactions <- tabapay_transactions %>% dplyr::select(-legal_name)

    tabapay_transactions <- tabapay_transactions[rev(order(tabapay_transactions$`Expected Deposit Date`)),]
    #
    tabapay_transactions <- datatable(tabapay_transactions, rownames = FALSE, options = list(paging = TRUE, pageLength = 25), escape = FALSE, filter = "top")
    formatCurrency(tabapay_transactions, c(3,4,5), currency = "$", interval = 3, mark = ",",
                   digits = 2, dec.mark = getOption("OutDec"), before = TRUE)

  })

  # output$exceptions <- DT::renderDataTable({
  #
  #   if (input$offering == 'All'){
  #     exceptions_table <- all_exceptions
  #   } else {
  #     exceptions_table <- all_exceptions %>% dplyr::filter(spv_name_value == input$offering)
  #   }
  #
  #   exceptions_table$net_transfer_amount <- NA
  #   exceptions_table$net_transfer_amount[which(exceptions_table$handling_instructions %in% c("Refund Transfer Amount", "Transfer Funds to MAS Only",  "Transfer Funds to Intended SPV Account", "Transfer Funds to Intended Account"))] <- -exceptions_table$transfer_amount[which(exceptions_table$handling_instructions %in% c("Refund Transfer Amount", "Transfer Funds to MAS Only",  "Transfer Funds to Intended SPV Account", "Transfer Funds to Intended Account"))]
  #   exceptions_table$net_transfer_amount[which(exceptions_table$handling_instructions %in% c("Additional Investor Funds", "Transfer Funds From MAS to SPV"))] <- exceptions_table$transfer_amount[which(exceptions_table$handling_instructions %in% c("Additional Investor Funds", "Transfer Funds From MAS to SPV"))]
  #
  #   exceptions_table$net_transfer_amount[which(exceptions_table$exception_type == 'Direction Error (Incoming)')] <- exceptions_table$transfer_amount[which(exceptions_table$exception_type == 'Direction Error (Incoming)')]
  #   exceptions_table$net_transfer_amount[which(exceptions_table$exception_type == 'Direction Error (Outgoing)')] <- -exceptions_table$transfer_amount[which(exceptions_table$exception_type == 'Direction Error (Outgoing)')]
  #
  #   exceptions_table <- datatable(exceptions_table, rownames = FALSE, options = list(paging = TRUE, pageLength = 25), escape = FALSE, filter = "top")
  #   formatCurrency(exceptions_table, c(6:9, 18), currency = "$", interval = 3, mark = ",",
  #                  digits = 2, dec.mark = getOption("OutDec"), before = TRUE)
  #
  #
  # })


  output$downloadInvestorLedger <- downloadHandler(
    filename = function() {
      paste(gsub(" ",".",paste(input$offering, "Investor.Ledger", gsub("-","",Sys.Date()))),".csv")
    },
    content = function(file) {

      if (input$offering == 'All'){
        investor_ledger <- full_investor_ledger
      } else {
        investor_ledger <- full_investor_ledger %>% dplyr::filter(Offering == input$offering)
      }
      write.csv(investor_ledger, file, row.names = FALSE)
    }
  )

  output$downloadBankTransactions <- downloadHandler(
    filename = function() {
      paste(gsub(" ",".",paste(input$offering, "Bank.Transactions", gsub("-","",Sys.Date()))),".csv")
    },
    content = function(file) {

      if (input$offering == 'All'){
        bank_transactions <- all_bank_transactions
      } else {
        bank_transactions <- all_bank_transactions %>% dplyr::filter(tolower(Account) == tolower(input$offering))
      }

      if (input$includeVirtualAccounts == FALSE){
        bank_transactions <- bank_transactions[which(bank_transactions$references_isVirtual == 0),]
      }
      write.csv(bank_transactions, file, row.names = FALSE)
    }
  )

  output$downloadTabapayReports <- downloadHandler(
    filename = function() {
      paste(gsub(" ",".",paste(input$offering, "Tabapay.Reports", gsub("-","",Sys.Date()))),".csv")
    },
    content = function(file) {

      if (input$offering == 'All'){
        tabapay_transactions <- all_tabapay_transactions
      } else {
        tabapay_transactions <- all_tabapay_transactions %>% dplyr::filter(Offering == input$offering)

        tabapay_transactions <- tabapay_transactions[rev(order(tabapay_transactions$`Expected Deposit Date`)),]
      }
      write.csv(tabapay_transactions, file, row.names = FALSE)
    }
  )

  output$performClosing <- downloadHandler(
    filename = function() {
      paste(gsub(" ",".",paste(input$offering, "Closing.Performed.At", gsub("-","",Sys.Date()))),".csv")
    },
    content = function(file) {

      if (input$offering == 'All'){
        closing_report <- full_investor_ledger
      } else {
        closing_report <- full_investor_ledger %>% dplyr::filter(Offering == input$offering)
      }
      colnames(closing_report)[1] <- c("order.id")
      write.csv(closing_report, file, row.names = FALSE)
    }
  )

}


shinyApp(ui = ui, server = server)



# library(shiny)
# library(DT)
# library(ggplot2)
# library(lubridate)
# library(plotly)
# library(stringi)
# library(stringr)
# library(formattable)
# library(shinymanager)
# library(DBI)
# library(pool)
# library(dplyr)
# library(httr)
# library(jsonlite)
# library(numbers)
# library(DT)
# 
# #
# 
# credentials <- data.frame(
#   user = c("jason", "nigel", "francisco", "ben", "alex", "jim", "thomas", "joe", "mos", "mike", "fandi", "jace", "jane", 'paul', 'kenny', 'sumit', 'vardhan', 'josh', 'anna', 'emily', 'ttominaga', 'natalie',
#            'acamacho@masterworks.com', 'vchavez@masterworks.com', 'izazueta@masterworks.com'),
#   password = c("jasonmw", "nigelmw", "franciscomw", "benmw","alexmw","jimmw","thomasmw", "joemw", "mosmw", "mikemw", "fandimw", "jacemw", "janemw", 'paulmw', 'kennymw', 'sumitmw', 'vardhanmw', 'joshmw', 'annamw', 'emilymw', 'Mw123$', 'nataliemw',
#                '$Test1234$', '$Test1234$', '$Test1234$'),
#   stringsAsFactors = FALSE
# )
# 
# 
# offerings_and_IDs <- GET("https://redash.masterworks.io/api/queries/518/results.json?api_key=lzyJFw3QgeKsE2g67SznSTAoPuTR4oQzQqGCySR5")
# offerings_and_IDs <- jsonlite::fromJSON(rawToChar(offerings_and_IDs$content))
# offerings_and_IDs <- as.data.frame(offerings_and_IDs$query_result$data$rows)
# offerings_list <- unique(offerings_and_IDs$legal_name)
# 
# #
# 
# ui <- secure_app(
#   # ui <- fluidPage(
#   fluidPage(
#     title = 'Reconciliation',
#     hr(),
#     titlePanel(
#       h1('T-REC: The Masterworks Reconciliation Dashboard', align = "center")
#     ),
#     h2(tags$small(tags$i('Adam Smith Awards 2022 Judges’ Choice Winner')), align = "center"),
#     hr(),
#     tabsetPanel(type = 'tabs',
#                 tabPanel('Reconciliation',
#                          hr(),
#                          # fluidRow(column(3, selectInput('offering', "Select an Offering:", c("All", sort(offerings_list), "MW CAYMAN SEGREGATED PORTFOLIOS"), selected = "All")),
#                          fluidRow(column(3, selectInput('offering', "Select an Offering:", c("All", sort(offerings_list), "MW CAYMAN SEGREGATED PORTFOLIOS"), selected = "Masterworks 183")),
#                                   column(3), actionButton("refresh", "Refresh")),
#                          hr(),
#                          tabsetPanel(type = 'tabs',
#                                      tabPanel('Investor Orders',
#                                               hr(),
#                                               fluidRow(column(12, DT::dataTableOutput('investorLedger'))),
#                                               hr(),
#                                               fluidRow(column(6, downloadButton('downloadInvestorLedger', 'Download Investor Ledger')))
#                                               # hr(),
#                                               # fluidRow(column(6, verbatimTextOutput('sumOS')))
#                                      )
#                                      # tabPanel('Bank Transactions',
#                                      #          hr(),
#                                      #          fluidRow(column(12, checkboxInput('includeVirtualAccounts', "Include Virtual Accounts?", value = FALSE))),
#                                      #          hr(),
#                                      #          fluidRow(column(12, DT::dataTableOutput('bankTransactions'))),
#                                      #          hr(),
#                                      #          fluidRow(column(6, downloadButton('downloadBankTransactions', 'Download  Bank Transactions')))
#                                      # ),
#                                      # tabPanel('TabaPay Reports',
#                                      #          hr(),
#                                      #          fluidRow(column(12, DT::dataTableOutput('tabapayReports'))),
#                                      #          hr(),
#                                      #          fluidRow(column(6, downloadButton('downloadTabapayReports', 'Download Tabapay Report')))
#                                      #          
#                                      # ),
#                                      # tabPanel('Exceptions',
#                                      #          hr(),
#                                      #          fluidRow(column(12, DT::dataTableOutput('exceptions')))
#                                      #         ),
#                                      # tabPanel('Closing',
#                                      #          fluidRow(hr(),
#                                      #                   column(6, downloadButton('performClosing', 'Perform Closing')))
#                                      # )
#                          )
#                 ),
#                 tabPanel('Closed Deals and Clearing Lookup',
#                          fluidRow(hr(),
#                                   column(6, tagList("", a("T-RIP: Closed Deals Reconciliation Dashboard", href="https://mwsd.shinyapps.io/MWTRIP/")))),
#                          fluidRow(hr(),
#                                   column(6, tagList("", a("Memo or Clearing Note Lookup", href="https://redash.masterworks.io/queries/1895?p_Memo%20or%20Clearing%20Note=From%3A%20Thomas%20Greer"))))
#                          
#                          
#                          
#                          
#                 )
#                 #           tabPanel('Offering Summary',
#                 #                    hr(),
#                 #                    code("Bank Data is updated daily for FRB and hourly for GS. Investor Ledger data is updated hourly. Only unhandled exceptions are summarized."),
#                 #                    hr(),
#                 #                    fluidRow(column(12, selectInput('offeringsForSummary', "Select one or more Offerings:", c("All", sort(offerings_list)), selected = c('Masterworks 054',
#                 #                                                                                                                                                         'Masterworks 058',
#                 #                                                                                                                                                         'Masterworks 059',
#                 #                                                                                                                                                         'Masterworks 061',
#                 #                                                                                                                                                         'Masterworks 064',
#                 #                                                                                                                                                         'Masterworks 065',
#                 #                                                                                                                                                         'Masterworks 067',
#                 #                                                                                                                                                         'Masterworks 068',
#                 #                                                                                                                                                         'Masterworks 069',
#                 #                                                                                                                                                         'Masterworks 070',
#                 #                                                                                                                                                         'Masterworks 072',
#                 #                                                                                                                                                         'Masterworks 073',
#                 #                                                                                                                                                         'Masterworks 074',
#                 #                                                                                                                                                         'Masterworks 076',
#                 #                                                                                                                                                         'Masterworks 078',
#                 #                                                                                                                                                         'Masterworks 079',
#                 #                                                                                                                                                         'Masterworks 080',
#                 #                                                                                                                                                         'Masterworks 081',
#                 #                                                                                                                                                         'Masterworks 083',
#                 #                                                                                                                                                         'Masterworks 084',
#                 #                                                                                                                                                         'Masterworks 086',
#                 #                                                                                                                                                         'Masterworks 087'), multiple = TRUE, width = "33%"))),
#                 #                    hr()
#                 #                    # fluidRow(column(12, DT::dataTableOutput('offeringsSummary')))
#                 #             
#                 #             ),
#                 #           tabPanel('Payment Types Summary',
#                 #                    # hr(),
#                 #                    # code("Bank Data is updated daily for FRB and hourly for GS. Investor Ledger data is updated hourly. Only unhandled exceptions are summarized."),
#                 #                    hr(),
#                 #                    fluidRow(column(12, selectInput('offeringsForPaymentsSummary', "Select one or more Offerings:", c("All", sort(offerings_list)), selected = c('Masterworks 054',
#                 #                                                                                                                                                         'Masterworks 058',
#                 #                                                                                                                                                         'Masterworks 059',
#                 #                                                                                                                                                         'Masterworks 061',
#                 #                                                                                                                                                         'Masterworks 064',
#                 #                                                                                                                                                         'Masterworks 065',
#                 #                                                                                                                                                         'Masterworks 067',
#                 #                                                                                                                                                         'Masterworks 068',
#                 #                                                                                                                                                         'Masterworks 069',
#                 #                                                                                                                                                         'Masterworks 070',
#                 #                                                                                                                                                         'Masterworks 072',
#                 #                                                                                                                                                         'Masterworks 073',
#                 #                                                                                                                                                         'Masterworks 074',
#                 #                                                                                                                                                         'Masterworks 076',
#                 #                                                                                                                                                         'Masterworks 078',
#                 #                                                                                                                                                         'Masterworks 079',
#                 #                                                                                                                                                         'Masterworks 080',
#                 #                                                                                                                                                         'Masterworks 081',
#                 #                                                                                                                                                         'Masterworks 083',
#                 #                                                                                                                                                         'Masterworks 084',
#                 #                                                                                                                                                         'Masterworks 086',
#                 #                                                                                                                                                         'Masterworks 087'), multiple = TRUE, width = "33%"))),
#                 #                    hr()
#                 #                    # fluidRow(column(12, DT::dataTableOutput('paymentTypesSummary')))
#                 #                    
#                 #           )
#                 # )
#                 # ,
#                 # hr()
#     )
#   )
# )
# 
# 
# 
# 
# #server start
# server <- function(input, output, session) {
#   
#   result_auth <- secure_server(check_credentials = check_credentials(credentials))
#   
#   set_labels(
#     language = "en",
#     "Please authenticate" = "First, authentication.",
#     "Username:" = "Username",
#     "Password:" = "Password"
#   )
# 
#   
#   x1 <- eventReactive(input$refresh, {
#     
#     res = POST("https://redash.masterworks.com/api/queries/1390/results",
#                add_headers(Authorization = "Q9XC****"),
#                body = list(max_age = 0, parameters = list(offering = input$offering, date_range_start = '2023-01-01', date_range_end = '2023-01-10')), encode = "json")
#                # body = list(max_age = 0, parameters = list(offering = 'Masterworks 183', date_range_start = '2023-01-02', date_range_end = '2023-01-05')), encode = "json")
#     rawToChar(res$content)
#     data = fromJSON(rawToChar(res$content))
#     
#     
#     # data
#     #print(data$response)
#     # data$job$status
#     
#     # paste("https://redash.masterworks.com/api/jobs/", data$job$id, sep="")
#     while (data$job$status != 3 & data$job$status != 4) {
#       res <- GET(paste("https://redash.masterworks.com/api/jobs/", data$job$id, sep=""),
#                  add_headers(Authorization = "Q9XC****"))
#       rawToChar(res$content)
#       data <- fromJSON(rawToChar(res$content))
#     }
#     
#     res = GET(paste(paste("https://redash.masterworks.com/api/queries/1390/results/", data$job$query_result_id, sep=""), ".json", sep=""),
#               add_headers(Authorization = "Q9XC****"))
#     # rawToChar(res$content)
#     data <- fromJSON(rawToChar(res$content))
#     
#     full_investor_ledger <- as.data.frame(data$query_result$data$rows)
#     
#     # full_investor_ledger <- rbind(full_investor_ledger, pre_november_investor_ledger)
#     # full_investor_ledger$`Payment Method`[which(is.na(full_investor_ledger$`Payment Method`))] <- "unclassified"
#     # full_investor_ledger$`Order Creation Date` <- as.Date(full_investor_ledger$`Order Creation Date`)
#     # full_investor_ledger$`Payment Method` <- as.factor(full_investor_ledger$`Payment Method`)
#     # full_investor_ledger$`Order Status` <- as.factor(full_investor_ledger$`Order Status`)
#     # full_investor_ledger$`In Bank` <- as.factor(full_investor_ledger$`In Bank`)
#     
#   })
#   
#   output$investorLedger <- DT::renderDataTable({
#     
#     datatable(x1())
#     
#   })
#   
# }
# 
# 
# shinyApp(ui = ui, server = server)
# 
# 
# # 
# # 
# # 
# # 
# # 
# # 
# # 
# # 
# # 
