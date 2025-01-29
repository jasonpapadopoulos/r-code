# libraries
library(dplyr)
library(purrr)
library(progressr)
library(jsonlite)
library(httr)
library(dplyr)
library(tidyr)
library(DBI)
library(RPostgreSQL)
library(lubridate)

# data warehouse
drv <- dbDriver("PostgreSQL")
con <- dbConnect(drv, dbname = "prod",
                 host = "mw-redshift-prod.ces2db0use8y.us-east-2.redshift.amazonaws.com",
                 port = 5439,  # Default Redshift port
                 user = "dwuser_ro",
                 password = "ercPzNjFvMiZ7SCi32nZW3H7scXYq2huQMea3BcDNUfZkVFd")
query <- "with

    transactions_base as

        (select
            user_id,
            offering_id,
            'IPO' as transaction_category,
            'buy' as transaction_type,
            num_tokens::integer as number_of_shares,
            (value_cents_int/100.000000)/(num_tokens::integer) as price_per_share,
            convert_timezone('GMT', 'US/Eastern', created_at) as transaction_time,
            date_trunc('quarter', transaction_time) - interval '1 day' as closest_appraisal_date
        from
            contributions
        where
            status in ('release', 'success')
        
        union all
        
        select
            from_user_id,
            offering_id,
            case when id in (select share_transaction_id from payouts) then 'Exit' else 'Trading' end as transaction_category,
            'sell' as transaction_type,
            - number_of_shares,
            price_per_share/100.000000,
            convert_timezone('GMT', 'US/Eastern', created_at) as transaction_time,
            date_trunc('quarter', transaction_time) - interval '1 day' as closest_appraisal_date
        from
            share_transactions
        where
            coalesce(note, '') != 'Masterworks to Templum'
        
        union all
        
        select
            to_user_id,
            offering_id,
            'Trading' as transaction_category,
            'buy' as transaction_type,
            number_of_shares,
            price_per_share/100.000000,
            convert_timezone('GMT', 'US/Eastern', created_at) as transaction_time,
            date_trunc('quarter', transaction_time) - interval '1 day' as closest_appraisal_date
        from
            share_transactions
        where
            coalesce(note, '') != 'Masterworks to Templum'
        ),
    
    navs_base as
    
        (select
            offering_id,
            active_at_date,
            price_per_share * 1.0 / 100 as nav_per_share,
            row_number() over (partition by offering_id order by active_at_date desc) as nth_nav_rev
        from
            offering_navs
        ),
    
    applicable_navs_base as
    
        (select
            *
        from
            navs_base
        where
            (offering_id in (select distinct(offering_id) from payouts) and nth_nav_rev > 1)
                or (offering_id not in (select distinct(offering_id) from payouts))
        ),
    
    appraisals_base as
    
        (select
            offering_id,
            appraisal_date,
            row_number() over (partition by offering_id order by appraisal_date desc) as nth_appraisal_rev
        from
            offering_appraisals
        where
            has_appraisal_report = 1
        ),
    
    latest_appraisals_base as
    
        (select
            navs_base.*,
            appraisal_date
        from
            navs_base
        left join
            appraisals_base
                on navs_base.offering_id = appraisals_base.offering_id
        where
            (navs_base.offering_id in (select distinct(offering_id) from payouts) and nth_appraisal_rev = 2 and nth_nav_rev = 2)
                or (navs_base.offering_id not in (select distinct(offering_id) from payouts) and nth_appraisal_rev = 1 and nth_nav_rev = 1)
        )


select
    user_id,
    legal_name as offering,
    transaction_category,
    transaction_type,
    number_of_shares,
    transactions_base.price_per_share,
    transaction_time,
    coalesce(latest_appraisals_base.nav_per_share, 20.000000) as latest_nav_per_share,
    case when latest_appraisals_base.nav_per_share is not null then 1 else 0 end as is_appraised,
    applicable_navs_base.nav_per_share as nav_at_time
from
    transactions_base
left join
    offerings
        on transactions_base.offering_id = offerings.id
left join
    latest_appraisals_base
        on transactions_base.offering_id = latest_appraisals_base.offering_id
left join
    applicable_navs_base
        on transactions_base.offering_id = applicable_navs_base.offering_id
            and date(transactions_base.closest_appraisal_date) = date(applicable_navs_base.active_at_date)"
transactions <- dbGetQuery(con, query)
dbDisconnect(con)
#


# minor data wrangling

# order by offering and transaction_time
transactions <- transactions %>%
  arrange(offering, transaction_time)

# exceptions (Jack Smith problems with returned transactions)
transactions <- transactions %>%
  filter(!(user_id == 'c7d83a1133b10b1ad86d9325f9febb20' &
             offering == 'Masterworks 025, LLC' &
             transaction_time >= as.Date('2022-11-30')
  ) &
    !(user_id == 'c7d83a1133b10b1ad86d9325f9febb20' &
        offering == 'Masterworks 022, LLC' &
        abs(number_of_shares) == 691
    )
  )
#

# FIFO function
fifo <- function(transactions) {
  
  # initialize
  buy_transactions <- list()
  total_shares_held <- 0
  shares_sold_in_trading <- 0
  shares_sold_in_exits <- 0
  cost_of_shares_sold_trading <- 0
  revenue_of_shares_sold_trading <- 0
  cost_of_shares_sold_exit <- 0
  revenue_of_shares_sold_exit <- 0
  cost_basis_currently_held <- 0
  
  #for each transaction
  for (i in 1:nrow(transactions)) {
    
    # easier to work with the row as an object instead of indexing the table every time
    row <- transactions[i, ]
    
    # if it's a buy transaction, append to the buy_transactions list and add cumulatively to total shares held
    if (row$transaction_type == 'buy') {
      
      buy_transactions <- append(buy_transactions, list(c(row$number_of_shares, row$price_per_share)))
      total_shares_held <- total_shares_held + row$number_of_shares
      
      # if it's a sell
    } else if (row$transaction_type == 'sell') {
      
      shares_to_sell <- abs(row$number_of_shares) # number of shares to be sold
      
      # track if they were sold in trading or in an exit
      if (row$transaction_category == 'Trading') {
        
        shares_sold_in_trading <- shares_sold_in_trading + shares_to_sell
        
      } else if (row$transaction_category == 'Exit') {
        
        shares_sold_in_exits <- shares_sold_in_exits + shares_to_sell
        
      }
      
      # while there are shares to be sold, reduce it on each iteration by taking shares off buy transactions
      # as long as there is at least 1 buy transaction (Jack Smith I hate you)
      
      while (shares_to_sell > 0 && length(buy_transactions) > 0) {
        
        # take the first buy transaction and its price
        buy_shares <- buy_transactions[[1]][1]
        buy_price <- buy_transactions[[1]][2]
        
        # if the earliest buy transaction CANNOT cover the entire sell transaction
        if (buy_shares <= shares_to_sell) {
          
          # remove as many as can be taken to be sold
          shares_to_sell <- shares_to_sell - buy_shares
          total_shares_held <- total_shares_held - buy_shares
          
          if (row$transaction_category == 'Trading') {
            
            # add the cost basis of the shares sold, which is the entire buy quantity at the price bought
            cost_of_shares_sold_trading <- cost_of_shares_sold_trading + (buy_shares * buy_price)
            # add revenue from shares sold, which is the entire buy quantity at the price sold (buy quantity is less than sell quantity in this branch)
            revenue_of_shares_sold_trading <- revenue_of_shares_sold_trading + (buy_shares * row$price_per_share)
            
          } else if (row$transaction_category == 'Exit') {
            
            # same for exits
            cost_of_shares_sold_exit <- cost_of_shares_sold_exit + (buy_shares * buy_price)
            revenue_of_shares_sold_exit <- revenue_of_shares_sold_exit + (buy_shares * row$price_per_share)
            
          }
          
          # remove the earliest buy transaction since all of its shares have been used
          buy_transactions <- buy_transactions[-1]
          
          # if the earliest buy transaction CAN cover the entire sell transaction
        } else {
          
          # reduce the amount of shares available for future use by as many shares are being sold
          buy_transactions[[1]][1] <- buy_transactions[[1]][1] - shares_to_sell
          total_shares_held <- total_shares_held - shares_to_sell
          
          if (row$transaction_category == 'Trading') {
            
            # add the cost basis of the shares sold, which is the entire sell quantity at the price bought
            cost_of_shares_sold_trading <- cost_of_shares_sold_trading + (shares_to_sell * buy_price)
            # add revenue from shares sold, which is the entire sell quantity at the price sold
            revenue_of_shares_sold_trading <- revenue_of_shares_sold_trading + (shares_to_sell * row$price_per_share)
            
          } else if (row$transaction_category == 'Exit') {
            
            # same for exits
            cost_of_shares_sold_exit <- cost_of_shares_sold_exit + (shares_to_sell * buy_price)
            revenue_of_shares_sold_exit <- revenue_of_shares_sold_exit + (shares_to_sell * row$price_per_share)
            
          }
          # in this iteration of the while loop under this ifelse branch, there are no more shares to sell (shares_to_sell < buy_shares)
          shares_to_sell <- 0
          
        }
      }
    }
  }
  
  # cost basis of all shares currently held
  if (length(buy_transactions) > 0){
    for (i in 1:length(buy_transactions)) {
      cost_basis_currently_held <- cost_basis_currently_held + (buy_transactions[[i]][1] * buy_transactions[[i]][2])
    }
  }
  
  # cost_basis_currently_held <- length(buy_transactions)
  return(list(
    cost_of_shares_sold_trading = cost_of_shares_sold_trading,
    revenue_of_shares_sold_trading = revenue_of_shares_sold_trading,
    cost_of_shares_sold_exit = cost_of_shares_sold_exit,
    revenue_of_shares_sold_exit = revenue_of_shares_sold_exit,
    shares_sold_in_trading = shares_sold_in_trading,
    shares_sold_in_exits = shares_sold_in_exits,
    total_shares_held = total_shares_held,
    cost_basis_currently_held = cost_basis_currently_held
  ))
  
}

# offering level data
offering_results <- transactions %>%
  group_by(user_id, offering) %>%
  do({
    fifo_results <- fifo(.)
    
    # non-FIFO related columns
    latest_nav <- max(.$latest_nav_per_share, na.rm = TRUE)
    is_appraised <- max(.$is_appraised, na.rm = TRUE)
    
    # apply FIFO to all offerings by all users
    data.frame(
      user_id = unique(.$user_id),
      offering = unique(.$offering),
      cost_of_shares_sold_trading = fifo_results$cost_of_shares_sold_trading,
      revenue_of_shares_sold_trading = fifo_results$revenue_of_shares_sold_trading,
      cost_of_shares_sold_exit = fifo_results$cost_of_shares_sold_exit,
      revenue_of_shares_sold_exit = fifo_results$revenue_of_shares_sold_exit,
      shares_sold_in_trading = fifo_results$shares_sold_in_trading,
      shares_sold_in_exits = fifo_results$shares_sold_in_exits,
      total_shares_held = fifo_results$total_shares_held,
      cost_basis_currently_held = fifo_results$cost_basis_currently_held,
      latest_nav = latest_nav,
      is_appraised = is_appraised
    )
  }) %>%
  ungroup()

# investor level data
investor_results <- offering_results %>%
  group_by(user_id) %>%
  summarise(
    Net_Portfolio_Value = sum(total_shares_held * latest_nav, na.rm = TRUE),
    Total_Payouts = sum(revenue_of_shares_sold_trading, na.rm = TRUE) + sum(revenue_of_shares_sold_exit, na.rm = TRUE),
    Total_Return = ((sum(revenue_of_shares_sold_trading, na.rm = TRUE) + sum(revenue_of_shares_sold_exit, na.rm = TRUE)) / (sum(cost_of_shares_sold_trading, na.rm = TRUE) + sum(cost_of_shares_sold_exit, na.rm = TRUE))) - 1,
    Cost_of_Shares_Sold_in_Trading = sum(cost_of_shares_sold_trading, na.rm = TRUE),
    Revenue_from_Shares_Sold_in_Trading = sum(revenue_of_shares_sold_trading, na.rm = TRUE),
    Cost_of_Shares_Sold_in_Exits = sum(cost_of_shares_sold_exit, na.rm = TRUE),
    Revenue_from_Shares_Sold_in_Exits = sum(revenue_of_shares_sold_exit, na.rm = TRUE),
    Total_Cost_of_Shares_Sold = sum(cost_of_shares_sold_trading, na.rm = TRUE) + sum(cost_of_shares_sold_exit, na.rm = TRUE),
    Total_Revenue_of_Shares_Sold = sum(revenue_of_shares_sold_trading, na.rm = TRUE) + sum(revenue_of_shares_sold_exit, na.rm = TRUE),
    Shares_Sold_in_Trading = sum(shares_sold_in_trading, na.rm = TRUE),
    Shares_Sold_in_Exits = sum(shares_sold_in_exits, na.rm = TRUE),
    Total_Shares_Sold = sum(shares_sold_in_trading, na.rm = TRUE) + sum(shares_sold_in_exits, na.rm = TRUE),
    Total_Shares_Currently_Held = sum(total_shares_held, na.rm = TRUE),
    Cost_Basis_of_Shares_Currently_Held = sum(cost_basis_currently_held, na.rm = TRUE),
    Number_Of_Appraised_Offerings = sum(ifelse(total_shares_held > 0, is_appraised, 0), na.rm = TRUE),
    Appraised_Portfolio_Value = sum(total_shares_held * ifelse(is_appraised == 1, latest_nav, 0), na.rm = TRUE),
    Appraised_Cost_Basis = sum(ifelse(is_appraised == 1, cost_basis_currently_held, 0), na.rm = TRUE),
    Offerings_Currently_Held = sum(ifelse(total_shares_held > 0, 1, 0), na.rm = TRUE),
    Appraised_Return = Appraised_Portfolio_Value / Appraised_Cost_Basis - 1
    
  ) %>%
  ungroup()

# standard deviation by number of offerings
sd_rets <- investor_results %>%
  group_by(Number_Of_Appraised_Offerings) %>%
  summarise(
    std = sd(Appraised_Return)
  )
sd_rets <- sd_rets %>% filter(between(Number_Of_Appraised_Offerings, 1, 10))

# plot
plot(sd_rets$Number_Of_Appraised_Offerings, sd_rets$std, type = 'b', main = 'Standard Deviation of Appraised Returns', ylab = 'SD', xlab = 'Appraised Offerings Held')











