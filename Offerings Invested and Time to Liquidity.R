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
                 host = "[HOST]",
                 port = 5439,  # Default Redshift port
                 user = "[USER]",
                 password = "[PASSWORD]")

query <- "with
    
    payouts_base as
    
        (select
            user_id,
            offering_id,
            min(convert_timezone('GMT', 'US/Eastern', created_at)) as payout_at
        from
            payouts
        group by 1,2
        )

select
    raw_layer.investor_offering_base.user_id,
    min(invested_at_date) as became_investor_at,
    count(distinct(raw_layer.investor_offering_base.offering_id)) as investments_before_first_payout,
    min(payout_at) as received_first_payout_at,
    datediff('day', became_investor_at, received_first_payout_at) as first_inv_first_payout_diff
from
    raw_layer.investor_offering_base
left join
    payouts_base
        on raw_layer.investor_offering_base.user_id = payouts_base.user_id
            and raw_layer.investor_offering_base.offering_id = payouts_base.offering_id
where
    (payout_at is null or invested_at_date < payout_at)
        and ipo_investments is not null
group by 1"
results <- dbGetQuery(con, query)
dbDisconnect(con)

# filter for users that have received a payout
results <- results %>%
  filter(complete.cases(received_first_payout_at))

# group results by number of offerings invested before their payout
results_grouped <- results %>%
  group_by(investments_before_first_payout) %>%
  summarise(
    avg_first_inv_first_payout_diff = mean(first_inv_first_payout_diff),
    med_first_inv_first_payout_diff = median(first_inv_first_payout_diff),
  )

# filter for 10 or less because not enough data
results_grouped <- results_grouped %>%
  filter(investments_before_first_payout <= 10)

# plot
plot(results_grouped$investments_before_first_payout,
     results_grouped$med_first_inv_first_payout_diff,
     main = 'Diversification and Liquidity',
     ylab = 'Average Day Difference between First Investment and First Payout',
     xlab = 'Offerings Invested before First Payout',
     type = 'b')

