# libraries
library(dplyr)
library(zoo)
library(caret)
library(randomForest)
library(pROC)
library(tidyr)
library(RPostgreSQL)

# querying the warehouse
drv <- dbDriver("PostgreSQL")
con <- dbConnect(drv, dbname = "[DB_NAME]",
                 host = "[DB_HOST]",
                 port = 5439,  # Default Redshift port
                 user = "[DB_USER]",
                 password = "[DB_PASSWORD]")
email_query <- "with

    investments_base as

        (select
            distinct
                user_id,
                final_attribution,
                1 as invested
        from
            raw_layer.attributed_contributions
        where
            order_attribution not in ('Unknown', 'Adviser Attributed')
        ),
    
    emails_base as

        (select
            user_id,
            campaign,
            received_email_at,
            nth_email_received_rec as nth_email_received_rev,
            non_auto_opened as opened,
            clicked_esp,
            web_or_app_session
        from
            raw_layer.aggregated_email_data
       where
            user_id in (select distinct user_id from raw_layer.investor_offering_base)
        )

select
    emails_base.*,
    invested
from
    emails_base
left join
    investments_base
        on emails_base.user_id = investments_base.user_id
            and emails_base.campaign = investments_base.final_attribution"

push_query <- "with

    investments_base as

        (select
            distinct
                user_id,
                final_attribution,
                1 as invested
        from
            raw_layer.attributed_contributions
        where
            order_attribution not in ('Unknown', 'Adviser Attributed')
        ),

    pushes_base as 

        (select
            coalesce(mp_distinct_id_before_identity, distinct_id) as implied_user_id,
            case
                when message_contents is not null then coalesce(campaign_id, '') || ' - ' || message_contents
                when body is not null then coalesce(title, '') || ' - ' || body
                when message_name is not null then message_name
                when message is not null then message
                when campaign_id is not null then campaign_id
            end as push_name,
            min(timestamp 'epoch' + time * interval '1 second') as notification_time,
            max(case when mp_event_name in ('$app_open', 'App - Interacted with Push') then 1 end) as tapped_push,
            row_number() over (partition by implied_user_id order by notification_time desc) as nth_notification_received_rev
        from
            mp_master_event
        where
            (mp_event_name in ('$campaign_delivery', '$campaign_received',  '$app_open', 'App - Interacted with Push') or (mp_event_name = 'Push Notification Received' and pushtype = 'artLaunch'))
            and coalesce(origin, 'not braze') <> 'Braze'
            and push_name is not null
            and (message_name not like '%test%' or message_name is null and message_name <> 'quiz_push_reminder2')
            and implied_user_id in (select distinct user_id from raw_layer.investor_offering_base)
        group by 1,2
        ),
    
    session_base as
    
        (select
            mp.id as user_id,
            date(timestamp 'epoch' + time * interval '1 second') as session_at,
            1 as app_session
        from
            mp_master_event mp
        where
            mp_event_name not in ('Push Notification Received', '$campaign_delivery', '$campaign_received', '$identify', 'Email Opened', 'Email Delivered', 'Email Sent', 'SMS Sent', 'SMS Delivered', 'Experiment Viewed', 'Web Push Support Detected', 'Push Notification Sent')
                and mp_app_version_string is not null
                and mp_event_name like 'App%'
        group by 1,2
        ),
    
    final_push_base as
    
        (select
            implied_user_id as user_id,
            notification_time,
            nth_notification_received_rev,
            tapped_push,
            coalesce(app_session,0) as app_session,
            invested
        from
            pushes_base
        left join
            investments_base
                on pushes_base.implied_user_id = investments_base.user_id
                    and pushes_base.push_name = investments_base.final_attribution
        left join
            session_base
                on pushes_base.implied_user_id = session_base.user_id
                    and date(pushes_base.notification_time) = date(session_base.session_at)
        )

select
    *
from
    final_push_base"

# get the data (takes a while, patience is a virtue)
email_data <- dbGetQuery(con, email_query)
push_data <- dbGetQuery(con, push_query)

# close connection
dbDisconnect(con)

# coalesce to 0s
# email
email_data$invested <- coalesce(email_data$invested, 0)
email_data$opened <- coalesce(email_data$opened, 0)
email_data$clicked_esp <- coalesce(email_data$clicked_esp, 0)
email_data$web_or_app_session <- coalesce(email_data$web_or_app_session, 0)
# push
push_data$invested <- coalesce(push_data$invested, 0)
push_data$tapped_push <- coalesce(push_data$tapped_push, 0)
push_data$app_session <- coalesce(push_data$app_session, 0)

# trailing metrics excluding current
window_length <- 15

# email
email_data <- email_data %>%
  group_by(user_id) %>%
  arrange(nth_email_received_rev) %>%
  mutate(
    trailing_inv_rate = (rollsum(as.numeric(invested), k = window_length + 1, fill = NA, align = 'left') - invested) / window_length,
    trailing_open_rate = (rollsum(as.numeric(opened), k = window_length + 1, fill = NA, align = 'left') - opened) / window_length,
    trailing_click_rate = (rollsum(as.numeric(clicked_esp), k = window_length + 1, fill = NA, align = 'left') - clicked_esp) / window_length,
    trailing_sess_rate = (rollsum(as.numeric(web_or_app_session), k = window_length + 1, fill = NA, align = 'left') - web_or_app_session) / window_length,
  ) %>%
  ungroup()
# push
push_data <- push_data %>%
  group_by(user_id) %>%
  arrange(nth_notification_received_rev) %>%
  mutate(
    trailing_inv_rate = (rollsum(as.numeric(invested), k = window_length + 1, fill = NA, align = 'left') - invested) / window_length,
    trailing_tap_rate = (rollsum(as.numeric(tapped_push), k = window_length + 1, fill = NA, align = 'left') - tapped_push) / window_length,
    trailing_sess_rate = (rollsum(as.numeric(app_session), k = window_length + 1, fill = NA, align = 'left') - app_session) / window_length,
  ) %>%
  ungroup()




############# push model #############

# split train and test at the user level, not observation
push_uuids <- unique(push_data$user_id)
push_train_data <- push_data[which(push_data$user_id %in% sample(push_uuids, length(push_uuids) * 0.8)),]
push_test_data <- push_data[which(!(push_data$user_id %in% unique(push_train_data$user_id))),]

# turn to factor to be used in downSample()
push_train_data$invested <- as.factor(push_train_data$invested)

# balance
push_train_balanced <- downSample(
  x = push_train_data[, c("user_id", "trailing_inv_rate", 'trailing_tap_rate', 'trailing_sess_rate')],
  y = push_train_data$invested,
  yname = "invested"
)

# remove nulls
push_train_balanced <- push_train_balanced %>% filter(complete.cases(trailing_inv_rate))

# fir the logistic regression model
push_logit_model <- glm(
  invested ~ trailing_inv_rate + trailing_tap_rate + trailing_sess_rate,
  data = push_train_balanced,
  family = binomial
)
summary(push_logit_model)

# again, turn to factor to be used in downSample()
push_test_data$invested <- as.factor(push_test_data$invested)

# balance the test
push_test_balanced <- downSample(
  x = push_test_data[, c("user_id", "trailing_inv_rate", 'trailing_tap_rate', 'trailing_sess_rate')],
  y = push_test_data$invested,
  yname = "invested"
)

# fit probabilities, classify, and create confusion matrix
push_test_balanced$prob_logit <- predict(push_logit_model, newdata = push_test_balanced, type = "response")
push_test_balanced$pred_inv <- ifelse(push_test_balanced$prob_logit >= 0.5, 1, 0)
table(push_test_balanced$pred_inv, push_test_balanced$invested) # counts
round(table(push_test_balanced$pred_inv, push_test_balanced$invested) / nrow(push_test_balanced), 2) # %s




############# email model #############

# split train and test at the user level, not observation
email_uuids <- unique(email_data$user_id)
email_train_data <- email_data[which(email_data$user_id %in% sample(email_uuids, length(email_uuids) * 0.8)),]
email_test_data <- email_data[which(!(email_data$user_id %in% unique(email_train_data$user_id))),]

# turn to factor to be used in downSample()
email_train_data$invested <- as.factor(email_train_data$invested)

# balance
email_train_balanced <- downSample(
  x = email_train_data[, c("user_id", "trailing_inv_rate", 'trailing_open_rate', 'trailing_click_rate', 'trailing_sess_rate')],
  y = email_train_data$invested,
  yname = "invested"
)

# remove nulls
email_train_balanced <- email_train_balanced %>% filter(complete.cases(trailing_inv_rate))

# fir the logistic regression model
email_logit_model <- glm(
  invested ~ trailing_inv_rate + trailing_open_rate + trailing_click_rate + trailing_sess_rate,
  data = email_train_balanced,
  family = binomial
)
summary(email_logit_model)

# again, turn to factor to be used in downSample()
email_test_data$invested <- as.factor(email_test_data$invested)

# balance the test
email_test_balanced <- downSample(
  x = email_test_data[, c("user_id", "trailing_inv_rate", 'trailing_open_rate', 'trailing_click_rate', 'trailing_sess_rate')],
  y = email_test_data$invested,
  yname = "invested"
)

# fit probabilities, classify, and create confusion matrix
email_test_balanced$prob_logit <- predict(email_logit_model, newdata = email_test_balanced, type = "response")
email_test_balanced$pred_inv <- ifelse(email_test_balanced$prob_logit >= 0.5, 1, 0)
table(email_test_balanced$pred_inv, email_test_balanced$invested) # counts
round(table(email_test_balanced$pred_inv, email_test_balanced$invested) / nrow(email_test_balanced), 2) # %s


############# coefficients of the models #############
# email
email_logit_model$coefficients
# push
push_logit_model$coefficients
