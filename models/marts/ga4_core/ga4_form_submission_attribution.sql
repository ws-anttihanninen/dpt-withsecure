{{
  config(
    materialized = "table",
    sort = 'attribution_model',
  )
}}


with variables as (
select
  30 as conversion_period,
  90 as attribution_window),
  
conversions as (
select
  user_pseudo_id,
  (select value.int_value from unnest(event_params) where key = 'ga_session_id') as session_id,
  concat(user_pseudo_id,event_timestamp) as conversion_id,
  max(timestamp_micros(event_timestamp)) as conversion_timestamp,
  row_number() over (partition by user_pseudo_id order by max(timestamp_micros(event_timestamp)) desc) as conversion_rank,
from
  `theta-byte-348711.analytics_307176780.events_*`,
  variables
where
  _table_suffix between format_date('%Y%m%d',date_sub(current_date(), interval conversion_period day))
  and format_date('%Y%m%d',date_sub(current_date(), interval 1 day))
  and device.web_info.hostname = 'www.withsecure.com'
  and event_name = 'form_submission'
  and regexp_contains((select value.string_value from unnest(event_params) where key = 'page_location'), r"telemarketing") is false
group by
  user_pseudo_id,
  session_id,
  conversion_id
qualify
  conversion_rank = 1),

sessions as (
select
  user_pseudo_id,
  (select value.int_value from unnest(event_params) where key = 'ga_session_id') as session_id,
  min(timestamp_micros(event_timestamp)) as session_start_timestamp,
  max(concat(coalesce((select value.string_value from unnest(event_params) where key = 'source'),'(direct)'),' / ',coalesce((select value.string_value from unnest(event_params) where key = 'medium'),'(none)'))) as source_medium
from
  `theta-byte-348711.analytics_307176780.events_*`,
  variables
where
  _table_suffix between format_date('%Y%m%d',date_sub(current_date(), interval attribution_window + conversion_period day))
  and format_date('%Y%m%d',date_sub(current_date(), interval 1 day))
  and device.web_info.hostname = 'www.withsecure.com'
  and user_pseudo_id in (select distinct user_pseudo_id from conversions)
  and regexp_contains((select value.string_value from unnest(event_params) where key = 'page_location'), r"telemarketing") is false
group by
  user_pseudo_id,
  session_id),

sessions_joined as (
select
  sessions.*,
  conversions.conversion_id,
  conversions.conversion_timestamp
from
  sessions
  left join conversions on sessions.user_pseudo_id = conversions.user_pseudo_id
  and sessions.session_id = conversions.session_id),

attribution_raw as (
select
  *,
  count(distinct session_id) over (partition by user_pseudo_id) as total_sessions_per_user,
  rank() over (partition by user_pseudo_id order by session_start_timestamp) as session_number
from
  sessions_joined as sessions,
  variables
where
  session_start_timestamp <= (select max(conversion_timestamp) from sessions_joined where sessions.user_pseudo_id = sessions_joined.user_pseudo_id)
  and session_start_timestamp >= timestamp_add((select max(conversion_timestamp) from sessions_joined where sessions.user_pseudo_id = sessions_joined.user_pseudo_id),interval - attribution_window day)
order by
  user_pseudo_id,
  session_number),

first_click as (
select
  user_pseudo_id,
  conversion_id,
  first_value(source_medium) over (partition by user_pseudo_id order by session_start_timestamp) as source_medium,
  1 as attribution_weight
from
  attribution_raw
where
  conversion_id is not null),

last_click as (
select
  user_pseudo_id,
  conversion_id,
  source_medium,
  1 as attribution_weight
from
  attribution_raw
where
  conversion_id is not null),

prep_last_non_direct_click as (
select
  user_pseudo_id,
  conversion_id,
  case
    when source_medium != '(direct) / (none)' then source_medium
    when source_medium = '(direct) / (none)' then last_value(nullif(source_medium,'(direct) / (none)') ignore nulls) over (partition by user_pseudo_id order by session_number)
  end as source_medium,
  1 as attribution_weight
from
  attribution_raw),

last_non_direct_click as (
select
  user_pseudo_id,
  conversion_id,
  coalesce(source_medium,'(direct) / (none)') as source_medium,
  attribution_weight
from prep_last_non_direct_click
where
  conversion_id is not null),

linear as (
select
  user_pseudo_id,
  source_medium,
  last_value(conversion_id) over (partition by user_pseudo_id order by conversion_timestamp asc rows between current row and unbounded following) as conversion,
  1 / (select max(session_number) from attribution_raw where conversion_id is not null and raw.user_pseudo_id = user_pseudo_id) as attribution_weight
from
  attribution_raw as raw),

time_decay as (
select
  user_pseudo_id,
  source_medium,
  last_value(conversion_id) over (partition by user_pseudo_id order by conversion_timestamp asc rows between current row and unbounded following) as conversion,
  case when total_sessions_per_user = 1 then 1
  else safe_divide(power(2,session_number / total_sessions_per_user),sum(power(2,session_number/total_sessions_per_user)) over (partition by user_pseudo_id)) end as attribution_weight
from
  attribution_raw),

position_based as (
select
  user_pseudo_id,
  source_medium,
  last_value(conversion_id) over (partition by user_pseudo_id order by conversion_timestamp asc rows between current row and unbounded following) as conversion,
  case when total_sessions_per_user = 1 then 1
  when total_sessions_per_user = 2 then 0.5
  when total_sessions_per_user > 2 then (
    case when session_number = 1 then 0.4
    when session_number = total_sessions_per_user then 0.4
    else 0.2 / (total_sessions_per_user - 2) end)
  end as attribution_weight
from
  attribution_raw)

select
  'first_click' as attribution_model,
  source_medium,
  sum(attribution_weight) as attribution_weight,
from
  first_click
group by
  attribution_model,
  source_medium
union all
select
  'last_click' as attribution_model,
  source_medium,
  sum(attribution_weight) as attribution_weight,
from
  last_click
group by
  attribution_model,
  source_medium
union all
select
  'last_non_direct_click' as attribution_model,
  source_medium,
  sum(attribution_weight) as attribution_weight,
from
  last_non_direct_click
group by
  attribution_model,
  source_medium
union all
select
  'linear' as attribution_model,
  source_medium,
  cast(round(sum(attribution_weight),0) as integer) as attribution_weight,
from
  linear
group by
  attribution_model,
  source_medium
union all
select
  'time_decay' as attribution_model,
  source_medium,
  cast(round(sum(attribution_weight),0) as integer) as attribution_weight,
from
  time_decay
group by
  attribution_model,
  source_medium
union all
select
  'position_based' as attribution_model,
  source_medium,
  cast(round(sum(attribution_weight),0) as integer) as attribution_weight,
from
  position_based
group by
  attribution_model,
  source_medium
order by
  attribution_weight desc