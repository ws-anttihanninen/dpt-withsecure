{% if var("static_incremental_days", false) %}
{% set partitions_to_replace = [] %}
{% for i in range(var("static_incremental_days")) %}
{% set partitions_to_replace = partitions_to_replace.append(
    "date_sub(current_date, interval " + (i + 1) | string + " day)"
) %}
{% endfor %}
{{
    config(
        pre_hook="{{ combine_property_data() }}"
        if var("property_ids", false)
        else "",
        materialized="incremental",
        incremental_strategy="insert_overwrite",
        partition_by={
            "field": "event_date_dt",
            "data_type": "date",
        },
        partitions=partitions_to_replace,
        cluster_by=["user"],
    )
}}
{% else %}
{{
    config(
        materialized="incremental",
        incremental_strategy="insert_overwrite",
        partition_by={
            "field": "event_date_dt",
            "data_type": "date",
        },
        cluster_by=["user"],
    )
}}
{% endif %}


with prep as (
select
    parse_date('%Y%m%d', event_date) as event_date_dt,
    event_timestamp,
    user_pseudo_id,
    geo.country as country,
    concat(user_pseudo_id,(select value.int_value from unnest(event_params) where key = 'ga_session_id')) as session_id,
    (select value.string_value from unnest(event_params) where key = 'page_location') as page_path,
    max((select value.string_value from unnest(event_params) where key = 'session_engaged')) as session_engaged,
    sum((select value.int_value from unnest(event_params) where key = 'engagement_time_msec')) as engagement_time_msec,
    max((select value.int_value from unnest(event_params) where key = 'ga_session_number')) as session_number, 
    countif(event_name = 'form_submission') as form_submission_cookieless_pings,
    count(case when event_name= 'form_submission' and user_pseudo_id is not null then 1 end) as form_submissions_consent
    from `theta-byte-348711.analytics_307176780.events_*`
    where
     device.web_info.hostname = 'labs.withsecure.com'            {% if is_incremental() %}

            {% if var("static_incremental_days", false) %}
            and parse_date('%Y%m%d', left(_table_suffix, 8))
            in ({{ partitions_to_replace | join(",") }})
            {% else %}
            -- Incrementally add new events. Filters on _TABLE_SUFFIX using the max
            -- event_date_dt value found in {{this}}
            -- See
            -- https://docs.getdbt.com/reference/resource-configs/bigquery-configs#the-insert_overwrite-strategy
            and parse_date('%Y%m%d', left(_table_suffix, 8)) >= _dbt_max_partition
            {% endif %}
            {% endif %}
group by
  event_date_dt,
  event_timestamp,
  country,
  user_pseudo_id,
  session_id,
  page_path),  

exit_prep as (
    select
    session_id,
    page_path,
    row_number() over (partition by session_id order by event_timestamp desc) as page_views_descending

from prep
)

select
  event_date_dt,
  country,
  user_pseudo_id as user,
  prep.session_id as session,
  {{extract_page_path('case when page_views_descending = 1 then exit_prep.page_path end') }} as exit_page,
  {{country_subfolder('max(exit_prep.page_path)')}} as country_subfolder,
  count(distinct case when session_number = 1 then user_pseudo_id end) as new_users,
  count(distinct case when session_number = 1 then prep.session_id end) as new_sessions,
  count(distinct case when session_engaged = '1' then prep.session_id end) as engaged_sessions,
  safe_divide(sum(engagement_time_msec/1000),count(distinct case when session_engaged = '1' then prep.session_id end)) as average_engagement_time_per_session_seconds,
  safe_divide(count(distinct case when session_engaged = '1' then prep.session_id end),count(distinct prep.session_id)) as engagement_rate,
  sum(form_submission_cookieless_pings) as form_submission_cookieless_pings,
  sum(form_submissions_consent) as form_submissions_consent,
  safe_divide(sum(form_submissions_consent),count(distinct prep.session_id)) as session_conversion_rate
from
  prep
  left join exit_prep on prep.session_id = exit_prep.session_id
group by
  event_date_dt,
  country,
  user,
  session,
  exit_page
  