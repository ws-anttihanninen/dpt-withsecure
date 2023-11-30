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


with events as (
select
  concat(user_pseudo_id,(select value.int_value from unnest(event_params) where key = 'ga_session_id')) as session_id,
  *
from
  `theta-byte-348711.analytics_307176780.events_*`
where
    device.web_info.hostname = 'labs.withsecure.com' 
     {% if is_incremental() %}

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
        ),

pages_prep as (
select
  session_id,
  (select value.string_value from unnest(event_params) where key = 'page_location') as page_path,
  (select value.string_value from unnest(event_params) where key = 'page_location') as page_location,
  (select value.string_value from unnest(event_params) where key = 'page_referrer') as page_referrer,
  row_number() over (partition by session_id order by event_timestamp asc) as page_views_ascending,
  row_number() over (partition by session_id order by event_timestamp desc) as page_views_descending
from
  events
where
  event_name = 'page_view'),

pages as (
select
  session_id,
  max(case when page_views_ascending = 1 then page_path end) as landing_page,
  max(case when page_views_descending = 1 then page_path end) as exit_page,
  max(case when page_views_ascending = 1 then page_referrer end) as page_referrer,
  count(page_location) as total_page_views,
  count(distinct page_location) as unique_page_views
from
  pages_prep
group by
  session_id)

select
  user_pseudo_id as user,
  events.session_id,
  min(parse_date('%Y%m%d', event_date)) as event_date_dt,
  min(timestamp_micros(event_timestamp)) as session_started_at,
  max(timestamp_micros(event_timestamp)) as session_last_event_at,
  max((select value.int_value from unnest(event_params) where key = 'ga_session_number')) as session_number,
  max((select value.int_value from unnest(event_params) where key = 'session_engaged')) as session_engaged,
  sum((select value.int_value from unnest(event_params) where key = 'engagement_time_msec'))/1000 as engagement_time_seconds,
  {{extract_page_path('max(pages.page_referrer)')}} as page_referrer,
  {{extract_page_path('max(pages.landing_page)')}} as landing_page,
  {{extract_page_path('max(pages.exit_page)')}} as exit_page,
  {{country_subfolder('max(pages.landing_page)')}} as country_subfolder,
  max(device.category) as device_category,
  max(device.operating_system) as device_operating_system,
  max(device.operating_system_version) as device_operating_system_version,
  max(device.language) as device_language,
  max(device.web_info.browser) as device_web_info_browser,
  max(geo.country) as geo_country,
  max(pages.total_page_views) as total_page_views,
  max(pages.unique_page_views) as unique_page_views,
  countif(event_name = 'file_download') as file_download,
  countif(event_name = 'form_submission') as form_submission
from
  events
  left join pages on events.session_id = pages.session_id
group by
  user_pseudo_id,
  session_id
    
