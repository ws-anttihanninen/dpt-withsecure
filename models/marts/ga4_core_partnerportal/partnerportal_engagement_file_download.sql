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
  user_pseudo_id as user,
  concat(user_pseudo_id,(select value.int_value from unnest(event_params) where key = 'ga_session_id')) as session_id,
  geo.country as country,
  device.language as device_language,
  (select value.string_value from unnest(event_params) where key = 'page_location') as page_path,
  (select value.string_value from unnest(event_params) where event_name = 'file_download' and key = 'link_url') as file_link_url,
  (select value.string_value from unnest(event_params) where event_name = 'file_download' and key = 'link_text') as file_link_text,
  (select value.string_value from unnest(event_params) where event_name = 'file_download' and key = 'file_name') as file_name,
  (select value.string_value from unnest(event_params) where event_name = 'file_download' and key = 'file_extension') as file_extension,
  countif(event_name = 'file_download') as file_download_cookieless,
  count(case when event_name = 'file_download' and user_pseudo_id is not null then 1 end) as file_download_fullconsent,

from
 `theta-byte-348711.analytics_277765685.events_*`

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
group by 
  event_date_dt,
  user,
  session_id,
  country,
  device_language,
  page_path,
  file_link_url,
  file_link_text,
  file_name,
  file_extension)

select
  event_date_dt,
  user,
  session_id as session,
  country,
  {{ extract_page_path('page_path') }} as page_path,
  device_language,
  file_link_url,
  file_link_text
  file_name,
  file_extension,
  sum(file_download_cookieless) as file_download_cookieless,
  sum(file_download_fullconsent) as file_download_fullconsent
from 
  prep

group by 
  event_date_dt,
  user,
  session,
  country,
  page_path,
  device_language,
  file_link_url,
  file_link_text,
  file_name,
  file_extension