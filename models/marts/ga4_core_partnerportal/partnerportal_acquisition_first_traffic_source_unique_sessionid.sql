{% if var("static_incremental_days", false) %}
{% set partitions_to_replace = [] %}
{% for i in range(var("static_incremental_days")) %}
{% set partitions_to_replace = partitions_to_replace.append(
    "date_sub(current_date, interval " + (i + 1) | string + " day)"
) %}
{% endfor %}
{{
    config(
        pre_hook="{{ combine_property_data() }}" if var("property_ids", false) else "",
        materialized="incremental",
        incremental_strategy="insert_overwrite",
        partition_by={
            "field": "event_date_dt",
            "data_type": "date",
        },
        partitions=partitions_to_replace,
        cluster_by=["session_id"],
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
        cluster_by=["session_id"],
    )
}}
{% endif %}

-- select the first event that is not a session_start 
-- or first_visit event from each session
with events as (
  select
    parse_date('%Y%m%d', event_date) as event_date,
    -- unique session id
    concat(user_pseudo_id, (select value.int_value from unnest(event_params) where key = 'ga_session_id')) as session_id,
    user_pseudo_id,
    -- ga_session_id is the unix timestamp in seconds when the session started
    (select value.int_value from unnest(event_params) where key = 'ga_session_id') as session_start,
    (select value.string_value from unnest(event_params) where key = 'source') as source,
    (select value.string_value from unnest(event_params) where key = 'medium') as medium,
    (select value.string_value from unnest(event_params) where key = 'campaign') as campaign,
    -- include the gclid parameter
    (select value.string_value from unnest(event_params) where key = 'gclid') as gclid,
    -- flag the session's first event
    if(
      row_number() over(
        partition by concat(user_pseudo_id, (select value.int_value from unnest(event_params) where key = 'ga_session_id'))
        order by
          event_timestamp asc
      ) = 1,
      true,
      false
    ) as session_first_event
from  `theta-byte-348711.analytics_277765685.events_*`
  where
    event_name not in ('session_start', 'first_visit') qualify session_first_event = true
{% if is_incremental() %}

        {% if var('static_incremental_days', false ) %}
            and parse_date('%Y%m%d', left(_TABLE_SUFFIX, 8)) in ({{ partitions_to_replace | join(',') }})
        {% else %}
            -- Incrementally add new events. Filters on _TABLE_SUFFIX using the max event_date_dt value found in {{this}}
            -- See https://docs.getdbt.com/reference/resource-configs/bigquery-configs#the-insert_overwrite-strategy
            and parse_date('%Y%m%d',left(_TABLE_SUFFIX, 8)) >= _dbt_max_partition
        {% endif %}
    {% endif %}
    )


select
  event_date as event_date_dt,
  session_id,
  user_pseudo_id,
  session_start,
  -- wrap the session details in a struct to make it easier 
  -- to identify sessions with traffic source data
  if(
    source is not null
    or medium is not null
    or campaign is not null
    or gclid is not null,
    (
      select
        as struct if(
          gclid is not null,
          'google',
          source
        ) as source,
        if(
          gclid is not null,
          'cpc',
          medium
        ) as medium,
        campaign,
        gclid
    ),
    null
  ) as session_traffic_source
from
  events