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
        cluster_by=["user", "country"],
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
        cluster_by=["user", "country"],
    )
}}
{% endif %}

select
    parse_date('%Y%m%d', event_date) as event_date_dt,
    geo.country as country,
    user_pseudo_id as user,
    ga4_udf.country_subfolder(max(case when (select value.int_value from unnest(event_params) where event_name = 'page_view' and key = 'entrances') = 1 then (select value.string_value from unnest(event_params) where event_name = 'page_view' and key = 'page_location') end)) as country_subfolder,
    count(distinct case when (select value.int_value from unnest(event_params) where key = 'engagement_time_msec') > 0  or (select value.string_value from unnest(event_params) where key = 'session_engaged') = '1' then user_pseudo_id else null end) as active_users,
    count(distinct case when (select value.int_value from unnest(event_params) where key = 'ga_session_number') = 1 then user_pseudo_id else null end) as new_users,
    count(distinct concat(user_pseudo_id,(select value.int_value from unnest(event_params) where key = 'ga_session_id'))) as sessions,
    count(distinct case when (select value.int_value from unnest(event_params) where key = 'ga_session_number') = 1 then concat(user_pseudo_id,(select value.int_value from unnest(event_params) where key = 'ga_session_id')) else null end) as new_sessions,
    count(distinct case when (select value.string_value from unnest(event_params) where key = 'session_engaged') = '1' then concat(user_pseudo_id,(select value.int_value from unnest(event_params) where key = 'ga_session_id')) end) as engaged_sessions

from `theta-byte-348711.analytics_307176780.events_*`
where
    device.web_info.hostname = 'www.withsecure.com'

    {% if is_incremental() %}

    {% if var("static_incremental_days", false) %}
    and parse_date('%Y%m%d', left(_table_suffix, 8))
    in ({{ partitions_to_replace | join(",") }})
    -- Incrementally add new events. Filters on _TABLE_SUFFIX using the max
    -- event_date_dt value found in {{this}}
    -- See
    -- https://docs.getdbt.com/reference/resource-configs/bigquery-configs#the-insert_overwrite-strategy
    {% else %} and parse_date('%Y%m%d', left(_table_suffix, 8)) >= _dbt_max_partition
    {% endif %}
    {% endif %}

group by event_date_dt, country, user
