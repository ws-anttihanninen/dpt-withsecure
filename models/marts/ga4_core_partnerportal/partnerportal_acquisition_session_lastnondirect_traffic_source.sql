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
        cluster_by=["user, country"],
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
        cluster_by=["user, country"],
    )
}}
{% endif %}

-- last non-direct traffic dimensions
with
    lastnondirect as (
        select
            *,
            ifnull(
                session_traffic_source,
                last_value(session_traffic_source ignore nulls) over (
                    partition by user_pseudo_id
                    order by
                        session_start range between unbounded preceding and current row
                )
            ) as session_traffic_source_last_non_direct,
        from {{ ref("partnerportal_acquisition_first_traffic_source_unique_sessionid") }}
    ),

    trafficsources as (
        select
            ifnull(session_traffic_source_last_non_direct.source, '(direct)') as source,
            ifnull(session_traffic_source_last_non_direct.medium, '(none)') as medium,
            ifnull(
                session_traffic_source_last_non_direct.campaign, '(none)'
            ) as campaign,
            session_id,
        from lastnondirect
    ),

    prep as (
        select
            parse_date('%Y%m%d', event_date) as event_date_dt,
            geo.country as country,
            device.language as device_language,
            user_pseudo_id,
            (
                    select value.int_value
                    from unnest(event_params)
                    where key = 'ga_session_id'
                ) as session_id,
            -- unique session id
            concat(
                user_pseudo_id,
                (
                    select value.int_value
                    from unnest(event_params)
                    where key = 'ga_session_id'
                )
            ) as unique_session_id,
                case
                    when
                        (
                            select value.int_value
                            from unnest(event_params)
                            where event_name = 'page_view' and key = 'entrances'
                        )
                        = 1
                    then
                        (
                            select value.string_value
                            from unnest(event_params)
                            where event_name = 'page_view' and key = 'page_location'
                        )
                end as landing_page,
            max(
                (
                    select value.string_value
                    from unnest(event_params)
                    where key = 'session_engaged'
                )
            ) as session_engaged,
            sum(
                (
                    select value.int_value
                    from unnest(event_params)
                    where key = 'engagement_time_msec'
                )
            )
            / 1000 as engagement_time_seconds,

            countif(event_name = 'file_download') as file_download_cookieless,
            count(case when event_name = 'file_download' and user_pseudo_id is not null then 1 end) as file_download_consented,
            from  `theta-byte-348711.analytics_277765685.events_*`
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
            event_date_dt, country, user_pseudo_id, device_language, session_id, unique_session_id, landing_page
    )

select
    prep.event_date_dt as event_date_dt,
    country,
    device_language,
    source,
    medium,
    campaign,
    max(ga4_udf.channel_group(source, medium, campaign)) as default_channel_grouping,
    {{ extract_page_path('landing_page') }} as landing_page,
    prep.user_pseudo_id as user,
    unique_session_id as unique_session,
    count(distinct case when session_engaged = '1' then unique_session_id end) as engaged_sessions,
    safe_divide(count(distinct case when session_engaged = '1' then unique_session_id end),count(distinct unique_session_id)) as engagement_rate,
    safe_divide(sum(engagement_time_seconds),count(distinct case when session_engaged = '1' then unique_session_id end)) as engagement_time,
    sum(file_download_consented) as file_download_consented,
    sum(file_download_cookieless) as file_download_cookieless

from prep
left join trafficsources on prep.unique_session_id = trafficsources.session_id

group by event_date_dt, country, device_language, source, medium, campaign, landing_page, user, unique_session
