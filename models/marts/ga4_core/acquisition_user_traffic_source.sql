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
            geo.country as country,
            traffic_source.source as source,
            traffic_source.medium as medium,
            traffic_source.name as campaign,
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
                 case
                    when
                        event_name = 'form_submission'
                    then
                        (
                            select value.string_value
                            from unnest(event_params)
                            where event_name = 'form_submission' and key = 'page_location'
                        )
                end as conversion_page,
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

            countif(event_name = 'form_submission') as form_submission_cookieless_pings,
            count(case when event_name = 'form_submission' and user_pseudo_id is not null then 1 end) as form_submissions_fullconsent,
            countif(event_name = 'file_download') as file_downloads,

        from `theta-byte-348711.analytics_307176780.events_*`
        where
            device.web_info.hostname = 'www.withsecure.com'

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
            event_date_dt, country, user_pseudo_id, session_id, unique_session_id, source, medium, campaign, landing_page, conversion_page
    )

select
    event_date_dt,
    country,
    source,
    medium,
    campaign,
    max(ga4_udf.channel_group(source, medium, campaign)) as default_channel_grouping,
    max({{ extract_page_path('landing_page') }}) as landing_page,
    max({{ extract_page_path('conversion_page') }}) as conversion_page,
    ga4_udf.country_subfolder(max(landing_page)) as country_subfolder,
    prep.user_pseudo_id as user,
    count(distinct unique_session_id) as sessions,
    count(distinct case when session_engaged = '1' then unique_session_id end) as engaged_sessions,
    safe_divide(count(distinct case when session_engaged = '1' then unique_session_id end),count(distinct unique_session_id)) as engagement_rate,
    safe_divide(sum(engagement_time_seconds),count(distinct case when session_engaged = '1' then unique_session_id end)) as engagement_time,
    sum(form_submission_cookieless_pings) as form_submission_cookieless_pings,
    sum(form_submissions_fullconsent) as form_submissions_fullconsent,
    sum(file_downloads) as file_downloads

from
prep

GROUP BY
  event_date_dt,
  country,
  source,
  medium,
  campaign,
  user