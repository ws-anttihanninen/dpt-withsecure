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
        cluster_by=["page_path"],
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
        cluster_by=["page_path"],
    )
}}
{% endif %}
with
    prep as (
        select
            parse_date('%Y%m%d', event_date) as event_date_dt,
            event_timestamp,
            user_pseudo_id,
            concat(
                user_pseudo_id,
                (
                    select value.int_value
                    from unnest(event_params)
                    where key = 'ga_session_id'
                )
            ) as session_id,
            case
                when
                    (
                        select value.string_value
                        from unnest(event_params)
                        where key = 'session_engaged'
                    )
                    = '1'
                then
                    concat(
                        user_pseudo_id,
                        (
                            select value.int_value
                            from unnest(event_params)
                            where key = 'ga_session_id'
                        )
                    )
            end as engaged_session,
            event_name,
            geo.country as country,
            (
                select value.string_value
                from unnest(event_params)
                where key = 'page_location'
            ) as page_path,
            (
                select value.int_value from unnest(event_params) where key = 'entrances'
            ) as entrances,
        from `theta-byte-348711.analytics_307176780.events_*`
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

    page_views as (
        select
            user_pseudo_id,
            session_id,
            engaged_session,
            event_date_dt,
            country,
            {{ extract_page_path('page_path ') }} as page_path,
            entrances,
            row_number() over (
                partition by session_id order by event_timestamp desc
            ) as page_view_descending
        from prep
        where event_name = 'page_view'
    ),

    conversion_prep as (
        select user_pseudo_id, session_id, event_date_dt, country, event_name, {{ extract_page_path('page_path') }} as page_path,
        from prep
        where event_name = 'form_submission'
    ),

    conversions as (
    select event_date_dt, country, user_pseudo_id, session_id, page_path, count(event_name) as form_submissions_cookielesspings,
    count(case when user_pseudo_id is not null then 1 end) as form_submissions_fullconsent
    from conversion_prep
    group by event_date_dt, country, user_pseudo_id, session_id, page_path
    )

select
    page_views.event_date_dt as event_date_dt,
    page_views.country as country,
    page_views.page_path as page_path,
    ga4_udf.country_subfolder(max(page_views.page_path)) as country_subfolder,
    page_views.user_pseudo_id as user,
    count(page_views.page_path) as total_pageviews,
    count(distinct page_views.session_id) as unique_pageviews,
    count(case when entrances = 1 then page_views.session_id end) as entrances,
    count(case when page_view_descending = 1 then page_views.session_id end) as exits,
    safe_divide(count(distinct page_views.engaged_session),count(distinct concat(page_views.session_id))) as engagement_rate,
    sum(form_submissions_cookielesspings) as form_submissions_cookielesspings,
    sum(form_submissions_fullconsent) as form_submissions_fullconsent
from page_views
left join
    conversions
    on page_views.event_date_dt = conversions.event_date_dt
    and page_views.page_path = conversions.page_path
    and page_views.country = conversions.country
    and page_views.user_pseudo_id = conversions.user_pseudo_id

group by
event_date_dt,
country,
page_path,
user