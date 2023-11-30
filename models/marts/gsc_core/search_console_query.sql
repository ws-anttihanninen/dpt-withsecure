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
        cluster_by=["country", "query", "country_subfolder", "brand_vs_nonbrand"],
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
        cluster_by=["country", "query", "country_subfolder", "brand_vs_nonbrand"],
    )
}}
{% endif %}

select
    data_date as event_date_dt,
    country,
    {{ extract_hostname_from_url('url') }} as hostname,
    query,
    search_type,
    device,
    {{extract_page_path('url')}} as page_path,
    {{country_subfolder('url')}} as country_subfolder,
    is_anonymized_query,
    CASE
      when is_anonymized_query is false and regexp_contains(query, r".*w(?:it(?:h(?: secur(?:ity|e)|secur(?:ity|e)?)| ?secure)|(?:(?:\\/th)? |hit[ eh]|hit)?secure).*|.*f(?: secur(?:ity|e)|secur(?:ity|e)|\\-secure?| ?secur).*|.*withセキュア.*|.*ウィズセキュア.*|.*ウィズセキュア株式会社.*|(?:w(?:ith)?[-_./]?secure|with ?secure|f[-_./]?secure)") then 'Brand'     
      when is_anonymized_query is false and NOT regexp_contains(query, r".*w(?:it(?:h(?: secur(?:ity|e)|secur(?:ity|e)?)| ?secure)|(?:(?:\\/th)? |hit[ eh]|hit)?secure).*|.*f(?: secur(?:ity|e)|secur(?:ity|e)|\\-secure?| ?secur).*|.*withセキュア.*|.*ウィズセキュア.*|.*ウィズセキュア株式会社.*|(?:w(?:ith)?[-_./]?secure|with ?secure|f[-_./]?secure)") then 'Non-Brand'
      ELSE '(Other)'
      END as brand_vs_nonbrand,
    impressions,
    clicks,
    sum_position as position

from `theta-byte-348711.searchconsole.searchdata_url_impression`

