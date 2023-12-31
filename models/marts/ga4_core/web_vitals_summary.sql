# Web Vitals Materialised Table v3.3
SELECT
DATE(event_timestamp) as event_date_dt,
  ga_session_id,
  IF(
    EXISTS(SELECT 1 FROM UNNEST(events) AS e WHERE e.event_name = 'first_visit'),
    'New user',
    'Returning user') AS user_type,
  IF(
    (SELECT MAX(session_engaged) FROM UNNEST(events)) > 0, 'Engaged', 'Not engaged')
    AS session_engagement,
  evt.* EXCEPT (session_engaged, event_name),
  event_name AS metric_name,
  FORMAT_TIMESTAMP('%Y%m%d', event_timestamp) AS event_date,
  
  CASE metric_rating
    WHEN 'good' THEN 'Good'
    WHEN 'ni' THEN 'Needs Improvement'
    WHEN 'needs-improvement' THEN 'Needs Improvement'
    WHEN 'poor' THEN 'Poor'
    ELSE metric_rating
  END AS metric_status


FROM
  (
    SELECT
      ga_session_id,
      ARRAY_AGG(custom_event) AS events
    FROM
      (
        SELECT
          ga_session_id,
          STRUCT(
            country,
            device_category,
            device_os,
            traffic_medium,
            traffic_name,
            traffic_source,
            page_path,
            IF(debug_target2 IS NULL, debug_target, CONCAT(debug_target, debug_target2)) AS debug_target,
            event_timestamp,
            event_name,
            metric_id,
            IF(event_name = 'LCP' OR event_name = 'TTFB' OR event_name = 'FCP', metric_value / 1000, metric_value) AS metric_value,
            user_pseudo_id,
            session_engaged,
            session_revenue,
            metric_rating,
            page_location,
            page_type,
            continent,
            region,
            device_browser,
            effective_connection_type,
            save_data,
            width,
            height

            ) AS custom_event
        FROM
          (
            SELECT
              (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS ga_session_id, # can be null in consent mode
              (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'metric_id') AS metric_id,
              ANY_VALUE(device.category) AS device_category,
              ANY_VALUE(device.operating_system) AS device_os,
              ANY_VALUE(traffic_source.medium) AS traffic_medium,
              ANY_VALUE(traffic_source.name) AS traffic_name,
              ANY_VALUE(traffic_source.source) AS traffic_source,
              ANY_VALUE(
                REGEXP_SUBSTR(
                  (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location'),
                  r'^[^?]+')) AS page_path,
              
              ANY_VALUE(
                (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'debug_target'))
                AS debug_target,
              ANY_VALUE(
                (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'debug_target2'))
                AS debug_target2,
              ANY_VALUE(user_pseudo_id) AS user_pseudo_id,
              ANY_VALUE(geo.country) AS country,
              ANY_VALUE(event_name) AS event_name,
              SUM(ecommerce.purchase_revenue) AS session_revenue,
              MAX(
                (
                  SELECT
                    COALESCE(
                      value.double_value, value.int_value, CAST(value.string_value AS NUMERIC))
                  FROM UNNEST(event_params)
                  WHERE key = 'session_engaged'
                )) AS session_engaged,
              TIMESTAMP_MICROS(MAX(event_timestamp)) AS event_timestamp,
              MAX(
                (
                  SELECT COALESCE(value.double_value, value.int_value)
                  FROM UNNEST(event_params)
                  WHERE key = 'metric_value'
                )) AS metric_value,


                ANY_VALUE((SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'metric_rating')) AS metric_rating,
                ANY_VALUE((SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location')) AS page_location,
                ANY_VALUE((SELECT COALESCE(value.string_value, CAST(value.int_value AS STRING)) FROM UNNEST(event_params) WHERE key = 'page_type')) AS page_type,
                ANY_VALUE(geo.continent) AS continent,
                ANY_VALUE(geo.region) AS region,
                ANY_VALUE(device.web_info.browser) AS device_browser,
                ANY_VALUE((SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'effective_connection_type')) AS effective_connection_type,
                ANY_VALUE((SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'save_data')) AS save_data,
                ANY_VALUE((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'width')) AS width,
                ANY_VALUE((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'height')) AS height

            FROM
              `theta-byte-348711.analytics_307176780.events_*`
            WHERE

              event_name IN ('LCP', 'FID', 'CLS', 'TTFB', 'FCP', 'INP', 'first_visit', 'form_submission')
              and device.web_info.hostname = 'www.withsecure.com'
            GROUP BY
              1, 2
          )
      )
    WHERE
      ga_session_id IS NOT NULL
    GROUP BY ga_session_id # can be null in consent mode ?
  )
CROSS JOIN UNNEST(events) AS evt
WHERE evt.event_name NOT IN ('first_visit', 'form_submission')