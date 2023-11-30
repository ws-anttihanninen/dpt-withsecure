{% macro country_subfolder(page_path) %}
    CASE
      when regexp_contains({{page_path}}, r"/en") then 'Global - English'     
      when regexp_contains({{page_path}}, r"dk-en") THEN 'Denmark - English'
      when regexp_contains({{page_path}}, r"dk-da") THEN 'Denmark - Danish'
      when regexp_contains({{page_path}}, r"se-sv") THEN 'Sweden - Swedish'
      when regexp_contains({{page_path}}, r"us-en") THEN 'United States - English'
      when regexp_contains({{page_path}}, r"gb-en") THEN 'United Kingdom - English'
      when regexp_contains({{page_path}}, r"nl-en") THEN 'Netherlands - English'
      when regexp_contains({{page_path}}, r"pt") THEN 'Portugal - Portuguese'
      when regexp_contains({{page_path}}, r"br-pt") THEN 'Brazil - Portuguese'
      when regexp_contains({{page_path}}, r"pl") THEN 'Poland - Polish'
      when regexp_contains({{page_path}}, r"no-en") THEN 'Norway - English'
      when regexp_contains({{page_path}}, r"jp-ja") THEN 'Japan - Japanese'
      when regexp_contains({{page_path}}, r"it") THEN 'Italy - Italian'
      when regexp_contains({{page_path}}, r"es") THEN 'Spain - Spanish'
      when regexp_contains({{page_path}}, r"mx-es") THEN 'LATAM - Spanish'
      when regexp_contains({{page_path}}, r"fr") THEN 'France - French'
      when regexp_contains({{page_path}}, r"fi") THEN 'Finland - Finnish'
      when regexp_contains({{page_path}}, r"de") THEN 'Germany - German'
      ELSE '(Other)'
      END
{% endmacro %}