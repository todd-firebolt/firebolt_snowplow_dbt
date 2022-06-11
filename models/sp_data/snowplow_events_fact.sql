
/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/

{{ config(
   materialized = 'incremental',
   incremental_strategy='append',
   table_type = 'fact',
   primary_index = ['event', 'app_id', 'platform', 'collector_date', 'collector_tstamp'],
   indexes = [
     {
       'index_type': 'aggregating',
       'key_columns': ['event', 'app_id', 'platform', 'collector_date', 'geo_country'],
       'aggregation': ['count(*)','count(distinct event_id)', 'count(distinct page_view_id)',
         'count(distinct user_id)', 'count(distinct txn_id)']
     },
     {
       'index_type': 'aggregating',
       'key_columns': ['event', 'domain_sessionid', 'collector_date'],
       'aggregation': ['count(*)','count(distinct event_id)', 'count(distinct page_view_id)']
     }
   ]
   )
}}


SELECT
    COALESCE(JSON_EXTRACT(raw_json, '/page_view_id', 'TEXT'),'') AS page_view_id
    ,COALESCE(JSON_EXTRACT(raw_json, '/app_id', 'TEXT'),'') AS app_id
    ,JSON_EXTRACT(raw_json, '/platform', 'TEXT') AS platform
    ,COALESCE(CONCAT(SUBSTR(JSON_EXTRACT(raw_json, '/etl_tstamp', 'TEXT'),1,10), ' ', SUBSTR(JSON_EXTRACT(raw_json, '/etl_tstamp', 'TEXT'),12,8))::DATETIME,TO_DATE('2000-01-01')) AS etl_tstamp
    ,CONCAT(SUBSTR(JSON_EXTRACT(raw_json, '/collector_tstamp', 'TEXT'),1,10), ' ', SUBSTR(JSON_EXTRACT(raw_json, '/collector_tstamp', 'TEXT'),12,8))::DATETIME AS collector_tstamp
    ,DATE_TRUNC('DAY',CONCAT(SUBSTR(JSON_EXTRACT(raw_json, '/collector_tstamp', 'TEXT'),1,10), ' ', SUBSTR(JSON_EXTRACT(raw_json, '/collector_tstamp', 'TEXT'),12,8))::DATETIME) AS collector_date
    ,COALESCE(CONCAT(SUBSTR(JSON_EXTRACT(raw_json, '/dvce_created_tstamp', 'TEXT'),1,10), ' ', SUBSTR(JSON_EXTRACT(raw_json, '/dvce_created_tstamp', 'TEXT'),12,8))::DATETIME,TO_DATE('2000-01-01')) AS dvce_created_tstamp
    ,JSON_EXTRACT(raw_json, '/event', 'TEXT') AS event
    ,JSON_EXTRACT(raw_json, '/event_id', 'TEXT') AS event_id
    ,COALESCE(JSON_EXTRACT(raw_json, '/txn_id', 'DOUBLE'),0) AS txn_id
    ,COALESCE(JSON_EXTRACT(raw_json, '/name_tracker', 'TEXT'),'') AS name_tracker
    ,JSON_EXTRACT(raw_json, '/v_tracker', 'TEXT') AS v_tracker
    ,JSON_EXTRACT(raw_json, '/v_collector', 'TEXT') AS v_collector
    ,JSON_EXTRACT(raw_json, '/v_etl', 'TEXT') AS v_etl
    ,COALESCE(JSON_EXTRACT(raw_json, '/user_id', 'TEXT'),'Unknown') AS user_id
    ,COALESCE(JSON_EXTRACT(raw_json, '/user_ipaddress', 'TEXT'),'') AS user_ipaddress
    ,COALESCE(JSON_EXTRACT(raw_json, '/user_fingerprint', 'TEXT'),'') AS user_fingerprint
    ,COALESCE(JSON_EXTRACT(raw_json, '/domain_userid', 'TEXT'),'') AS domain_userid
    ,COALESCE(JSON_EXTRACT(raw_json, '/domain_sessionidx', 'DOUBLE'),0) AS domain_sessionidx
    ,COALESCE(JSON_EXTRACT(raw_json, '/network_userid', 'TEXT'),'') AS network_userid
    ,COALESCE(JSON_EXTRACT(raw_json, '/geo_country', 'TEXT'),'') AS geo_country
    ,COALESCE(JSON_EXTRACT(raw_json, '/geo_region', 'TEXT'),'') AS geo_region
    ,COALESCE(JSON_EXTRACT(raw_json, '/geo_city', 'TEXT'),'') AS geo_city
    ,COALESCE(JSON_EXTRACT(raw_json, '/geo_zipcode', 'TEXT'),'') AS geo_zipcode
    ,COALESCE(JSON_EXTRACT(raw_json, '/geo_latitude', 'DOUBLE'),0) AS geo_latitude
    ,COALESCE(JSON_EXTRACT(raw_json, '/geo_longitude', 'DOUBLE'),0) AS geo_longitude
    ,COALESCE(JSON_EXTRACT(raw_json, '/geo_region_name', 'TEXT'),'') AS geo_region_name
    ,COALESCE(JSON_EXTRACT(raw_json, '/ip_isp', 'TEXT'),'') AS ip_isp
    ,COALESCE(JSON_EXTRACT(raw_json, '/ip_organization', 'TEXT'),'') AS ip_organization
    ,COALESCE(JSON_EXTRACT(raw_json, '/ip_domain', 'TEXT'),'') AS ip_domain
    ,COALESCE(JSON_EXTRACT(raw_json, '/ip_netspeed', 'TEXT'),'') AS ip_netspeed
    ,COALESCE(JSON_EXTRACT(raw_json, '/page_url', 'TEXT'),'') AS page_url
    ,COALESCE(JSON_EXTRACT(raw_json, '/page_title', 'TEXT'),'') AS page_title
    ,COALESCE(JSON_EXTRACT(raw_json, '/page_referrer', 'TEXT'),'') AS page_referrer
    ,COALESCE(JSON_EXTRACT(raw_json, '/page_urlscheme', 'TEXT'),'') AS page_urlscheme
    ,COALESCE(JSON_EXTRACT(raw_json, '/page_urlhost', 'TEXT'),'') AS page_urlhost
    ,COALESCE(JSON_EXTRACT(raw_json, '/page_urlport', 'DOUBLE'),0) AS page_urlport
    ,COALESCE(JSON_EXTRACT(raw_json, '/page_urlpath', 'TEXT'),'') AS page_urlpath
    ,COALESCE(JSON_EXTRACT(raw_json, '/page_urlquery', 'TEXT'),'') AS page_urlquery
    ,COALESCE(JSON_EXTRACT(raw_json, '/page_urlfragment', 'TEXT'),'') AS page_urlfragment
    ,COALESCE(JSON_EXTRACT(raw_json, '/refr_urlscheme', 'TEXT'),'') AS refr_urlscheme
    ,COALESCE(JSON_EXTRACT(raw_json, '/refr_urlhost', 'TEXT'),'') AS refr_urlhost
    ,COALESCE(JSON_EXTRACT(raw_json, '/refr_urlport', 'DOUBLE'),0) AS refr_urlport
    ,COALESCE(JSON_EXTRACT(raw_json, '/refr_urlpath', 'TEXT'),'') AS refr_urlpath
    ,COALESCE(JSON_EXTRACT(raw_json, '/refr_urlquery', 'TEXT'),'') AS refr_urlquery
    ,COALESCE(JSON_EXTRACT(raw_json, '/refr_urlfragment', 'TEXT'),'') AS refr_urlfragment
    ,COALESCE(JSON_EXTRACT(raw_json, '/refr_medium', 'TEXT'),'') AS refr_medium
    ,COALESCE(JSON_EXTRACT(raw_json, '/refr_source', 'TEXT'),'') AS refr_source
    ,COALESCE(JSON_EXTRACT(raw_json, '/refr_term', 'TEXT'),'') AS refr_term
    ,COALESCE(JSON_EXTRACT(raw_json, '/mkt_medium', 'TEXT'),'') AS mkt_medium
    ,COALESCE(JSON_EXTRACT(raw_json, '/mkt_source', 'TEXT'),'') AS mkt_source
    ,COALESCE(JSON_EXTRACT(raw_json, '/mkt_term', 'TEXT'),'') AS mkt_term
    ,COALESCE(JSON_EXTRACT(raw_json, '/mkt_content', 'TEXT'),'') AS mkt_content
    ,COALESCE(JSON_EXTRACT(raw_json, '/mkt_campaign', 'TEXT'),'') AS mkt_campaign
    ,COALESCE(JSON_EXTRACT(raw_json, '/se_category', 'TEXT'),'') AS se_category
    ,COALESCE(JSON_EXTRACT(raw_json, '/se_action', 'TEXT'),'') AS se_action
    ,COALESCE(JSON_EXTRACT(raw_json, '/se_label', 'TEXT'),'') AS se_label
    ,COALESCE(JSON_EXTRACT(raw_json, '/se_property', 'TEXT'),'') AS se_property
    ,COALESCE(JSON_EXTRACT(raw_json, '/se_value', 'DOUBLE'),0) AS se_value
    ,COALESCE(JSON_EXTRACT(raw_json, '/tr_orderid', 'TEXT'),'') AS tr_orderid
    ,COALESCE(JSON_EXTRACT(raw_json, '/tr_affiliation', 'TEXT'),'') AS tr_affiliation
    ,COALESCE(JSON_EXTRACT(raw_json, '/tr_total', 'DOUBLE'),0) AS tr_total
    ,COALESCE(JSON_EXTRACT(raw_json, '/tr_tax', 'DOUBLE'),0) AS tr_tax
    ,COALESCE(JSON_EXTRACT(raw_json, '/tr_shipping', 'DOUBLE'),0) AS tr_shipping
    ,COALESCE(JSON_EXTRACT(raw_json, '/tr_city', 'TEXT'),'') AS tr_city
    ,COALESCE(JSON_EXTRACT(raw_json, '/tr_state', 'TEXT'),'') AS tr_state
    ,COALESCE(JSON_EXTRACT(raw_json, '/tr_country', 'TEXT'),'') AS tr_country
    ,COALESCE(JSON_EXTRACT(raw_json, '/ti_orderid', 'TEXT'),'') AS ti_orderid
    ,COALESCE(JSON_EXTRACT(raw_json, '/ti_sku', 'TEXT'),'') AS ti_sku
    ,COALESCE(JSON_EXTRACT(raw_json, '/ti_name', 'TEXT'),'') AS ti_name
    ,COALESCE(JSON_EXTRACT(raw_json, '/ti_category', 'TEXT'),'') AS ti_category
    ,COALESCE(JSON_EXTRACT(raw_json, '/ti_price', 'DOUBLE'),0) AS ti_price
    ,COALESCE(JSON_EXTRACT(raw_json, '/ti_quantity', 'DOUBLE'),0) AS ti_quantity
    ,COALESCE(JSON_EXTRACT(raw_json, '/pp_xoffset_min', 'DOUBLE'),0) AS pp_xoffset_min
    ,COALESCE(JSON_EXTRACT(raw_json, '/pp_xoffset_max', 'DOUBLE'),0) AS pp_xoffset_max
    ,COALESCE(JSON_EXTRACT(raw_json, '/pp_yoffset_min', 'DOUBLE'),0) AS pp_yoffset_min
    ,COALESCE(JSON_EXTRACT(raw_json, '/pp_yoffset_max', 'DOUBLE'),0) AS pp_yoffset_max
    ,COALESCE(JSON_EXTRACT(raw_json, '/useragent', 'TEXT'),'') AS useragent
    ,COALESCE(JSON_EXTRACT(raw_json, '/br_name', 'TEXT'),'') AS br_name
    ,COALESCE(JSON_EXTRACT(raw_json, '/br_family', 'TEXT'),'') AS br_family
    ,COALESCE(JSON_EXTRACT(raw_json, '/br_version', 'TEXT'),'') AS br_version
    ,COALESCE(JSON_EXTRACT(raw_json, '/br_type', 'TEXT'),'') AS br_type
    ,COALESCE(JSON_EXTRACT(raw_json, '/br_renderengine', 'TEXT'),'') AS br_renderengine
    ,COALESCE(JSON_EXTRACT(raw_json, '/br_lang', 'TEXT'),'') AS br_lang
    ,COALESCE(JSON_EXTRACT(raw_json, '/br_features_pdf', 'BOOLEAN'),0)  AS br_features_pdf
    ,COALESCE(JSON_EXTRACT(raw_json, '/br_features_flash', 'BOOLEAN'),0) AS br_features_flash
    ,COALESCE(JSON_EXTRACT(raw_json, '/br_features_java', 'BOOLEAN'),0) AS br_features_java
    ,COALESCE(JSON_EXTRACT(raw_json, '/br_features_director', 'BOOLEAN'),0) AS br_features_director
    ,COALESCE(JSON_EXTRACT(raw_json, '/br_features_quicktime', 'BOOLEAN'),0) AS br_features_quicktime
    ,COALESCE(JSON_EXTRACT(raw_json, '/br_features_realplayer', 'BOOLEAN'),0) AS br_features_realplayer
    ,COALESCE(JSON_EXTRACT(raw_json, '/br_features_windowsmedia', 'BOOLEAN'),0) AS br_features_windowsmedia
    ,COALESCE(JSON_EXTRACT(raw_json, '/br_features_gears', 'BOOLEAN'),0) AS br_features_gears
    ,COALESCE(JSON_EXTRACT(raw_json, '/br_features_silverlight', 'BOOLEAN'),0) AS br_features_silverlight
    ,COALESCE(JSON_EXTRACT(raw_json, '/br_cookies', 'BOOLEAN'),0) AS br_cookies
    ,COALESCE(JSON_EXTRACT(raw_json, '/br_colordepth', 'TEXT'),'') AS br_colordepth
    ,COALESCE(JSON_EXTRACT(raw_json, '/br_viewwidth', 'DOUBLE'),0) AS br_viewwidth
    ,COALESCE(JSON_EXTRACT(raw_json, '/br_viewheight', 'DOUBLE'),0) AS br_viewheight
    ,COALESCE(JSON_EXTRACT(raw_json, '/os_name', 'TEXT'),'') AS os_name
    ,COALESCE(JSON_EXTRACT(raw_json, '/os_family', 'TEXT'),'') AS os_family
    ,COALESCE(JSON_EXTRACT(raw_json, '/os_manufacturer', 'TEXT'),'') AS os_manufacturer
    ,COALESCE(JSON_EXTRACT(raw_json, '/os_timezone', 'TEXT'),'') AS os_timezone
    ,COALESCE(JSON_EXTRACT(raw_json, '/dvce_type', 'TEXT'),'') AS dvce_type
    ,COALESCE(JSON_EXTRACT(raw_json, '/dvce_ismobile', 'BOOLEAN'),0) AS dvce_ismobile
    ,COALESCE(JSON_EXTRACT(raw_json, '/dvce_screenwidth', 'DOUBLE'),0) AS dvce_screenwidth
    ,COALESCE(JSON_EXTRACT(raw_json, '/dvce_screenheight', 'DOUBLE'),0) AS dvce_screenheight
    ,COALESCE(JSON_EXTRACT(raw_json, '/doc_charset', 'TEXT'),'') AS doc_charset
    ,COALESCE(JSON_EXTRACT(raw_json, '/doc_width', 'DOUBLE'),0) AS doc_width
    ,COALESCE(JSON_EXTRACT(raw_json, '/doc_height', 'DOUBLE'),0) AS doc_height
    ,COALESCE(JSON_EXTRACT(raw_json, '/tr_currency', 'TEXT'),'') AS tr_currency
    ,COALESCE(JSON_EXTRACT(raw_json, '/tr_total_base', 'DOUBLE'),0) AS tr_total_base
    ,COALESCE(JSON_EXTRACT(raw_json, '/tr_tax_base', 'DOUBLE'),0) AS tr_tax_base
    ,COALESCE(JSON_EXTRACT(raw_json, '/tr_shipping_base', 'DOUBLE'),0) AS tr_shipping_base
    ,COALESCE(JSON_EXTRACT(raw_json, '/ti_currency', 'TEXT'),'') AS ti_currency
    ,COALESCE(JSON_EXTRACT(raw_json, '/ti_price_base', 'DOUBLE'),0) AS ti_price_base
    ,COALESCE(JSON_EXTRACT(raw_json, '/base_currency', 'TEXT'),'') AS base_currency
    ,COALESCE(JSON_EXTRACT(raw_json, '/geo_timezone', 'TEXT'),'') AS geo_timezone
    ,COALESCE(JSON_EXTRACT(raw_json, '/mkt_clickid', 'TEXT'),'') AS mkt_clickid
    ,COALESCE(JSON_EXTRACT(raw_json, '/mkt_network', 'TEXT'),'') AS mkt_network
    ,COALESCE(JSON_EXTRACT(raw_json, '/etl_tags', 'TEXT'),'') AS etl_tags
    ,COALESCE(CONCAT(SUBSTR(JSON_EXTRACT(raw_json, '/dvce_sent_tstamp', 'TEXT'),1,10), ' ', SUBSTR(JSON_EXTRACT(raw_json, '/dvce_sent_tstamp', 'TEXT'),12,8))::DATETIME,TO_DATE('2000-01-01')) AS dvce_sent_tstamp
    ,COALESCE(JSON_EXTRACT(raw_json, '/refr_domain_userid', 'TEXT'),'') AS refr_domain_userid
    ,COALESCE(CONCAT(SUBSTR(JSON_EXTRACT(raw_json, '/refr_dvce_tstamp', 'TEXT'),1,10), ' ', SUBSTR(JSON_EXTRACT(raw_json, '/refr_dvce_tstamp', 'TEXT'),12,8))::DATETIME,TO_DATE('2000-01-01')) AS refr_dvce_tstamp
    ,COALESCE(JSON_EXTRACT(raw_json, '/domain_sessionid', 'TEXT'),'') AS domain_sessionid
    ,COALESCE(CONCAT(SUBSTR(JSON_EXTRACT(raw_json, '/derived_tstamp', 'TEXT'),1,10), ' ', SUBSTR(JSON_EXTRACT(raw_json, '/derived_tstamp', 'TEXT'),12,8))::DATETIME,TO_DATE('2000-01-01')) AS derived_tstamp
    ,COALESCE(JSON_EXTRACT(raw_json, '/event_vendor', 'TEXT'),'') AS event_vendor
    ,COALESCE(JSON_EXTRACT(raw_json, '/event_name', 'TEXT'),'') AS event_name
    ,COALESCE(JSON_EXTRACT(raw_json, '/event_format', 'TEXT'),'') AS event_format
    ,COALESCE(JSON_EXTRACT(raw_json, '/event_version', 'TEXT'),'') AS event_version
    ,COALESCE(JSON_EXTRACT(raw_json, '/event_fingerprint', 'TEXT'),'') AS event_fingerprint
    ,COALESCE(CONCAT(SUBSTR(JSON_EXTRACT(raw_json, '/true_tstamp', 'TEXT'),1,10), ' ', SUBSTR(JSON_EXTRACT(raw_json, '/true_tstamp', 'TEXT'),12,8))::DATETIME,TO_DATE('2000-01-01')) AS true_tstamp
   from snowplow_events_ext
{% if is_incremental() %}
   where CONCAT(SUBSTR(JSON_EXTRACT(raw_json, '/collector_tstamp', 'TEXT'),1,10), ' ', SUBSTR(JSON_EXTRACT(raw_json, '/collector_tstamp', 'TEXT'),12,8))::DATETIME > (select max(collector_tstamp) from {{ this }})
{% endif %}
