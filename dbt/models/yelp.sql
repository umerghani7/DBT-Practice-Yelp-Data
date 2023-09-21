
{{
   config(materialized='table')
}}


WITH BASE AS (
SELECT
id,
name AS restaurant_name,
url,
review_count,
categories,
rating,
price,
phone,
location_address1 as location,
coordinates_latitude as lat,
coordinates_longitude as lon
  FROM dbt-project-399518.raw.yelp_data
)

SELECT * FROM Base
