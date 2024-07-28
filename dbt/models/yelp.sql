{{
   config(materialized='table')
}}


WITH base AS (
	SELECT
		id
		,name AS restaurant_name
		,url
		,new_col
		,review_count
		,categories as cat
		,rating
		,price
		,phone
		,location_address1
			AS location_add
		,coordinates_latitude AS lat
		,coordinates_longitude AS lon
	FROM {{ source('jaffle_shop', 'orders') }}
)

SELECT * FROM base
