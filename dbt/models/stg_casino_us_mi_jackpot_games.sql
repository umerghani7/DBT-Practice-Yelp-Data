{{ config(
    tags="sportsbook",
    dist="user_id",
    )
}}

WITH last_record AS (
	SELECT
		game_id
		,jackpot_id
		,MAX(inserted_at / 100) AS max_inserted_at
	FROM {{ source('casino_us_mi', 'jackpot_games') }}
	GROUP BY 1,2
)

SELECT

	jg.game_id
	,jg.jackpot_id
	,TIMESTAMP_MICROS(jg.inserted_at) AS _inserted_at
	,TIMESTAMP_MICROS(jg.updated_at) AS date
	,CURRENT_TIMESTAMP() AS _ae_updated_at
FROM {{ source('casino_us_mi', 'jackpot_games') }} AS jg
INNER JOIN last_record AS lr
ON
	jg.game_id = lr.game_id
	AND jg.jackpot_id = lr.jackpot_id
	AND jg.inserted_at = lr.max_inserted_at
