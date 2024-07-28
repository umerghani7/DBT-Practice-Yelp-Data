{{ config(
    tags="sportsbook",
    dist="user_id",
    )
}}

WITH unioned_edges_legs_us AS (

	{% for region in get_offering_regions(offering='edgebook', include_us=True) %}

		{% set table_name = 'int_edgebook_us_' ~ region.region_code ~ '_legs_bets' %}

		/* {{ region.region_name }} */
		SELECT col1
		FROM {{ ref(table_name) }}
		WHERE {{ filter_online_retail() }}

		{%- if not loop.last %}
   UNION ALL
  {% endif %}

	{%- endfor %}
)

,unioned_countries_and_currency_conversion AS (
	/* USA - USD */
	SELECT *

	FROM unioned_edges_legs_us

	UNION ALL
	/* Canada - CAD --> USD */
	SELECT
		base_ca.id
		,base_ca.bet_id
		,base_ca.free_bet_id
		,base_ca.market_id
		,base_ca.market_selection_id
		,base_ca.user_id
		,base_ca.placed_at
		,base_ca.closed_at
		,base_ca.platform
		,base_ca.type
		,base_ca.status
		,base_ca.outcome
		,base_ca.conditionally_graded
		,base_ca.stale_outcome
		,base_ca.is_free_bet
		,base_ca.is_promo_credit_bet
		,base_ca.is_cash_bet
		,base_ca.decimal_odds_bet
		,base_ca.american_odds_bet
		,base_ca.num_legs
		,base_ca.crs_value
		,CAST((ROUND(1.0 * base_ca.wagered_cash_bet / fx_rates.fx_rate,2)) AS NUMERIC) AS wagered_cash_bet
		,CAST((ROUND(1.0 * base_ca.wagered_free_bet / fx_rates.fx_rate,2)) AS NUMERIC) AS wagered_free_bet
		,CAST((ROUND(1.0 * base_ca.wagered_credits_bet / fx_rates.fx_rate,2)) AS NUMERIC) AS wagered_credits_bet
		,CAST((ROUND(1.0 * base_ca.ggr_bet / fx_rates.fx_rate,2)) AS NUMERIC) AS ggr_bet
		,CAST((ROUND(1.0 * base_ca.ggr_adjusted_free_bet / fx_rates.fx_rate,2)) AS NUMERIC) AS ggr_adjusted_free_bet
		,base_ca.decimal_odds_leg
		,base_ca.american_odds_leg
		,base_ca.winnings_proportion_leg
		,base_ca.market_classification
		,base_ca.market_selection_name
		,base_ca.market_selection_type
		,base_ca.points
		,base_ca.is_market_live
		,base_ca.is_quick_bet
		,base_ca.regraded
		,base_ca.ordinal
		,base_ca.outcome_leg
		,base_ca.region
		,base_ca.country
		,'USD' AS currency
		,CAST((ROUND(1.0 * base_ca.wagered_cash_leg / fx_rates.fx_rate,2)) AS NUMERIC) AS wagered_cash_leg
		,CAST((ROUND(1.0 * base_ca.settled_cash_leg / fx_rates.fx_rate,2)) AS NUMERIC) AS settled_cash_leg
		,CAST((ROUND(1.0 * base_ca.payout_winnings_cash_leg / fx_rates.fx_rate,2)) AS NUMERIC) AS payout_winnings_cash_leg
		,CAST((ROUND(1.0 * base_ca.payout_stake_cash_leg / fx_rates.fx_rate,2)) AS NUMERIC) AS payout_stake_cash_leg
		,CAST((ROUND(1.0 * base_ca.cashout_winnings_leg / fx_rates.fx_rate,2)) AS NUMERIC) AS cashout_winnings_leg
		,CAST((ROUND(1.0 * base_ca.cashout_losings_leg / fx_rates.fx_rate,2)) AS NUMERIC) AS cashout_losings_leg
		,CAST((ROUND(1.0 * base_ca.dead_heat_losings_leg / fx_rates.fx_rate,2)) AS NUMERIC) AS dead_heat_losings_leg
		,CAST((ROUND(1.0 * base_ca.wagered_free_leg / fx_rates.fx_rate,2)) AS NUMERIC) AS wagered_free_leg
		,CAST((ROUND(1.0 * base_ca.settled_free_leg / fx_rates.fx_rate,2)) AS NUMERIC) AS settled_free_leg
		,CAST((ROUND(1.0 * base_ca.payout_free_leg / fx_rates.fx_rate,2)) AS NUMERIC) AS payout_free_leg
		,CAST((ROUND(1.0 * base_ca.wagered_credits_leg / fx_rates.fx_rate,2)) AS NUMERIC) AS wagered_credits_leg
		,CAST((ROUND(1.0 * base_ca.settled_credits_leg / fx_rates.fx_rate,2)) AS NUMERIC) AS settled_credits_leg
		,CAST((ROUND(1.0 * base_ca.payout_winnings_credits_leg / fx_rates.fx_rate,2)) AS NUMERIC)
			AS payout_winnings_credits_leg
		,CAST((ROUND(1.0 * base_ca.payout_stake_credits_leg / fx_rates.fx_rate,2)) AS NUMERIC) AS payout_stake_credits_leg
		,CAST((ROUND(1.0 * base_ca.ggr_leg / fx_rates.fx_rate,2)) AS NUMERIC) AS ggr_leg
		,CAST((ROUND(1.0 * base_ca.ggr_adjusted_free_leg / fx_rates.fx_rate,2)) AS NUMERIC) AS ggr_adjusted_free_leg
		,base_ca.is_past_7_days
		,base_ca.is_past_14_days
		,base_ca.is_past_30_days
		,base_ca.is_past_90_days
		,base_ca.is_past_180_days
		,base_ca.is_past_365_days
		,base_ca._inserted_at
		,base_ca._updated_at

	FROM {{ ref('fct_edgebook_cad_bet_legs') }} AS base_ca
	INNER JOIN {{ ref('dim_fx_rates') }} AS fx_rates
	ON CAST(DATE_TRUNC(base_ca.placed_at,month) AS DATE) = fx_rates.fx_date
	WHERE {{ filter_online_retail() }}
)

SELECT
	*
	,CURRENT_TIMESTAMP() AS _ae_updated_at

FROM unioned_countries_and_currency_conversion
