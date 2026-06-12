{{ config(
    materialized='table',
    schema='gold'
) }}

WITH base AS (
    SELECT
        open_time,
        close_time,
        symbol,
        open,
        high,
        low,
        close,
        volume
    FROM {{ ref('fact_ohlcv') }}
),

with_ma AS (
    SELECT
        *,
        AVG(close) OVER (
            PARTITION BY symbol
            ORDER BY open_time
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS ma7,
        AVG(close) OVER (
            PARTITION BY symbol
            ORDER BY open_time
            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
        ) AS ma20
    FROM base
),

with_zscore AS (
    SELECT
        *,
        AVG(close) OVER (
            PARTITION BY symbol
            ORDER BY open_time
            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
        ) AS mean_20,
        STDDEV(close) OVER (
            PARTITION BY symbol
            ORDER BY open_time
            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
        ) AS std_20
    FROM with_ma
),

with_signals AS (
    SELECT
        *,
        SAFE_DIVIDE(close - mean_20, std_20)    AS z_score,
        LAG(ma7) OVER (
            PARTITION BY symbol ORDER BY open_time
        )                                       AS prev_ma7,
        LAG(ma20) OVER (
            PARTITION BY symbol ORDER BY open_time
        )                                       AS prev_ma20
    FROM with_zscore
),

final AS (
    SELECT
        open_time,
        close_time,
        symbol,
        open,
        high,
        low,
        close,
        volume,
        ROUND(ma7, 6)                           AS ma7,
        ROUND(ma20, 6)                          AS ma20,
        ROUND(z_score, 4)                       AS z_score,

        -- Golden Cross: MA7 crosses above MA20
        CASE
            WHEN prev_ma7 < prev_ma20 AND ma7 > ma20 THEN TRUE
            ELSE FALSE
        END                                     AS golden_cross,

        -- Death Cross: MA7 crosses below MA20
        CASE
            WHEN prev_ma7 > prev_ma20 AND ma7 < ma20 THEN TRUE
            ELSE FALSE
        END                                     AS death_cross,

        -- Trading Signal
        CASE
            WHEN prev_ma7 < prev_ma20 AND ma7 > ma20 THEN 'BUY'
            WHEN prev_ma7 > prev_ma20 AND ma7 < ma20 THEN 'SELL'
            ELSE 'HOLD'
        END                                     AS signal

    FROM with_signals
    WHERE ma20 IS NOT NULL
)

SELECT * FROM final
ORDER BY symbol, open_time