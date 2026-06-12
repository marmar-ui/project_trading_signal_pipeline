{{ config(
    materialized='table',
    schema='silver'
) }}

WITH deduped AS (
    SELECT
        TIMESTAMP_MILLIS(open_time)                         AS open_time,
        TIMESTAMP_MILLIS(close_time)                        AS close_time,
        CAST(open AS FLOAT64)                               AS open,
        CAST(high AS FLOAT64)                               AS high,
        CAST(low AS FLOAT64)                                AS low,
        CAST(close AS FLOAT64)                              AS close,
        CAST(volume AS FLOAT64)                             AS volume,
        CAST(quote_volume AS FLOAT64)                       AS quote_volume,
        CAST(num_trades AS INT64)                           AS num_trades,
        REGEXP_EXTRACT(
            _FILE_NAME,
            r'([A-Z]+USDT)_'
        )                                                   AS symbol,
        ROW_NUMBER() OVER (
            PARTITION BY open_time, REGEXP_EXTRACT(_FILE_NAME, r'([A-Z]+USDT)_')
            ORDER BY open_time
        )                                                   AS row_num
    FROM {{ source('bronze', 'bronze_ohlcv') }}
    WHERE open_time IS NOT NULL
)

SELECT
    open_time,
    close_time,
    symbol,
    open,
    high,
    low,
    close,
    volume,
    quote_volume,
    num_trades
FROM deduped
WHERE row_num = 1
ORDER BY symbol, open_time