WITH stg_data AS (
    SELECT * FROM {{ ref('stg_stock_prices') }}
)
SELECT
    date,
    ticker,
    open,
    high,
    low,
    close,
    volume,
    AVG(close) OVER (
        PARTITION BY ticker
        ORDER BY date
        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    ) AS moving_avg_30d
FROM stg_data