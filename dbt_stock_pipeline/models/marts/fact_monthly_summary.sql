WITH stg_data AS (
    SELECT * FROM {{ ref('stg_stock_prices') }}
)
SELECT
    DATE_TRUNC('month', date) AS month,
    ticker,
    AVG(open) AS avg_open,
    AVG(close) AS avg_close,
    SUM(volume) AS total_volume,
    COUNT(CASE WHEN close > open THEN 1 END) AS green_days
FROM stg_data
GROUP BY DATE_TRUNC('month', date), ticker