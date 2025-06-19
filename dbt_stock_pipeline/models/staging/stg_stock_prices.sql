SELECT
    date,
    ticker,
    open,
    high,
    low,
    close,
    volume
FROM {{ source('stock_source', 'raw_stock_data') }}
WHERE
    date IS NOT NULL
    AND ticker IS NOT NULL
    AND close IS NOT NULL