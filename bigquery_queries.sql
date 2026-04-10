SELECT
    category,
    COUNT(*) AS transaction_count,
    ROUND(
        SUM(
            quantity * unit_price * (1 - discount_pct / 100.0) 
            * (1 + tax_pct / 100.0) + shipping_cost
        ), 2
    ) AS total_net_revenue_inr
FROM
    `your-project.ecomm_staging.raw_transactions`
WHERE
    transaction_date BETWEEN '2024-04-01' AND '2024-04-30'
GROUP BY
    category
ORDER BY
    total_net_revenue_inr DESC;


SELECT
    category,
    COUNT(*) AS total_orders,
    COUNTIF(returned = 'Yes') AS returned_orders,
    ROUND(COUNTIF(returned = 'Yes') / COUNT(*) * 100.0, 2) AS return_rate_pct
FROM
    `your-project.ecomm_staging.raw_transactions`
WHERE
    transaction_date BETWEEN '2024-04-01' AND '2024-04-30'
GROUP BY
    category
ORDER BY
    return_rate_pct DESC;