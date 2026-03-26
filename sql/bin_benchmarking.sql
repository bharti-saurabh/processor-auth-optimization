-- BIN-Level Auth Rate Benchmarking vs. Network Averages
-- Straive Strategic Analytics | Processor Practice

WITH bin_performance AS (
    SELECT
        LEFT(pan, 6)                                            AS bin_prefix,
        LEFT(pan, 8)                                           AS bin_8digit,
        i.issuer_name,
        i.card_brand,
        i.card_product,
        i.country_code                                         AS issuer_country,
        COUNT(*)                                               AS total_attempts,
        SUM(CASE WHEN response_code = '00' THEN 1 ELSE 0 END) AS approvals,
        SUM(amount)                                            AS total_volume,
        SUM(CASE WHEN response_code != '00' THEN amount ELSE 0 END) AS declined_volume,
        SUM(CASE WHEN response_code = '05' THEN 1 ELSE 0 END) AS dnh_count,
        SUM(CASE WHEN response_code IN ('91','92','96') THEN 1 ELSE 0 END) AS technical_decline_count
    FROM fact_authorizations a
    LEFT JOIN dim_issuers i ON LEFT(a.pan, 6) = i.bin_prefix
    WHERE a.auth_timestamp BETWEEN :start_date AND :end_date
    GROUP BY 1, 2, 3, 4, 5, 6
),

network_benchmark AS (
    SELECT
        card_brand,
        card_product,
        issuer_country,
        PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY auth_rate) AS median_auth_rate,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY auth_rate) AS p75_auth_rate,
        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY auth_rate) AS p25_auth_rate
    FROM (
        SELECT
            LEFT(pan, 6) AS bin_prefix,
            i.card_brand,
            i.card_product,
            i.country_code AS issuer_country,
            SUM(CASE WHEN response_code = '00' THEN 1.0 ELSE 0 END) / COUNT(*) AS auth_rate
        FROM fact_authorizations a
        JOIN dim_issuers i ON LEFT(a.pan, 6) = i.bin_prefix
        WHERE a.auth_timestamp BETWEEN :start_date AND :end_date
        GROUP BY 1, 2, 3, 4
    ) sub
    GROUP BY 1, 2, 3
)

SELECT
    bp.bin_prefix,
    bp.issuer_name,
    bp.card_brand,
    bp.card_product,
    bp.issuer_country,
    bp.total_attempts,
    bp.approvals / NULLIF(bp.total_attempts, 0)                AS auth_rate,
    nb.median_auth_rate                                        AS network_median_auth_rate,
    nb.p75_auth_rate                                           AS network_p75_auth_rate,
    (bp.approvals / NULLIF(bp.total_attempts, 0)) - nb.median_auth_rate AS vs_network_median,
    bp.dnh_count / NULLIF(bp.total_attempts, 0)               AS dnh_rate,
    bp.technical_decline_count / NULLIF(bp.total_attempts, 0) AS technical_decline_rate,
    bp.declined_volume                                         AS at_risk_volume,
    (nb.median_auth_rate - bp.approvals / NULLIF(bp.total_attempts, 0))
        * bp.total_volume                                      AS recovery_opportunity_usd
FROM bin_performance bp
LEFT JOIN network_benchmark nb
    ON bp.card_brand = nb.card_brand
    AND bp.card_product = nb.card_product
    AND bp.issuer_country = nb.issuer_country
ORDER BY recovery_opportunity_usd DESC NULLS LAST
