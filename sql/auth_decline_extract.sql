-- Authorization Event Extraction
-- Straive Strategic Analytics | Processor Practice

SELECT
    a.auth_id                                                   AS txn_id,
    a.merchant_id,
    a.account_id,
    LEFT(a.pan, 6)                                              AS bin_prefix,
    a.issuer_id,
    a.processor_id,
    a.auth_timestamp,
    a.amount,
    a.currency,
    a.mcc_code,
    a.channel,
    a.is_card_not_present,
    a.is_cross_border,
    a.response_code,
    CASE WHEN a.response_code = '00' THEN 1 ELSE 0 END         AS is_approved,
    CASE WHEN a.response_code != '00' THEN 1 ELSE 0 END        AS is_declined,
    CASE WHEN a.response_code != '00' THEN a.amount ELSE 0 END AS declined_amount,

    -- Retry detection
    CASE WHEN LAG(a.auth_id) OVER (
        PARTITION BY a.account_id, a.merchant_id, ROUND(a.amount, 0)
        ORDER BY a.auth_timestamp
    ) IS NOT NULL THEN 1 ELSE 0 END                            AS is_retry,

    DATEDIFF('second',
        LAG(a.auth_timestamp) OVER (
            PARTITION BY a.account_id, a.merchant_id, ROUND(a.amount, 0)
            ORDER BY a.auth_timestamp
        ),
        a.auth_timestamp
    )                                                           AS retry_delay_seconds,

    LAG(a.response_code) OVER (
        PARTITION BY a.account_id, a.merchant_id, ROUND(a.amount, 0)
        ORDER BY a.auth_timestamp
    )                                                           AS original_response_code

FROM fact_authorizations a
WHERE a.auth_timestamp BETWEEN :start_date AND :end_date
ORDER BY a.auth_timestamp
