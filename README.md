# Processor Authorization Rate Optimization

**Client Segment:** Processor
**Category:** Benchmarking / Decline Analytics
**Owner:** Straive Strategic Analytics
**Year:** 2024

## Objective
Identify root causes of declined transactions at the processor layer and recommend targeted interventions to improve overall authorisation rates, reducing false declines that erode merchant GMV and cardholder experience.

## Methodology
1. Decline code taxonomy — map raw ISO 8583 response codes to strategic categories (insufficient funds, card restrictions, risk rules, technical)
2. Merchant-level and BIN-level decline pattern analysis
3. Issuer benchmarking — compare decline rates by BIN against network averages
4. Intervention simulation — model GMV recovery for each lever
5. Straight-through processing (STP) opportunity sizing

## Key Findings Framework
| Decline Category | Typical Share | Actionable? |
|---|---|---|
| Issuer Risk Rules (false declines) | 28–35% | Yes — BIN-level outreach |
| Insufficient Funds | 22–30% | Partial — retry logic |
| Card Restrictions (caps/blocks) | 15–20% | Yes — issuer policy |
| Technical / Timeout | 8–12% | Yes — routing optimisation |
| Velocity Controls | 5–10% | Yes — threshold tuning |

## Assets
- `src/auth_rate_analysis.py` — Decline taxonomy, BIN analysis, intervention sizing
- `src/retry_optimizer.py` — Smart retry logic and timing recommendations
- `sql/auth_decline_extract.sql` — Authorization event extraction
- `sql/bin_benchmarking.sql` — BIN-level performance vs. network benchmarks

## Requirements
```
pandas>=2.0
numpy>=1.26
scikit-learn>=1.3
plotly>=5.18
sqlalchemy>=2.0
```
