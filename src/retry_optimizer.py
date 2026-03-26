"""
Smart Retry Logic & Timing Recommendations
Straive Strategic Analytics — Processor Practice
"""

import pandas as pd
import numpy as np
from typing import Tuple


RETRYABLE_CODES = {"91", "92", "96", "06"}
TIMING_WINDOWS = [0, 30, 60, 300, 900, 3600]  # seconds


def compute_retry_success_rate(
    df: pd.DataFrame,
    initial_code: str,
    window_seconds: int,
) -> float:
    """
    For a given initial decline code, compute the approval rate
    when the same transaction is retried within window_seconds.
    """
    retries = df[
        (df["original_response_code"] == initial_code)
        & (df["is_retry"] == 1)
        & (df["retry_delay_seconds"] <= window_seconds)
    ]
    if len(retries) == 0:
        return 0.0
    return retries["is_approved"].mean()


def build_retry_policy(df: pd.DataFrame) -> pd.DataFrame:
    """
    Build an optimal retry policy: for each retryable decline code,
    find the time window that maximises approval probability.
    """
    rows = []
    for code in RETRYABLE_CODES:
        best_rate, best_window = 0.0, None
        for window in TIMING_WINDOWS[1:]:
            rate = compute_retry_success_rate(df, code, window)
            if rate > best_rate:
                best_rate, best_window = rate, window
        rows.append({
            "decline_code": code,
            "optimal_retry_window_seconds": best_window,
            "expected_approval_rate": best_rate,
        })
    return pd.DataFrame(rows)


def estimate_retry_gmv_uplift(
    df: pd.DataFrame,
    retry_policy: pd.DataFrame,
) -> float:
    """Estimate additional GMV from applying the optimal retry policy."""
    total_uplift = 0.0
    for _, row in retry_policy.iterrows():
        eligible = df[
            (df["response_code"] == row["decline_code"])
            & (df["is_declined"] == 1)
        ]
        total_uplift += eligible["amount"].sum() * row["expected_approval_rate"]
    return total_uplift
