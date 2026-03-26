"""
Authorization Rate Analysis & Decline Root-Cause Decomposition
Straive Strategic Analytics — Processor Practice
"""

import pandas as pd
import numpy as np
from typing import Dict, List
import logging

log = logging.getLogger(__name__)

# ISO 8583 decline code → strategic category mapping
DECLINE_TAXONOMY = {
    # Insufficient funds
    "51": "INSUFFICIENT_FUNDS",
    "61": "EXCEEDS_WITHDRAWAL_LIMIT",
    "65": "EXCEEDS_WITHDRAWAL_FREQUENCY",
    # Issuer risk / false declines
    "05": "DO_NOT_HONOUR",
    "14": "INVALID_CARD",
    "41": "LOST_CARD",
    "43": "STOLEN_CARD",
    "57": "TRANSACTION_NOT_PERMITTED",
    "62": "RESTRICTED_CARD",
    "93": "ISSUER_VIOLATION",
    # Velocity controls
    "06": "ERROR",
    "55": "INCORRECT_PIN",
    "75": "PIN_TRIES_EXCEEDED",
    # Technical
    "91": "ISSUER_UNAVAILABLE",
    "92": "UNABLE_TO_ROUTE",
    "96": "SYSTEM_MALFUNCTION",
    "96": "TIMEOUT",
    # Other
    "12": "INVALID_TRANSACTION",
    "13": "INVALID_AMOUNT",
    "15": "INVALID_ISSUER",
}

ACTIONABLE_CATEGORIES = {
    "DO_NOT_HONOUR", "TRANSACTION_NOT_PERMITTED", "RESTRICTED_CARD",
    "ISSUER_UNAVAILABLE", "UNABLE_TO_ROUTE", "SYSTEM_MALFUNCTION",
}


def classify_declines(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["decline_category"] = df["response_code"].map(DECLINE_TAXONOMY).fillna("OTHER")
    df["is_actionable"] = df["decline_category"].isin(ACTIONABLE_CATEGORIES)
    df["is_false_decline"] = df["decline_category"].isin({
        "DO_NOT_HONOUR", "TRANSACTION_NOT_PERMITTED", "RESTRICTED_CARD", "ISSUER_VIOLATION"
    })
    return df


def compute_auth_rates(df: pd.DataFrame, group_by: List[str]) -> pd.DataFrame:
    grouped = df.groupby(group_by).agg(
        total_attempts=("txn_id", "count"),
        approvals=("is_approved", "sum"),
        declines=("is_declined", "sum"),
        false_declines=("is_false_decline", "sum"),
        actionable_declines=("is_actionable", "sum"),
        total_amount=("amount", "sum"),
        declined_amount=("declined_amount", "sum"),
    ).reset_index()
    grouped["auth_rate"] = grouped["approvals"] / grouped["total_attempts"]
    grouped["false_decline_rate"] = grouped["false_declines"] / grouped["total_attempts"]
    grouped["declined_gmv_pct"] = grouped["declined_amount"] / grouped["total_amount"]
    return grouped.sort_values("auth_rate")


def size_gmv_recovery(df: pd.DataFrame, target_auth_rate: float = 0.92) -> pd.DataFrame:
    """
    For each BIN below target_auth_rate, estimate GMV recovery
    if auth rate improved to target.
    """
    bin_rates = compute_auth_rates(df[df["is_declined"] | df["is_approved"]], ["bin_prefix"])
    underperformers = bin_rates[bin_rates["auth_rate"] < target_auth_rate].copy()
    underperformers["recovery_opportunity"] = (
        (target_auth_rate - underperformers["auth_rate"]) * underperformers["total_amount"]
    )
    underperformers["priority_tier"] = pd.cut(
        underperformers["recovery_opportunity"],
        bins=3, labels=["LOW", "MEDIUM", "HIGH"]
    )
    return underperformers.sort_values("recovery_opportunity", ascending=False)


def decline_waterfall(df: pd.DataFrame) -> pd.DataFrame:
    classified = classify_declines(df[df["is_declined"] == 1])
    breakdown = classified.groupby("decline_category").agg(
        count=("txn_id", "count"),
        declined_volume=("amount", "sum"),
    ).reset_index()
    breakdown["pct_of_declines"] = breakdown["count"] / breakdown["count"].sum() * 100
    breakdown["is_actionable"] = breakdown["decline_category"].isin(ACTIONABLE_CATEGORIES)
    return breakdown.sort_values("count", ascending=False)


def run(data_path: str, target_auth_rate: float = 0.92):
    df = pd.read_parquet(data_path)
    df = classify_declines(df)

    overall = compute_auth_rates(df, ["processor_id"])
    log.info(f"Overall auth rate: {overall['auth_rate'].iloc[0]:.2%}")

    waterfall = decline_waterfall(df)
    log.info("\nDecline Breakdown:\n" + waterfall.to_string(index=False))

    opportunities = size_gmv_recovery(df, target_auth_rate)
    total_opportunity = opportunities["recovery_opportunity"].sum()
    log.info(f"\nTotal GMV recovery opportunity: ${total_opportunity:,.0f}")
    return waterfall, opportunities
