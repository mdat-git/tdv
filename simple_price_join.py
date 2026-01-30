import pandas as pd
import numpy as np

def _norm(s: pd.Series) -> pd.Series:
    return s.astype("string").str.strip().replace({"": pd.NA})

def attach_unit_price(df_to_accrue: pd.DataFrame, df_pricing: pd.DataFrame) -> pd.DataFrame:
    df = df_to_accrue.copy()
    p = df_pricing.copy()

    # --- normalize strings ---
    for c in ["vendor", "billing_bucket", "object_type", "voltage"]:
        if c in df.columns:
            df[c] = _norm(df[c])
        if c in p.columns:
            p[c] = _norm(p[c])

    # enforce numeric price
    p["unit_price"] = pd.to_numeric(p["unit_price"], errors="coerce")

    # ignore voltage for non-tower rows (tower-only pricing)
    df["__voltage_join"] = df["voltage"]
    not_tower = df["object_type"].fillna("").ne("ET_TOWER")
    df.loc[not_tower, "__voltage_join"] = pd.NA

    # also ignore voltage in pricing rows unless it is for ET_TOWER
    p["__voltage_rule"] = p["voltage"]
    p_not_tower = p["object_type"].fillna("").ne("ET_TOWER")
    p.loc[p_not_tower, "__voltage_rule"] = pd.NA

    # base join on vendor + billing_bucket to generate candidates
    base = df.reset_index(drop=False).rename(columns={"index": "__row_id"})
    cand = base.merge(
        p,
        on=["vendor", "billing_bucket"],
        how="left",
        suffixes=("", "_price"),
        indicator=True,
    )

    # candidate filter rules:
    # object_type matches if pricing.object_type is null OR equals df.object_type
    obj_ok = cand["object_type_price"].isna() | (cand["object_type_price"] == cand["object_type"])

    # voltage matches if pricing.voltage is null OR equals df.__voltage_join (tower-only already handled)
    volt_ok = cand["__voltage_rule"].isna() | (cand["__voltage_rule"] == cand["__voltage_join"])

    cand = cand[obj_ok & volt_ok].copy()

    # match diagnostics
    match_counts = cand.groupby("__row_id")["unit_price"].count()
    df["pricing_match_count"] = df.index.map(match_counts).fillna(0).astype(int)

    df["pricing_match_status"] = np.select(
        [df["pricing_match_count"].eq(0), df["pricing_match_count"].eq(1)],
        ["NO_MATCH", "MATCHED"],
        default="MULTI_MATCH",
    )

    # pick the "most specific" match deterministically:
    # specificity score = (pricing has object_type) + (pricing has voltage)
    cand["__spec"] = (~cand["object_type_price"].isna()).astype(int) + (~cand["__voltage_rule"].isna()).astype(int)

    # within each df row: take highest specificity, then first (stable) row
    cand_sorted = cand.sort_values(["__row_id", "__spec"], ascending=[True, False])
    best = cand_sorted.groupby("__row_id", as_index=False).first()

    # attach chosen price
    df = df.merge(best[["__row_id", "unit_price"]], left_index=True, right_on="__row_id", how="left")
    df = df.drop(columns=["__row_id"])

    # cleanup helper
    df = df.drop(columns=["__voltage_join"], errors="ignore")

    return df


# ---- usage ----
df_to_accrue_priced = attach_unit_price(df_to_accrue, df_pricing)

# quick checks
df_to_accrue_priced["unit_price"].isna().value_counts(), df_to_accrue_priced["pricing_match_status"].value_counts()
