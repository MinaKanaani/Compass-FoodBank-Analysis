"""
Demand Analysis (Public-Friendly)

Purpose:
- Clean and summarize daily demand + distribution time series.
- Generate aggregated insights (no raw data exported).
- Export summaries to outputs/Insight.xlsx

Inputs (expected columns):
- Date
- Shopping Trips by Day
- Quantity (lbs) by Day
"""

from __future__ import annotations

from pathlib import Path
import pandas as pd
import holidays


# -----------------------------
# Config
# -----------------------------
DATA_PATH = Path("data/Daily Shopping trips and quantity since Jan 2017.xlsx")
OUTPUT_DIR = Path("outputs")
OUTPUT_FILE = OUTPUT_DIR / "Insight.xlsx"

YEARS_FOR_HOLIDAYS = range(2017, 2030)

SEASONS = {1: "Winter", 2: "Spring", 3: "Summer", 4: "Autumn"}
MONTH_ORDER = [
    "January", "February", "March", "April", "May", "June",
    "July", "August", "September", "October", "November", "December"
]
SEASON_ORDER = ["Winter", "Spring", "Summer", "Autumn"]


# -----------------------------
# Helpers
# -----------------------------
def load_and_clean_data(path: Path) -> pd.DataFrame:
    df = pd.read_excel(path)

    if "Date" not in df.columns:
        raise ValueError("Expected a 'Date' column in the input file.")

    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
    df = df.dropna(subset=["Date"]).set_index("Date").sort_index()

    required = {"Shopping Trips by Day", "Quantity (lbs) by Day"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    # Feature engineering
    df["lbs_per_visit"] = df["Quantity (lbs) by Day"] / df["Shopping Trips by Day"]
    df["Day_of_week"] = df.index.day_name()
    df["Month"] = df.index.month_name()
    df["Year"] = df.index.year
    df["quarter"] = df.index.quarter
    df["Season"] = df["quarter"].map(SEASONS)

    # Holiday flags (Canada)
    ca_holidays = holidays.Canada(years=YEARS_FOR_HOLIDAYS)
    df["is_holiday"] = df.index.to_series().dt.date.isin(ca_holidays)
    df["holiday_name"] = df.index.map(ca_holidays.get)

    # Basic outlier score on pounds distributed
    df["z_score_lbs"] = (
        (df["Quantity (lbs) by Day"] - df["Quantity (lbs) by Day"].mean())
        / df["Quantity (lbs) by Day"].std()
    )

    return df


def classify_volatility(cv: float) -> str:
    if cv < 0.10:
        return "Very Low Volatility"
    if cv < 0.20:
        return "Low Volatility"
    if cv < 0.40:
        return "Moderate Volatility"
    if cv < 0.60:
        return "High Volatility"
    return "Very High Volatility"


# -----------------------------
# Analysis blocks
# -----------------------------
def daily_summary(df: pd.DataFrame) -> pd.DataFrame:
    visits = df["Shopping Trips by Day"]
    lbs = df["Quantity (lbs) by Day"]
    ppv = df["lbs_per_visit"]

    unusual_high = df[df["z_score_lbs"] > 3]
    unusual_low = df[df["z_score_lbs"] < -3]

    def stats(series: pd.Series) -> dict:
        mean = series.mean()
        std = series.std()
        cv = std / mean if mean != 0 else float("nan")
        return {
            "mean": mean,
            "std": std,
            "cv": cv,
            "volatility_label": classify_volatility(cv) if pd.notna(cv) else "NA",
        }

    visits_stats = stats(visits)
    lbs_stats = stats(lbs)
    ppv_stats = stats(ppv)

    summary = pd.DataFrame([{
        "avg_daily_visits": visits.mean(),
        "avg_lbs_per_visit": ppv.mean(),

        "visits_mean": visits_stats["mean"],
        "visits_std": visits_stats["std"],
        "visits_cv": visits_stats["cv"],
        "visits_volatility": visits_stats["volatility_label"],

        "lbs_mean": lbs_stats["mean"],
        "lbs_std": lbs_stats["std"],
        "lbs_cv": lbs_stats["cv"],
        "lbs_volatility": lbs_stats["volatility_label"],

        "ppv_mean": ppv_stats["mean"],
        "ppv_std": ppv_stats["std"],
        "ppv_cv": ppv_stats["cv"],
        "ppv_volatility": ppv_stats["volatility_label"],

        # NOTE: for public repo, we do not export specific outlier dates
        "unusual_high_count": len(unusual_high),
        "unusual_low_count": len(unusual_low),
    }])

    return summary


def weekly_summary(df: pd.DataFrame) -> pd.DataFrame:
    demand = df.groupby("Day_of_week")["Shopping Trips by Day"].sum()
    dist = df.groupby("Day_of_week")["Quantity (lbs) by Day"].sum()

    demand_pct = demand / demand.sum()
    dist_pct = dist / dist.sum()

    out = pd.DataFrame({
        "demand_share": demand_pct,
        "distribution_share": dist_pct,
    })

    return out


def monthly_summary(df: pd.DataFrame) -> pd.DataFrame:
    avg_demand = df.groupby("Month")["Shopping Trips by Day"].mean()
    avg_dist = df.groupby("Month")["Quantity (lbs) by Day"].mean()
    avg_ppv = df.groupby("Month")["lbs_per_visit"].mean()

    std_demand = df.groupby("Month")["Shopping Trips by Day"].std()
    std_dist = df.groupby("Month")["Quantity (lbs) by Day"].std()
    std_ppv = df.groupby("Month")["lbs_per_visit"].std()

    cv_demand = std_demand / avg_demand
    cv_dist = std_dist / avg_dist
    cv_ppv = std_ppv / avg_ppv

    result = pd.DataFrame({
        "avg_demand": avg_demand,
        "std_demand": std_demand,
        "cv_demand": cv_demand,
        "avg_dist": avg_dist,
        "std_dist": std_dist,
        "cv_dist": cv_dist,
        "avg_lbs_per_visit": avg_ppv,
        "std_lbs_per_visit": std_ppv,
        "cv_lbs_per_visit": cv_ppv,
    }).reindex(MONTH_ORDER)

    return result


def seasonal_summary(df: pd.DataFrame) -> pd.DataFrame:
    grp = df.groupby("Season")[["Shopping Trips by Day", "Quantity (lbs) by Day", "lbs_per_visit"]]

    avg = grp.mean()
    std = grp.std()
    cv = std / avg

    out = pd.concat(
        {
            "avg": avg,
            "std": std,
            "cv": cv,
        },
        axis=1
    ).reindex(SEASON_ORDER)

    return out


def yearly_summary(df: pd.DataFrame) -> pd.DataFrame:
    yearly = df.groupby("Year")[["Shopping Trips by Day", "Quantity (lbs) by Day"]].sum()
    yearly["lbs_per_visit"] = yearly["Quantity (lbs) by Day"] / yearly["Shopping Trips by Day"]

    # YoY growth (kept numeric; avoid formatting into strings)
    yearly["yoy_demand_growth"] = yearly["Shopping Trips by Day"].pct_change()
    yearly["yoy_distribution_growth"] = yearly["Quantity (lbs) by Day"].pct_change()

    return yearly


# -----------------------------
# Export
# -----------------------------
def export_insights(
    daily: pd.DataFrame,
    weekly: pd.DataFrame,
    monthly: pd.DataFrame,
    seasonal: pd.DataFrame,
    yearly: pd.DataFrame,
) -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    with pd.ExcelWriter(OUTPUT_FILE, engine="openpyxl", mode="w") as writer:
        daily.to_excel(writer, sheet_name="Daily", index=False)
        weekly.to_excel(writer, sheet_name="Weekly", index=True)
        monthly.to_excel(writer, sheet_name="Monthly", index=True)
        seasonal.to_excel(writer, sheet_name="Seasonal", index=True)
        yearly.to_excel(writer, sheet_name="Yearly", index=True)


def main() -> None:
    df = load_and_clean_data(DATA_PATH)

    daily = daily_summary(df)
    weekly = weekly_summary(df)
    monthly = monthly_summary(df)
    seasonal = seasonal_summary(df)
    yearly = yearly_summary(df)

    export_insights(daily, weekly, monthly, seasonal, yearly)
    print(f"Done. Aggregated summaries exported to: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
