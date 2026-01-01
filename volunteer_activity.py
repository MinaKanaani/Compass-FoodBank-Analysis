"""
Volunteer Activity Analysis (Public-Friendly)

Purpose:
- Clean Compass logged-hours data
- Merge volunteer demographics (optional)
- Produce aggregated engagement, retention, and category summaries
- Export safe summary tables (no raw logs exported)
"""

from __future__ import annotations

from pathlib import Path
import math
import numpy as np
import pandas as pd

# Optional mapping dependencies
try:
    import geopandas as gpd
    import matplotlib.pyplot as plt
except Exception:
    gpd = None
    plt = None


# -----------------------------
# Config
# -----------------------------
DATA_DIR = Path("data")
OUTPUT_DIR = Path("outputs")

HOURS_FILE = DATA_DIR / "Cleaned_Logged_Hours_Data.csv"
VOLUNTEER_FILE = DATA_DIR / "Compass Export - All volunteers since system implementation - UserData.csv"

# Optional mapping (shapefile)
FSA_SHP = DATA_DIR / "lfsa000b21a_e.shp"

OUTPUT_FILE = OUTPUT_DIR / "Volunteer_Engagement_Analysis.xlsx"

SEASONS = {1: "Winter", 2: "Spring", 3: "Summer", 4: "Autumn"}
MONTH_ORDER = ["January","February","March","April","May","June","July","August","September","October","November","December"]
DAY_ORDER = ["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"]


# -----------------------------
# Cleaning helpers
# -----------------------------
CITY_CANONICAL_MAP = {
    "mississauga": "Mississauga",
    "missisauga": "Mississauga",
    "mississauaga": "Mississauga",
    "mississouga": "Mississauga",
    "oakville": "Oakville",
    "toronto": "Toronto",
    "brampton": "Brampton",
    "burlington": "Burlington",
    "hamilton": "Hamilton",
    "vaughan": "Vaughan",
    "richmond hill": "Richmond Hill",
}


def clean_city(city) -> str | float:
    if pd.isna(city):
        return np.nan

    s = str(city).strip().lower()

    if "," in s:
        s = s.split(",")[0].strip()

    for suffix in [" on", " ab", " bc", " can", " canada", " ontario"]:
        if s.endswith(suffix):
            s = s.replace(suffix, "").strip()

    if s.startswith("mis"):
        return "Mississauga"

    if s in CITY_CANONICAL_MAP:
        return CITY_CANONICAL_MAP[s]

    return s.title()


def clean_volunteer_status(status) -> str:
    if pd.isna(status):
        return "Unknown"

    s = str(status).strip().lower()

    if "applicant" in s:
        return "Applicant"
    if "process" in s:
        return "In Process"
    if "accepted" in s:
        return "Accepted"
    if "inactive" in s:
        return "Inactive"
    if "archived" in s:
        return "Archived"
    return "Other"


def extract_fsa(postal) -> str | float:
    if pd.isna(postal):
        return np.nan
    s = str(postal).replace(" ", "").upper()
    return s[:3] if len(s) >= 3 else np.nan


def classify_language(text: str) -> str:
    if text.strip() == "":
        return "Other/Unknown"

    t = text.lower()
    has_english = "english" in t
    is_multi = any(sep in t for sep in [",", ";", " and ", "/"])

    if has_english and is_multi:
        return "Multilingual"
    if has_english:
        return "English only"
    return "Other/Unknown"


# -----------------------------
# Load & clean
# -----------------------------
def load_and_clean_hours(path: Path = HOURS_FILE) -> pd.DataFrame:
    df = pd.read_csv(path)

    df["DateVolunteered"] = pd.to_datetime(
        df["DateVolunteered"],
        format="%d/%m/%Y %I:%M:%S %p",
        errors="coerce",
    )
    df = df.dropna(subset=["DateVolunteered"])

    df["Year"] = df["DateVolunteered"].dt.year
    df["Month"] = df["DateVolunteered"].dt.month_name()
    df["DayName"] = df["DateVolunteered"].dt.day_name()
    df["Quarter"] = df["DateVolunteered"].dt.quarter
    df["Week"] = df["DateVolunteered"].dt.isocalendar().week.astype(int)
    df["Season"] = df["Quarter"].map(SEASONS)

    # Rename to stable names
    df = df.rename(columns={
        "DatabaseUserId": "Volunteer ID",
        "HoursWorked": "Hours",
        "FinalCategory": "Category",
        "EventSubcategory": "SubCategory",
    })

    # Basic filters (your intent preserved)
    df["Category"] = df["Category"].replace({"Market/Ware house Operation": "Market/Warehouse"})
    df = df[df["Year"] != 2016]
    df = df[df["Hours"] <= 16]
    df = df[df["SubCategory"] != "Holiday & Seasonal Meal Programs"]

    # Keep only columns we use (prevents leaking internal fields)
    keep = ["Volunteer ID", "DateVolunteered", "Year", "Month", "DayName", "Week", "Season", "Category", "SubCategory", "Hours"]
    df = df[[c for c in keep if c in df.columns]]

    return df


def merge_volunteer_data(df_hours: pd.DataFrame, path: Path = VOLUNTEER_FILE) -> pd.DataFrame:
    df_vol = pd.read_csv(path)

    df_vol = df_vol.rename(columns={
        "DatabaseUserId": "Volunteer ID",
        "CF - 2025 Update - Age Range (archived Nov 2025)": "Age Range",
        "CF - Skills & Experience - Languages spoken:": "Language",
        "YearsSinceVolunteerDateJoined": "YearsJoined",
    })

    vol_cols = [
        "Volunteer ID",
        "City",
        "PostalCode",
        "Province",
        "Country",
        "YearsJoined",
        "VolunteerStatus",
        "Age Range",
        "Language",
    ]
    df_vol = df_vol[[c for c in vol_cols if c in df_vol.columns]]

    df_vol["City"] = df_vol["City"].apply(clean_city)
    df_vol["VolunteerStatus"] = df_vol["VolunteerStatus"].apply(clean_volunteer_status)
    df_vol["FSA"] = df_vol["PostalCode"].apply(extract_fsa)

    df_merged = df_hours.merge(df_vol, on="Volunteer ID", how="left")
    return df_merged


# -----------------------------
# Metrics
# -----------------------------
def compute_yearly_engagement(df: pd.DataFrame) -> pd.DataFrame:
    totals = df.groupby(["Year", "Volunteer ID"])["Hours"].sum().reset_index()
    desc = totals.groupby("Year")["Hours"].describe().reset_index()
    desc = desc.rename(columns={"count": "VolunteerCount"})
    return desc


def compute_top20_engagement(df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for yr in sorted(df["Year"].unique()):
        df_y = df[df["Year"] == yr]
        totals = df_y.groupby("Volunteer ID")["Hours"].sum().sort_values(ascending=False)
        if totals.empty:
            continue

        n_vol = len(totals)
        top_n = math.ceil(0.20 * n_vol)
        top20 = totals.iloc[:top_n]
        other80 = totals.iloc[top_n:]

        rows.append({
            "Year": yr,
            "Total Volunteers": n_vol,
            "Top 20% Volunteers": top_n,
            "Total Hours": totals.sum(),
            "Top 20% Hours": top20.sum(),
            "Top 20% Share (%)": (top20.sum() / totals.sum()) * 100,
            "Mean Hours (Top 20%)": top20.mean(),
            "Mean Hours (Other 80%)": other80.mean() if len(other80) else 0,
        })

    return pd.DataFrame(rows)


def compute_retention_rolling_6mo(df: pd.DataFrame) -> pd.DataFrame:
    """
    Rolling inactivity rule:
    - For each year, consider volunteers who had at least one shift in that year.
    - Inactive if last shift in that year is >180 days before Dec 31.
    """
    df_nt = df[df["Category"] != "Training"].copy()
    threshold_days = 180

    rows = []
    for yr in sorted(df_nt["Year"].unique()):
        df_y = df_nt[df_nt["Year"] == yr]
        if df_y.empty:
            continue

        year_end = pd.Timestamp(year=yr, month=12, day=31)
        last_shift = df_y.groupby("Volunteer ID")["DateVolunteered"].max()
        inactivity_days = (year_end - last_shift).dt.days

        inactive = inactivity_days > threshold_days
        total = len(last_shift)

        rows.append({
            "Year": yr,
            "Total Volunteers": total,
            "Active Volunteers": int((~inactive).sum()),
            "Inactive Volunteers": int(inactive.sum()),
            "Active (%)": float((~inactive).sum()) / total * 100 if total else 0,
            "Inactive (%)": float(inactive.sum()) / total * 100 if total else 0,
        })

    return pd.DataFrame(rows)


def compute_category_hours(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    category_totals = (
        df.groupby("Category")["Hours"]
        .sum()
        .reset_index()
        .rename(columns={"Hours": "TotalHours"})
        .sort_values("TotalHours", ascending=False)
    )

    category_yearly = (
        df.groupby(["Year", "Category"])["Hours"]
        .sum()
        .reset_index()
        .rename(columns={"Hours": "TotalHours"})
        .sort_values(["Year", "TotalHours"], ascending=[True, False])
    )

    return category_totals, category_yearly


def compute_trends(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    df_nt = df[df["Category"] != "Training"].copy()

    active_by_year = df_nt.groupby("Year")["Volunteer ID"].nunique()

    first_year = df_nt.groupby("Volunteer ID")["Year"].min()
    new_by_year = first_year.value_counts().sort_index()
    aligned_new = new_by_year.reindex(active_by_year.index, fill_value=0)
    returning_by_year = active_by_year - aligned_new

    growth_explainer = pd.DataFrame({
        "Year": active_by_year.index,
        "ActiveVolunteers": active_by_year.values,
        "NewVolunteers": aligned_new.values,
        "ReturningVolunteers": returning_by_year.values,
    })
    growth_explainer["% New"] = growth_explainer["NewVolunteers"] / growth_explainer["ActiveVolunteers"] * 100
    growth_explainer["% Returning"] = growth_explainer["ReturningVolunteers"] / growth_explainer["ActiveVolunteers"] * 100

    # Seasonal average hours per volunteer
    season_year_hours = df.groupby(["Year", "Season"])["Hours"].sum().unstack(fill_value=0)
    season_year_vol = df.groupby(["Year", "Season"])["Volunteer ID"].nunique().unstack(fill_value=0)
    season_avg = (season_year_hours / season_year_vol).fillna(0)

    # Monthly average hours per volunteer
    month_year_hours = df.groupby(["Year", "Month"])["Hours"].sum().unstack(fill_value=0)
    month_year_vol = df.groupby(["Year", "Month"])["Volunteer ID"].nunique().unstack(fill_value=0)
    month_avg = (month_year_hours / month_year_vol).reindex(columns=MONTH_ORDER).fillna(0)

    return growth_explainer, season_avg, month_avg


# -----------------------------
# Optional: mapping stopped volunteers by FSA (safe aggregated)
# -----------------------------
def mapping_stopped_by_fsa(df_merged: pd.DataFrame) -> pd.DataFrame | None:
    if gpd is None or not FSA_SHP.exists():
        return None

    volunteer_status = (
        df_merged[["Volunteer ID", "FSA", "VolunteerStatus"]]
        .dropna(subset=["FSA"])
        .drop_duplicates(subset=["Volunteer ID"])
        .copy()
    )
    volunteer_status["Stopped"] = volunteer_status["VolunteerStatus"].isin(["Inactive", "Archived"])

    fsa_summary = (
        volunteer_status.groupby("FSA")
        .agg(
            TotalVolunteers=("Volunteer ID", "nunique"),
            StoppedVolunteers=("Stopped", "sum")
        )
        .reset_index()
    )
    fsa_summary["StoppedPct"] = fsa_summary["StoppedVolunteers"] / fsa_summary["TotalVolunteers"] * 100

    fsa = gpd.read_file(FSA_SHP)
    if "CFSAUID" in fsa.columns:
        fsa = fsa.rename(columns={"CFSAUID": "FSA"})

    # Keep only FSAs seen in data (privacy-friendly)
    fsa = fsa[fsa["FSA"].isin(fsa_summary["FSA"].unique())]

    gdf = fsa.merge(fsa_summary, on="FSA", how="left").fillna(0)
    return gdf


# -----------------------------
# Export
# -----------------------------
def export_all(
    yearly_engagement: pd.DataFrame,
    top20: pd.DataFrame,
    retention_6mo: pd.DataFrame,
    category_totals: pd.DataFrame,
    category_yearly: pd.DataFrame,
    growth_explainer: pd.DataFrame,
    season_avg: pd.DataFrame,
    month_avg: pd.DataFrame,
    fsa_gdf: pd.DataFrame | None = None,
) -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    with pd.ExcelWriter(OUTPUT_FILE, engine="openpyxl", mode="w") as writer:
        yearly_engagement.to_excel(writer, sheet_name="Yearly Engagement", index=False)
        top20.to_excel(writer, sheet_name="Top 20% Concentration", index=False)
        retention_6mo.to_excel(writer, sheet_name="Inactivity (Rolling 6mo)", index=False)
        category_totals.to_excel(writer, sheet_name="Category Summary", index=False)
        category_yearly.to_excel(writer, sheet_name="Category Trend", index=False)
        growth_explainer.to_excel(writer, sheet_name="New vs Returning", index=False)
        season_avg.to_excel(writer, sheet_name="Season Avg Hours", index=True)
        month_avg.to_excel(writer, sheet_name="Month Avg Hours", index=True)

        if fsa_gdf is not None:
            # Store only the aggregated table (not geometry) for safety
            safe_cols = ["FSA", "TotalVolunteers", "StoppedVolunteers", "StoppedPct"]
            fsa_gdf[safe_cols].to_excel(writer, sheet_name="FSA Stopped Summary", index=False)


def main() -> None:
    df_hours = load_and_clean_hours(HOURS_FILE)
    df_merged = merge_volunteer_data(df_hours, VOLUNTEER_FILE)

    yearly_engagement = compute_yearly_engagement(df_hours)
    top20 = compute_top20_engagement(df_hours)
    retention_6mo = compute_retention_rolling_6mo(df_hours)
    category_totals, category_yearly = compute_category_hours(df_hours)
    growth_explainer, season_avg, month_avg = compute_trends(df_hours)

    fsa_gdf = mapping_stopped_by_fsa(df_merged)

    export_all(
        yearly_engagement,
        top20,
        retention_6mo,
        category_totals,
        category_yearly,
        growth_explainer,
        season_avg,
        month_avg,
        fsa_gdf,
    )

    print(f"Done. Aggregated summaries exported to: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
