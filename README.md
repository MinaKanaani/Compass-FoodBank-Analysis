# Compass Food Bank Analysis

This repository contains a public-friendly version of a data analysis project built to support operational learning for a food bank using exports from the Compass volunteer management system and daily service logs.

The goal of this repo is to document **methods and workflow** (not results).  
To protect privacy and organizational confidentiality, **no raw data** and **no outcome figures** are included here.

---

## Project Overview

This project is split into two independent analyses:

### 1) Demand Analysis (`demand_analysis.py`)
Focuses on demand patterns over time using daily service logs (e.g., daily visits and pounds distributed).  
It includes:
- data cleaning and feature engineering (weekday, month, season, holiday flags)
- volatility + stability indicators (standard deviation and coefficient of variation)
- daily/weekly/monthly/seasonal/yearly summaries
- exporting summary tables for internal review (Excel)

### 2) Volunteer Activity Analysis (`volunteer_activity_analysis.py`)
Focuses on volunteer engagement patterns using Compass logged-hours data.  
It includes:
- cleaning logged-hours records
- merging volunteer demographics (when available)
- engagement distribution summaries (yearly totals, weekly intensity)
- retention/inactivity metrics (rolling 6-month inactivity rule)
- category concentration analysis (how much work is done by top contributors)
- optional geographic aggregation (e.g., FSA mapping) without storing sensitive details

---

## Data Privacy & Ethics

This repo intentionally does **not** include:
- raw Compass exports
- volunteer identifiers
- client/service user information
- charts that expose sensitive quantities or identifiable trends

Outputs are designed to be **aggregated** and safe for internal sharing only.

---

## How to Run

### 1) Install dependencies
```bash
pip install -r requirements.txt
