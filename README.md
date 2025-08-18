# 📊 End-to-End E-Commerce Data Analysis Project

## 🚀 Project Overview

This project presents an end-to-end analysis simulating an E-commerce website scenario aimed at enhancing customer retention, optimizing conversion rates, and improving strategic decision-making through data-driven insights.

## 📌 Project Objective

Turn messy, multi-table e-commerce data into analysis-ready models and ship decision-ready dashboards. The focus:
- Quantify funnel health and drop-offs,
- Measure churn and cohort behaviour,
- Segment customers with RFM to drive targeted actions.

## Dataset & Scale

- Tables: Customers, Transactions, SessionEvents, Churn
- Records: 9,976 customers • 29,184 transactions • 256,591 session events • 9,897 churn/RFM rows
- Synthetic data with realistic issues (missing values, out-of-order events, inconsistent IDs)

## Architecture at a Glance

- Storage/ETL: CSV → PostgreSQL 
- Processing: SQL (CTEs, window functions, PL/pgSQL procedures) for cleaning/standardisation
- Analysis: Python (pandas, numpy, matplotlib/seaborn) for EDA & feature engineering
- BI: Power BI dashboards (Funnel, Churn, RFM) with slicers, KPIs, drill-through, bookmarks, custom tooltips

## Data Preparation (SQL)

File: SQL_data_exploration_cleaning.sql

- Removed duplicates and extreme outliers.
- Imputed demographics (Age, City, State, Income, Education, Occupation) with window functions & grouped averages.
- Standardised session logs via PL/pgSQL:
  - Fixed event order,
  - Inserted missing events,
  - Back/Forward-Filled timestamps using customer-level and global average inter-event gaps.
- Reconciled Transactions ↔ SessionEvents so purchase counts align (→ 29,184).

## Analysis & Feature Engineering (Python)
Files: Data_exploration_analysis.ipynb

- Connected to PostgreSQL with SQLAlchemy; validated schema and data types.
- EDA: distributions, category splits, time series sanity checks.
- Features/Metrics:
  - RFM (Recency, Frequency, Monetary) scoring and labelling, exported to rfm.csv.
  - Funnel metrics: stage conversions & drop-offs.
  - Churn metrics: labelled churn %, trends, cohort comparisons.

## Dashboards (Power BI)
Reports: E-Commerce_Project.pbix

- Funnel report: date/hour/DOW/payment slicers, KPI cards, funnel + waterfall, tooltip (avg age/income/churn), bookmarks (Reset / Highlight Drop-offs).
- Churn report: date & demographic slicers, churn vs retention, monthly trend, recency-frequency scatter, churn-rate bars.
- RFM report: segment & demographic slicers, segment KPIs, combo chart (avg R/F bars + M line), RFM matrix (heat), drill-through to boxplots & Top-10 customers.

## Key Findings (What the data says)

- Biggest friction: Visit → Add to Cart.
- Overall funnel conversion: ~23.36%.
- Strongest step: Checkout → Purchase ≈ 75.7% (bottlenecks are upstream).
- Churn (labelled): ~33.7% with distinct behavioural and demographic skews.
- RFM distribution: Loyal 3,056, At-Risk 2,405, Hibernating 2,185, Champion 1,343, Potential Loyalist 908.
- Data consistency: Purchases reconciled across SessionEvents and Transactions at 29,184, ensuring one source of truth for KPIs.

## Actions (What to do next)

Funnel (reduce Add-to-Cart drop-off):
- Tighten PDP → Add to Cart: surface size/availability, trust badges, delivery dates, returns; preload size guides; simplify variant selection.
- Experiment with free/flat shipping thresholds and guest checkout visibility earlier in the flow.
- Run A/B on CTA prominence and sticky cart.

Churn / Lifecycle:
- Win-back sequences for At-Risk/Hibernating: 2–3 message cadence over 14–21 days, segment-specific incentives.
- Save offers for recent high-M but deteriorating R customers.
- Feedback loop: trigger surveys when R crosses risk thresholds.

RFM / Growth:
- Champions/Loyal: early access, bundles, referral nudge.
- Potential Loyalists: nudge to 2nd/3rd purchase with low-friction offers and replenishment reminders.

## 📂 Repository Structure

```
E-Commerce-Aata-Analysis/
├── Datasets/
│   ├── Raw/
│   ├── Cleaned/
├── SQL Scripts/
│   ├── SQL_data_exploration_cleaning.sql
|   ├── sql_script.pdf
├── Notebooks/
│   └── Data_exploration_analysis.ipynb
├── Visualisations/
│   └── E-Commerce_Project.pbix
│   ├── Report Pages
|       └── Start_Page.png
│       └── Funnel_Analysis.png
│       └── Churn_Analysis.png
│       └── Customer_Segmentation.png
│       └── RFM_Segment_Drillthrough.png
|       └── Start_Page_Tooltip.png
│       └── Funnel Tooltip.png
└── README.md
```
## Tech Stack
- PostgreSQL (CTEs, window functions, PL/pgSQL)
- Python (pandas, numpy, matplotlib/seaborn, SQLAlchemy)
- Power BI

## 📈 Visual Previews

  *Start Page*

![Start_Page](https://github.com/geettandon/E-Commerce-Data-Analysis/blob/main/Visualizations/Report%20Pages/Start_Page.png)

  *Funnel Analysis Dashboard*

![Funnel_Analysis](https://github.com/geettandon/E-Commerce-Data-Analysis/blob/main/Visualizations/Report%20Pages/Funnel_Analysis.png)

  *Churn Analysis Dashboard*

![Churn_Analysis](https://github.com/geettandon/E-Commerce-Data-Analysis/blob/main/Visualizations/Report%20Pages/Churn_Analysis.png)

  *Customer Segmentation with RFM Dashboard*

![Customer_Segmentation](https://github.com/geettandon/E-Commerce-Data-Analysis/blob/main/Visualizations/Report%20Pages/Customer_Segmentation.png)

  *RFM Segment Details Drillthrough Page*

![RFM_Segment_Details_Drillthrough](https://github.com/geettandon/E-Commerce-Data-Analysis/blob/main/Visualizations/Report%20Pages/RFM_Segment_Details_Drillthrough.png)

  *Start Page Tooltip Page*

![Start_Page_Tooltip](https://github.com/geettandon/E-Commerce-Data-Analysis/blob/main/Visualizations/Report%20Pages/Start_Page_Tooltip.png)

  *Funnel Analysis Tooltip Page*

![Funnel_tooltip](https://github.com/geettandon/E-Commerce-Data-Analysis/blob/main/Visualizations/Report%20Pages/Funnel_Tooltip.png)

  
## 🌟 Contact
                                   
LinkedIn: https://www.linkedin.com/in/geettandon







