Fraud Risk Command Center

End-to-End Fraud Detection System | SQL + Python + Machine Learning + Power BI

Project Overview

The Fraud Risk Command Center is a production-style fraud analytics system that simulates how enterprise fraud teams operate.

This project integrates:

Data engineering (SQL warehouse architecture)

Feature engineering & machine learning modeling (Python)

Risk segmentation & explainability

Operational investigation dashboard (Power BI)

Model monitoring & governance layer

The goal was not just prediction accuracy — but operational usability, decision enablement, and governance readiness.

Business Problem

Retail / digital payment ecosystems process thousands of transactions daily, exposing organizations to financial loss from fraudulent activity.

Key business challenges:

Where is fraud concentrated?

What patterns precede fraudulent behavior?

How should investigation teams prioritize cases?

What threshold optimizes precision vs recall?

Is the model stable over time?

This project answers those questions through a structured analytics pipeline.

Architecture

Raw Excel Dataset
        ↓
SQL Server (staging)
        ↓
Curated Dimensions (Dim_Customer, Dim_Geo, Dim_Merchant, etc.)
        ↓
Fact_Transactions (analytics-ready)
        ↓
Python Feature Engineering + ML Modeling
        ↓
Scored Transactions (fraud_probability + risk_band)
        ↓
Power BI Command Center (4 Dashboards)
Architecture Layers

 Data Layer

Raw transactions ingested into staging

Conformed dimensions built in curated

Analytics-ready fact table in marts

2. Feature Layer

Behavioral features (velocity, anomaly, switching)

Merchant risk aggregation

Customer history signals

3. Model Layer

Logistic Regression (baseline)

Random Forest (strong model)

Threshold optimization (F1-based)

4. Visualization Layer

Executive dashboard

Operations workbench

Pattern & driver analysis

Model monitoring & governance

Dataset Summary

Transactions: 5,000

Fraud rate: 23.42%

Fraud transactions: 1,171

Genuine transactions: 3,829

Missing / invalid rows after cleaning: 0

Data Quality & Cleaning

Applied production-style quality rules:

Invalid timestamps → coerced / removed

Amount ≤ 0 → removed

Age outside realistic range → handled

Duplicate transaction_id → de-duplicated

Output:

clean_transactions_v1.csv

 Feature Engineering

Engineered features aligned to fraud behavior patterns:

Customer Behavior

txn_count_1h

txn_count_24h

avg_amount_7d

max_amount_24h

time_since_last_txn

Transaction Context

txn_hour

weekend_flag

channel

payment_method

Merchant Risk

merchant_fraud_rate

Geo & Device Signals

is_international

geo_switch_flag

device switch indicators

Modeling & Evaluation

Models Tested
Model	ROC-AUC	PR-AUC
Logistic Regression (scaled)	0.7256	0.5685
Random Forest	0.7002	0.5588
Why PR-AUC?

Fraud detection is an imbalanced classification problem, making PR-AUC more representative of real-world performance.

Threshold Optimization

Instead of default 0.5, threshold was optimized using F1 score.

Selected operating threshold:

0.59

This balances:

Precision (investigation efficiency)

Recall (fraud capture rate)

Risk Segmentation Framework

Transactions categorized into:

Low Risk

Medium Risk

High Risk

Used in:

Investigation prioritization

Queue volume monitoring

SLA-style workload management

 Power BI Command Center

Executive Overview

Fraud loss trends

Fraud rate trends

High-risk percentage

Concentration by region/category/channel

Fraud Ops Workbench

Investigation queue table

Risk band distribution

SLA volume cards

Drill-down capabilities

Pattern & Driver Intelligence

Category × Channel fraud heatmap

Geo/device switching analysis

Transaction anomaly scatter

Model feature importance

Model Monitoring & Governance

ROC-AUC & PR-AUC cards

Precision / Recall / F1

Threshold impact curve

Risk band calibration

Drift proxy (avg fraud_probability over time)

Top Fraud Drivers (Random Forest)

previous_fraud_flag

avg_amount_7d

max_amount_24h

account_age_days

transaction_amount

is_international

customer_age

txn_hour

merchant_fraud_rate

Key Insights

Fraud is concentrated in specific merchant categories and digital channels.

Velocity spikes (multiple txns within short time windows) are strong predictors.

International transactions combined with previous fraud history increase risk significantly.

Threshold tuning meaningfully impacts investigation workload.

Operational Recommendations

Immediate Actions

Prioritize High Risk transactions for manual review.

Use Medium Risk for rule-based secondary checks.

Monitor Low Risk via alert system only.

Rule Enhancements

previous_fraud_flag + high amount + international

Abnormal amount vs avg_amount_7d

Multiple txns within 10–60 minutes

Geo/device switching within short time window

Monitoring Enhancements

Weekly PR-AUC tracking

Drift detection via fraud_probability trend

Threshold sensitivity monitoring

Repository Structure

Fraud_Risk_Command_Center/

How To Run

Step 1 — SQL

Create schemas: staging, curated, marts

Load raw dataset into staging

Run dimension + fact creation scripts

Step 2 — Python

Run cleaning script

Run feature engineering script

Train model and generate fraud_probability

Export scored_transactions.csv

Step 3 — Power BI

Connect to SQL or scored CSV

Refresh

Validate measures

Publish dashboards

Limitations

No out-of-time validation split

Drift proxy not full PSI implementation

Feature importance not SHAP-based

No live streaming / automated pipeline

Future Enhancements

XGBoost + SHAP explainability

Drift monitoring using PSI / KS

Automated pipeline (Airflow / ADF)

Real-time alerting layer

REST API scoring endpoint

Skills Demonstrated

SQL Data Modeling

Feature Engineering

Imbalanced Classification

Threshold Optimization

Risk Segmentation Design

Power BI Advanced DAX

Model Governance Concepts

Fraud Analytics Strategy

Author

Veda Vaishnavi
     -Aspiring Data Analyst