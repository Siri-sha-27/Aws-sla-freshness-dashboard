![Python](https://img.shields.io/badge/Python-3.9+-blue)
![AWS](https://img.shields.io/badge/AWS-Cloud-orange)
![Lambda](https://img.shields.io/badge/AWS-Lambda-yellow)
![Athena](https://img.shields.io/badge/AWS-Athena-blueviolet)
![API Gateway](https://img.shields.io/badge/AWS-API_Gateway-green)
![Streamlit](https://img.shields.io/badge/Streamlit-Dashboard-red)
![SLA](https://img.shields.io/badge/SLA-Monitoring-critical)
![License](https://img.shields.io/badge/License-MIT-brightgreen)

# SLA Freshness & Business SLA Monitoring Dashboard (AWS)

## Technologies Used

### Programming & Analytics
- Python 3
- Pandas
- Streamlit
- SQL (Athena)

### AWS Services
- Amazon S3
- AWS Lambda
- Amazon Athena
- AWS Glue Data Catalog
- Amazon API Gateway
- Amazon SNS
- Amazon EC2
- IAM

---

## Project Overview

Modern data platforms must ensure that **data arrives on time** and that **business operations meet delivery expectations**.  
This project implements a **two-layer SLA monitoring system** on AWS that tracks **data pipeline freshness** and **business delivery performance**, and exposes both through a **single API and interactive dashboard**.

The system is designed to simulate a **real-world data engineering observability use case** using real e-commerce data.

---

## Dataset Used

**Olist E-commerce Dataset (Brazil)**  
Source: Kaggle

The dataset includes:
- Orders
- Payments
- Products
- Estimated and actual delivery timestamps

This makes it suitable for both **technical SLA monitoring** and **business SLA analysis**.

---

## What Is SLA Monitoring?

A **Service Level Agreement (SLA)** defines the expected time or quality guarantees for data and services.

In data engineering:
- Late data can break dashboards
- Stale data can lead to wrong business decisions
- Missing data can halt downstream pipelines

This project monitors SLAs at **two levels**.

---

## SLA Layers Implemented

### Layer 1 — Pipeline Freshness SLA (Technical SLA)

**Purpose:** Detect late or missing data pipelines.

Monitors:
- S3 object arrival timestamps

Evaluates:
- Hourly feeds (payments)
- Daily feeds (orders)
- Weekly feeds (products)

Outputs:
- SLA status (`on_time`, `late`, `critically_late`)
- Freshness score (0–100)
- Delay in minutes
- Latest object key

Alerts:
- Amazon SNS sends email alerts for critical SLA breaches

---

### Layer 2 — Business SLA (Business Impact SLA)

**Purpose:** Measure customer-facing delivery performance.

Computed using Athena views:
- Total delivered orders
- Late deliveries
- Late delivery percentage
- Average days late
- 90-day delivery trend

This layer answers:
> “Even if pipelines are healthy, is the business still meeting delivery expectations?”

---

## Architecture

### High-Level Architecture Diagram
<img width="831" height="611" alt="sla_freshness_architecture drawio" src="https://github.com/user-attachments/assets/828e8b76-077d-40ca-8e8f-ddeb6f007b8a" />
### Architecture Flow

1. Raw data lands in Amazon S3
2. Lambda checks pipeline freshness and computes SLA metrics
3. SLA results are written back to S3
4. AWS Glue catalogs SLA metrics
5. Amazon Athena creates analytical views
6. A single Lambda API aggregates all SLA results
7. API Gateway exposes a public endpoint
8. Streamlit dashboard (EC2) visualizes the results
9. SNS sends alerts on critical SLA breaches

---

## Dashboard

### Dashboard Screenshots
<img width="1916" height="988" alt="image" src="https://github.com/user-attachments/assets/33efe45c-c644-4c1c-9ef2-e783dae73a74" />

<img width="1917" height="989" alt="image" src="https://github.com/user-attachments/assets/ddfc8506-0056-4085-a979-176a0f75da91" />


### Dashboard Features

- Single API endpoint
- Auto-refresh with latest data
- SLA alert banners
- Pipeline SLA status table
- Business KPI metrics
- 90-day SLA trend visualization

---

## AWS Services Used

- **Amazon S3** – Raw data and SLA metrics storage
- **AWS Lambda** – SLA computation and API layer
- **AWS Glue** – Metadata and schema catalog
- **Amazon Athena** – SLA queries and analytics
- **API Gateway** – Public API exposure
- **Amazon SNS** – Alerting on SLA breaches
- **Amazon EC2** – Streamlit dashboard hosting
- **IAM** – Secure access control

---

## Effectiveness of the Solution

This system is effective because it:

- Detects pipeline issues before downstream failures
- Separates technical SLAs from business SLAs
- Uses real production-like data
- Provides actionable alerts
- Exposes insights via a single unified dashboard

This mirrors how **real data platforms monitor reliability in production**.

---

## Limitations

- Streamlit runs on EC2 (manual scaling)
- Athena introduces small query latency
- SLA alerts are stateless (no historical alert store)

These trade-offs were intentional to stay within **free-tier and learning constraints**.

---

## Future Improvements

- Custom domain + HTTPS
- Authentication (Cognito)
- Historical SLA breach storage (DynamoDB)
- Data quality checks (volume, nulls, schema drift)
- CI/CD for Lambda deployment
- Cost optimization and partition pruning
- Multi-region SLA monitoring

---

## Conclusion

This project demonstrates:
- End-to-end data engineering design
- SLA-driven monitoring systems
- Business-aware analytics
- Serverless AWS architecture
- Realistic observability use cases

It reflects **industry-style SLA monitoring**, not a toy example.

---

## Author

Sirisha Gajula  
GitHub: https://github.com/Siri-sha-27

