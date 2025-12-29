# NYC Taxi Data Pipeline  
**End-to-End ETL Pipeline with Data Quality Checks and CI/CD**

---

##  Project Overview

This project implements a **production-style end-to-end data engineering pipeline** using NYC Yellow Taxi trip data.  
It demonstrates how raw data is **extracted, validated, transformed, loaded into a warehouse, quality-checked, and modeled into analytics-ready tables**, with **CI/CD automation** using GitHub Actions.

---

##  Project Objectives

- Build a scalable **ETL (Extract, Transform, Load) pipeline**
- Process large-scale Parquet datasets
- Apply **data cleaning and validation rules**
- Load curated data into an analytical warehouse
- Create **business-ready analytics marts**
- Implement **CI/CD automation** for pipeline reliability
- Follow production-grade repository and workflow standards

---

##  High-Level Architecture
Raw Parquet Files
↓
Extraction Layer
↓
Transformation & Cleaning
↓
DuckDB Data Warehouse
↓
Data Quality Checks
↓
Analytics Marts
↓
CSV Exports / BI Consumption


---

##  Technology Stack

| Category | Tools |
|--------|------|
| Programming Language | Python 3.12 |
| Storage Format | Parquet |
| Data Processing | Pandas |
| Data Warehouse | DuckDB |
| Data Quality | Custom rule-based checks |
| CI/CD | GitHub Actions |
| Orchestration | Bash |
| Version Control | Git & GitHub |

---

##  Pipeline Workflow

### Step 1: Extract
- Reads NYC Yellow Taxi Parquet files
- Validates raw file availability
- Writes extracted data to the processed layer

### Step 2: Transform & Load
- Parses pickup and dropoff timestamps
- Derives date and hour attributes
- Joins taxi zone lookup (borough & zone)
- Removes invalid or corrupt records
- Loads cleaned data into DuckDB

### Step 3: Data Quality Checks
The pipeline enforces the following checks:
- Table is not empty
- Pickup and dropoff timestamps are not null
- Fare amount and distance are non-negative
- Passenger count is within a reasonable range
- Trip duration is positive

 The pipeline fails automatically if any check fails.

### Step 4: Analytics Marts
Creates analytics-ready tables:
- **Trips by Borough**
- **Trips by Hour of Day**

These tables are optimized for reporting and dashboarding.

### Step 5: Export Layer
- Exports analytics marts to CSV format
- Ready for BI tools or downstream consumption

---

## Sample Analytics Outputs

**Trips by Borough**
- Total number of trips
- Average trip distance
- Average fare amount

**Trips by Hour of Day**
- Hourly trip volume
- Average trip distance
- Average fare amount

---

## CI/CD Automation

This project includes a GitHub Actions CI pipeline that automatically:
- Installs dependencies
- Validates Python syntax
- Runs import smoke tests
- Ensures pipeline code integrity without requiring raw data

This ensures **every commit is safe, reproducible, and production-ready**.

---




















