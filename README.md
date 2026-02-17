# Airflow Data Validation Pipeline (TaskFlow API)

Production-style Apache Airflow DAG that simulates a batch ingestion workflow and applies rule-based data validation with structured anomaly logging.

This project demonstrates how a data quality layer can be orchestrated within an ETL pipeline using Airflow’s TaskFlow API.

---

## 📌 Project Overview

This pipeline runs on a scheduled interval and performs two primary stages:

1. **Generate synthetic booking records**
2. **Validate records against schema and business rules**

The goal is to simulate real-world ingestion scenarios where data may contain:
- Missing fields
- Invalid categorical values
- Incomplete timestamps
- Corrupted records

Each DAG execution writes both raw records and detected anomalies to execution-scoped directories.

---

## 🏗 Pipeline Architecture

Execution Flow: generate_bookings >> validate_bookings


### Stage 1 – Data Generation

- Generates 5–15 synthetic booking records per run
- Simulates realistic data corruption using probabilistic field failures
- Writes structured JSON output to: "/tmp/data/bookings/<execution_timestamp>/bookings.json"


Each record contains:

- `booking_id`
- `listing_id`
- `user_id`
- `booking_time`
- `status`

Some fields are intentionally left invalid to test validation robustness.

---

### Stage 2 – Validation Layer

Applies rule-based validation checks:

- Required field presence validation
- Null / empty value checks
- Status validation (`confirmed`, `pending`, `cancelled`)
- Invalid status detection (`unknown`, `error`, empty values)

Invalid records are captured with:

- Record index
- Violated rules
- Full record snapshot

Anomalies are written to: "/tmp/data/bookings/<execution_timestamp>/anomalies.json"


This structure enables downstream auditing, alerting, or threshold-based failure logic.

---

## 🧠 Design Decisions

- Built using **Airflow TaskFlow API** for clean Python-based DAG structure
- Uses execution-date-based directory isolation for reproducibility
- Explicit task dependencies (`>>`) for orchestration clarity
- Structured JSON logging for deterministic anomaly tracking
- Modular helper functions for synthetic data generation

The pipeline is intentionally designed to resemble a real-world data quality enforcement layer in production ETL systems.

---

## ⚙️ Tech Stack

- **Python**
- **Apache Airflow 2.x**
- TaskFlow API
- JSON-based structured data handling
- Rule-based anomaly detection logic
- Local file system storage (execution-scoped)

---

## 🚀 Running the DAG

1. Place `data_validation_dag.py` inside your Airflow `dags/` directory.
2. Start Airflow locally:
```md
```bash
airflow standalone

or via Docker: 
docker compose up
3. Enable and trigger data_quality_pipeline in the Airflow UI.

## Engineering Concepts Demonstrated

- DAG orchestration & dependency management
- Rule-based data quality validation
- Execution-scoped batch processing
- Structured anomaly logging

## Repository Structure

airflow-data-validation/
├── dags/
│   └── data_validation_dag.py
├── requirements.txt
└── README.md
