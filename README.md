# dbt + Airflow + Postgres + scikit-learn

End-to-end ML pipeline built on the modern open-source data stack — dbt transforms raw trip data into a training-ready feature table, Airflow orchestrates the full run, and scikit-learn trains a binary classifier tracked with MLflow.


### Purpose
This is a learning and practice project built to demonstrate fluency with the modern data stack independent of any managed cloud platform (no Databricks, no Azure, no AWS).

### The primary goals were:
1. Practice dbt Core transformations end-to-end: source → staging → intermediate → mart
2. Understand how dbt model lineage feeds an ML training set (the handoff most DE portfolios skip)
3. Work with Airflow locally via Astro CLI without relying on a managed service
4. Demonstrate reproducibility: every layer from raw ingestion to registered model is versioned and re-runnable


### Data Origin: 

Used the NYC TLC Yellow Taxi Trip Datasets, record period SEP-NOV 2025 (3 months, ~10M rows) 
Source: NYC Taxi & Limousine CommissionFormatParquet (publicly available, no account or API key required)
URL: - https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page -

The TLC dataset was chosen because it is large enough to be realistic, domain-neutral (urban mobility vs. retail vs. finance), widely recognized in data engineering interviews, and freely available without registration.

IMPORTANT data caveat: Only credit card trips (payment_type = 1) are included in training. Cash trips always record tip_amount = 0 regardless of the actual tip paid — including them would poison the target variable. This filter is enforced in the dbt staging model, documented in the lineage graph, and explained in the README to show awareness of label integrity.

# Architecture
```
NYC TLC (Parquet download)
        │
        ▼
[Raw layer]          Postgres schema: raw
                     Table: yellow_trips
                     Loaded via pandas + SQLAlchemy in notebook 01
        │
        ▼  dbt run
[Staging model]      stg_yellow_trips (view)
                     Cleans: filters zero fares, bad distances, cash trips
                     Casts timestamps, renames columns to snake_case
        │
        ▼  dbt run
[Intermediate model] int_trips_features (view)
                     Derives: pickup_hour, pickup_dow, is_weekend,
                              trip_duration_min, fare_per_mile, is_high_tip (target)
        │
        ▼  dbt run + dbt test
[Mart model]         mart_trip_features (table)
                     Training-ready: range-filtered, tested for uniqueness + nulls
                     Materialized as a Postgres table in schema: marts
        │
        ├── dbt test passes → training proceeds
        │
        ▼  notebook 03 / Airflow PythonOperator
[Training]           RandomForestClassifier (scikit-learn)
                     MLflow tracking: params, ROC-AUC, feature importance
                     Model registered: taxi_tip_classifier
        │
        ▼  notebook 04
[Batch inference]    Predictions written to marts.trip_predictions
```

## Orchestration: An Airflow DAG (taxi_ml_pipeline) chains dbt_run → dbt_test → train_model on a monthly schedule, running inside Docker via Astro CLI.

## Stack
LayerToolRoleWarehousePostgres 15 (Docker)Stores all layers: raw, staging, martsTransformationsdbt Core 1.8 + dbt-postgresBronze → Silver → Gold SQL models with testsOrchestrationApache Airflow 2.9 (Astro CLI)Schedules dbt + training as a DAGModel trainingscikit-learn · RandomForestClassifierBinary tip classifierExperiment trackingMLflow (local file backend)Tracks runs, registers model versionsData validationGreat Expectations 1.xQuality gate on raw data before transformationCIGitHub ActionsRuns pytest + dbt parse on every push
Everything runs locally or in Docker. No cloud accounts, no trial expirations, no costs.

## Prediction target
Is this trip a high-tip trip? Defined as tip_amount > 20% of fare_amount, for credit card trips only. Binary classification (0 / 1).
This framing was chosen because it is a realistic business question (driver earnings, zone demand forecasting), the signal is present in the raw data without scraping or joining external sources, and the class balance (~40/60) is tractable without heavy resampling.

## Results


Key design decisions
dbt as the feature engineering layer, not Python
All feature derivations (pickup_hour, fare_per_mile, is_high_tip) live in dbt SQL models. This means the lineage graph shows exactly where every training column comes from — auditable, testable, and decoupled from the training code.
Cash trip exclusion enforced at the dbt layer
Filtering payment_type = 1 in the staging model (not in a notebook) means the exclusion is tested, versioned, and visible in the lineage. A future engineer can't accidentally re-include cash trips by re-running a notebook with a different filter.
MLflow with no remote server
mlflow.set_tracking_uri('file:./mlruns') stores all experiment data locally. No Databricks, no MLflow server setup, no cloud credentials — the experiment UI runs with mlflow ui from the venv.
Airflow DAG enforces test-before-train
dbt_run → dbt_test → train_model means training never starts if dbt tests fail. This is an explicit architectural decision: data quality gates are owned by the pipeline, not by the training notebook.

# Reproduce for use the same structure
#### Prerequisites: Docker Desktop running, Python 3.11, Astro CLI installed

## 1. Clone and create virtual environment
git clone https://github.com/<your-handle>/taxi-tip-prediction
cd taxi-tip-prediction
python3 -m venv .venv && source .venv/bin/activate

## 2. Install Python dependencies
pip install dbt-core dbt-postgres scikit-learn mlflow pandas pyarrow \
            psycopg2-binary great-expectations jupyterlab

## 3. Start Postgres
docker run -d --name taxi_pg \
  -e POSTGRES_USER=taxi -e POSTGRES_PASSWORD=taxi -e POSTGRES_DB=taxi_db \
  -p 5432:5432 postgres:15

## 4. Run notebooks in order
###    notebooks/01_ingest_raw.ipynb       ← download + load raw data
###    notebooks/02_explore_validate.ipynb ← EDA + GE quality gate
###    notebooks/03_train_model.ipynb      ← train + log to MLflow
###    notebooks/04_batch_inference.ipynb  ← score + write predictions

## 5. Run dbt models
cd dbt_taxi && dbt deps && dbt run && dbt test

## 6. View lineage
dbt docs generate && dbt docs serve --port 8081

## 7. Start Airflow (optional — runs the full pipeline as a DAG)
cd ../airflow && astro dev start
## Open http://localhost:8080 · trigger taxi_ml_pipeline DAG
All steps are idempotent — re-running produces the same result.

# What I would do differently at production scale

A. Incremental dbt models — use {{ is_incremental() }} to process only new months instead of rebuilding the full mart on every run
B. Airflow sensors — replace the hardcoded monthly schedule with a file sensor that triggers when a new TLC Parquet is published
C. Online serving — wrap the registered MLflow model in a FastAPI endpoint for real-time prediction instead of batch scoring
D. Drift monitoring — add Evidently AI to compare the monthly feature distribution against the training baseline and alert when fare_per_mile or pickup_hour distributions shift
E. Managed Postgres — replace the local Docker container with RDS or Cloud SQL for persistence across machines


Stack versions: dbt Core 1.8 · Apache Airflow 2.9 · Postgres 15 · scikit-learn 1.4 · MLflow 2.x · Python 3.11
