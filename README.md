# Taxi Tip Prediction — Analytics Engineering + ML Pipeline
### dbt Core · Airflow · Postgres · scikit-learn · MLflow

> dbt transforms 10M raw NYC taxi trips into a tested, training-ready feature
> mart; Airflow orchestrates the full run on a schedule; scikit-learn trains a
> tip classifier tracked and registered in MLflow. Built entirely on the
> open-source data stack — no managed cloud platform required.

**Stack:** dbt Core 1.8 · Apache Airflow 2.9 (Astro CLI) · PostgreSQL 15 ·
scikit-learn · MLflow · Great Expectations 1.x · GitHub Actions · Docker

---

## What this project demonstrates
- dbt transformations end-to-end: source → staging → intermediate → mart, with tests
- The dbt-to-ML handoff most pipelines skip: model lineage feeding a training set
- Local orchestration with Airflow (Astro CLI), no managed service dependency
- Full reproducibility: every layer from raw ingestion to registered model is versioned and re-runnable

---

## Architecture

```
NYC TLC (Parquet, ~10M rows)
        │
        ▼
[Raw layer]          Postgres schema: raw · table: yellow_trips
                     Loaded via pandas + SQLAlchemy
        │
        ▼  dbt run
[Staging model]      stg_yellow_trips (view)
                     Filters zero fares, bad distances, cash trips
                     Casts timestamps, normalizes column names
        │
        ▼  dbt run
[Intermediate model] int_trips_features (view)
                     Derives pickup_hour, pickup_dow, is_weekend,
                     trip_duration_min, fare_per_mile, is_high_tip (target)
        │
        ▼  dbt run + dbt test
[Mart model]         mart_trip_features (table)
                     Range-filtered · tested for uniqueness + nulls
        │
        ├── dbt test passes → training proceeds
        ▼
[Training]           RandomForestClassifier · MLflow tracking
                     Model registered: taxi_tip_classifier
        │
        ▼
[Batch inference]    Predictions written to marts.trip_predictions
```

**Orchestration:** Airflow DAG `taxi_ml_pipeline` chains
`dbt_run → dbt_test → train_model` on a monthly schedule, running in Docker
via Astro CLI. dbt tests act as a quality gate — training never runs on
unvalidated data.

---

## Stack detail

| Layer | Tool | Role |
|---|---|---|
| Warehouse | PostgreSQL 15 (Docker) | Stores all layers: raw, staging, marts |
| Transformations | dbt Core 1.8 + dbt-postgres | Bronze → Silver → Gold SQL models with tests |
| Orchestration | Apache Airflow 2.9 (Astro CLI) | Schedules dbt + training as a single DAG |
| Model training | scikit-learn (RandomForestClassifier) | Binary high-tip classifier |
| Experiment tracking | MLflow | Tracks runs, registers model versions |
| Data validation | Great Expectations 1.x | Quality gate on raw data before transformation |
| CI | GitHub Actions | Runs pytest + dbt parse on every push |

Everything runs locally or in Docker — no cloud accounts, no costs.

---

## Key design decisions

**Cash trips excluded to protect label integrity**
Cash trips always record `tip_amount = 0` regardless of the actual tip paid —
including them would poison the target variable. Only credit card trips
(`payment_type = 1`) enter training. The filter is enforced in the dbt staging
model and visible in the lineage graph.

**dbt tests as a training gate, not an afterthought**
The Airflow DAG only proceeds to training if `dbt test` passes on the mart.
Bad data fails the pipeline before it can produce a bad model.

**Prediction target defined upstream, in SQL**
`is_high_tip` (tip > 20% of fare) is derived in the intermediate dbt model, so
the target definition is versioned, tested, and documented alongside the
features — not buried in a notebook.

**Local-first stack on purpose**
Postgres + dbt Core + Airflow proves the same lineage-to-training pattern as a
Databricks/Snowflake setup, without trial expirations — and the skills transfer
1:1 to managed platforms.

---

## Results

| Item | Value |
|---|---|
| Input volume | ~10M trips (Sep–Nov 2025) |
| Model | RandomForestClassifier |
| Primary metric | ROC-AUC (tracked in MLflow) — [VERIFY: add your number] |
| Registered model | `taxi_tip_classifier` |

---

## How to run

```bash
git clone https://github.com/Brandonvg26/taxi_tip_prediction
cd taxi_tip_prediction

# Warehouse
docker compose up -d            # Postgres 15

# Transformations
dbt run && dbt test

# Orchestrated run
astro dev start                 # Airflow at http://localhost:8080
# Trigger DAG: taxi_ml_pipeline
```

---

## Dataset

NYC TLC Yellow Taxi Trip Records — Sep–Nov 2025, ~10M rows, Parquet.
Public, no registration required: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
Chosen for realistic scale, domain neutrality, and recognition in DE interviews.
