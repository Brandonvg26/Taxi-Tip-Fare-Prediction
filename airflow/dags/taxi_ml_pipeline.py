from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta
import subprocess, sys


DBT_PROJECT = '/usr/local/airflow/include/yellowtaxi_project'


def run_training():
    """Runs training notebook logic with memory-efficient practices."""
    import pandas as pd
    import mlflow
    import mlflow.sklearn
    from sqlalchemy import create_engine
    from sklearn.ensemble import RandomForestClassifier
    from sklearn.model_selection import train_test_split
    from sklearn.metrics import roc_auc_score
    from mlflow.models.signature import infer_signature

    # ------------------------------------------------------------
    # 1. Connect and fetch only required columns with optimal dtypes
    # ------------------------------------------------------------
    engine = create_engine('postgresql://taxi:taxi@host.docker.internal:5432/taxi_db')

    # Define column names and their desired pandas dtypes
    features = [
        'pickup_hour', 'pickup_dow', 'pickup_month', 'is_weekend',
        'trip_duration_min', 'trip_distance', 'fare_per_mile',
        'passenger_count', 'ratecodeid'
    ]
    target = 'is_high_tip'

    # Build SQL to select only needed columns
    columns = features + [target]
    sql = f'SELECT {", ".join(columns)} FROM marts.mart_trip_features'

    # Specify dtypes to save memory (adjust based on actual data ranges)
    dtype_map = {
        'pickup_hour': 'int8',        # 0-23 fits in tinyint
        'pickup_dow': 'int8',         # 0-6
        'pickup_month': 'int8',       # 1-12
        'is_weekend': 'bool',
        'trip_duration_min': 'float32',
        'trip_distance': 'float32',
        'fare_per_mile': 'float32',
        'passenger_count': 'int8',    # usually small
        'ratecodeid': 'int8',
        'is_high_tip': 'bool'
    }

    df = pd.read_sql(sql, engine, dtype=dtype_map)
    engine.dispose()                 # close the connection pool

    # ------------------------------------------------------------
    # 2. Optional: sample if dataset is huge (tune fraction as needed)
    # ------------------------------------------------------------
    # If the table is enormous (e.g., > 1 million rows), uncomment the next line:
    df = df.sample(frac=0.2, random_state=42)

    # ------------------------------------------------------------
    # 3. Prepare features and target
    # ------------------------------------------------------------
    X = df[features]
    y = df[target]

    # Free the original dataframe to reclaim memory
    del df

    # ------------------------------------------------------------
    # 4. Impute missing values (in-place to avoid copying)
    # ------------------------------------------------------------
    # Compute medians once (returns a Series)
    medians = X.median(numeric_only=True)
    # Fill missing values in-place
    X.fillna(medians, inplace=True)

    # Remove the median Series (optional, helps GC)
    del medians

    # ------------------------------------------------------------
    # 5. Train/test split
    # ------------------------------------------------------------
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.05, random_state=42, stratify=y
    )

    # Free the original X and y (now split into train/test)
    del X, y

    # ------------------------------------------------------------
    # 6. Train model with memory-friendly settings
    # ------------------------------------------------------------
    mlflow.set_tracking_uri('file:/usr/local/airflow/include/mlruns')
    mlflow.set_experiment('taxi_tip_prediction_airflow')

    with mlflow.start_run(run_name=f'rf_{datetime.now().strftime("%Y%m%d")}'):
        # Use n_jobs=1 to avoid memory overhead from parallel workers
        model = RandomForestClassifier(
            n_estimators=200, max_depth=8,
            class_weight='balanced', random_state=42, n_jobs=1
        )
        model.fit(X_train, y_train)

        # Predict probabilities (only keep the positive class column)
        y_pred_proba = model.predict_proba(X_test)[:, 1]
        auc = roc_auc_score(y_test, y_pred_proba)

        mlflow.log_metric('roc_auc', auc)

        # Infer signature and log model
        signature = infer_signature(X_train, model.predict(X_train))
        mlflow.sklearn.log_model(
            model, 'tip_classifier',
            signature=signature,
            registered_model_name='taxi_tip_classifier'
        )

    # Explicitly delete large objects (helps memory pressure during GC)
    del X_train, X_test, y_train, y_test, model, y_pred_proba

    print(f'Training complete. ROC-AUC: {auc:.4f}')


default_args = {'retries': 1, 'retry_delay': timedelta(minutes=2)}


with DAG(
    dag_id='taxi_ml_pipeline',
    start_date=datetime(2024, 1, 1),
    schedule='@monthly',
    default_args=default_args,
    catchup=False,
    tags=['ml', 'dbt', 'taxi'],
) as dag:

    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command=f'cd {DBT_PROJECT} && dbt run --profiles-dir {DBT_PROJECT}',
    )

    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command=f'cd {DBT_PROJECT} && dbt test --profiles-dir {DBT_PROJECT}',
    )

    train = PythonOperator(
        task_id='train_model',
        python_callable=run_training,
    )

    dbt_run >> dbt_test >> train