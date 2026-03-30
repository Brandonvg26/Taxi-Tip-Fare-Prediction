import pandas as pd, pytest
from datetime import datetime


def compute_features(df):
    df = df.copy()
    df['pickup_hour']      = pd.to_datetime(df['pickup_at']).dt.hour
    df['pickup_dow']       = pd.to_datetime(df['pickup_at']).dt.dayofweek
    df['is_weekend']       = df['pickup_dow'].isin([5,6]).astype(int)
    df['trip_duration_min']= (pd.to_datetime(df['dropoff_at'])
                             - pd.to_datetime(df['pickup_at'])).dt.total_seconds() / 60
    df['fare_per_mile']    = df['fare_amount'] / df['trip_distance'].replace(0, float('nan'))
    df['is_high_tip']      = ((df['payment_type']==1) &
                              (df['tip_amount'] > df['fare_amount']*0.2)).astype(int)
    return df


SAMPLE = pd.DataFrame([{
    'pickup_at':'2024-01-15 14:30:00', 'dropoff_at':'2024-01-15 14:52:00',
    'fare_amount':18.5, 'tip_amount':4.5, 'trip_distance':3.2,
    'passenger_count':1, 'payment_type':1, 'ratecodeid':1
}])


def test_pickup_hour():
    df = compute_features(SAMPLE)
    assert df['pickup_hour'].iloc[0] == 14


def test_is_weekend_weekday():
    df = compute_features(SAMPLE)   # Jan 15 2024 = Monday
    assert df['is_weekend'].iloc[0] == 0


def test_trip_duration():
    df = compute_features(SAMPLE)
    assert df['trip_duration_min'].iloc[0] == pytest.approx(22.0)


def test_fare_per_mile():
    df = compute_features(SAMPLE)
    assert df['fare_per_mile'].iloc[0] == pytest.approx(18.5/3.2)


def test_high_tip_true():
    # 4.5/18.5 = 24.3% > 20%
    df = compute_features(SAMPLE)
    assert df['is_high_tip'].iloc[0] == 1


def test_cash_trip_not_high_tip():
    cash = SAMPLE.copy()
    cash['payment_type'] = 2
    df = compute_features(cash)
    assert df['is_high_tip'].iloc[0] == 0


def test_no_nulls_in_features():
    df = compute_features(SAMPLE)
    cols = ['pickup_hour','is_weekend','trip_duration_min','fare_per_mile']
    assert df[cols].isnull().sum().sum() == 0