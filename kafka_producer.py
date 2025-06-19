import pandas as pd
from kafka import KafkaProducer
import json
import time
from datetime import datetime

def load_filtered_data(path):
    df = pd.read_csv(path)
    df['ts_event'] = pd.to_datetime(df['ts_event'])

    # Filter between 13:36:32 and 13:45:14 UTC
    start = pd.to_datetime("13:36:32").time()
    end = pd.to_datetime("13:45:14").time()
    df = df[df['ts_event'].dt.time.between(start, end)]

    return df.sort_values('ts_event')

def produce_snapshots(df):
    producer = KafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    prev_ts = None
    for _, row in df.iterrows():
        event_time = row['ts_event']
        if prev_ts:
            delta = (event_time - prev_ts).total_seconds()
            time.sleep(min(delta, 1))  # cap delay to 1s max for faster test runs
        prev_ts = event_time

        snapshot = {
            "timestamp": event_time.strftime('%H:%M:%S.%f')[:-3],
            "venue": row['publisher_id'],
            "ask_px": row['ask_px_00'],
            "ask_sz": row['ask_sz_00']
        }

        producer.send("mock_l1_stream", value=snapshot)
        print(f"Sent: {snapshot}")

    producer.flush()

if __name__ == "__main__":
    df = load_filtered_data("l1_day.csv")
    produce_snapshots(df)
