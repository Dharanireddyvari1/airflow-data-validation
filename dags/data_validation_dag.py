from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from datetime import datetime
import os
import json
import random


# Runs every minute, no catchup
@dag(
    "data_quality_pipeline",
    schedule="* * * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["data-quality", "validation", "taskflow"],
)
def data_quality_pipeline():

    CORRECT_PROB = 0.7

    def get_bookings_path(context):
        execution_date = context["execution_date"]
        file_date = execution_date.strftime("%Y-%m-%d_%H-%M")
        return f"/tmp/data/bookings/{file_date}/bookings.json"

    # anomalies path (same folder, separate file)
    def get_anomalies_path(context):
        execution_date = context["execution_date"]
        file_date = execution_date.strftime("%Y-%m-%d_%H-%M")
        return f"/tmp/data/bookings/{file_date}/anomalies.json"

    def generate_booking_id(i):
        if random.random() < CORRECT_PROB:
            return i + 1
        return ""

    def generate_listing_id():
        if random.random() < CORRECT_PROB:
            return random.choice([1, 2, 3, 4, 5])
        return ""

    def generate_user_id(correct_prob=0.7):
        return random.randint(1000, 5000) if random.random() < correct_prob else ""

    def generate_booking_time(execution_date):
        if random.random() < CORRECT_PROB:
            return execution_date.strftime('%Y-%m-%d %H:%M:%S')
        return ""

    def generate_status():
        if random.random() < CORRECT_PROB:
            return random.choice(["confirmed", "pending", "cancelled"])
        return random.choice(["unknown", "", "error"])

    # generating bookings data with some anomalies
    @task
    def generate_bookings():
        context = get_current_context()
        booking_path = get_bookings_path(context)

        num_bookings = random.randint(5, 15)
        bookings = []

        for i in range(num_bookings):
            booking = {
                "booking_id": generate_booking_id(i),
                "listing_id": generate_listing_id(),
                "user_id": generate_user_id(),
                "booking_time": generate_booking_time(context["execution_date"]),
                "status": generate_status()
            }
            bookings.append(booking)

        directory = os.path.dirname(booking_path)
        if not os.path.exists(directory):
            os.makedirs(directory)

        with open(booking_path, "w") as f:
            json.dump(bookings, f, indent=4)

        print(f"Written to file: {booking_path}")

        return booking_path

    # validating bookings data and writing anomalies to a separate file
    @task
    def validate_bookings(booking_path: str):
        context = get_current_context()
        anomalies_path = get_anomalies_path(context)

        with open(booking_path, "r") as f:
            bookings = json.load(f)

        valid_statuses = {"confirmed", "pending", "cancelled"}
        required_fields = ["booking_id", "listing_id", "user_id", "booking_time", "status"]

        anomalies = []

        for idx, record in enumerate(bookings):
            violations = []

            # Missing field checks
            for field in required_fields:
                if field not in record or record[field] in ("", None):
                    violations.append(f"missing_{field}")

            # Status validation
            if record.get("status") not in valid_statuses:
                violations.append("invalid_status")

            if violations:
                anomalies.append({
                    "record_index": idx,
                    "violations": violations,
                    "record": record
                })

        directory = os.path.dirname(anomalies_path)
        if not os.path.exists(directory):
            os.makedirs(directory)

        with open(anomalies_path, "w") as f:
            json.dump(anomalies, f, indent=4)

        print(f"Anomalies written to file: {anomalies_path}")
        print(f"Total anomalies: {len(anomalies)}")

        return anomalies_path

    # dependencies 
    gen_task = generate_bookings()
    val_task = validate_bookings(gen_task)

    gen_task >> val_task


# DAG object
dag = data_quality_pipeline()
