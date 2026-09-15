import argparse
import json
import logging
import random
import time
import uuid
from datetime import datetime, timezone

from google.cloud import pubsub_v1

# =====================================================
# CONFIG
# =====================================================
PROJECT_ID = "dev-banking-2026-499415" # replace with your GCP project ID
TOPIC_ID = "banking-transactions-topic"

EVENTS_PER_SECOND = 2   # control load (TPS)
RUN_FOREVER = True      # set False for testing

# =====================================================
# PUBSUB CLIENT
# =====================================================
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)

# =====================================================
# SAMPLE MASTER DATA (SIMULATION)
# =====================================================
CHANNELS = ["ATM", "UPI", "CARD", "NETBANKING"]
MERCHANTS = ["AMAZON", "FLIPKART", "SWIGGY", "ZOMATO", "IRCTC"]
STATUSES = ["SUCCESS", "FAILED"]
CURRENCY = "INR"


def generate_transaction_event():
    return {
        "event_id": f"evt_{uuid.uuid4().hex[:8]}",
        "transaction_id": f"txn_{uuid.uuid4().hex[:10]}",
        "account_id": random.randint(10001, 11000),
        "customer_id": random.randint(1, 500),
        "transaction_type": random.choice(["DEBIT", "CREDIT"]),
        "channel": random.choice(CHANNELS),
        "amount": round(random.uniform(10, 50000), 2),
        "currency": CURRENCY,
        "merchant": random.choice(MERCHANTS),
        "event_ts": datetime.now(timezone.utc).isoformat(),
        "status": random.choices(STATUSES, weights=[90, 10])[0],
    }


def parse_args():
    parser = argparse.ArgumentParser(description="Publish sample banking transactions to Pub/Sub.")
    parser.add_argument("--project-id", required=True)
    parser.add_argument("--topic-id", default="banking-transactions-topic")
    parser.add_argument("--events-per-second", type=float, default=2.0)
    parser.add_argument("--max-events", type=int, default=0, help="0 means run forever.")
    return parser.parse_args()


def publish_events(project_id: str, topic_id: str, events_per_second: float, max_events: int):
    if events_per_second <= 0:
        raise ValueError("events_per_second must be greater than zero")

    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project_id, topic_id)
    interval_seconds = 1 / events_per_second
    published = 0

    logging.info("Starting Banking Transaction Producer for %s", topic_path)
    while max_events == 0 or published < max_events:
        event = generate_transaction_event()
        future = publisher.publish(topic_path, json.dumps(event).encode("utf-8"))
        future.result(timeout=30)

        published += 1
        logging.info("Published transaction_id=%s", event["transaction_id"])
        time.sleep(interval_seconds)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = parse_args()
    publish_events(args.project_id, args.topic_id, args.events_per_second, args.max_events)
