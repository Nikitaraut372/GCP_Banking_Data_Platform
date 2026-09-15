import json
import logging
from datetime import datetime, timezone

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms.window import FixedWindows
from apache_beam.io.gcp.pubsub import ReadFromPubSub
from apache_beam.io.gcp.bigquery import WriteToBigQuery

# =====================================================
# CONFIGURATION
# =====================================================

PROJECT_ID = "dev-banking-2026-499415"
REGION = "us-central1"

INPUT_SUBSCRIPTION = (
    "projects/dev-banking-2026-499415/subscriptions/"
    "banking-transactions-sub"
)

BQ_TABLE = (
    "dev-banking-2026-499415:"
    "banking_bronze.bronze_streaming_transactions"
)

BQ_TEMP = "gs://banking-temp-dev-bkt/tempp/"

DEADLETTER_TOPIC = (
    "projects/dev-banking-2026-499415/topics/"
    "banking-transactions-deadletter"
)

TEMP_LOCATION = "gs://banking-temp-dev-bkt/tempp/"
STAGING_LOCATION = "gs://banking-temp-dev-bkt/stagingp/"


# =====================================================
# PIPELINE OPTIONS
# =====================================================

pipeline_options = PipelineOptions(
    # runner="DataflowRunner",
    # project=PROJECT_ID,
    # region=REGION,
    # temp_location=TEMP_LOCATION,
    # staging_location=STAGING_LOCATION,
    streaming=True,
    # save_main_session=True
)


# =====================================================
# PARSE & VALIDATE
# =====================================================

class ParseAndValidateEvent(beam.DoFn):

    def process(self, message):

        try:

            event = json.loads(message.decode("utf-8"))

            required_fields = [
                "event_id",
                "transaction_id",
                "account_id",
                "customer_id",
                "amount",
                "currency",
                "transaction_type",
                "channel",
                "merchant",
                "event_ts"
            ]

            for field in required_fields:
                if field not in event:
                    raise ValueError(f"Missing field: {field}")

            parsed_ts = datetime.fromisoformat(event["event_ts"].replace("Z", "+00:00"))

            event["event_ts"] = parsed_ts.isoformat()
            event["_event_time"] = parsed_ts.timestamp()
            event["ingest_ts"] = (datetime.now(timezone.utc).isoformat())

            yield event

        except Exception as e:
            logging.error(f"Invalid message: {str(e)} | "f"Message: {message}")
            yield beam.pvalue.TaggedOutput("deadletter", message.decode("utf-8"))


# =====================================================
# FORMAT FOR BIGQUERY
# =====================================================

class FormatForBigQuery(beam.DoFn):

    def process(self, event):

        yield {
            "event_id": event["event_id"],
            "transaction_id": event["transaction_id"],
            "account_id": event["account_id"],
            "customer_id": event["customer_id"],
            "amount": float(event["amount"]),
            "currency": event["currency"],
            "transaction_type": event["transaction_type"],
            "channel": event["channel"],
            "merchant": event["merchant"],
            "event_ts": event["event_ts"],
            "ingest_ts": event["ingest_ts"]
        }


# =====================================================
# DEDUP FUNCTION
# =====================================================

class KeepFirstRecord(beam.CombineFn):

    def create_accumulator(self):
        return None

    def add_input(self, accumulator, element):
        return accumulator if accumulator is not None else element

    def merge_accumulators(self, accumulators):
        for acc in accumulators:
            if acc is not None:
                return acc
        return None

    def extract_output(self, accumulator):
        return accumulator


# =====================================================
# PIPELINE
# =====================================================

def run():

    with beam.Pipeline(options=pipeline_options) as p:

        # -----------------------------------------
        # READ PUBSUB
        # -----------------------------------------

        events = (
            p
            | "ReadFromPubSub">> ReadFromPubSub(subscription=INPUT_SUBSCRIPTION)
            | "ParseAndValidate">> beam.ParDo(ParseAndValidateEvent()).with_outputs("deadletter",main="valid")
        )

        valid_events = events.valid
        deadletter_events = events.deadletter

        # -----------------------------------------
        # DEAD LETTER
        # -----------------------------------------

        (deadletter_events| "LogDeadLetter">> beam.Map(lambda x: logging.error(f"Deadletter: {x}")))

        # Uncomment if DLQ topic exists

        # deadletter_events | "WriteDeadLetter" >> beam.io.WriteToPubSub(DEADLETTER_TOPIC)

        # -----------------------------------------
        # EVENT TIME + WINDOWING
        # -----------------------------------------

        windowed_events = (
            valid_events
            | "AssignEventTime" >> beam.Map(lambda e: beam.window.TimestampedValue(e, e["_event_time"]))
            | "WindowInto" >> beam.WindowInto(FixedWindows(60), allowed_lateness=300)
        )

        # -----------------------------------------
        # DEDUPLICATE
        # -----------------------------------------
        deduped_events = (
            windowed_events
            | "KeyByEventId" >> beam.Map(lambda e: (e["event_id"], e))
            | "DeduplicateByEventId" >> beam.CombinePerKey(KeepFirstRecord())
            | "DropKey" >> beam.Values()
        )

        # -----------------------------------------
        # WRITE TO BIGQUERY
        # -----------------------------------------

        (
            deduped_events
            | "FormatForBQ" >> beam.ParDo(FormatForBigQuery())
            # | beam.Map(print)
            | "WriteToBigQuery" >> WriteToBigQuery(
                table=BQ_TABLE,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER,
                custom_gcs_temp_location=BQ_TEMP
            )
        )


# =====================================================
# ENTRY POINT
# =====================================================

if __name__ == "__main__":
    run()