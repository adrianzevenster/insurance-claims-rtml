import json
import os
import signal
from typing import Any, Dict, Optional

import clickhouse_connect
from confluent_kafka import Consumer
from dotenv import load_dotenv

load_dotenv()

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9093")
DECISIONS_TOPIC = os.getenv("DECISIONS_TOPIC", "claims_decisions")
OUTCOMES_TOPIC = os.getenv("OUTCOMES_TOPIC", "claims_outcomes")

CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "localhost")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", "8123"))
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "adrian")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "adrian123")
CLICKHOUSE_DB = os.getenv("CLICKHOUSE_DB", "default")

GROUP_ID = os.getenv("EVENTS_WRITER_GROUP", "events-writer")

running = True


def handle_sig(*_):
    global running
    running = False


signal.signal(signal.SIGINT, handle_sig)
signal.signal(signal.SIGTERM, handle_sig)


def parse_json(value: Optional[bytes]) -> Optional[Dict[str, Any]]:
    if not value:
        return None
    try:
        return json.loads(value.decode("utf-8"))
    except Exception:
        return None


def main():
    ch = clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        username=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
        database=CLICKHOUSE_DB,
    )

    consumer = Consumer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP,
            "group.id": GROUP_ID,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": True,
        }
    )
    consumer.subscribe([DECISIONS_TOPIC, OUTCOMES_TOPIC])

    print(
        f"[events-writer] consuming {DECISIONS_TOPIC}, {OUTCOMES_TOPIC} from {KAFKA_BOOTSTRAP} "
        f"-> ClickHouse {CLICKHOUSE_HOST}:{CLICKHOUSE_PORT} user={CLICKHOUSE_USER}"
    )

    decision_cols = [
        "claim_id",
        "member_id",
        "provider_id",
        "policy_id",
        "decision",
        "risk_score",
        "model_version",
        "features_version",
    ]
    outcome_cols = ["claim_id", "outcome", "payout_amount"]

    while running:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            continue

        topic = msg.topic()
        payload = parse_json(msg.value())
        if not payload:
            continue

        try:
            if topic == DECISIONS_TOPIC:
                row = (
                    str(payload.get("claim_id", "")),
                    str(payload.get("member_id", "")),
                    str(payload.get("provider_id", "")),
                    str(payload.get("policy_id", "")),
                    str(payload.get("decision", "")),
                    float(payload.get("risk_score", 0.0) or 0.0),
                    str(payload.get("model_version", "")),
                    str(payload.get("features_version", "")),
                )
                ch.insert("decision_events", [row], column_names=decision_cols)

            elif topic == OUTCOMES_TOPIC:
                row = (
                    str(payload.get("claim_id", "")),
                    str(payload.get("outcome", "")),
                    float(payload.get("payout_amount", 0.0) or 0.0),
                )
                ch.insert("outcome_events", [row], column_names=outcome_cols)

        except Exception as e:
            print(f"[events-writer] insert failed: {type(e).__name__}({e}) topic={topic} payload={payload}")

    consumer.close()
    print("[events-writer] stopped")


if __name__ == "__main__":
    main()