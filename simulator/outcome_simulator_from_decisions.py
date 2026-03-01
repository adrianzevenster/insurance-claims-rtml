import json
import os
import random
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, Any, Optional

from confluent_kafka import Consumer, Producer
from dotenv import load_dotenv

load_dotenv()

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9093")
DECISIONS_TOPIC = os.getenv("DECISIONS_TOPIC", "claims_decisions")
OUTCOMES_TOPIC = os.getenv("OUTCOMES_TOPIC", "claims_outcomes")

GROUP_ID = os.getenv("OUTCOME_SIM_GROUP", "outcome-sim-from-decisions")

MIN_DELAY_SEC = float(os.getenv("OUTCOME_MIN_DELAY_SEC", "3"))
MAX_DELAY_SEC = float(os.getenv("OUTCOME_MAX_DELAY_SEC", "20"))

def decide_outcome(decision: str, risk_score: float) -> Dict[str, Any]:
    """
    Simple demo mapping:
    - APPROVE -> mostly PAID, sometimes CHARGEBACK
    - REVIEW -> mix of PAID/DENIED/FRAUD_CONFIRMED
    - DECLINE -> mostly DENIED/FRAUD_CONFIRMED
    """
    decision = (decision or "").upper()
    r = float(risk_score or 0.0)

    if decision == "APPROVE":
        outcome = random.choices(
            ["PAID", "CHARGEBACK", "FRAUD_CONFIRMED"],
            weights=[0.92, 0.06, 0.02],
            k=1,
        )[0]
    elif decision == "REVIEW":
        outcome = random.choices(
            ["PAID", "DENIED", "FRAUD_CONFIRMED"],
            weights=[0.55, 0.35, 0.10],
            k=1,
        )[0]
    else:  # DECLINE or unknown
        outcome = random.choices(
            ["DENIED", "FRAUD_CONFIRMED", "PAID"],
            weights=[0.75, 0.23, 0.02],
            k=1,
        )[0]

    payout = 0.0
    if outcome == "PAID":
        # correlate payout a bit with "lower risk"
        base = random.uniform(100, 5000)
        payout = round(base * (1.0 - min(0.9, r)), 2)
    return {"outcome": outcome, "payout_amount": payout}


def parse_json(value: Optional[bytes]) -> Optional[Dict[str, Any]]:
    if not value:
        return None
    try:
        return json.loads(value.decode("utf-8"))
    except Exception:
        return None


def main():
    consumer = Consumer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP,
            "group.id": GROUP_ID,
            "auto.offset.reset": "latest",
            "enable.auto.commit": True,
        }
    )
    consumer.subscribe([DECISIONS_TOPIC])

    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})

    pool = ThreadPoolExecutor(max_workers=8)

    print(
        f"[outcome-sim] consuming {DECISIONS_TOPIC} -> producing {OUTCOMES_TOPIC} on {KAFKA_BOOTSTRAP} "
        f"delay={MIN_DELAY_SEC}-{MAX_DELAY_SEC}s"
    )

    def emit_later(event: Dict[str, Any]):
        delay = random.uniform(MIN_DELAY_SEC, MAX_DELAY_SEC)
        time.sleep(delay)

        claim_id = str(event.get("claim_id", ""))
        decision = str(event.get("decision", ""))
        risk_score = float(event.get("risk_score", 0.0) or 0.0)

        mapped = decide_outcome(decision, risk_score)
        out = {
            "claim_id": claim_id,
            "outcome": mapped["outcome"],
            "payout_amount": mapped["payout_amount"],
        }

        producer.produce(
            OUTCOMES_TOPIC,
            key=claim_id,
            value=json.dumps(out).encode("utf-8"),
        )
        producer.poll(0)

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                continue

            payload = parse_json(msg.value())
            if not payload:
                continue

            pool.submit(emit_later, payload)

    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()
        producer.flush(5)
        pool.shutdown(wait=False)
        print("[outcome-sim] stopped")


if __name__ == "__main__":
    main()