import json
import os
import random
import time
from confluent_kafka import Producer
from dotenv import load_dotenv

load_dotenv()

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
OUTCOMES_TOPIC = os.getenv("OUTCOMES_TOPIC", "claims_outcomes")


def main():
    p = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})
    print(f"[outcome-sim] producing to {OUTCOMES_TOPIC} on {KAFKA_BOOTSTRAP}")

    while True:
        claim_id = f"demo-{random.randint(1, 500)}"

        outcome = random.choices(
            population=["PAID", "DENIED", "FRAUD_CONFIRMED", "CHARGEBACK"],
            weights=[0.75, 0.18, 0.05, 0.02],
            k=1,
        )[0]

        payout = 0.0
        if outcome == "PAID":
            payout = round(random.uniform(100, 5000), 2)

        event = {
            "claim_id": claim_id,
            "outcome": outcome,
            "payout_amount": payout,
        }

        p.produce(OUTCOMES_TOPIC, key=claim_id, value=json.dumps(event).encode("utf-8"))
        p.poll(0)

        time.sleep(0.5)

if __name__ == "__main__":
    main()