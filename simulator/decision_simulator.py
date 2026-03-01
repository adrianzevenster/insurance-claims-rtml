import json
import os
import random
import time
from datetime import datetime, timezone

from confluent_kafka import Producer
from dotenv import load_dotenv

load_dotenv()

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9093")
DECISIONS_TOPIC = os.getenv("DECISIONS_TOPIC", "claims_decisions")

MEMBERS = [f"mem_{i:05d}" for i in range(1, 2001)]
PROVIDERS = [f"prov_{i:03d}" for i in range(1, 51)]
POLICIES = [f"pol_{i:05d}" for i in range(1, 2001)]

def main():
    p = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})
    print(f"[decision-sim] producing to {DECISIONS_TOPIC} on {KAFKA_BOOTSTRAP}")

    i = 0
    while True:
        i += 1
        claim_id = f"demo-{i}"

        member_id = random.choice(MEMBERS)
        provider_id = random.choice(PROVIDERS)
        policy_id = POLICIES[int(member_id.split("_")[1]) - 1]

        risk = min(0.999, max(0.001, random.betavariate(2, 6)))
        if random.random() < 0.06:
            risk = min(0.999, risk + random.uniform(0.4, 0.8))

        if risk >= 0.75:
            decision = "DECLINE"
        elif risk >= 0.50:
            decision = "REVIEW"
        else:
            decision = "APPROVE"

        event = {
            "claim_id": claim_id,
            "member_id": member_id,
            "provider_id": provider_id,
            "policy_id": policy_id,
            "decision": decision,
            "risk_score": float(risk),
            "model_version": "demo",
            "features_version": "demo",
            "ts": datetime.now(timezone.utc).isoformat(),
        }

        p.produce(DECISIONS_TOPIC, key=claim_id, value=json.dumps(event).encode("utf-8"))
        p.poll(0)
        time.sleep(0.2)

if __name__ == "__main__":
    main()