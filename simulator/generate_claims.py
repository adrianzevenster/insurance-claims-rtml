import json
import os
import random
import time
from datetime import datetime, timezone
from uuid import uuid4

from confluent_kafka import Producer
from dotenv import load_dotenv

load_dotenv()

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = os.getenv("CLAIMS_TOPIC", "claims_raw")

PROVIDERS = [f"prov_{i:03d}" for i in range(1, 51)]
MEMBERS = [f"mem_{i:05d}" for i in range(1, 2001)]
POLICIES = [f"pol_{i:05d}" for i in range(1, 2001)]

DIAG = ["D10", "D11", "D50", "D70", "D80"]
PROC = ["P100", "P200", "P250", "P300", "P900"]

def make_claim():
    claim_id = str(uuid4())
    member_id = random.choice(MEMBERS)
    policy_id = POLICIES[int(member_id.split("_")[1]) - 1]
    provider_id = random.choice(PROVIDERS)

    claim_amount = max(50.0, random.lognormvariate(6.0, 0.7))
    channel = random.choice(["web", "agent", "api"])
    attachments = random.choice([0, 0, 1, 1, 2, 3])
    diagnosis_code = random.choice(DIAG)
    procedure_code = random.choice(PROC)

    suspicious = 0
    if claim_amount > 3000: suspicious += 1
    if procedure_code == "P900": suspicious += 1
    if attachments == 0 and channel == "api": suspicious += 1

    event = {
        "claim_id": claim_id,
        "member_id": member_id,
        "policy_id": policy_id,
        "provider_id": provider_id,
        "claim_amount": round(claim_amount, 2),
        "currency": "ZAR",
        "diagnosis_code": diagnosis_code,
        "procedure_code": procedure_code,
        "channel": channel,
        "attachments_count": attachments,
        "submit_ts": datetime.now(timezone.utc).isoformat(),
        "label_should_decline": 1 if suspicious >= 2 else 0
    }
    return event

def delivery(err, msg):
    if err:
        print("Delivery failed:", err)

def main():
    p = Producer({"bootstrap.servers": BOOTSTRAP})
    print(f"Producing to {TOPIC} on {BOOTSTRAP}")
    while True:
        event = make_claim()
        p.produce(TOPIC, key=event["claim_id"], value=json.dumps(event).encode("utf-8"), callback=delivery)
        p.poll(0)
        time.sleep(0.05)

if __name__ == "__main__":
    main()