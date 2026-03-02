import os
import time
from typing import Optional, Dict, Any

import mlflow
import pandas as pd
from dotenv import load_dotenv
from fastapi import FastAPI
from pydantic import BaseModel, Field
from feast import FeatureStore
import json
import math
import numpy as np
from confluent_kafka import Producer
import clickhouse_connect
import os
from feast import FeatureStore

from serving.decision_policy import DecisionPolicy

load_dotenv()

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9093")
DECISIONS_TOPIC = os.getenv("DECISIONS_TOPIC", "claims_decisions")
kafka_producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})

MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5000")
MODEL_URI = os.getenv("MODEL_URI", "models:/claims-approval-model/Production")

FEAST_REPO = os.getenv("FEAST_REPO", "/app/feature_store")

STORE = FeatureStore(repo_path=FEAST_REPO)

CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "localhost")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", "8123"))

policy = DecisionPolicy()

app = FastAPI(title="Claims Approval API", version="1.0")

class ClaimRequest(BaseModel):
    claim_id: str
    member_id: str
    provider_id: str
    policy_id: str
    claim_amount: float
    channel: str = Field(default="api")
    attachments_count: int = Field(default=0)

class ClaimResponse(BaseModel):
    claim_id: str
    decision: str
    risk_score: float
    model_uri: str
    latency_ms: float

def load_model():
    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    return mlflow.pyfunc.load_model(MODEL_URI)

try:
    MODEL = load_model()
    print(f"[serving] loaded model from {MODEL_URI}")
except Exception as e:
    print(f"[serving] WARNING: failed to load mlflow model ({e}); using fallback model")
    MODEL = None

def fallback_predict_proba(row: Dict[str, Any]) -> float:
    amt = float(row.get("log_claim_amount", 0.0))
    mcnt = float(row.get("member_claim_count_30m", 0.0))
    pcnt = float(row.get("provider_claim_count_30m", 0.0))
    no_attach = float(row.get("no_attachments", 0.0))
    z = 0.6 * amt + 0.15 * mcnt + 0.08 * pcnt + 0.3 * no_attach - 1.2
    return float(1.0 / (1.0 + math.exp(-z)))
STORE = FeatureStore(repo_path=FEAST_REPO)

CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "adrian")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "adrian123")
CLICKHOUSE_DB = os.getenv("CLICKHOUSE_DB", "default")

ch = clickhouse_connect.get_client(
    host=CLICKHOUSE_HOST,
    port=CLICKHOUSE_PORT,
    username=CLICKHOUSE_USER,
    password=CLICKHOUSE_PASSWORD,
    database=CLICKHOUSE_DB,
)

def fetch_online_features(member_id: str, provider_id: str, policy_id: str) -> Dict[str, Any]:
    feature_refs = [
        "member_features:member_claim_count_30m",
        "member_features:member_claim_amount_sum_30m",
        "member_features:member_claim_amount_avg_30m",
        "provider_features:provider_claim_count_30m",
        "provider_features:provider_claim_amount_avg_30m",
    ]
    entity_rows = [{
        "member_id": member_id,
        "provider_id": provider_id,
        "policy_id": policy_id,
    }]
    fs = STORE.get_online_features(features=feature_refs, entity_rows=entity_rows).to_dict()
    # Feast returns dict of lists
    return {k: (v[0] if isinstance(v, list) else v) for k, v in fs.items()}

@app.get("/v1/health")
def health():
    return {"ok": True, "model_uri": MODEL_URI}

@app.post("/v1/claims:score", response_model=ClaimResponse)
def score(req: ClaimRequest):
    t0 = time.time()

    online = fetch_online_features(req.member_id, req.provider_id, req.policy_id)

    is_api = 1 if req.channel == "api" else 0
    no_attachments = 1 if req.attachments_count == 0 else 0

    row = {
        "log_claim_amount": float(math.log1p(req.claim_amount)),
        "is_api": is_api,
        "no_attachments": no_attachments,
        "member_claim_count_30m": float(online.get("member_claim_count_30m", 0) or 0),
        "member_claim_amount_sum_30m": float(online.get("member_claim_amount_sum_30m", 0) or 0),
        "member_claim_amount_avg_30m": float(online.get("member_claim_amount_avg_30m", 0) or 0),
        "provider_claim_count_30m": float(online.get("provider_claim_count_30m", 0) or 0),
        "provider_claim_amount_avg_30m": float(online.get("provider_claim_amount_avg_30m", 0) or 0),
    }

    X = pd.DataFrame([row])
    if MODEL is None:
        proba = fallback_predict_proba(row)
    else:
        raw = MODEL.predict(X)
        proba = float(raw[0] if hasattr(raw, "__len__") else raw)
    decision = policy.decide(proba)

    latency_ms = (time.time() - t0) * 1000.0

    try:
        ch.insert(
            "inference_logs",
            [[
                req.claim_id,
                req.member_id,
                req.provider_id,
                req.policy_id,
                float(proba),
                decision,
                MODEL_URI,
                float(latency_ms),
            ]],
            column_names=["claim_id","member_id","provider_id","policy_id","risk_score","decision","model_uri","latency_ms"],
        )
    except Exception as e:
        print(f"[serving] clickhouse insert failed: {type(e).__name__}: {e}")

    event = {
        "claim_id": req.claim_id,
        "member_id": req.member_id,
        "provider_id": req.provider_id,
        "policy_id": req.policy_id,
        "decision": decision,
        "risk_score": float(proba),
        "model_version": MODEL_URI,
        "features_version": os.getenv("FEATURES_VERSION", "dev"),
    }
    kafka_producer.produce(
        DECISIONS_TOPIC,
        key=req.claim_id,
        value=json.dumps(event).encode("utf-8"),
    )
    kafka_producer.poll(0)

    return ClaimResponse(
        claim_id=req.claim_id,
        decision=decision,
        risk_score=proba,
        model_uri=MODEL_URI,
        latency_ms=latency_ms,
    )