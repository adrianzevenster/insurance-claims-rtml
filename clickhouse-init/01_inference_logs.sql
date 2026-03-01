CREATE TABLE IF NOT EXISTS default.inference_logs
(
    event_time  DateTime DEFAULT now(),
    claim_id    String,
    member_id   String,
    provider_id String,
    policy_id   String,
    risk_score  Float64,
    decision    String,
    model_uri   String,
    latency_ms  Float64
    )
    ENGINE = MergeTree
    ORDER BY (event_time, claim_id);