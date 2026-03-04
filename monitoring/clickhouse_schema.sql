CREATE TABLE IF NOT EXISTS decision_events
(
    claim_id String,
    member_id String,
    provider_id String,
    policy_id String,
    decision LowCardinality(String),
    risk_score Float64,
    model_version String,
    features_version String,
    ts DateTime DEFAULT now()
    )
    ENGINE = MergeTree
    ORDER BY (ts, claim_id);

CREATE TABLE IF NOT EXISTS outcome_events
(
    claim_id String,
    outcome LowCardinality(String),   -- e.g. PAID, DENIED, FRAUD_CONFIRMED, CHARGEBACK
    payout_amount Float64,
    ts DateTime DEFAULT now()
    )
    ENGINE = MergeTree
    ORDER BY (ts, claim_id);