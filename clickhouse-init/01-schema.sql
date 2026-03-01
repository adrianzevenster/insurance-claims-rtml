-- Core tables
CREATE TABLE IF NOT EXISTS default.decision_events
(
    ts DateTime64(3) DEFAULT now64(3),
    claim_id String,
    member_id String,
    provider_id String,
    policy_id String,
    decision LowCardinality(String),
    risk_score Float64,
    model_version String,
    features_version String
    )
    ENGINE = MergeTree
    ORDER BY (claim_id, ts);

CREATE TABLE IF NOT EXISTS default.outcome_events
(
    ts DateTime64(3) DEFAULT now64(3),
    claim_id String,
    outcome LowCardinality(String),
    payout_amount Float64
    )
    ENGINE = MergeTree
    ORDER BY (claim_id, ts);

CREATE TABLE IF NOT EXISTS default.inference_logs
(
    ts DateTime64(3) DEFAULT now64(3),
    claim_id String,
    member_id String,
    provider_id String,
    policy_id String,
    risk_score Float64,
    decision LowCardinality(String),
    model_uri String,
    latency_ms Float64
    )
    ENGINE = MergeTree
    ORDER BY (claim_id, ts);

-- Kafka engine tables
DROP TABLE IF EXISTS default.decision_events_kafka;
CREATE TABLE default.decision_events_kafka
(
    claim_id String,
    member_id String,
    provider_id String,
    policy_id String,
    decision String,
    risk_score Float64,
    model_version String,
    features_version String,
    ts String
)
    ENGINE = Kafka
SETTINGS
  kafka_broker_list = 'kafka:9092',
  kafka_topic_list = 'claims_decisions',
  kafka_group_name = 'ch_decision_events',
  kafka_format = 'JSONEachRow',
  kafka_num_consumers = 1,
  kafka_skip_broken_messages = 1000;

DROP TABLE IF EXISTS default.outcome_events_kafka;
CREATE TABLE default.outcome_events_kafka
(
    claim_id String,
    outcome String,
    payout_amount Float64
)
    ENGINE = Kafka
SETTINGS
  kafka_broker_list = 'kafka:9092',
  kafka_topic_list = 'claims_outcomes',
  kafka_group_name = 'ch_outcome_events',
  kafka_format = 'JSONEachRow',
  kafka_num_consumers = 1,
  kafka_skip_broken_messages = 1000;

-- Materialized views into MergeTree
DROP VIEW IF EXISTS default.mv_decision_events;
CREATE MATERIALIZED VIEW default.mv_decision_events
TO default.decision_events
AS
SELECT
    parseDateTime64BestEffortOrNull(ts) AS ts,
    claim_id,
    member_id,
    provider_id,
    policy_id,
    decision,
    risk_score,
    model_version,
    features_version
FROM default.decision_events_kafka;

DROP VIEW IF EXISTS default.mv_outcome_events;
CREATE MATERIALIZED VIEW default.mv_outcome_events
TO default.outcome_events
AS
SELECT
    now64(3) AS ts,
    claim_id,
    outcome,
    payout_amount
FROM default.outcome_events_kafka;