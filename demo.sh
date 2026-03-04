#!/bin/bash
set -euo pipefail

# ---------- CONFIG ----------
INFER_API_CONT="insurance-claims-rtml-inference-api-1"
CH_CONT="insurance-claims-rtml-clickhouse-1"
REDIS_CONT="insurance-claims-rtml-redis-1"
KAFKA_CONT="insurance-claims-rtml-kafka-1"

API_URL="${API_URL:-http://localhost:8000}"
MLFLOW_URL="${MLFLOW_URL:-http://localhost:5000}"
SUPERSET_URL="${SUPERSET_URL:-http://localhost:8088}"
CLICKHOUSE_HTTP_URL="${CLICKHOUSE_HTTP_URL:-http://localhost:8123}"

CH_USER="${CH_USER:-adrian}"
CH_PASS="${CH_PASS:-adrian123}"
CH_DB="${CH_DB:-default}"

MEMBER_ID="${MEMBER_ID:-m1}"
PROVIDER_ID="${PROVIDER_ID:-p1}"
POLICY_ID="${POLICY_ID:-pol1}"
CLAIM_AMOUNT="${CLAIM_AMOUNT:-1234.56}"

ok()   { echo -e "✅ $*"; }
warn() { echo -e "⚠️  $*"; }
die()  { echo -e "❌ $*"; exit 1; }

need() { command -v "$1" >/dev/null 2>&1 || die "Missing required command: $1"; }

need docker
need curl

echo
echo "============================================================"
echo " INSURANCE CLAIMS RTML — FULL DEMO VALIDATION"
echo "============================================================"
echo

# ----------------------------------------------------------
# 1️⃣ Containers
# ----------------------------------------------------------
echo "1) Containers running"
docker ps --format "table {{.Names}}\t{{.Status}}" | grep insurance-claims-rtml || die "Containers missing"
ok "All containers detected"

# ----------------------------------------------------------
# 2️⃣ Feature Store (Feast) — Repo + Registry
# ----------------------------------------------------------
echo
echo "2) Feature Store — Repo + Feature Views"

docker exec -it "$INFER_API_CONT" sh -lc '
cd /app/feature_store
echo "Entities:"
feast entities list
echo
echo "Feature Views:"
feast feature-views list
' || die "Feast repo invalid or not applied"

ok "Feast repo + registry OK"

# ----------------------------------------------------------
# 3️⃣ Feature Store — Online Store (Redis)
# ----------------------------------------------------------
echo
echo "3) Feature Store — Online Store"

redis_keys=$(docker exec "$REDIS_CONT" redis-cli DBSIZE | tr -d '\r')
echo "Redis key count: $redis_keys"

if [[ "$redis_keys" -gt 0 ]]; then
  ok "Online store contains materialized features"
else
  warn "Redis empty — materialization may not have run"
fi

# ----------------------------------------------------------
# 4️⃣ Online Feature Retrieval Test
# ----------------------------------------------------------
echo
echo "4) Online feature retrieval test"

docker exec -it "$INFER_API_CONT" sh -lc "
python - <<PY
import os
from feast import FeatureStore
fs = FeatureStore(repo_path=os.getenv('FEAST_REPO','/app/feature_store'))
res = fs.get_online_features(
  features=[
    'member_features:member_claim_count_30m',
    'provider_features:provider_claim_count_30m',
    'policy_features:policy_dummy'
  ],
  entity_rows=[{
    'member_id':'$MEMBER_ID',
    'provider_id':'$PROVIDER_ID',
    'policy_id':'$POLICY_ID'
  }]
).to_dict()
print(res)
if all(v[0] is None for k,v in res.items() if k not in ('member_id','provider_id','policy_id')):
  raise SystemExit('Features returned None')
PY
" || die "Online feature retrieval failed"

ok "Online feature retrieval successful"

# ----------------------------------------------------------
# 5️⃣ API Health + Scoring
# ----------------------------------------------------------
echo
echo "5) API health + scoring"

health=$(curl -s "$API_URL/v1/health")
echo "Health: $health"
echo "$health" | grep -q '"ok"' || die "API unhealthy"

REQ_ID="demo-$(date +%s)"

response=$(curl -s -X POST "$API_URL/v1/claims:score" \
  -H "Content-Type: application/json" \
  -d "{\"claim_id\":\"$REQ_ID\",\"member_id\":\"$MEMBER_ID\",\"provider_id\":\"$PROVIDER_ID\",\"policy_id\":\"$POLICY_ID\",\"claim_amount\":$CLAIM_AMOUNT,\"channel\":\"api\",\"attachments_count\":0}")

echo "$response"

echo "$response" | grep -q '"risk_score"' || die "Scoring failed"
ok "API scoring works"

# ----------------------------------------------------------
# 6️⃣ ClickHouse Logging
# ----------------------------------------------------------
echo
echo "6) ClickHouse logging"

before=$(docker exec "$CH_CONT" clickhouse-client -q "SELECT count() FROM $CH_DB.inference_logs" | awk '{print $1}')
sleep 1
curl -s -X POST "$API_URL/v1/claims:score" \
  -H "Content-Type: application/json" \
  -d "{\"claim_id\":\"$REQ_ID-2\",\"member_id\":\"$MEMBER_ID\",\"provider_id\":\"$PROVIDER_ID\",\"policy_id\":\"$POLICY_ID\",\"claim_amount\":$CLAIM_AMOUNT,\"channel\":\"api\",\"attachments_count\":0}" >/dev/null
after=$(docker exec "$CH_CONT" clickhouse-client -q "SELECT count() FROM $CH_DB.inference_logs" | awk '{print $1}')

echo "Before: $before  After: $after"

if [[ "$after" -gt "$before" ]]; then
  ok "Inference logs written to ClickHouse"
else
  die "ClickHouse logging failed"
fi

# ----------------------------------------------------------
# 7️⃣ ClickHouse HTTP UI
# ----------------------------------------------------------
echo
echo "7) ClickHouse UI Links"
echo "Ping:"
echo "  $CLICKHOUSE_HTTP_URL/ping"
echo
echo "Query count:"
echo "  $CLICKHOUSE_HTTP_URL/?user=$CH_USER&password=$CH_PASS&database=$CH_DB&query=SELECT%20count()%20FROM%20$CH_DB.inference_logs"
echo
echo "Latest rows:"
echo "  $CLICKHOUSE_HTTP_URL/?user=$CH_USER&password=$CH_PASS&database=$CH_DB&query=SELECT%20event_time,claim_id,decision,risk_score%20FROM%20$CH_DB.inference_logs%20ORDER%20BY%20event_time%20DESC%20LIMIT%2020"

# ----------------------------------------------------------
# 8️⃣ Superset UI
# ----------------------------------------------------------
echo
echo "8) Superset UI"
echo "Login:"
echo "  $SUPERSET_URL"
echo "  user: admin"
echo "  pass: admin123"
echo
echo "Recommended SQL in Superset:"
echo "  SELECT event_time, decision, risk_score FROM default.inference_logs ORDER BY event_time DESC LIMIT 100;"

# ----------------------------------------------------------
# 9️⃣ Kafka Topics
# ----------------------------------------------------------
echo
echo "9) Kafka topics"

docker exec "$KAFKA_CONT" sh -lc \
"/opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --list"

ok "Kafka reachable"

echo
echo "============================================================"
ok "FULL PLATFORM DEMO PASSED"
echo "============================================================"
echo