#!/bin/sh
set -eu

i=1
echo "Generating inference traffic..."
while true; do
  echo "sending $i"
  code=$(curl -s -o /dev/null -w "%{http_code}" \
    -X POST http://inference-api:8000/v1/claims:score \
    -H "Content-Type: application/json" \
    -d "{\"claim_id\":\"api-$i\",\"member_id\":\"m1\",\"provider_id\":\"p1\",\"policy_id\":\"pol1\",\"claim_amount\":1234.56,\"channel\":\"api\",\"attachments_count\":0}")
  echo "req=$i http=$code"
  i=$((i+1))
  sleep 0.2
done
