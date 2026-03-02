from datetime import timedelta

from feast import FeatureView, Field, FileSource
from feast.types import Float32, Int64, String

from entities import member, provider, policy

gold_source = FileSource(
    path="../data/gold_features",
    timestamp_field="event_ts",
)

member_features = FeatureView(
    name="member_features",
    entities=[member],
    ttl=timedelta(hours=1),
    schema=[
        Field(name="member_claim_count_30m", dtype=Int64),
        Field(name="member_claim_amount_sum_30m", dtype=Float32),
        Field(name="member_claim_amount_avg_30m", dtype=Float32),
    ],
    online=True,
    source=gold_source,
)

provider_features = FeatureView(
    name="provider_features",
    entities=[provider],
    ttl=timedelta(hours=1),
    schema=[
        Field(name="provider_claim_count_30m", dtype=Int64),
        Field(name="provider_claim_amount_avg_30m", dtype=Float32),
    ],
    online=True,
    source=gold_source,
)

policy_features = FeatureView(
    name="policy_features",
    entities=[policy],
    ttl=timedelta(hours=1),
    schema=[
        Field(name="policy_dummy", dtype=Int64),
    ],
    online=True,
    source=gold_source,
)