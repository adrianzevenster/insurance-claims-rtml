from feast import Entity, ValueType

member = Entity(name="member_id", value_type=ValueType.STRING, description="Member")
provider = Entity(name="provider_id", value_type=ValueType.STRING, description="Provider")
policy = Entity(name="policy_id", value_type=ValueType.STRING, description="Policy")