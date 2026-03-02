from dataclasses import dataclass

@dataclass(frozen=True)
class DecisionPolicy:
    decline_threshold: float = 0.99
    review_threshold: float = 0.90

    def decide(self, risk_score: float) -> str:
        if risk_score >= self.decline_threshold:
            return "DECLINE"
        if risk_score >= self.review_threshold:
            return "REVIEW"
        return "APPROVE"