from dataclasses import dataclass
from typing import List, Dict, Any

@dataclass
class AWSService:
    name: str
    service_type: str
    region: str

@dataclass
class Database:
    name: str
    type: str
    operation_times: Dict[str, List[int]]
    error_rate: float

@dataclass
class Config:
    dynatrace_url: str
    api_token: str
    duration_minutes: int = 15
    trace_interval: int = 5
    metric_interval: int = 30
    log_interval: int = 10