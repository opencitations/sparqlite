from dataclasses import dataclass


@dataclass
class BenchmarkResult:
    library: str
    operation: str
    query_name: str
    requests_per_sec: float
    total_time: float
    avg_request_time: float
    min_request_time: float
    max_request_time: float
    std_dev: float
    response_size_bytes: int
    success_rate: float
    run_number: int
