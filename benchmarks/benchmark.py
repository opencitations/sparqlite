import csv
from pathlib import Path
import random
import statistics
import time
from typing import Any

import analytics
from clients import CLIENTS
from model import BenchmarkResult
from queries import QUERIES, UPDATES
from setup_virtuoso import get_sparql_endpoint, setup_virtuoso, stop_virtuoso
from test_data import load_test_data

NUM_RUNS = 101
ITERATIONS_PER_QUERY = 100
RESULTS_FILE = "results/benchmark_results.csv"


def calculate_result(
    library: str,
    operation: str,
    query_name: str,
    results: list[tuple[Any, float]],
    run_number: int,
) -> BenchmarkResult:
    times = [r[1] for r in results]
    return BenchmarkResult(
        library=library,
        operation=operation,
        query_name=query_name,
        requests_per_sec=len(results) / sum(times),
        total_time=sum(times),
        avg_request_time=statistics.mean(times),
        min_request_time=min(times),
        max_request_time=max(times),
        std_dev=statistics.stdev(times) if len(times) > 1 else 0.0,
        response_size_bytes=0,
        success_rate=1.0,
        run_number=run_number,
    )


def run_client(client_class, method: str, query: str, iterations: int) -> list[tuple[Any, float]]:
    client = client_class()
    client.setup(get_sparql_endpoint())
    results = []
    for _ in range(iterations):
        start = time.perf_counter()
        response = getattr(client, method)(query)
        elapsed = time.perf_counter() - start
        results.append((response, elapsed))
    client.teardown()
    return results


def get_method_for_operation(operation: str) -> str:
    return {
        "SELECT": "select",
        "ASK": "ask",
        "CONSTRUCT": "construct",
        "DESCRIBE": "describe",
        "INSERT": "update",
        "DELETE": "update",
        "UPDATE": "update",
    }[operation]


def run_benchmark() -> list[BenchmarkResult]:
    all_results = []
    for run in range(NUM_RUNS):
        is_warmup = run == 0
        clients = list(CLIENTS)
        random.shuffle(clients)
        for client_class in clients:
            for query_name, query_info in QUERIES.items():
                operation = query_info["operation"]
                method = get_method_for_operation(operation)
                results = run_client(client_class, method, query_info["sparql"], ITERATIONS_PER_QUERY)
                if not is_warmup:
                    all_results.append(calculate_result(client_class.name, operation, query_name, results, run))
            for query_name, query_info in UPDATES.items():
                operation = query_info["operation"]
                results = run_client(client_class, "update", query_info["sparql"], ITERATIONS_PER_QUERY)
                if not is_warmup:
                    all_results.append(calculate_result(client_class.name, operation, query_name, results, run))
    return all_results


def save_results(results: list[BenchmarkResult]) -> None:
    Path(RESULTS_FILE).parent.mkdir(parents=True, exist_ok=True)
    with open(RESULTS_FILE, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            "library", "operation", "query_name", "requests_per_sec", "total_time",
            "avg_request_time", "min_request_time", "max_request_time", "std_dev",
            "response_size_bytes", "success_rate", "run_number",
        ])
        for result in results:
            writer.writerow([
                result.library, result.operation, result.query_name, result.requests_per_sec,
                result.total_time, result.avg_request_time, result.min_request_time,
                result.max_request_time, result.std_dev, result.response_size_bytes,
                result.success_rate, result.run_number,
            ])


def main() -> None:
    setup_virtuoso()
    load_test_data()
    results = run_benchmark()
    save_results(results)
    analytics.main()
    stop_virtuoso()


if __name__ == "__main__":
    main()