import argparse
import asyncio
import logging
import time
from dataclasses import dataclass, asdict

import numpy as np
from tabulate import tabulate

import featureform as ff

logging.basicConfig(
    filename="benchmarking_error.log",
    filemode="w",
    level=logging.ERROR,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

client = ff.Client(host="benchmark.featureform.com")


def get_features(features):
    try:
        start_time = time.perf_counter()  # Start timing

        result = client.features(
            features, {"entity": "9119"}
        )  # The operation you want to measure

        end_time = time.perf_counter()  # End timing
        execution_time = (
            end_time - start_time
        ) * 1000  # Execution time in milliseconds

        return result, execution_time  # Return the result and the execution time
    except Exception as e:
        logging.error(e, exc_info=True)
        raise e


async def get_features_async(features):
    try:
        # Wrap the synchronous get_features call in asyncio.to_thread
        _, execution_time = await asyncio.to_thread(get_features, features)

        return execution_time  # Return only the execution time
    except Exception as e:
        logging.error("Error in get_features_async", exc_info=True)
        raise e


def build_features(feature_count, use_ondemand=False):
    features = []
    for i in range(feature_count):
        if use_ondemand:
            features.append((f"ondemand_{i}", "v10"))
        else:
            features.append((f"feature_{i}", "v10"))
    return features


async def schedule_calls(feature_count, rps, duration=60, use_ondemand=False):
    tasks = []
    interval = 1 / rps  # Interval between each call to maintain the desired RPS
    total_requests = int(rps * duration)  # Total number of requests to make

    print(f"Making {total_requests} requests at {rps} RPS for {feature_count} features")

    for i in range(total_requests):
        features = build_features(feature_count, use_ondemand)
        task = asyncio.create_task(get_features_async(features))
        tasks.append(task)
        await asyncio.sleep(interval)

    return await asyncio.gather(*tasks, return_exceptions=True)


async def gather_stats(feature_count, rps, duration, use_ondemand=False):
    latencies = await schedule_calls(feature_count, rps, duration, use_ondemand)

    # Process the latencies to compute stats
    valid_latencies = [
        latency for latency in latencies if not isinstance(latency, Exception)
    ]
    errors = len(latencies) - len(valid_latencies)

    return Stats.build(valid_latencies, errors)


@dataclass
class Stats:
    avg_latency: float
    min_latency: float
    max_latency: float
    p50: float
    p90: float
    p95: float
    p99: float
    errors: int

    @staticmethod
    def build(latencies, errors):
        avg_latency = sum(latencies) / len(latencies) if latencies else 0
        min_latency = min(latencies) if latencies else 0
        max_latency = max(latencies) if latencies else 0

        p50 = np.percentile(latencies, 50) if latencies else 0
        p90 = np.percentile(latencies, 90) if latencies else 0
        p95 = np.percentile(latencies, 95) if latencies else 0
        p99 = np.percentile(latencies, 99) if latencies else 0

        return Stats(
            avg_latency=avg_latency,
            min_latency=min_latency,
            max_latency=max_latency,
            p50=p50,
            p90=p90,
            p95=p95,
            p99=p99,
            errors=errors,
        )

    def format(self):
        return {
            "avg_latency": f"{self.avg_latency:.2f}",
            "min_latency": f"{self.min_latency:.2f}",
            "max_latency": f"{self.max_latency:.2f}",
            "p50": f"{self.p50:.2f}",
            "p90": f"{self.p90:.2f}",
            "p95": f"{self.p95:.2f}",
            "p99": f"{self.p99:.2f}",
            "errors": self.errors,
        }


@dataclass
class FeatureServingRun:
    features: int
    rps: int
    stats: Stats


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--duration", type=int, default=60, help="Duration for the benchmark in seconds"
    )
    parser.add_argument(
        "--ondemand", type=bool, default=False, help="Use ondemand features"
    )
    args = parser.parse_args()

    duration = args.duration
    use_ondemand = args.ondemand

    feature_counts = [1]
    rps_values = [10]

    runs = []

    # Print Table Header
    headers = [
        "Features Per Request",
        "RPS",
        "avg_latency (ms)",
        "min_latency (ms)",
        "max_latency (ms)",
        "p50",
        "p90",
        "p95",
        "p99",
        "errors",
    ]

    for rps in rps_values:
        for feature_count in feature_counts:
            stats = await gather_stats(feature_count, rps, duration, use_ondemand)
            run = FeatureServingRun(feature_count, rps, stats)

            # Convert the run to a dictionary and flatten stats
            formatted_stats = run.stats.format()
            run_dict = asdict(run)
            run_dict.pop("stats")
            run_dict.update(formatted_stats)

            # Prepare row in the order of headers
            row = [
                run_dict['features'],
                run_dict['rps'],
                run_dict['avg_latency'],
                run_dict['min_latency'],
                run_dict['max_latency'],
                run_dict['p50'],
                run_dict['p90'],
                run_dict['p95'],
                run_dict['p99'],
                run_dict['errors']
            ]
            runs.append(row)

    # Display table
    print(tabulate(runs, headers=headers, tablefmt="pretty"))


if __name__ == "__main__":
    asyncio.run(main())
