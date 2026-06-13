#!/usr/bin/env python3
"""
Analyze dbt run_results.json performance.

Examples:
  uv run scripts/analyze_run_results.py target/run_results.json
  uv run scripts/analyze_run_results.py target/run_results.json --top 25
"""

from __future__ import annotations

import argparse
import csv
import json
import statistics
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass
class NodeResult:
    unique_id: str
    resource_type: str
    package_name: str
    node_name: str
    status: str
    execution_time: float
    compile_time: float
    execute_time: float
    other_timing_time: float
    rows_affected: int | None
    rows_per_second: float | None
    message: str | None


def parse_unique_id(unique_id: str) -> tuple[str, str, str]:
    """
    Parse unique_id values like:
      model.my_project.my_model
      test.my_project.not_null_my_model_id.abcd123
      seed.my_project.my_seed

    Returns:
      resource_type, package_name, node_name
    """
    parts = unique_id.split(".")
    resource_type = parts[0] if len(parts) >= 1 else "unknown"
    package_name = parts[1] if len(parts) >= 2 else "unknown"

    if len(parts) >= 3:
        node_name = ".".join(parts[2:])
    else:
        node_name = unique_id

    return resource_type, package_name, node_name


def parse_timing(timing: list[dict[str, Any]]) -> tuple[float, float, float]:
    """
    dbt timing usually includes entries named compile and execute.
    Each entry may include a duration, or only started_at/completed_at depending on version.
    This function uses duration when present.
    """
    compile_time = 0.0
    execute_time = 0.0
    other_time = 0.0

    for item in timing or []:
        name = str(item.get("name", "unknown")).lower()
        duration = item.get("duration")

        if duration is None:
            continue

        try:
            duration = float(duration)
        except (TypeError, ValueError):
            continue

        if name == "compile":
            compile_time += duration
        elif name == "execute":
            execute_time += duration
        else:
            other_time += duration

    return compile_time, execute_time, other_time


def get_rows_affected(adapter_response: dict[str, Any] | None) -> int | None:
    if not adapter_response:
        return None

    candidates = [
        "rows_affected",
        "rows_inserted",
        "rows_updated",
        "rows_deleted",
        "num_rows",
        "rowcount",
    ]

    for key in candidates:
        value = adapter_response.get(key)
        if isinstance(value, int):
            return value
        if isinstance(value, str) and value.isdigit():
            return int(value)

    return None


def load_results(path: Path) -> tuple[dict[str, Any], list[NodeResult]]:
    payload = json.loads(path.read_text())

    node_results: list[NodeResult] = []

    for result in payload.get("results", []):
        unique_id = result.get("unique_id", "")
        resource_type, package_name, node_name = parse_unique_id(unique_id)

        execution_time = float(result.get("execution_time") or 0.0)
        compile_time, execute_time, other_time = parse_timing(result.get("timing", []))

        rows_affected = get_rows_affected(result.get("adapter_response"))
        rows_per_second = None
        if rows_affected is not None and execution_time > 0:
            rows_per_second = rows_affected / execution_time

        node_results.append(
            NodeResult(
                unique_id=unique_id,
                resource_type=resource_type,
                package_name=package_name,
                node_name=node_name,
                status=str(result.get("status", "unknown")),
                execution_time=execution_time,
                compile_time=compile_time,
                execute_time=execute_time,
                other_timing_time=other_time,
                rows_affected=rows_affected,
                rows_per_second=rows_per_second,
                message=result.get("message"),
            )
        )

    return payload, node_results


def pct(value: float, denominator: float) -> float:
    if denominator == 0:
        return 0.0
    return value / denominator * 100


def fmt_seconds(value: float) -> str:
    return f"{value:,.2f}s"


def print_table(title: str, rows: list[dict[str, Any]], columns: list[str]) -> None:
    print(f"\n{title}")
    print("=" * len(title))

    if not rows:
        print("No rows.")
        return

    widths = {
        col: max(
            len(col),
            max(len(str(row.get(col, ""))) for row in rows),
        )
        for col in columns
    }

    header = "  ".join(col.ljust(widths[col]) for col in columns)
    print(header)
    print("-" * len(header))

    for row in rows:
        print("  ".join(str(row.get(col, "")).ljust(widths[col]) for col in columns))


def summarize_by_resource_type(results: list[NodeResult]) -> list[dict[str, Any]]:
    grouped: dict[str, list[NodeResult]] = defaultdict(list)

    for result in results:
        grouped[result.resource_type].append(result)

    rows = []
    for resource_type, items in sorted(grouped.items()):
        times = [item.execution_time for item in items]
        statuses = Counter(item.status for item in items)

        rows.append(
            {
                "resource_type": resource_type,
                "count": len(items),
                "total_time": fmt_seconds(sum(times)),
                "avg_time": fmt_seconds(statistics.mean(times)) if times else "0.00s",
                "p95_time": fmt_seconds(quantile(times, 0.95))
                if len(times) >= 2
                else fmt_seconds(times[0] if times else 0.0),
                "max_time": fmt_seconds(max(times)) if times else "0.00s",
                "statuses": ", ".join(f"{k}={v}" for k, v in sorted(statuses.items())),
            }
        )

    return rows


def quantile(values: list[float], q: float) -> float:
    """
    Simple nearest-rank quantile.
    """
    if not values:
        return 0.0

    sorted_values = sorted(values)
    index = round((len(sorted_values) - 1) * q)
    return sorted_values[index]


def top_nodes(
    results: list[NodeResult],
    resource_type: str,
    top_n: int,
    total_time: float,
) -> list[dict[str, Any]]:
    filtered = [result for result in results if result.resource_type == resource_type]

    filtered.sort(key=lambda item: item.execution_time, reverse=True)

    rows = []
    for result in filtered[:top_n]:
        rows.append(
            {
                "name": result.node_name,
                "status": result.status,
                "time": fmt_seconds(result.execution_time),
                "pct_total_node_time": f"{pct(result.execution_time, total_time):.1f}%",
                "compile": fmt_seconds(result.compile_time),
                "execute": fmt_seconds(result.execute_time),
                "rows": result.rows_affected
                if result.rows_affected is not None
                else "",
                "rows_per_sec": f"{result.rows_per_second:,.0f}"
                if result.rows_per_second
                else "",
            }
        )

    return rows


def slowest_compile_nodes(
    results: list[NodeResult], top_n: int
) -> list[dict[str, Any]]:
    filtered = [result for result in results if result.compile_time > 0]

    filtered.sort(key=lambda item: item.compile_time, reverse=True)

    return [
        {
            "resource_type": result.resource_type,
            "name": result.node_name,
            "compile": fmt_seconds(result.compile_time),
            "execute": fmt_seconds(result.execute_time),
            "total": fmt_seconds(result.execution_time),
        }
        for result in filtered[:top_n]
    ]


def failed_or_warned_nodes(results: list[NodeResult]) -> list[dict[str, Any]]:
    bad_statuses = {"error", "fail", "warn", "skipped"}

    return [
        {
            "resource_type": result.resource_type,
            "name": result.node_name,
            "status": result.status,
            "time": fmt_seconds(result.execution_time),
            "message": (result.message or "")[:160],
        }
        for result in results
        if result.status.lower() in bad_statuses
    ]


def package_summary(results: list[NodeResult]) -> list[dict[str, Any]]:
    grouped: dict[str, list[NodeResult]] = defaultdict(list)

    for result in results:
        grouped[result.package_name].append(result)

    rows = []
    for package_name, items in sorted(grouped.items()):
        total = sum(item.execution_time for item in items)
        rows.append(
            {
                "package": package_name,
                "count": len(items),
                "total_time": fmt_seconds(total),
                "avg_time": fmt_seconds(total / len(items)) if items else "0.00s",
                "models": sum(1 for item in items if item.resource_type == "model"),
                "tests": sum(1 for item in items if item.resource_type == "test"),
            }
        )

    rows.sort(
        key=lambda row: float(row["total_time"].replace(",", "").replace("s", "")),
        reverse=True,
    )
    return rows


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    if not rows:
        return

    with path.open("w", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("run_results_json", type=Path)
    parser.add_argument("--top", type=int, default=15)
    parser.add_argument("--output-dir", type=Path, default=None)
    args = parser.parse_args()

    payload, results = load_results(args.run_results_json)

    total_node_time = sum(result.execution_time for result in results)
    elapsed_time = float(payload.get("elapsed_time") or 0.0)

    model_time = sum(
        result.execution_time for result in results if result.resource_type == "model"
    )
    test_time = sum(
        result.execution_time for result in results if result.resource_type == "test"
    )

    print("\ndbt run_results.json performance summary")
    print("========================================")
    print(f"artifact path: {args.run_results_json}")
    print(f"dbt invocation id: {payload.get('metadata', {}).get('invocation_id', '')}")
    print(f"dbt version: {payload.get('metadata', {}).get('dbt_version', '')}")
    print(f"top-level elapsed time: {fmt_seconds(elapsed_time)}")
    print(f"sum of node execution time: {fmt_seconds(total_node_time)}")
    print(f"model execution time: {fmt_seconds(model_time)}")
    print(f"test execution time: {fmt_seconds(test_time)}")
    print(f"test/model time ratio: {pct(test_time, model_time):.1f}%")

    if elapsed_time > 0:
        print(
            "parallelism estimate: "
            f"{total_node_time / elapsed_time:.2f}x "
            "(sum of node time divided by elapsed time)"
        )

    resource_rows = summarize_by_resource_type(results)
    package_rows = package_summary(results)
    slow_model_rows = top_nodes(results, "model", args.top, total_node_time)
    slow_test_rows = top_nodes(results, "test", args.top, total_node_time)
    compile_rows = slowest_compile_nodes(results, args.top)
    bad_rows = failed_or_warned_nodes(results)

    print_table(
        "Time by resource type",
        resource_rows,
        [
            "resource_type",
            "count",
            "total_time",
            "avg_time",
            "p95_time",
            "max_time",
            "statuses",
        ],
    )

    print_table(
        f"Slowest {args.top} models",
        slow_model_rows,
        [
            "name",
            "status",
            "time",
            "pct_total_node_time",
            "compile",
            "execute",
            "rows",
            "rows_per_sec",
        ],
    )

    print_table(
        f"Slowest {args.top} tests",
        slow_test_rows,
        ["name", "status", "time", "pct_total_node_time", "compile", "execute"],
    )

    print_table(
        f"Slowest {args.top} compile phases",
        compile_rows,
        ["resource_type", "name", "compile", "execute", "total"],
    )

    print_table(
        "Time by package",
        package_rows,
        ["package", "count", "total_time", "avg_time", "models", "tests"],
    )

    print_table(
        "Failed, warned, or skipped nodes",
        bad_rows,
        ["resource_type", "name", "status", "time", "message"],
    )

    # if args.output_dir:
    #     args.output_dir.mkdir(parents=True, exist_ok=True)

    #     all_rows = [asdict(result) for result in results]
    #     write_csv(args.output_dir / "all_node_results.csv", all_rows)
    #     write_csv(args.output_dir / "slowest_models.csv", slow_model_rows)
    #     write_csv(args.output_dir / "slowest_tests.csv", slow_test_rows)
    #     write_csv(args.output_dir / "resource_type_summary.csv", resource_rows)
    #     write_csv(args.output_dir / "package_summary.csv", package_rows)
    #     write_csv(args.output_dir / "slowest_compile_phases.csv", compile_rows)
    #     write_csv(args.output_dir / "failed_warned_skipped_nodes.csv", bad_rows)

    #     print(f"\nWrote CSV outputs to: {args.output_dir}")


if __name__ == "__main__":
    main()
