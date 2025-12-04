import matplotlib.pyplot as plt
import pandas as pd
from scipy import stats

RESULTS_FILE = "results/benchmark_results.csv"
OUTPUT_DIR = "results/"


def load_results() -> pd.DataFrame:
    return pd.read_csv(RESULTS_FILE)


def plot_rps_by_query(df: pd.DataFrame) -> None:
    plt.figure(figsize=(14, 8))
    pivot_mean = df.pivot_table(values="requests_per_sec", index="query_name", columns="library", aggfunc="mean")
    pivot_ci = df.pivot_table(
        values="requests_per_sec",
        index="query_name",
        columns="library",
        aggfunc=lambda x: stats.sem(x) * stats.t.ppf(0.975, len(x) - 1) if len(x) > 1 else 0,
    )
    ax = pivot_mean.plot(kind="bar", yerr=pivot_ci, capsize=3)
    plt.title("Requests per second by query type (with 95% CI)")
    plt.xlabel("Query type")
    plt.ylabel("Requests/sec")
    plt.xticks(rotation=45, ha="right")
    plt.legend(title="Library", bbox_to_anchor=(1.02, 1), loc="upper left")
    plt.tight_layout()
    plt.savefig(f"{OUTPUT_DIR}rps_by_query.png", dpi=150)
    plt.close()


def plot_operation_comparison(df: pd.DataFrame) -> None:
    fig, axes = plt.subplots(2, 2, figsize=(14, 10))
    operations = ["SELECT", "ASK", "CONSTRUCT", "UPDATE"]
    for ax, op in zip(axes.flat, operations):
        op_df = df[df["operation"] == op]
        if op_df.empty:
            ax.set_title(f"{op} (no data)")
            continue
        avg_rps = op_df.groupby("library")["requests_per_sec"].mean().sort_values(ascending=False)
        colors = ["#2ecc71" if lib == "sparqlite" else "#3498db" for lib in avg_rps.index]
        avg_rps.plot(kind="bar", ax=ax, color=colors)
        ax.set_title(f"{op} queries - requests/sec")
        ax.set_xlabel("")
        ax.tick_params(axis="x", rotation=45)
    plt.tight_layout()
    plt.savefig(f"{OUTPUT_DIR}operation_comparison.png", dpi=150)
    plt.close()


def main() -> None:
    df = load_results()
    plot_rps_by_query(df)
    plot_operation_comparison(df)