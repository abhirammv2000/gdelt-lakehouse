"""Benchmark PySpark against DuckDB and Polars on the bronze-to-silver transform.

The question: the pipeline uses Spark, and at one 15-minute batch that is hard to
justify. At what volume does it stop being the wrong tool? Measure it rather than
argue about it.

Method, and the reasons for it:

* The zips are expanded to TSV once, up front (``--prepare``). That cost is shared
  and is not attributed to any engine.
* Each engine then reads the same TSV files with its own native CSV reader, casts
  the 61 columns to the silver types, drops rows with no ``global_event_id``,
  keeps the newest row per id by ``date_added``, and writes Parquet. Same work,
  each engine using the idiom you would actually write for it.
* One engine, one scale, one process. Interpreter and JVM start-up therefore land
  on the engine that pays them, and nothing warms up a later measurement. Running
  several engines in a single process was the first version of this script and it
  produced nonsense: DuckDB looked slower at 24 files than at 1.
* Row counts are printed so the runs can be checked against each other. If two
  engines disagree, the benchmark is wrong.

    python bench/benchmark_engines.py --prepare
    python bench/benchmark_engines.py --engine duckdb --files 24
"""

from __future__ import annotations

import argparse
import json
import pathlib
import shutil
import sys
import tempfile
import time
import zipfile

from gdelt_pipeline.schema.events import EVENT_COLUMN_NAMES, EVENT_COLUMNS

EVENT_KEY = "global_event_id"
RECENCY = "date_added"
DATA = "/home/iceberg/work/bench/data"
TSV = "/home/iceberg/work/bench/tsv"


def prepare(data: str, tsv: str) -> None:
    """Expand every zip to a TSV once so no engine pays the unzip cost."""
    tsv_dir = pathlib.Path(tsv)
    tsv_dir.mkdir(parents=True, exist_ok=True)
    for path in sorted(pathlib.Path(data).glob("*.zip")):
        out = tsv_dir / path.name.replace(".zip", ".tsv")
        if out.exists():
            continue
        with zipfile.ZipFile(path) as zf:
            names = zf.namelist()
            if not names:
                continue
            out.write_bytes(zf.read(names[0]))
    print(f"prepared {len(list(tsv_dir.glob('*.tsv')))} tsv files")


def _files(tsv: str, n: int) -> list[str]:
    return [str(p) for p in sorted(pathlib.Path(tsv).glob("*.tsv"))[:n]]


# ---------------------------------------------------------------- duckdb ----
def run_duckdb(paths: list[str], out: str) -> int:
    import duckdb

    con = duckdb.connect()
    cols = "{" + ", ".join(f"'{c}': 'VARCHAR'" for c in EVENT_COLUMN_NAMES) + "}"
    reader = (
        f"read_csv({paths!r}, delim='\t', header=false, quote='', "
        f"columns={cols}, ignore_errors=true)"
    )

    def cast(name: str, semantic: str) -> str:
        c = f"nullif(trim({name}), '')"
        if semantic == "date_yyyymmdd":
            return f"try_strptime({c}, '%Y%m%d')::date as {name}"
        if semantic == "timestamp_yyyymmddhhmmss":
            return f"try_strptime({c}, '%Y%m%d%H%M%S') as {name}"
        if semantic == "string":
            return f"{c} as {name}"
        types = {"long": "BIGINT", "int": "INTEGER", "double": "DOUBLE"}
        return f"try_cast({c} as {types[semantic]}) as {name}"

    select = ", ".join(cast(n, s) for n, s in EVENT_COLUMNS)
    con.execute(
        f"""
        copy (
          select * exclude (_rn) from (
            select {select},
                   row_number() over (partition by {EVENT_KEY}
                                      order by {RECENCY} desc nulls last) as _rn
            from {reader}
          ) where _rn = 1 and {EVENT_KEY} is not null
        ) to '{out}' (format parquet)
        """
    )
    n = con.execute(f"select count(*) from read_parquet('{out}')").fetchone()[0]
    con.close()
    return int(n)


# ---------------------------------------------------------------- polars ----
def run_polars(paths: list[str], out: str) -> int:
    import polars as pl

    frames = [
        pl.read_csv(
            p,
            separator="\t",
            has_header=False,
            new_columns=EVENT_COLUMN_NAMES,
            quote_char=None,
            infer_schema=False,
            truncate_ragged_lines=True,
        )
        for p in paths
    ]
    df = pl.concat(frames, how="vertical_relaxed")

    exprs = []
    for name, semantic in EVENT_COLUMNS:
        c = pl.col(name).str.strip_chars()
        c = pl.when(c == "").then(None).otherwise(c)
        if semantic == "date_yyyymmdd":
            e = c.str.to_date("%Y%m%d", strict=False)
        elif semantic == "timestamp_yyyymmddhhmmss":
            e = c.str.to_datetime("%Y%m%d%H%M%S", strict=False)
        elif semantic == "string":
            e = c
        else:
            t = {"long": pl.Int64, "int": pl.Int32, "double": pl.Float64}[semantic]
            e = c.cast(t, strict=False)
        exprs.append(e.alias(name))

    result = (
        df.select(*exprs)
        .filter(pl.col(EVENT_KEY).is_not_null())
        .sort(RECENCY, descending=True, nulls_last=True)
        .unique(subset=[EVENT_KEY], keep="first")
    )
    result.write_parquet(out)
    return result.height


# ----------------------------------------------------------------- spark ----
def run_spark(paths: list[str], out: str) -> int:
    from gdelt_spark.transform import to_silver
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
    from pyspark.sql.types import StringType, StructField, StructType

    spark = (
        SparkSession.builder.appName("gdelt-benchmark")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.defaultCatalog", "spark_catalog")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    schema = StructType([StructField(c, StringType(), True) for c in EVENT_COLUMN_NAMES])
    raw = (
        spark.read.option("sep", "\t")
        .option("header", "false")
        .option("quote", "\u0000")  # disable quoting; GDELT is raw tab-delimited
        .schema(schema)
        .csv(paths)
        .withColumn("_source_file", F.input_file_name())
    )
    silver = to_silver(raw)
    silver.write.mode("overwrite").parquet(out)
    n = spark.read.parquet(out).count()
    spark.stop()
    return int(n)


ENGINES = {"duckdb": run_duckdb, "polars": run_polars, "spark": run_spark}


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--data", default=DATA)
    ap.add_argument("--tsv", default=TSV)
    ap.add_argument("--prepare", action="store_true")
    ap.add_argument("--engine", choices=sorted(ENGINES))
    ap.add_argument("--files", type=int, default=1)
    args = ap.parse_args(argv)

    if args.prepare:
        prepare(args.data, args.tsv)
        return 0
    if not args.engine:
        ap.error("--engine is required unless --prepare")

    paths = _files(args.tsv, args.files)
    if not paths:
        print(f"no .tsv under {args.tsv}; run --prepare first", file=sys.stderr)
        return 1

    input_rows = 0
    for path in paths:
        text = pathlib.Path(path).read_text(encoding="utf-8", errors="replace")
        input_rows += sum(1 for line in text.splitlines() if line.strip())

    tmp = tempfile.mkdtemp(prefix=f"bench-{args.engine}-")
    out = str(pathlib.Path(tmp) / "out.parquet")
    try:
        start = time.perf_counter()
        rows = ENGINES[args.engine](paths, out)
        elapsed = time.perf_counter() - start
        print("RESULT " + json.dumps({
            "engine": args.engine,
            "files": args.files,
            "input_rows": input_rows,
            "output_rows": rows,
            "seconds": round(elapsed, 3),
        }))
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
