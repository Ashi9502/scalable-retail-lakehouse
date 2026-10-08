from __future__ import annotations

import shutil

from src.common.config import load_config, project_path
from src.common.spark import build_spark


def main() -> None:
    cfg = load_config()
    export_root = project_path(cfg["paths"]["exports"]) / "silver"
    if export_root.exists():
        shutil.rmtree(export_root)
    export_root.mkdir(parents=True, exist_ok=True)

    spark = build_spark("retail-export-silver")
    for table in ["customers", "products", "orders"]:
        out = export_root / table
        spark.table(f"silver.{table}").write.mode("overwrite").parquet(str(out))
        print(f"exported silver.{table} -> {out}")
    spark.stop()


if __name__ == "__main__":
    main()
