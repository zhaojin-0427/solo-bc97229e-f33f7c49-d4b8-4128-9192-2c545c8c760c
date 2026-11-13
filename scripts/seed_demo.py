#!/usr/bin/env python3
"""把 samples/ 下的示例 CSV 发布进数据湖（演示用；重复执行会重复入批）。

用法（容器内）：python scripts/seed_demo.py [--retract]

--retract 会演示撤回：发布后按批次 ID 撤回「芦苇荡监测站」那批，
查看 /batches 的撤回记录与回收状态。
"""
import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app import service  # noqa: E402
from app.config import Config  # noqa: E402

# (站点, 样例文件, 各鸟种列声明类型)
SAMPLES = [
    ("青湾监测站", "station_qingwan_2023.csv", ["uint64", "uint64"]),
    ("青湾监测站", "station_qingwan_2024.csv", ["uint64", "uint64"]),
    ("芦苇荡监测站", "station_luweidang_2023.csv", ["text", "uint64"]),  # 触发「白鹭」提升为 text
]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--retract", action="store_true",
                    help="发布后撤回最后一批，演示撤回记录与文件回收")
    args = ap.parse_args()

    cfg = Config()
    service.init_storage(cfg)
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    results = []
    for station, fname, types in SAMPLES:
        path = os.path.join(root, "samples", fname)
        r = service.publish(cfg, station, fname, path, types)
        results.append(r)
        print(f"published {fname}: batch={r['batch_id'][:8]} rows={r['n_rows']} changes={r['changes']}")

    if args.retract and results:
        target = results[-1]
        r = service.retract_batch(cfg, target["batch_id"], "演示：误交撤回")
        # 无读者时撤回后已自动回收；再确认一次终态并打印
        from app import catalog, retention
        retention.reclaim(cfg, batch_id=target["batch_id"])
        conn = catalog.connect(cfg.db_path)
        try:
            row = dict(conn.execute("SELECT * FROM batches WHERE id = ?",
                                    (target["batch_id"],)).fetchone())
        finally:
            conn.close()
        print(f"retracted {target['batch_id'][:8]}: status={r['status']} "
              f"station={row['station']} rows={row['n_rows']} at={row['retracted_at']} "
              f"note={row['retract_note']} reclaim={row['reclaim_state']}")
        print("打开 /batches 查看撤回记录与回收状态；新预览 / 下载已排除该批。")


if __name__ == "__main__":
    main()
