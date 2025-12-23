from __future__ import annotations

import sys
from pathlib import Path
import pandas as pd

THIS_DIR = Path(__file__).resolve().parent

def find_repo_root(start: Path) -> Path:
    for p in [start] + list(start.parents):
        if (p / "configs" / "config.yaml").exists() or (p / ".git").exists():
            return p
    return Path.cwd()

REPO_ROOT = find_repo_root(THIS_DIR)
RAW = REPO_ROOT / "data" / "raw"
OUT = REPO_ROOT / "data" / "outputs"

def main() -> None:
    if len(sys.argv) != 4:
        print("Usage: python export_shared_papers_with_doi.py AUTHOR_A AUTHOR_B shared_papers_txt")
        print("Example: python export_shared_papers_with_doi.py A1 A2 data/outputs/shared_papers_A1_A2.txt")
        return

    a = sys.argv[1]
    b = sys.argv[2]
    shared_path = Path(sys.argv[3])

    shared_ids = [x.strip() for x in shared_path.read_text(encoding="utf-8").splitlines() if x.strip()]
    shared_ids = list(dict.fromkeys(shared_ids)) 

    papers = pd.read_parquet(
        RAW / "sciscinet_papers.parquet",
        columns=["paperid", "doi", "year", "doctype"],
    )
    papers["paperid"] = papers["paperid"].astype(str)

    sub = papers[papers["paperid"].isin(shared_ids)].copy()
    sub["doi"] = sub["doi"].where(~sub["doi"].isna(), None)
    sub["doctype"] = sub["doctype"].fillna("").astype(str)

    # keep the same order as shared_ids
    order = {pid: i for i, pid in enumerate(shared_ids)}
    sub["__ord"] = sub["paperid"].map(order)
    sub = sub.sort_values("__ord").drop(columns="__ord")

    out_tsv = OUT / f"shared_papers_{a}_{b}.tsv"
    sub.to_csv(out_tsv, sep="\t", index=False)

    print("shared_papers:", len(shared_ids))
    print("rows_found_in_papers_table:", len(sub))
    print("saved:", out_tsv)

if __name__ == "__main__":
    main()
