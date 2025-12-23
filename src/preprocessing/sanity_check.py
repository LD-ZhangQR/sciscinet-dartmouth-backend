from __future__ import annotations

import sys
from pathlib import Path

THIS_DIR = Path(__file__).resolve().parent

def find_repo_root(start: Path) -> Path:
    for p in [start] + list(start.parents):
        if (p / "configs" / "config.yaml").exists() or (p / ".git").exists():
            return p
    print("[WARN] repo root not found by markers; falling back to current working directory:", Path.cwd())
    return Path.cwd()

REPO_ROOT = find_repo_root(THIS_DIR)
sys.path.insert(0, str(THIS_DIR))

import pandas as pd  
from utils import load_config, compile_keywords 

RAW = REPO_ROOT / "data" / "raw"

def main() -> None:
    cfg = load_config(REPO_ROOT / "configs" / "config.yaml")

    year_from = int(cfg["year_from"])
    year_to = int(cfg["year_to"])
    uni_pat = compile_keywords(cfg["university_keywords"])
    field_pat = compile_keywords(cfg["field_keywords"])

    fields = pd.read_parquet(RAW / "sciscinet_fields.parquet", columns=["fieldid", "display_name"])
    cs_fields = fields[fields["display_name"].fillna("").str.contains(field_pat)]
    cs_fieldids = set(cs_fields["fieldid"].astype(str))

    aff = pd.read_parquet(RAW / "sciscinet_affiliations.parquet", columns=["institution_id", "display_name"])
    dart_aff = aff[aff["display_name"].fillna("").str.contains(uni_pat)]
    whitelist = set(cfg.get("institution_whitelist", []))
    if whitelist:
        dart_aff = dart_aff[dart_aff["display_name"].isin(whitelist)]
    dart_inst_ids = set(dart_aff["institution_id"].astype(str))

    papers = pd.read_parquet(RAW / "sciscinet_papers.parquet", columns=["paperid", "year"])
    papers["paperid"] = papers["paperid"].astype(str)
    papers = papers[(papers["year"] >= year_from) & (papers["year"] <= year_to)]
    year_papers = set(papers["paperid"])

    pf = pd.read_parquet(RAW / "sciscinet_paperfields.parquet", columns=["paperid", "fieldid"])
    pf["paperid"] = pf["paperid"].astype(str)
    pf["fieldid"] = pf["fieldid"].astype(str)
    cs_papers = set(pf.loc[pf["fieldid"].isin(cs_fieldids), "paperid"])

    paa = pd.read_parquet(RAW / "sciscinet_paper_author_affiliation.parquet", columns=["paperid", "institutionid"])
    paa["paperid"] = paa["paperid"].astype(str)
    paa["institutionid"] = paa["institutionid"].astype(str)
    dart_papers = set(paa.loc[paa["institutionid"].isin(dart_inst_ids), "paperid"])

    final_papers = year_papers & cs_papers & dart_papers

    print(f"repo_root: {REPO_ROOT}")
    print(f"raw_dir: {RAW}")
    print(f"year_range: {year_from}-{year_to}")
    print(f"field_keywords: {cfg['field_keywords']}")
    print(f"university_keywords: {cfg['university_keywords']}")
    print(f"cs_fieldids_count: {len(cs_fieldids)}")
    print("cs_field_examples:", cs_fields["display_name"].head(5).tolist())
    print(f"dartmouth_institution_ids_count: {len(dart_inst_ids)}")
    print("dartmouth_institution_matches:", dart_aff["display_name"].tolist())
    print(f"papers_after_filters_count: {len(final_papers)}")
    print("paperid_examples:", list(final_papers)[:5])

if __name__ == "__main__":
    main()
