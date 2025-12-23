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
sys.path.insert(0, str(THIS_DIR))

from utils import load_config, compile_keywords, write_json 

RAW = REPO_ROOT / "data" / "raw"
OUT = REPO_ROOT / "data" / "outputs"


def main() -> None:
    cfg = load_config(REPO_ROOT / "configs" / "config.yaml")

    year_to = int(cfg["year_to"])
    year_from_10 = year_to - 9  
    years_list = list(range(year_from_10, year_to + 1))

    uni_pat = compile_keywords(cfg["university_keywords"])
    field_pat = compile_keywords(cfg["field_keywords"])
    whitelist = set(cfg.get("institution_whitelist", []))

    OUT.mkdir(parents=True, exist_ok=True)


    papers = pd.read_parquet(
        RAW / "sciscinet_papers.parquet",
        columns=["paperid", "doi", "year", "doctype", "patent_count"],
    )
    papers["paperid"] = papers["paperid"].astype(str)

    papers = papers[(papers["year"] >= year_from_10) & (papers["year"] <= year_to)]

    dt_white = set(cfg.get("doctype_whitelist", []))
    if dt_white:
        papers["doctype"] = papers["doctype"].fillna("").astype(str)
        papers = papers[papers["doctype"].isin(dt_white)]

    doi_blacklist = cfg.get("doi_blacklist_regex", [])
    if doi_blacklist:
        doi_series = papers["doi"].fillna("").astype(str)
        bad = pd.Series(False, index=papers.index)
        for pat in doi_blacklist:
            bad = bad | doi_series.str.contains(pat, regex=True)
        papers = papers[~bad]

    year_papers = set(papers["paperid"])

    fields = pd.read_parquet(
        RAW / "sciscinet_fields.parquet",
        columns=["fieldid", "display_name"],
    )
    cs_fields = fields[fields["display_name"].fillna("").str.contains(field_pat)]
    cs_fieldids = set(cs_fields["fieldid"].astype(str))

    pf = pd.read_parquet(
        RAW / "sciscinet_paperfields.parquet",
        columns=["paperid", "fieldid"],
    )
    pf["paperid"] = pf["paperid"].astype(str)
    pf["fieldid"] = pf["fieldid"].astype(str)
    cs_papers = set(pf.loc[pf["fieldid"].isin(cs_fieldids), "paperid"])

    aff = pd.read_parquet(
        RAW / "sciscinet_affiliations.parquet",
        columns=["institution_id", "display_name"],
    )
    dart_aff = aff[aff["display_name"].fillna("").str.contains(uni_pat)]
    if whitelist:
        dart_aff = dart_aff[dart_aff["display_name"].isin(whitelist)]
    dart_inst_ids = set(dart_aff["institution_id"].astype(str))

    paa = pd.read_parquet(
        RAW / "sciscinet_paper_author_affiliation.parquet",
        columns=["paperid", "institutionid"],
    )
    paa["paperid"] = paa["paperid"].astype(str)
    paa["institutionid"] = paa["institutionid"].astype(str)
    dart_papers = set(paa.loc[paa["institutionid"].isin(dart_inst_ids), "paperid"])

    final_papers = year_papers & cs_papers & dart_papers

    sub = papers[papers["paperid"].isin(final_papers)].copy()

    timeline = [{"year": y, "paper_count": 0} for y in years_list]
    patents_by_year: dict[str, list[int]] = {str(y): [] for y in years_list}

    if not sub.empty:
        sub["year"] = sub["year"].astype(int)
        sub["patent_count"] = pd.to_numeric(sub["patent_count"], errors="coerce").fillna(0).astype(int)

        tmp = sub.groupby("year")["paperid"].nunique().reset_index(name="paper_count")
        all_years = pd.DataFrame({"year": years_list})
        tmp = all_years.merge(tmp, on="year", how="left").fillna({"paper_count": 0})
        tmp["paper_count"] = tmp["paper_count"].astype(int)
        timeline = tmp.to_dict(orient="records")

        for y, g in sub.groupby("year"):
            patents_by_year[str(int(y))] = g["patent_count"].astype(int).tolist()

    out_timeline = {
        "meta": {
            "type": "t2_timeline",
            "year_range": [year_from_10, year_to],
            "field": cfg["field_keywords"],
            "institutions": list(whitelist),
            "metric": "paper_count",
        },
        "data": timeline,
    }

    out_patents = {
        "meta": {
            "type": "t2_patent_histogram_source",
            "year_range": [year_from_10, year_to],
            "field": cfg["field_keywords"],
            "institutions": list(whitelist),
            "column": "patent_count",
            "notes": "Values are per-paper patent_count for papers in the selected year.",
        },
        "data": patents_by_year,
    }

    write_json(out_timeline, OUT / "t2_timeline.json")
    write_json(out_patents, OUT / "t2_patent_counts_by_year.json")

    print(f"[OK] t2_timeline.json | years={len(out_timeline['data'])}")
    n_years = len(out_patents["data"])
    n_vals = sum(len(v) for v in out_patents["data"].values())
    print(f"[OK] t2_patent_counts_by_year.json | years={n_years} values={n_vals}")


if __name__ == "__main__":
    main()