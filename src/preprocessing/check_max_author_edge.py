from __future__ import annotations

import sys
import json
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

from utils import load_config, compile_keywords 

RAW = REPO_ROOT / "data" / "raw"
OUT = REPO_ROOT / "data" / "outputs"

def main() -> None:

    cfg = load_config(REPO_ROOT / "configs" / "config.yaml")
    year_from, year_to = int(cfg["year_from"]), int(cfg["year_to"])
    uni_pat = compile_keywords(cfg["university_keywords"])
    field_pat = compile_keywords(cfg["field_keywords"])
    inst_whitelist = set(cfg.get("institution_whitelist", []))
    doctype_whitelist = set(cfg.get("doctype_whitelist", []))

    with open(OUT / "authors_graph.json", "r", encoding="utf-8") as f:
        g = json.load(f)

    edges = g["edges"]
    max_edge = max(edges, key=lambda e: int(e["weight"]))
    a, b, w = max_edge["source"], max_edge["target"], int(max_edge["weight"])

    print("Max-weight edge:")
    print("  author A:", a)
    print("  author B:", b)
    print("  edge weight:", w)
    print()

    papers = pd.read_parquet(
        RAW / "sciscinet_papers.parquet",
        columns=["paperid", "year", "doctype"],
    )
    papers["paperid"] = papers["paperid"].astype(str)
    papers = papers[(papers["year"] >= year_from) & (papers["year"] <= year_to)]

    papers["doctype"] = papers["doctype"].fillna("").astype(str)
    if doctype_whitelist:
        papers = papers[papers["doctype"].isin(doctype_whitelist)]

    year_papers = set(papers["paperid"])

    fields = pd.read_parquet(
        RAW / "sciscinet_fields.parquet",
        columns=["fieldid", "display_name"],
    )
    cs_fieldids = set(
        fields.loc[
            fields["display_name"].fillna("").str.contains(field_pat),
            "fieldid",
        ].astype(str)
    )

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
    if inst_whitelist:
        dart_aff = dart_aff[dart_aff["display_name"].isin(inst_whitelist)]
    dart_inst_ids = set(dart_aff["institution_id"].astype(str))

    paa = pd.read_parquet(
        RAW / "sciscinet_paper_author_affiliation.parquet",
        columns=["paperid", "institutionid"],
    )
    paa["paperid"] = paa["paperid"].astype(str)
    paa["institutionid"] = paa["institutionid"].astype(str)
    dart_papers = set(paa.loc[paa["institutionid"].isin(dart_inst_ids), "paperid"])

    final_papers = year_papers & cs_papers & dart_papers

    print("Final papers count:", len(final_papers))
    print()


    ap = pd.read_parquet(
        RAW / "sciscinet_authors_paperid.parquet",
        columns=["authorid", "paperid"],
    )
    ap["authorid"] = ap["authorid"].astype(str)
    ap["paperid"] = ap["paperid"].astype(str)
    ap = ap[ap["paperid"].isin(final_papers)]

    papers_a = set(ap.loc[ap["authorid"] == a, "paperid"])
    papers_b = set(ap.loc[ap["authorid"] == b, "paperid"])
    shared = sorted(papers_a & papers_b)

    print("Shared papers count:", len(shared))
    print("Matches edge weight:", len(shared) == w)
    print("First 20 shared paper IDs:")
    print(shared[:20])
    print()

    sub = papers[papers["paperid"].isin(shared)][["paperid", "year", "doctype"]]
    if len(sub) > 0:
        print("Shared papers year range:", int(sub["year"].min()), "-", int(sub["year"].max()))
        print()
        print("Shared papers doctype distribution:")
        print(sub["doctype"].value_counts().to_string())
        print()
    else:
        print("No shared papers found under current filters (unexpected if weight is correct).")
        print()

    out_file = OUT / f"shared_papers_{a}_{b}.txt"
    out_file.write_text("\n".join(shared) + "\n", encoding="utf-8")
    print("Saved shared paper list to:", out_file)

if __name__ == "__main__":
    main()
