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

    year_from, year_to = int(cfg["year_from"]), int(cfg["year_to"])
    uni_pat = compile_keywords(cfg["university_keywords"])
    field_pat = compile_keywords(cfg["field_keywords"])
    whitelist = set(cfg.get("institution_whitelist", []))

    max_nodes = int(cfg["paper_graph"]["max_nodes"])
    max_edges = int(cfg["paper_graph"]["max_edges"])
    sort_key = "citation_count"  # A

    OUT.mkdir(parents=True, exist_ok=True)

    papers = pd.read_parquet(
        RAW / "sciscinet_papers.parquet",
        columns=["paperid", "doi", "year", "doctype", sort_key],
    )
    papers["paperid"] = papers["paperid"].astype(str)
    papers = papers[(papers["year"] >= year_from) & (papers["year"] <= year_to)]
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

    final_papers = cs_papers & dart_papers & set(papers["paperid"])

    papers_sub = papers[papers["paperid"].isin(final_papers)]
    papers_sub = papers_sub.sort_values(sort_key, ascending=False).head(max_nodes)
    node_ids = set(papers_sub["paperid"])

    nodes = []
    for r in papers_sub.itertuples(index=False):
        doi = getattr(r, "doi", None)
        nodes.append(
            {
                "id": str(getattr(r, "paperid")),
                "doi": None if pd.isna(doi) else str(doi),
                "year": int(getattr(r, "year")),
                sort_key: int(getattr(r, sort_key)),
            }
        )

    refs = pd.read_parquet(
        RAW / "sciscinet_paperrefs.parquet",
        columns=["citing_paperid", "cited_paperid"],
    )
    refs["citing_paperid"] = refs["citing_paperid"].astype(str)
    refs["cited_paperid"] = refs["cited_paperid"].astype(str)

    refs = refs[
        refs["citing_paperid"].isin(node_ids)
        & refs["cited_paperid"].isin(node_ids)
    ].head(max_edges)

    edges = [{"source": r.citing_paperid, "target": r.cited_paperid} for r in refs.itertuples(index=False)]

    graph = {
        "meta": {
            "type": "paper_citation_graph",
            "year_range": [year_from, year_to],
            "field": cfg["field_keywords"],
            "institutions": list(whitelist),
            "sort_key": sort_key,
            "max_nodes": max_nodes,
            "max_edges": max_edges,
        },
        "nodes": nodes,
        "edges": edges,
    }

    write_json(graph, OUT / "papers_graph.json")
    print(f"[OK] papers_graph.json | nodes={len(nodes)} edges={len(edges)}")

if __name__ == "__main__":
    main()
