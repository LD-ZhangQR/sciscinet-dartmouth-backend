from __future__ import annotations

import sys
import re
import itertools
from pathlib import Path
from collections import defaultdict

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


def norm_pair(a: str, b: str) -> tuple[str, str]:
    return (a, b) if a < b else (b, a)


def main() -> None:
    cfg = load_config(REPO_ROOT / "configs" / "config.yaml")

    year_from, year_to = int(cfg["year_from"]), int(cfg["year_to"])
    uni_pat = compile_keywords(cfg["university_keywords"])
    field_pat = compile_keywords(cfg["field_keywords"])
    whitelist = set(cfg.get("institution_whitelist", []))

    max_nodes = int(cfg["author_graph"]["max_nodes"])
    min_w = int(cfg["author_graph"]["min_edge_weight"])
    strongest_k = int(cfg["author_graph"]["strongest_k"])

    papers = pd.read_parquet(
        RAW / "sciscinet_papers.parquet",
        columns=["paperid", "year", "doctype", "doi"],
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

    year_papers = set(papers["paperid"])

    fields = pd.read_parquet(RAW / "sciscinet_fields.parquet", columns=["fieldid", "display_name"])
    cs_fields = fields[fields["display_name"].fillna("").str.contains(field_pat)]
    cs_fieldids = set(cs_fields["fieldid"].astype(str))

    pf = pd.read_parquet(RAW / "sciscinet_paperfields.parquet", columns=["paperid", "fieldid"])
    pf["paperid"] = pf["paperid"].astype(str)
    pf["fieldid"] = pf["fieldid"].astype(str)
    cs_papers = set(pf.loc[pf["fieldid"].isin(cs_fieldids), "paperid"])

    aff = pd.read_parquet(RAW / "sciscinet_affiliations.parquet", columns=["institution_id", "display_name"])
    dart_aff = aff[aff["display_name"].fillna("").str.contains(uni_pat)]
    if whitelist:
        dart_aff = dart_aff[dart_aff["display_name"].isin(whitelist)]
    dart_inst_ids = set(dart_aff["institution_id"].astype(str))

    paa = pd.read_parquet(
        RAW / "sciscinet_paper_author_affiliation.parquet",
        columns=["paperid", "authorid", "institutionid"],
    )
    paa["paperid"] = paa["paperid"].astype(str)
    paa["authorid"] = paa["authorid"].astype(str)
    paa["institutionid"] = paa["institutionid"].astype(str)

    dart_papers = set(paa.loc[paa["institutionid"].isin(dart_inst_ids), "paperid"])

    final_papers = year_papers & cs_papers & dart_papers

    ap = pd.read_parquet(RAW / "sciscinet_authors_paperid.parquet", columns=["authorid", "paperid"])
    ap["paperid"] = ap["paperid"].astype(str)
    ap["authorid"] = ap["authorid"].astype(str)
    ap = ap[ap["paperid"].isin(final_papers)]

    by_paper = ap.groupby("paperid")["authorid"].apply(list)

    edge_w: dict[tuple[str, str], int] = defaultdict(int)
    for authors in by_paper:
        uniq = sorted(set(authors))
        if len(uniq) < 2:
            continue
        for a, b in itertools.combinations(uniq, 2):
            edge_w[norm_pair(a, b)] += 1

    edges = [
        {"source": a, "target": b, "weight": int(w)}
        for (a, b), w in edge_w.items()
        if w >= min_w
    ]

    by_node: dict[str, list[dict]] = defaultdict(list)
    for e in edges:
        by_node[e["source"]].append(e)
        by_node[e["target"]].append(e)

    keep_pairs: set[tuple[str, str]] = set()
    for node, es in by_node.items():
        es_sorted = sorted(es, key=lambda x: (-x["weight"], x["source"], x["target"]))
        for e in es_sorted[:strongest_k]:
            keep_pairs.add(norm_pair(e["source"], e["target"]))

    edges = [e for e in edges if norm_pair(e["source"], e["target"]) in keep_pairs]

    deg: dict[str, int] = defaultdict(int)
    wdeg: dict[str, int] = defaultdict(int)
    for e in edges:
        s, t, w = e["source"], e["target"], int(e["weight"])
        deg[s] += 1
        deg[t] += 1
        wdeg[s] += w
        wdeg[t] += w

    top_nodes = [a for a, _ in sorted(wdeg.items(), key=lambda x: -x[1])[:max_nodes]]
    top_set = set(top_nodes)

    edges = [e for e in edges if e["source"] in top_set and e["target"] in top_set]


    authors = pd.read_parquet(
        RAW / "sciscinet_authors.parquet",
        columns=["authorid", "display_name", "h_index", "productivity"],
    )
    authors["authorid"] = authors["authorid"].astype(str)
    aid2 = authors.set_index("authorid").to_dict(orient="index")

    inst_name = aff.copy()
    inst_name["institution_id"] = inst_name["institution_id"].astype(str)
    inst_name = inst_name.rename(columns={"institution_id": "institutionid", "display_name": "institution_name"})

    paai = paa[paa["paperid"].isin(final_papers)].copy()
    paai = paai[paai["authorid"].isin(top_set)]
    paai = paai.merge(inst_name, on="institutionid", how="left")

    author_insts: dict[str, list[str]] = defaultdict(list)
    for aid, sub in paai.groupby("authorid"):
        names = sub["institution_name"].dropna().astype(str).unique().tolist()
        author_insts[aid] = sorted(names)

    def is_dartmouth_author(aid: str) -> bool:
        insts = author_insts.get(aid, [])
        if not insts:
            return False
        if whitelist:
            return any(x in insts for x in whitelist)
        s = " | ".join(insts)
        return re.search(uni_pat, s, flags=re.IGNORECASE) is not None

    nodes = []
    for a in top_nodes:
        info = aid2.get(a, {})
        h = info.get("h_index", None)
        p = info.get("productivity", None)

        nodes.append(
            {
                "id": a,
                "name": info.get("display_name", ""),
                "h_index": int(h) if h is not None and pd.notna(h) else None,
                "productivity": int(p) if p is not None and pd.notna(p) else None,
                "institutions": author_insts.get(a, []),
                "is_dartmouth": bool(is_dartmouth_author(a)),
                "degree": int(deg.get(a, 0)),
                "weighted_degree": int(wdeg.get(a, 0)),
            }
        )

    graph = {
        "meta": {
            "type": "author_collaboration_graph",
            "year_range": [year_from, year_to],
            "field": cfg["field_keywords"],
            "institutions": list(whitelist),
            "min_edge_weight": min_w,
            "strongest_k": strongest_k,
            "max_nodes": max_nodes,
        },
        "nodes": nodes,
        "edges": edges,
    }

    OUT.mkdir(parents=True, exist_ok=True)
    write_json(graph, OUT / "authors_graph.json")
    print(f"[OK] authors_graph.json | nodes={len(nodes)} edges={len(edges)}")


if __name__ == "__main__":
    main()