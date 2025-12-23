import json
from pathlib import Path
from typing import Dict, Any, List, Tuple

import networkx as nx
from networkx.algorithms.community import greedy_modularity_communities


REPO_ROOT = Path(__file__).resolve().parents[2]
OUT = REPO_ROOT / "data" / "outputs"


def read_json(path: Path) -> Dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def write_json(path: Path, obj: Dict[str, Any]) -> None:
    path.write_text(json.dumps(obj, ensure_ascii=False), encoding="utf-8")


def build_undirected_graph(nodes: List[Dict[str, Any]], edges: List[Dict[str, Any]]) -> nx.Graph:
    """
    Build an undirected graph for community detection.
    For citation networks (directed in meaning), we still use undirected modularity to get clusters.
    """
    G = nx.Graph()
    for n in nodes:
        G.add_node(n["id"])

    for e in edges:
        s = e.get("source")
        t = e.get("target")
        if s is None or t is None:
            continue
        if s == t:
            continue
        if G.has_node(s) and G.has_node(t):
            G.add_edge(s, t)

    return G


def compute_communities(G: nx.Graph) -> Dict[str, int]:
    """
    Returns: node_id -> community_id
    """
    if G.number_of_nodes() == 0:
        return {}

    if G.number_of_edges() == 0:
        return {str(n): 0 for n in G.nodes()}

    comms = list(greedy_modularity_communities(G))

    comms.sort(key=lambda c: len(c), reverse=True)

    node2comm: Dict[str, int] = {}
    for cid, cset in enumerate(comms):
        for nid in cset:
            node2comm[str(nid)] = cid

    for nid in G.nodes():
        node2comm.setdefault(str(nid), -1)

    return node2comm


def add_fields(graph: Dict[str, Any]) -> Dict[str, Any]:
    nodes = graph.get("nodes", [])
    edges = graph.get("edges", [])

    G = build_undirected_graph(nodes, edges)
    node2comm = compute_communities(G)

    deg = dict(G.degree())

    for n in nodes:
        nid = str(n["id"])
        n["community"] = int(node2comm.get(nid, -1))
        n["degree"] = int(deg.get(nid, 0))

    graph["nodes"] = nodes
    graph["edges"] = edges
    return graph


def main():
    for name in ["papers_graph.json", "authors_graph.json"]:
        in_path = OUT / name
        if not in_path.exists():
            print(f"[skip] not found: {in_path}")
            continue

        graph = read_json(in_path)
        graph2 = add_fields(graph)

        out_path = OUT / name 
        write_json(out_path, graph2)
        print(f"[ok] wrote community+degree into: {out_path}")


if __name__ == "__main__":
    main()