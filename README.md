# SciSciNet Dartmouth Backend (T1–T3)

Builds and serves Dartmouth-centric **Computer Science** research networks and dashboard datasets from **SciSciNet** parquet tables.

Covers:
- **T1**: paper citation network + author collaboration network
- **T2**: dashboard datasets (timeline + distribution source)
- **T3**: metadata enrichment (**community**, **degree**) consumed by the frontend’s refined view (radial layout + hierarchical edge bundling)

---

## Setup

### Conda (recommended)
```bash
cd sciscinet-dartmouth-backend
conda env create -f environment.yml
conda activate sciscinet-dartmouth
```

Minimum deps (if not using conda): pandas, pyarrow, fastapi, uvicorn, networkx.

⸻

### Configure
```bash
Edit:
	•	configs/config.yaml

Key knobs:
	•	year_from, year_to
	•	field_keywords
	•	university_keywords / institution_whitelist
	•	paper_graph.max_nodes, paper_graph.max_edges
	•	author_graph.max_nodes, author_graph.min_edge_weight, author_graph.strongest_k
```
⸻

### Data required
```bash
Place SciSciNet parquet tables in:
	•	data/raw/  (DO NOT commit)

Common tables used:
	•	sciscinet_papers.parquet, sciscinet_paperrefs.parquet
	•	sciscinet_fields.parquet, sciscinet_paperfields.parquet
	•	sciscinet_affiliations.parquet, sciscinet_paper_author_affiliation.parquet
	•	sciscinet_authors.parquet, sciscinet_authors_paperid.parquet
```
⸻

## Build outputs (T1–T3)
```bash
Run from repo root:

T1: Paper citation graph

python src/preprocessing/build_paper_graph.py

Output: data/outputs/papers_graph.json

T1: Author collaboration graph

python src/preprocessing/build_author_graph.py

Output: data/outputs/authors_graph.json

Metrics
	•	Degree (collaborators) = # unique co-authors (after filtering)
	•	Weighted degree (total co-authored papers) = sum of co-authorship edge weights (after filtering)

T3: Add communities + degree (enrichment)

python src/preprocessing/add_communities.py

Overwrites (in place):
	•	data/outputs/papers_graph.json
	•	data/outputs/authors_graph.json

T2: Dashboard datasets

python src/preprocessing/build_t2_dashboards.py

Outputs:
	•	data/outputs/t2_timeline.json
	•	data/outputs/t2_patent_counts_by_year.json
```
⸻

## Run API server
```bash
uvicorn src.api.main:app --reload --port 8000

Endpoints (expected by frontend):
	•	GET /api/papers_graph
	•	GET /api/authors_graph
	•	GET /api/t2_timeline
	•	GET /api/t2_patent_counts_by_year
```
