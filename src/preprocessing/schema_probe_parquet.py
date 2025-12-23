from pathlib import Path
import pyarrow.parquet as pq

FILES = [
    "sciscinet_papers.parquet",
    "sciscinet_paperrefs.parquet",
    "sciscinet_fields.parquet",
    "sciscinet_paperfields.parquet",
    "sciscinet_affiliations.parquet",
    "sciscinet_paper_author_affiliation.parquet",
    "sciscinet_authors.parquet",
    "sciscinet_authors_paperid.parquet",
]

def main() -> None:
    for fname in FILES:
        path = Path("data/raw") / fname
        print("\n===", fname, "===")
        pf = pq.ParquetFile(path)
        cols = [f.name for f in pf.schema_arrow]
        print("num_row_groups:", pf.num_row_groups)
        print("columns:", cols)

if __name__ == "__main__":
    main()
