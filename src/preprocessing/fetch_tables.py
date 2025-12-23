import os
from pathlib import Path
from huggingface_hub import hf_hub_download

REPO_ID = "Northwestern-CSSI/sciscinet-v2"
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
    token = os.environ.get("HF_TOKEN")
    if not token:
        print("[ERROR] HF_TOKEN not set.")
        print("Run: export HF_TOKEN='hf_...'\n")
        return

    outdir = Path("data/raw")
    outdir.mkdir(parents=True, exist_ok=True)

    for fname in FILES:
        cached_path = hf_hub_download(
            repo_id=REPO_ID,
            filename=fname,
            repo_type="dataset",
            token=token,
        )
        target = outdir / fname

        # Make it idempotent
        if target.exists():
            print("OK (exists):", fname)
            continue

        target.symlink_to(Path(cached_path))
        print("OK:", fname)

if __name__ == "__main__":
    main()