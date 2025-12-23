from __future__ import annotations

from pathlib import Path
import json
import re
import yaml

def load_config(path: str | Path = "configs/config.yaml") -> dict:
    path = Path(path)
    with path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def compile_keywords(keywords: list[str]) -> re.Pattern:
    esc = [re.escape(k) for k in keywords if isinstance(k, str) and k.strip()]
    if not esc:
        return re.compile(r"$^") 
    return re.compile("|".join(esc), flags=re.IGNORECASE)

def write_json(obj: dict, out_path: str | Path) -> None:
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)
