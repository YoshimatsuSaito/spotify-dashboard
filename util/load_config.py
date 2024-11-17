from pathlib import Path

from yaml import safe_load


def load_config(path: Path) -> dict:
    with open(path, encoding="utf-8") as f:
        config = safe_load(f)
    config["start_year"] = int(config["start_year"])
    config["end_year"] = int(config["end_year"])
    config["num_to_search"] = int(config["num_to_search"])
    return config
