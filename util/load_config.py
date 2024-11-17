from pathlib import Path

from yaml import safe_load


def load_config(path: Path) -> dict:
    with open(path, encoding="utf-8") as f:
        config = safe_load(f)
    config["year"] = int(config["year"])
    config["num_to_search"] = int(config["num_to_search"])
    return config
