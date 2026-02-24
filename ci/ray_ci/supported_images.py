from functools import lru_cache
from pathlib import Path
from typing import List

import yaml


def _find_ray_root() -> Path:
    """Walk up from this file and cwd looking for .rayciversion."""
    start = Path(__file__).resolve()
    for parent in [start, *start.parents]:
        if (parent / ".rayciversion").exists():
            return parent
    if (Path.cwd() / ".rayciversion").exists():
        return Path.cwd()
    raise FileNotFoundError("Could not find Ray root (missing .rayciversion).")


@lru_cache(maxsize=1)
def load_supported_images():
    yaml_path = _find_ray_root() / "ray-images.yaml"
    with open(yaml_path) as f:
        return yaml.safe_load(f)


def get_image_config(image_type: str) -> dict:
    return load_supported_images()[image_type]


def get_python_versions(image_type: str) -> List[str]:
    return get_image_config(image_type)["python"]


def get_platforms(image_type: str) -> List[str]:
    return get_image_config(image_type)["platforms"]


def get_architectures(image_type: str) -> List[str]:
    return get_image_config(image_type)["architectures"]


def get_default(image_type: str, key: str) -> str:
    return get_image_config(image_type)["defaults"][key]
