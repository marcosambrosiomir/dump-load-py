import os
import tempfile

import yaml

def load_config(path="config/config.yaml"):
    with open(path, "r") as f:
        return yaml.safe_load(f)


def save_config(config, path="config/config.yaml"):
    directory = os.path.dirname(path)
    if directory:
        os.makedirs(directory, exist_ok=True)

    prefix = os.path.basename(path) + "."
    with tempfile.NamedTemporaryFile("w", encoding="utf-8", dir=directory or None, prefix=prefix, suffix=".tmp", delete=False) as handle:
        yaml.safe_dump(config, handle, allow_unicode=True, default_flow_style=False, indent=2)
        handle.flush()
        os.fsync(handle.fileno())
        temporary_path = handle.name

    os.replace(temporary_path, path)
