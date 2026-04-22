from __future__ import annotations

import os
from pathlib import Path


def _huggingface_hub_dir() -> Path:
    hub_dir = os.getenv("HF_HUB_CACHE")
    if hub_dir:
        return Path(hub_dir)

    hf_home = os.getenv("HF_HOME")
    if hf_home:
        return Path(hf_home) / "hub"

    return Path.home() / ".cache" / "huggingface" / "hub"


def resolver_modelo(model_id: str) -> str:
    """Usa el snapshot local del modelo si ya existe en caché."""
    if os.path.isdir(model_id):
        return model_id

    repo_dir = _huggingface_hub_dir() / f"models--{model_id.replace('/', '--')}"
    ref_main = repo_dir / "refs" / "main"

    if ref_main.exists():
        revision = ref_main.read_text(encoding="utf-8").strip()
        snapshot = repo_dir / "snapshots" / revision
        if snapshot.exists():
            return str(snapshot)

    snapshots_dir = repo_dir / "snapshots"
    if snapshots_dir.exists():
        snapshots = [path for path in snapshots_dir.iterdir() if path.is_dir()]
        if snapshots:
            latest = max(snapshots, key=lambda path: path.stat().st_mtime)
            return str(latest)

    return model_id
