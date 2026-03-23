from __future__ import annotations

import os
import shutil
import zipfile
from pathlib import Path

import gdown


def ensure_dataset_present(data_dir: Path, root_dir: Path) -> bool:
    """
    Ensure dataset files exist locally.
    Returns True when a download/extract was performed, else False.
    """
    existing = list(data_dir.rglob("*.csv")) + list(data_dir.rglob("*.jsonl"))
    if existing:
        return False

    file_id = os.getenv("DATASET_FILE_ID", "1UqaLbFaveV-3MEuiUrzKydhKmkeC1iAL")
    tmp_dir = root_dir / ".tmp_dataset"
    zip_path = tmp_dir / "dataset.zip"
    unzip_dir = tmp_dir / "unzipped"

    tmp_dir.mkdir(parents=True, exist_ok=True)
    data_dir.mkdir(parents=True, exist_ok=True)

    url = f"https://drive.google.com/uc?id={file_id}"
    gdown.download(url, str(zip_path), quiet=False, fuzzy=True)

    with zipfile.ZipFile(zip_path, "r") as zf:
        zf.extractall(unzip_dir)

    for path in unzip_dir.rglob("*"):
        if path.is_file() and path.suffix.lower() in {".csv", ".jsonl"}:
            relative = path.relative_to(unzip_dir)
            target = data_dir / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(path, target)

    return True
