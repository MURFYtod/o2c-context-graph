from __future__ import annotations

import shutil
import zipfile
from pathlib import Path

import gdown


FILE_ID = "1UqaLbFaveV-3MEuiUrzKydhKmkeC1iAL"
ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
TMP_DIR = ROOT / ".tmp_dataset"
ZIP_PATH = TMP_DIR / "dataset.zip"


def main() -> None:
    TMP_DIR.mkdir(parents=True, exist_ok=True)
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    url = f"https://drive.google.com/uc?id={FILE_ID}"
    gdown.download(url, str(ZIP_PATH), quiet=False, fuzzy=True)

    extracted_files = 0
    with zipfile.ZipFile(ZIP_PATH, "r") as zf:
        zf.extractall(TMP_DIR / "unzipped")

    for path in (TMP_DIR / "unzipped").rglob("*"):
        if path.is_file() and path.suffix.lower() in {".csv", ".jsonl"}:
            relative = path.relative_to(TMP_DIR / "unzipped")
            target = DATA_DIR / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(path, target)
            extracted_files += 1

    print(f"Copied {extracted_files} data files (.csv/.jsonl) to: {DATA_DIR}")


if __name__ == "__main__":
    main()
