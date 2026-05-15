"""
Shared input file loader for all Locust load-test scripts.

Centralises file discovery, random selection, base64 pre-loading,
and prompt reading so every script uses one consistent implementation.
"""

import os
import sys
import base64
import random
from pathlib import Path
from io import BytesIO

# Ensure the project root is importable from any script location
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))


def get_base_input_path() -> Path:
    """Return the root INPUTS directory.

    Reads ``BASE_INPUT_PATH`` from the environment (set in the root ``.env``).
    Falls back to ``<project_root>/INPUTS`` when the variable is absent.
    """
    env_val = os.getenv("BASE_INPUT_PATH")
    if env_val:
        return Path(env_val)
    return _PROJECT_ROOT / "INPUTS"


def discover_files(subdir: str, extensions: tuple[str, ...]) -> list[str]:
    """Scan *BASE_INPUT_PATH/<subdir>* and return paths matching *extensions*.

    Parameters
    ----------
    subdir : str
        Relative path under the INPUTS root, e.g.
        ``"AI-TOOLS/Background Change/input_images"``.
    extensions : tuple[str, ...]
        Lowercase file suffixes **including the dot**, e.g.
        ``(".jpg", ".jpeg", ".png")``.

    Returns
    -------
    list[str]
        Absolute path strings for every matching file, or an empty list
        if the directory does not exist.
    """
    target = get_base_input_path() / subdir
    if not target.exists():
        return []
    return [
        str(f) for f in target.iterdir()
        if f.is_file() and f.suffix.lower() in extensions
    ]


def load_random_file(subdir: str, extensions: tuple[str, ...]) -> str | None:
    """Return a single random file path from *subdir*, or ``None``."""
    files = discover_files(subdir, extensions)
    return random.choice(files) if files else None


def load_all_as_base64(subdir: str, extensions: tuple[str, ...],
                       *, convert_rgb: bool = False) -> list[str]:
    """Pre-load every file in *subdir* as a base64-encoded string.

    Parameters
    ----------
    subdir : str
        Relative path under the INPUTS root.
    extensions : tuple[str, ...]
        Lowercase suffixes with dot.
    convert_rgb : bool
        When ``True`` the file is opened with PIL, converted to RGB,
        re-saved as JPEG, then base64-encoded.  Useful for image files
        that may have alpha channels.  Requires ``Pillow``.

    Returns
    -------
    list[str]
        One base64 string per file.
    """
    paths = discover_files(subdir, extensions)
    result: list[str] = []

    for p in paths:
        if convert_rgb:
            from PIL import Image
            with Image.open(p) as img:
                if img.mode != "RGB":
                    img = img.convert("RGB")
                buf = BytesIO()
                img.save(buf, format="JPEG")
                result.append(base64.b64encode(buf.getvalue()).decode("utf-8"))
        else:
            with open(p, "rb") as fh:
                result.append(base64.b64encode(fh.read()).decode("utf-8"))
        print(f"  -> Loaded: {Path(p).name}")

    return result


def load_prompts(subdir: str, filename: str = "prompts.txt") -> list[str]:
    """Read a text file of prompts (one per line) from *subdir/filename*.

    Blank lines are silently skipped.

    Returns
    -------
    list[str]
        Stripped, non-empty lines.
    """
    target = get_base_input_path() / subdir / filename
    if not target.exists():
        return []
    return [
        line.strip()
        for line in target.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
