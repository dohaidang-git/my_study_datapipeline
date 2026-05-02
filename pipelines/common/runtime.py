"""Runtime helpers for direct script execution."""

from __future__ import annotations

import sys
from pathlib import Path


def ensure_project_root_on_path() -> None:
    current_file = Path(__file__).resolve()
    project_root = current_file.parents[2]
    project_root_str = str(project_root)
    if project_root_str not in sys.path:
        sys.path.insert(0, project_root_str)
