from __future__ import annotations

import json
from pathlib import Path

from .models import AppState


class AppStateStore:
    def __init__(self, path: Path | None = None) -> None:
        self.path = path or (Path.home() / ".mesh_bc_tester" / "state.json")

    def load(self) -> AppState:
        if not self.path.exists():
            return AppState()
        try:
            return AppState.from_dict(json.loads(self.path.read_text(encoding="utf-8")))
        except (json.JSONDecodeError, OSError, TypeError):
            return AppState()

    def save(self, state: AppState) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(json.dumps(state.to_dict(), ensure_ascii=False, indent=2), encoding="utf-8")
