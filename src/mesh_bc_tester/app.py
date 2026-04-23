from __future__ import annotations

import sys

from PySide6.QtWidgets import QApplication

from .ui.main_window import MainWindow


def run() -> int:
    app = QApplication(sys.argv)
    app.setApplicationName("Mesh BC Tester")
    app.setOrganizationName("Codex")
    window = MainWindow()
    window.show()
    return app.exec()
