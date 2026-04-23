from __future__ import annotations

import argparse
import re
from pathlib import Path


def replace_setting(section: str, key: str, value: str, text: str) -> str:
    pattern = rf"(?ms)(^\[{re.escape(section)}\]\s.*?^({re.escape(key)}\s*=\s*)[^\n]*$)"
    match = re.search(pattern, text)
    if not match:
        raise ValueError(f"Could not find '{key}' in [{section}] section.")
    start, _ = match.span(2)
    line_end = text.find("\n", match.end(2))
    if line_end == -1:
        line_end = len(text)
    replacement = f"{match.group(2)}{value}"
    return text[:start] + replacement + text[line_end:]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--spec", required=True)
    parser.add_argument("--python-path", required=True)
    parser.add_argument("--exec-directory", required=True)
    parser.add_argument("--mode", choices=("standalone", "onefile"), default="standalone")
    parser.add_argument("--title", default="mesh-bc-tester")
    parser.add_argument("--icon", default="")
    args = parser.parse_args()

    spec_path = Path(args.spec)
    text = spec_path.read_text(encoding="utf-8")

    updates = [
        ("app", "title", args.title),
        ("app", "project_dir", "."),
        ("app", "input_file", "main.py"),
        ("app", "exec_directory", args.exec_directory),
        ("app", "icon", args.icon),
        ("python", "python_path", args.python_path),
        ("python", "packages", "Nuitka==2.7.11"),
        ("qt", "modules", "Core,Gui,Widgets"),
        ("qt", "plugins", "platforms,imageformats,styles,platformthemes,iconengines"),
        ("nuitka", "mode", args.mode),
        ("nuitka", "extra_args", "--quiet --noinclude-qt-translations=True"),
    ]

    for section, key, value in updates:
        text = replace_setting(section, key, value, text)

    spec_path.write_text(text, encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
