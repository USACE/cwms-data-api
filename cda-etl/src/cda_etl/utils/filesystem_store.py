#  MIT License
#  Copyright (c) 2026 Hydrologic Engineering Center
#  Permission is hereby granted, free of charge, to any person obtaining a copy
#  of this software and associated documentation files (the "Software"), to deal
#  in the Software without restriction, including without limitation the rights
#  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
#  copies of the Software, and to permit persons to whom the Software is
#  furnished to do so, subject to the following conditions:
#  The above copyright notice and this permission notice shall be included in all
#  copies or substantial portions of the Software.
#  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
#  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
#  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
#  AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
#  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
#  OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
#  SOFTWARE.
from __future__ import annotations

import json
from pathlib import Path
from typing import Any
from urllib.parse import quote, unquote

_STORAGE_ROOT = Path("./data")

# CWMS ids and property names legitimately contain characters that NTFS forbids
# in a filename. REGI's association properties are the live example - "Regi_project_INPUT.Elev_Area.?GLOBAL?"
# cannot be written on Windows at all ([Errno 22] Invalid argument), while the
# same name is fine on Linux, so this only shows up outside the container.
#
# "%" is escaped too, otherwise a real "%3F" in an id would decode back to "?" and collide.
# list_json_stems reverses this, so names handed back to callers
# (and on to CDA) are always the true ones.
_ILLEGAL_IN_FILENAMES = '<>:"/\\|?*%'
_SAFE_CHARACTERS = "".join(
    chr(code) for code in range(32, 127) if chr(code) not in _ILLEGAL_IN_FILENAMES
)


def _encode_part(part: str) -> str:
    return quote(part, safe=_SAFE_CHARACTERS)


def decode_part(part: str) -> str:
    """
    Reverses _encode_part. Exposed because a name read back off disk has to be
    decoded before it is used as a CWMS id.
    """
    return unquote(part)


def set_storage_root(path: str | Path) -> None:
    global _STORAGE_ROOT
    _STORAGE_ROOT = Path(path)


def read_json(*path_parts: str) -> Any | None:
    path = _build_path(*path_parts)
    if path is None:
        return None

    if not path.exists():
        return None

    with path.open("r", encoding="utf-8") as file:
        return json.load(file)
    return None


def write_json(value: Any, *path_parts: str) -> None:
    path = _build_path(*path_parts)
    if path is None:
        raise ValueError("At least one path component is required.")

    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as file:
        json.dump(value, file, indent=2)


def list_json_stems(*path_parts: str) -> list[str]:
    """
    Returns the true names of the staged files in a directory - decoded, so a
    stem can be passed straight back into read_json or used as a CWMS id.
    """
    if not path_parts:
        return []

    directory = _STORAGE_ROOT.joinpath(*_normalize_path_parts(*path_parts))
    if not directory.exists() or not directory.is_dir():
        return []

    return sorted(decode_part(path.stem) for path in directory.glob("*.json") if path.is_file())


def _build_path(*path_parts: str) -> Path | None:
    if not path_parts:
        return None

    normalized_parts = _normalize_path_parts(*path_parts)
    if not normalized_parts[-1].endswith(".json"):
        normalized_parts[-1] = f"{normalized_parts[-1]}.json"

    return _STORAGE_ROOT.joinpath(*normalized_parts)


def _normalize_path_parts(*path_parts: str) -> list[str]:
    normalized_parts = list(path_parts)

    if len(normalized_parts) >= 6 and normalized_parts[-1] == "data":
        normalized_parts = normalized_parts[:-4] + [normalized_parts[2]]

    return [_encode_part(part) for part in normalized_parts]


__all__ = ["decode_part", "list_json_stems", "read_json", "set_storage_root", "write_json"]
