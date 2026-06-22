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
"""
Guards the one-way dependency rule.

Both packages sit under the same src/ tree and share an image, so nothing
stops someone reaching across by accident. The two tools are only ever
allowed to meet through the generated YAML file, so:

* cda_etl must not import cda_expander - that would put id-resolution surface
  back inside the pipeline, which is the entire thing this restructuring
  removed.
* cda_expander must not import cda_etl - the expander's input schema has to
  stay free to evolve without dragging cda-etl's config layer with it.

The check walks each module's AST and inspects real import statements rather
than grepping source text. Both packages' docstrings legitimately discuss the
other by name - including the sentence you are reading - and a text search
flags that prose as a violation.
"""
import ast
from pathlib import Path

import pytest

SRC = Path(__file__).resolve().parents[2] / "src"
CDA_ETL = SRC / "cda_etl"
CDA_EXPANDER = SRC / "cda_expander"


def _python_files(root: Path) -> list[Path]:
    return sorted(path for path in root.rglob("*.py") if "__pycache__" not in path.parts)


def _imported_roots(path: Path) -> set[str]:
    """
    Every top-level module name this file imports, from either import form.
    """
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    roots: set[str] = set()

    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                roots.add(alias.name.split(".")[0])
        elif isinstance(node, ast.ImportFrom):
            # node.module is None for relative imports ("from . import x"),
            # which are intra-package and never a violation.
            if node.module:
                roots.add(node.module.split(".")[0])

    return roots


@pytest.mark.parametrize("path", _python_files(CDA_ETL), ids=lambda path: path.name)
def test_cda_etl_does_not_import_cda_expander(path):
    assert "cda_expander" not in _imported_roots(path), (
        f"{path.name} imports cda_expander. Ids are resolved before cda-etl "
        "runs, not during it."
    )


@pytest.mark.parametrize("path", _python_files(CDA_EXPANDER), ids=lambda path: path.name)
def test_cda_expander_does_not_import_cda_etl(path):
    assert "cda_etl" not in _imported_roots(path), (
        f"{path.name} imports cda_etl. The two tools meet only through the "
        "generated config file."
    )


def test_both_packages_were_actually_scanned():
    """
    Guards against the parametrized tests silently passing because a path typo
    made the file list empty.
    """
    assert _python_files(CDA_ETL), f"No cda_etl sources found under {CDA_ETL}"
    assert _python_files(CDA_EXPANDER), f"No cda_expander sources found under {CDA_EXPANDER}"


def test_detects_a_planted_violation(tmp_path):
    """
    The check above only means something if it can actually fail. An earlier
    text-based version of it "passed" while matching nothing but prose.
    """
    planted = tmp_path / "planted.py"
    planted.write_text("from cda_etl.config import DownloadConfig\n", encoding="utf-8")

    assert "cda_etl" in _imported_roots(planted)


def test_ignores_the_other_package_named_only_in_prose(tmp_path):
    prose_only = tmp_path / "prose_only.py"
    prose_only.write_text(
        '"""This module deliberately does not import cda_etl.session_manager."""\n'
        "# cda_etl is also mentioned here, in a comment.\n"
        "import logging\n",
        encoding="utf-8",
    )

    assert "cda_etl" not in _imported_roots(prose_only)
