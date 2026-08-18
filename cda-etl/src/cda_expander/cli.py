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
Command line entry point for the config expander.

    python -m cda_expander --base      sample-app.yml \
                           --templates sample-app.templates.yml \
                           --out       sample-app.generated.yml

Add --check to compare instead of writing.

Exit codes:
    0  success (or, under --check, the output on disk is up to date)
    1  under --check, the output on disk differs from freshly generated output
    2  error (bad input, missing env, resolution failure)
"""
from __future__ import annotations

import argparse
import difflib
import hashlib
import logging
import sys
from pathlib import Path
from typing import Any, Sequence

import yaml

from cda_expander import __version__
from cda_expander.expander import ExpansionError, expand_config
from cda_expander.session import init_source_session

logger = logging.getLogger(__name__)

EXIT_OK = 0
EXIT_DRIFT = 1
EXIT_ERROR = 2

GENERATED_BANNER = "GENERATED FILE - DO NOT EDIT"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="cda-expander",
        description=(
            "Resolves association templates against a CWMS instance and appends the "
            "resulting literal ids to a base cda-etl config."
        ),
    )
    parser.add_argument(
        "--base",
        required=True,
        help="Path to the hand-edited base cda-etl config (literal ids only).",
    )
    parser.add_argument(
        "--templates",
        required=True,
        help="Path to the association templates file.",
    )
    parser.add_argument(
        "--out",
        required=True,
        help="Path to the generated config cda-etl reads.",
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help=(
            "Do not write. Regenerate and compare against --out, exiting 1 if they "
            "differ. Used by CI to catch a stale or hand-edited generated file."
        ),
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        help="Logging level (default: INFO).",
    )

    return parser


def _load_yaml(path: Path, label: str) -> tuple[dict[str, Any], str]:
    if not path.exists():
        raise ExpansionError(f"{label} does not exist: {path}")

    raw_bytes = path.read_bytes()
    digest = hashlib.sha256(raw_bytes).hexdigest()

    data = yaml.safe_load(raw_bytes.decode("utf-8"))
    if data is None:
        raise ExpansionError(f"{label} is empty: {path}")

    return data, digest


def render_config(
    config: dict[str, Any],
    *,
    base_name: str,
    base_digest: str,
    templates_name: str,
    templates_digest: str,
) -> str:
    """
    Serializes an expanded config to YAML text, with a provenance header.
    Deterministic for a given (config, input names, input digests).
    """
    header = "\n".join(
        [
            f"# {GENERATED_BANNER}",
            "#",
            "# Produced by cda-expander from the two inputs below: the base config plus",
            "# resolved association templates. To change anything here, edit whichever",
            "# input owns it and regenerate:",
            "#",
            f"#     python -m cda_expander --base {base_name} \\",
            f"#                            --templates {templates_name} \\",
            "#                            --out <this file>",
            "#",
            f"# expander version : {__version__}",
            f"# base config      : {base_name}",
            f"# base sha256      : {base_digest}",
            f"# templates        : {templates_name}",
            f"# templates sha256 : {templates_digest}",
            "",
            "",
        ]
    )

    body = yaml.safe_dump(
        config,
        sort_keys=False,
        default_flow_style=False,
        allow_unicode=True,
        width=4096,
    )

    return header + body


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)

    logging.basicConfig(
        level=getattr(logging, str(args.log_level).upper(), logging.INFO),
        format="%(levelname)s %(name)s: %(message)s",
    )

    base_path = Path(args.base)
    templates_path = Path(args.templates)
    out_path = Path(args.out)

    try:
        base, base_digest = _load_yaml(base_path, "Base config")
        templates, templates_digest = _load_yaml(templates_path, "Templates file")

        source_url = init_source_session()
        logger.info(
            "Expanding %s with %s against %s", base_path, templates_path, source_url
        )

        expanded = expand_config(base, templates)
        rendered = render_config(
            expanded,
            base_name=base_path.name,
            base_digest=base_digest,
            templates_name=templates_path.name,
            templates_digest=templates_digest,
        )
    except (ExpansionError, ValueError) as error:
        logger.error("%s", error)
        return EXIT_ERROR
    except Exception:
        logger.exception("Unhandled error while expanding %s", base_path)
        return EXIT_ERROR

    if args.check:
        return _check(out_path, rendered)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(rendered, encoding="utf-8", newline="\n")
    logger.info("Wrote %s", out_path)

    return EXIT_OK


def _check(out_path: Path, rendered: str) -> int:
    if not out_path.exists():
        logger.error(
            "%s does not exist. Run the expander without --check to generate it.", out_path
        )
        return EXIT_DRIFT

    existing = out_path.read_text(encoding="utf-8")

    if existing == rendered:
        logger.info("%s is up to date.", out_path)
        return EXIT_OK

    logger.error(
        "%s is out of date. Either an input changed, an underlying association "
        "changed, or the generated file was edited by hand. Regenerate it and "
        "commit the result.",
        out_path,
    )
    _log_diff(existing, rendered, out_path)

    return EXIT_DRIFT


def _log_diff(existing: str, rendered: str, out_path: Path) -> None:
    diff = difflib.unified_diff(
        existing.splitlines(keepends=True),
        rendered.splitlines(keepends=True),
        fromfile=f"{out_path} (on disk)",
        tofile=f"{out_path} (regenerated)",
        n=2,
    )

    for line in diff:
        logger.error("%s", line.rstrip("\n"))


if __name__ == "__main__":
    sys.exit(main())
