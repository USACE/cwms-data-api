"""Render generated API/model Markdown and the executable examples as one site."""

import importlib.metadata
import json
import os
import re
from pathlib import Path
import shutil
import subprocess
import sys

root = Path(__file__).resolve().parents[1]
source = root / "build/docs-source"
output = root / "build/docs/html"
for directory in (source, output):
    if directory.exists():
        shutil.rmtree(directory)
source.mkdir(parents=True)
generated = root / "build/cda-python"
shutil.copytree(generated / "docs", source / "reference")
shutil.copyfile(generated / "README.md", source / "README.md")
readme = (source / 'README.md').read_text(encoding='utf-8')
readme = '# Generated SDK reference\n\n' + readme[readme.index('## Documentation for API Endpoints'):]
(source / 'README.md').write_text(readme, encoding='utf-8')
# Generator links use docs/ and README.md; retain that relative layout.
(source / "reference").rename(source / "docs")
filenames = {page.name.casefold(): page.name for page in source.rglob('*.md')}
for page in source.rglob('*.md'):
    text = page.read_text(encoding='utf-8')
    # OpenAPI Generator emits multiple H1 method sections and GitHub anchors;
    # normalize these for Sphinx's heading hierarchy and anchor spelling.
    text = re.sub(r'^# (\*\*.*\*\*)$', r'## \1', text, flags=re.MULTILINE)
    text = re.sub(r'\[([^\]]+)\]\(\)', r'\1', text)
    text = text.replace('[[Back to top]](#)', '')
    text = re.sub(r'\[([^\]]+)\]\(\.md\)', r'\1', text)
    text = re.sub(r'\[(.*?)\]\((?:str|int|float|bool)\.md\)', r'\1', text)
    text = re.sub(r'(?<=\()([^()\s]*?)([^/()\s]+\.md)(?=[#)])', lambda match: match[1] + filenames.get(match[2].casefold(), match[2]), text)
    text = re.sub(r'\]\(([^)\s]*#)([^)]+)\)', lambda match: '](' + match[1] + match[2].lower() + ')', text)
    page.write_text(text, encoding='utf-8')
shutil.copyfile(root / "examples/read_data.py", source / "read_data.py")
version = importlib.metadata.version("cda-python")
(source / "conf.py").write_text(
    f'project = "cda-python"\nrelease = {version!r}\n'
    'extensions = ["myst_parser"]\nhtml_theme = "alabaster"\n'
    'myst_heading_anchors = 4\nexclude_patterns = []\n', encoding="utf-8")
references = "\n".join(f"   docs/{path.stem}" for path in sorted((source / "docs").glob("*.md")))
(source / "index.rst").write_text(
    f'cda-python {version}\n' + '=' * (11 + len(version)) + '\n\n'
    'Python SDK for the CWMS Data API. Install the ``cda-python`` distribution; use ``import cda``.\n\n'
    f'CDA version: ``{os.environ["CDA_VERSION"]}``.\n\n'
    'The package is not yet published on PyPI. Install a wheel built from this repository.\n\n'
    '.. toctree::\n   :maxdepth: 1\n\n   examples\n   README\n\n'
    '.. toctree::\n   :maxdepth: 1\n   :caption: API and model reference\n\n' + references + '\n', encoding="utf-8")
(source / "examples.rst").write_text(
    'Tested examples\n===============\n\n'
    'These functions are executed against a local HTTP server by the SDK tests before this site builds.\n'
    'Change ``CDA_ROOT`` to select your deployment, including its context path.\n\n'
    '.. literalinclude:: read_data.py\n   :language: python\n\n'
    'The time-series example retrieves one page. Pass ``series.next_page`` as ``page`` to fetch the next page.\n', encoding="utf-8")
subprocess.run([sys.executable, '-m', 'sphinx', '-W', '--keep-going', '-b', 'html', str(source), str(output)], check=True)
(output / "sdk.json").write_text(json.dumps({"name": "cda-python", "version": version, "cda_version": os.environ["CDA_VERSION"]}) + '\n', encoding="utf-8")
