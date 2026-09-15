"""Assemble SDK documentation and retain released versions on GitHub Pages."""

import argparse
import html
import json
from pathlib import Path
import re
import shutil


def copy_sdk(source, destination):
    if not (source / "index.html").is_file():
        raise ValueError(f"Missing SDK documentation index: {source}")
    if any(path.is_symlink() for path in source.rglob('*')):
        raise ValueError("Pages documentation must not contain symbolic links")
    if destination.exists():
        shutil.rmtree(destination)
    shutil.copytree(source, destination)


def landing(directory, title, links):
    directory.mkdir(parents=True, exist_ok=True)
    items = ''.join(f'<li><a href="{html.escape(url, quote=True)}">{html.escape(label)}</a></li>' for label, url in links)
    (directory / 'index.html').write_text(
        '<!doctype html><html lang="en"><meta charset="utf-8"><meta name="viewport" content="width=device-width">'
        f'<title>{html.escape(title)}</title><style>body{{font:18px system-ui;max-width:60rem;margin:4rem auto;padding:0 1rem;line-height:1.6}}a{{color:#145a91}}</style>'
        f'<h1>{html.escape(title)}</h1><ul>{items}</ul>'
        '<p><a href="https://cwms-data-api.readthedocs.io/en/latest/libraries/">CDA guides and client repositories</a></p></html>\n', encoding='utf-8')


def stage(root, output, version):
    sdks = [("javascript", "cwmsjs", root / 'clients/typescript/cwmsjs/docs')]
    if (root / 'clients/python/build.gradle').exists():
        sdks.append(("python", "cda-python", root / 'clients/python/build/docs/html'))
    links = []
    for slug, name, source in sdks:
        copy_sdk(source, output / 'sdk' / slug)
        if slug == 'javascript':
            package_version = json.loads((root / 'clients/typescript/cwmsjs/package.json').read_text())['version']
        else:
            package_version = json.loads((source / 'sdk.json').read_text())['version']
        links.append((f'{name} {package_version}', f'{slug}/'))
    landing(output / 'sdk', f'CDA {version} SDK documentation', links)
    (output / 'version.json').write_text(json.dumps({'cda_version': version}) + '\n', encoding='utf-8')


def publish(source, site, version, release):
    if version in {'.', '..'} or not re.fullmatch(r'[A-Za-z0-9._-]+', version):
        raise ValueError('Invalid documentation version path')
    target = site / ('releases/' + version if release else 'development')
    copy_sdk(source / 'sdk', target / 'sdk')
    # Stable releases own /sdk. Development is the initial default until one exists.
    if release and re.fullmatch(r'\d{4}\.\d{2}\.\d{2}(?:-[a-z])?', version):
        current_file = site / 'stable.json'
        previous = json.loads(current_file.read_text())['version'] if current_file.exists() else ''
        if version >= previous:
            copy_sdk(source / 'sdk', site / 'sdk')
            current_file.write_text(json.dumps({'version': version}), encoding='utf-8')
    elif not (site / 'stable.json').exists() and not release:
        copy_sdk(source / 'sdk', site / 'sdk')
    links = []
    if (site / 'sdk/index.html').exists():
        links.append(('Current SDK documentation', 'sdk/'))
    if (site / 'development/sdk/index.html').exists():
        links.append(('Development SDK documentation', 'development/sdk/'))
    for entry in sorted((site / 'releases').glob('*'), reverse=True):
        if (entry / 'sdk/index.html').exists():
            links.append((f'CDA {entry.name}', f'releases/{entry.name}/sdk/'))
    landing(site, 'CWMS Data API SDK documentation', links)
    (site / '.nojekyll').touch()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('mode', choices=['stage', 'publish'])
    parser.add_argument('source', type=Path)
    parser.add_argument('output', type=Path)
    parser.add_argument('version')
    parser.add_argument('--release', action='store_true')
    args = parser.parse_args()
    if args.mode == 'stage':
        stage(args.source.resolve(), args.output.resolve(), args.version)
    else:
        publish(args.source.resolve(), args.output.resolve(), args.version, args.release)
