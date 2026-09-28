import importlib.util
from pathlib import Path
import tempfile
import unittest

spec = importlib.util.spec_from_file_location('sdk_site', Path(__file__).resolve().parents[1] / 'site.py')
site = importlib.util.module_from_spec(spec)
spec.loader.exec_module(site)


class SiteTest(unittest.TestCase):
    def test_stage_includes_python_when_its_generator_is_present(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            javascript = root / 'clients/typescript/cwmsjs'
            (javascript / 'docs').mkdir(parents=True)
            (javascript / 'docs/index.html').write_text('javascript')
            (javascript / 'package.json').write_text('{"version": "1.0.0"}')
            site.stage(root, root / 'js-only', '2026.09.03')
            self.assertTrue((root / 'js-only/sdk/javascript/index.html').exists())
            self.assertFalse((root / 'js-only/sdk/python').exists())
            python = root / 'clients/python'
            (python / 'build/docs/html').mkdir(parents=True)
            (python / 'build.gradle').touch()
            with self.assertRaises(ValueError):
                site.stage(root, root / 'missing-python-docs', '2026.09.03')
            (python / 'build/docs/html/index.html').write_text('python')
            (python / 'build/docs/html/sdk.json').write_text('{"version": "2026.9.3"}')
            site.stage(root, root / 'combined', '2026.09.03')
            self.assertTrue((root / 'combined/sdk/python/index.html').exists())
            self.assertIn('cda-python 2026.9.3', (root / 'combined/sdk/index.html').read_text())

    def test_versions_are_retained_and_development_does_not_replace_stable(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            source = root / 'source'
            (source / 'sdk/javascript').mkdir(parents=True)
            (source / 'sdk/index.html').write_text('sdk')
            page = source / 'sdk/javascript/index.html'
            page.write_text('release one')
            site.publish(source, root / 'site', '2026.09.03', True)
            page.write_text('development')
            site.publish(source, root / 'site', '2026.09.04-snapshot', False)
            self.assertEqual((root / 'site/sdk/javascript/index.html').read_text(), 'release one')
            self.assertEqual((root / 'site/development/sdk/javascript/index.html').read_text(), 'development')
            page.write_text('release two')
            site.publish(source, root / 'site', '2026.09.04', True)
            self.assertEqual((root / 'site/releases/2026.09.03/sdk/javascript/index.html').read_text(), 'release one')
            page.write_text('older release rebuilt')
            site.publish(source, root / 'site', '2026.09.03', True)
            self.assertEqual((root / 'site/sdk/javascript/index.html').read_text(), 'release two')

    def test_missing_docs_and_unsafe_version_fail(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            for version in ('../escape', '.', '..'):
                with self.assertRaises(ValueError):
                    site.publish(root, root / 'site', version, True)
            with self.assertRaises(ValueError):
                site.copy_sdk(root / 'absent', root / 'target')
