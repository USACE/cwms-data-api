import unittest
from packaging.version import Version
from scripts.package_version import package_version


class VersionTest(unittest.TestCase):
    def test_untagged_ci_commit_versions_are_development_artifacts(self):
        for revision in ["7098006", "abcdef1", "F9A6B660F8DE10ECCA746AAA3599DBDF802BB966"]:
            with self.subTest(revision=revision):
                version = Version(package_version(revision))
                self.assertEqual(str(version), f"0.dev0+g{revision.lower()}")
                self.assertTrue(version.is_devrelease)
                self.assertLess(version, Version("2026.9.3"))

    def test_cda_release_versions_and_order(self):
        inputs = ["2026.09.03-dev", "2026.09.03-deva", "2026.09.03-test", "2026.09.03-testa", "2026.09.03", "2026.09.03-a"]
        expected = ["2026.9.3.dev0", "2026.9.3.dev1", "2026.9.3rc0", "2026.9.3rc1", "2026.9.3", "2026.9.3.post1"]
        self.assertEqual([package_version(value) for value in inputs], expected)
        self.assertEqual(sorted(map(Version, expected)), list(map(Version, expected)))

    def test_development_and_invalid_versions(self):
        self.assertEqual(package_version("2026.09.03-feature/python-sdk"), "2026.9.3.dev0+feature.python.sdk")
        for value in ["2026.02.30", "0.1.0"]:
            with self.subTest(value=value), self.assertRaises(ValueError):
                package_version(value)
