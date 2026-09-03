"""Translate CDA CalVer tags to Python's PEP 440 spelling."""

from datetime import date
import re
import sys


def package_version(value):
    if re.fullmatch(r"[a-zA-Z0-9._/-]+-nightly", value):
        value = f"{date.today():%Y.%m.%d}-{value}"
    match = re.fullmatch(r"(\d{4})\.(\d{2})\.(\d{2})(?:-(.+))?", value)
    if not match:
        raise ValueError("Expected a CDA version YYYY.MM.DD with an optional suffix")
    year, month, day = map(int, match.group(1, 2, 3))
    date(year, month, day)  # Reject invalid calendar dates.
    base = f"{year}.{month}.{day}"
    suffix = match.group(4)
    if not suffix:
        return base
    suffix = suffix.lower()
    release = re.fullmatch(r"(dev|test)([a-z]*)", suffix)
    if release:
        number = letter_number(release.group(2))
        return f"{base}{'.dev' if release.group(1) == 'dev' else 'rc'}{number}"
    if re.fullmatch(r"[a-z]", suffix):
        return f"{base}.post{letter_number(suffix)}"
    label = re.sub(r"[^a-z0-9]+", ".", suffix).strip(".")
    if not label:
        raise ValueError("The CDA development suffix must contain letters or numbers")
    return f"{base}.dev0+{label}"


def letter_number(value):
    number = 0
    for letter in value:
        number = number * 26 + ord(letter) - ord('a') + 1
    return number


if __name__ == "__main__":
    print(package_version(sys.argv[1]))
