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
cda-expander - resolves association-based ids into a literal-id cda-etl config.

This package is a standalone preprocessor. It reads a "template" config
describing how to *find* timeseries/rating/location-level ids (by reading
CWMS properties, and later by reading the A2W/PublishedTimeSeries API),
resolves them, and writes a fully expanded YAML file containing only literal
ids in cda-etl's own, unmodified config schema.

cda-etl itself knows nothing about any of this: it consumes the generated
file exactly as it would a hand-written one.

Dependency rule: this package must not import anything from cda_etl, and
cda_etl must not import anything from here. The two only ever meet through
the generated YAML file. Enforced by tests/expander/test_import_isolation.py.
"""

# Bumped whenever the generated output format changes in a way that would
# alter the bytes written for an unchanged input. Recorded in the generated
# file's header so a reviewer can tell what produced it.
__version__ = "1.0.0"
