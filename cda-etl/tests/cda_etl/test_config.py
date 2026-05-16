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

import os
import sys
from datetime import datetime
from config import Config

def test_config():
    os.environ["SOURCE_CDA_URL"] = "http://source"
    os.environ["DEST_CDA_URL"] = "http://dest"
    os.environ["START_TIME"] = "2023-01-01"
    os.environ["END_TIME"] = "2023-01-31"
    
    # Test valid MAX_THREADS
    os.environ["MAX_THREADS"] = "5"
    config = Config()
    assert config.max_threads == 5
    print("Test valid MAX_THREADS passed")

    # Test default MAX_THREADS
    del os.environ["MAX_THREADS"]
    config = Config()
    assert config.max_threads == 1
    print("Test default MAX_THREADS passed")

    # Test invalid MAX_THREADS
    os.environ["MAX_THREADS"] = "abc"
    try:
        Config()
        print("Test invalid MAX_THREADS failed (no exception raised)")
    except ValueError as e:
        assert str(e) == "MAX_THREADS must be a number"
        print("Test invalid MAX_THREADS passed (exception raised)")

