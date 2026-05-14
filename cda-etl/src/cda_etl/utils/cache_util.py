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
import json

__folder = "./cache"

def get_from_cache(*args):
    """
    :param args: Path components for cache file
    :return: Cached json data or None if not found
    """
    path = _get_cache_path(*args)
    if not path:
        return None

    if not os.path.exists(path):
        return None

    with open(path, 'r') as f:
        return json.load(f)


def _get_cache_path(*args):
    '''
    :param args: Path components for cache file
    :return: Full path to cache file or None if the file doesn't exist
    '''
    if not args:
        return None

    # Convert args to list and ensure last element has .json extension
    path_parts = list(args)
    if not path_parts[-1].endswith('.json'):
        path_parts[-1] = path_parts[-1] + '.json'

    # Build full path starting with __folder
    full_path = os.path.join(__folder, *path_parts)
    return full_path


def put_in_cache(value, *args):
    pass