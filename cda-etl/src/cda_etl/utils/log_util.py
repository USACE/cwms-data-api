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
Shared vocabulary for cda-etl's log output.

Three things live here because they were previously spelled differently in every
module, which made one run read like several tools:

* **One display format for timestamps.** timeseries, rating and location_level
  each define a `DATE_TIME_FORMAT` of ``"%Y-%m-%d %H.%M.%S"``. Those dots exist
  so the value can go in a filename - a storage concern - and it should not be
  what a person reads. `display` and `window` are for the log; the dotted format
  stays where it belongs, in the paths.

* **Which half of the pipeline is running.** Extract and load log near-identical
  wording, so a line lifted out of context - pasted into a ticket, grepped out of
  a file - could not say which direction it described. `phase` records that,
  `install_phase_tag` puts it on every record, and `direction` renders it as
  prose for lines that report a failed call rather than the phase itself.
  `session_manager` enters `phase` with the session, so the two cannot disagree.

* **Per-item outcomes.** "Nothing here" is an ordinary, bulk outcome: whole
  association categories are applied to every project, so most ids have nothing
  for most projects. One line each buried the run. `Tally` collects them so a
  batch can account for itself once.
"""
from __future__ import annotations

import logging
import re
import threading
import time
from contextlib import contextmanager
from datetime import datetime
from typing import Iterable, Iterator
from urllib.parse import unquote_plus

logger = logging.getLogger(__name__)
_pipeline_logger = logging.getLogger("pipeline")

DISPLAY_FORMAT = "%Y-%m-%d %H:%M"

EXTRACT = "EXTRACT"
LOAD = "LOAD"

_DIRECTIONS = {
    EXTRACT: "reading from the source",
    LOAD: "writing to the destination",
}

UNKNOWN_DIRECTION = "an unknown phase"

_BANNER_RULE = "=" * 78
_SUMMARY_RULE = "-" * 78

_PLURALS = {
    "property": "properties",
    "category": "categories",
    "entry": "entries",
    "timeseries": "timeseries",
}


def plural(count: int, noun: str) -> str:
    """
    ``plural(1, "property")`` -> ``"1 property"``; ``plural(0, "property")`` ->
    ``"0 properties"``.

    Multi-word nouns inflect on the last word, so "property category" gives
    "property categories" rather than "property categorys".
    """
    if count == 1:
        return f"{count} {noun}"

    head, _, last = noun.rpartition(" ")
    inflected = _PLURALS.get(last, last + "s")

    return f"{count} {head} {inflected}".replace("  ", " ") if head else f"{count} {inflected}"


def display(value: object) -> str:
    """
    A timestamp as a person should read it: minutes, no microseconds, no offset.

    Microseconds arrive because an end time of "now" is `datetime.now()`, and
    they were reaching the log as "2026-08-03 10:47:04.197293+00:00".
    """
    if isinstance(value, datetime):
        return value.strftime(DISPLAY_FORMAT)

    return str(value)


def window(begin: object, end: object) -> str:
    return f"{display(begin)} to {display(end)}"


_STAMP_IN_TEXT = re.compile(r"(\d{4}-\d{2}-\d{2})[T ](\d{2}:\d{2})")


def shorten_timestamp(value: str) -> str:
    """
    Trims a timestamp that arrived as text to the display format.

    cwms-python's messages and URLs carry their own renderings -
    ``2026-08-03 11:48:37.148135``, ``2026-07-27T00%3A00%3A00%2B00%3A00`` - and
    those strings are all the filter and the retry reporter have to work from.
    """
    decoded = unquote_plus(value)
    match = _STAMP_IN_TEXT.search(decoded)

    return f"{match.group(1)} {match.group(2)}" if match else decoded


def duration(seconds: float) -> str:
    if seconds < 1:
        return f"{seconds * 1000:.0f}ms"

    if seconds < 60:
        return f"{seconds:.1f}s"

    minutes, remainder = divmod(int(seconds), 60)

    return f"{minutes}m{remainder:02d}s"


# Process-wide rather than thread-local: worker threads have to see it, and a
# threading.local set on the main thread is invisible to them.
_current_phase: str | None = None


def current_phase() -> str | None:
    return _current_phase


def direction() -> str:
    """
    The current phase named alongside what it is doing - ``"EXTRACT (reading
    from the source)"``.
    """
    name = current_phase()

    if name is None:
        return UNKNOWN_DIRECTION

    described = _DIRECTIONS.get(name)

    return f"{name} ({described})" if described else name


@contextmanager
def phase(name: str, *, endpoint: str | None = None, detail: str | None = None) -> Iterator[None]:
    """
    Marks a half of the pipeline, banners it, and reports how long it took.

    The banner is one record with embedded newlines rather than several, so the
    level/logger prefix appears once above the block instead of on each rule.
    """
    global _current_phase

    previous = _current_phase
    _current_phase = name
    started = time.monotonic()

    lines = [f" {name}"]
    if endpoint:
        lines[0] = f" {name} - {endpoint}"
    if detail:
        lines.append(f" {detail}")

    _pipeline_logger.info("\n%s\n%s\n%s", _BANNER_RULE, "\n".join(lines), _BANNER_RULE)

    failed = False

    try:
        yield
    except BaseException:
        failed = True
        raise
    finally:
        elapsed = duration(time.monotonic() - started)
        _pipeline_logger.info(
            "\n%s\n %s %s %s\n%s",
            _SUMMARY_RULE,
            name,
            "failed after" if failed else "complete in",
            elapsed,
            _SUMMARY_RULE,
        )
        _current_phase = previous


def install_phase_tag() -> None:
    """
    Puts the current phase on every record, so ``%(phase)s`` can go in the
    format and no line is ambiguous about direction.

    Idempotent - wrapping the factory twice would just set the attribute twice,
    but the guard keeps the chain from growing if an entry point is re-entered.
    """
    existing = logging.getLogRecordFactory()

    if getattr(existing, "_cda_phase_tag", False):
        return

    def factory(*args: object, **kwargs: object) -> logging.LogRecord:
        record = existing(*args, **kwargs)
        record.phase = _current_phase or "-"
        return record

    factory._cda_phase_tag = True  # type: ignore[attr-defined]
    logging.setLogRecordFactory(factory)


FORMAT = "%(asctime)s %(levelname)-7s %(phase)-7s %(name)-14s %(message)s"
DEBUG_FORMAT = "%(asctime)s %(levelname)-7s %(phase)-7s %(name)-14s [%(threadName)s] %(message)s"
DATE_FORMAT = "%H:%M:%S"
_FORMATTER_DEFAULTS = {"phase": "-"}


def formatter(level: int) -> logging.Formatter:
    return logging.Formatter(
        DEBUG_FORMAT if level <= logging.DEBUG else FORMAT,
        datefmt=DATE_FORMAT,
        defaults=_FORMATTER_DEFAULTS,
    )


def configure(level: int) -> None:
    """
    Installs the phase tag, the root handler and the format, in that order.
    """
    install_phase_tag()
    logging.basicConfig(level=level)
    reformat(level)


def reformat(level: int) -> None:
    """
    Re-applies the format after the level changes - the config file can turn
    DEBUG on after the handler already exists, and thread names are the main
    reason to want DEBUG here.
    """
    for handler in logging.getLogger().handlers:
        handler.setFormatter(formatter(level))


class Tally:
    """
    Collects per-item outcomes so a batch reports them once instead of per item.

    Thread-safe: the items in a batch run concurrently on the shared executor,
    and each records its own outcome from its own thread.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._by_reason: dict[str, list[str]] = {}

    def record(self, reason: str, label: str) -> None:
        with self._lock:
            self._by_reason.setdefault(reason, []).append(label)

    def labels(self, reason: str) -> list[str]:
        with self._lock:
            return sorted(self._by_reason.get(reason, ()))

    def count(self, reason: str | None = None) -> int:
        with self._lock:
            if reason is None:
                return sum(len(labels) for labels in self._by_reason.values())

            return len(self._by_reason.get(reason, ()))

    def reasons(self) -> list[str]:
        with self._lock:
            return sorted(self._by_reason)

    def describe(self) -> str:
        with self._lock:
            counted = [
                (len(labels), reason) for reason, labels in self._by_reason.items() if labels
            ]

        # Largest cause first, so the line leads with what most of the batch did.
        # Reason breaks ties, which keeps the output deterministic.
        counted.sort(key=lambda entry: (-entry[0], entry[1]))

        return ", ".join(f"{count} {reason}" for count, reason in counted)

    def log_details(self, log: logging.Logger) -> None:
        if not log.isEnabledFor(logging.DEBUG):
            return

        with self._lock:
            snapshot = {
                reason: sorted(label for label in labels if label)
                for reason, labels in self._by_reason.items()
            }

        for reason, labels in sorted(snapshot.items()):
            if labels:
                log.debug("%s: %s", reason.capitalize(), ", ".join(labels))


# An item the load phase found no staged file for. Named here rather than in each
# module because threading_util records it and outcome reacts to it: it is the one
# tallied outcome that might mean something is actually wrong.
NOTHING_STAGED = "with nothing staged"


def outcome(
    log: logging.Logger,
    *,
    action: str,
    noun: str,
    total: int,
    tally: Tally,
    office_id: str,
    elapsed: float | None = None,
) -> None:
    """
    The one line that accounts for a batch, in place of "Completed staging X for
    office Y" - which said only that the code reached the end of the function.

        Staged 22 of 37 timeseries for SWT in 41.2s (15 with no values in the window)

    Items with nothing staged are tallied by threading_util rather than warned
    about there, because a warning naming a count immediately next to a line
    naming the same count for the same batch is the pattern this was cleaning up.
    When there are any, the whole line rises to WARNING and carries the advice, so
    the severity matches the one tallied outcome that might mean something is
    wrong.

    The ids behind the counts go out at DEBUG, so "which ones?" is still
    answerable without spending a line each on the answer.
    """
    handled = max(total - tally.count(), 0)
    message = f"{action} {handled} of {plural(total, noun)} for {office_id}"

    if elapsed is not None:
        message = f"{message} in {duration(elapsed)}"

    detail = tally.describe()
    if detail:
        message = f"{message} ({detail})"

    if tally.count(NOTHING_STAGED):
        log.warning(
            "%s. If this is unexpected, run the extract phase and publish again.",
            message,
        )
    else:
        log.info(message)

    tally.log_details(log)


def dedupe(items: Iterable[object], key=lambda item: item) -> tuple[list[object], list[object]]:
    """
    Order-preserving dedupe, returning (unique, duplicates).

    Config duplicates were previously invisible: two identical timeseries ids
    meant the same window fetched twice, written to the same file twice and
    published twice, with nothing in the log to say so.
    """
    seen: set[object] = set()
    unique: list[object] = []
    duplicates: list[object] = []

    for item in items:
        item_key = key(item)
        if item_key in seen:
            duplicates.append(item)
            continue

        seen.add(item_key)
        unique.append(item)

    return unique, duplicates


__all__ = [
    "DATE_FORMAT",
    "DEBUG_FORMAT",
    "DISPLAY_FORMAT",
    "EXTRACT",
    "FORMAT",
    "LOAD",
    "Tally",
    "UNKNOWN_DIRECTION",
    "configure",
    "current_phase",
    "dedupe",
    "direction",
    "display",
    "duration",
    "formatter",
    "install_phase_tag",
    "outcome",
    "phase",
    "plural",
    "reformat",
    "shorten_timestamp",
    "window",
]
