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
import logging
import threading
import time
from datetime import datetime
from typing import Iterable, Any

import cwms
import utils.filesystem_store as filesystem_store
import utils.cda_errors as cda_errors
import utils.log_util as log_util
import utils.threading_util as threading_util
from config import RatingConfig

logger = logging.getLogger(__name__)
DATE_TIME_FORMAT = "%Y-%m-%d %H.%M.%S"
RATINGS_FOLDER = "Ratings"

_NO_CURVE = "with no rating curve"
_NO_CURVE_INFERRED = "inferred absent from an ambiguous 500"

_tally = log_util.Tally()


def _start_batch() -> log_util.Tally:
    global _tally
    _tally = log_util.Tally()

    return _tally


def _label(work_item) -> str:
    if work_item[4]:
        return f"{work_item[1]} [POR]"

    return f"{work_item[1]} [{log_util.window(work_item[2], work_item[3])}]"

_AMBIGUOUS_LOCK = threading.Lock()
_AMBIGUOUS_COUNT: list[int] = []

def _reset_ambiguous_skips() -> None:
    with _AMBIGUOUS_LOCK:
        _AMBIGUOUS_COUNT.clear()


def _record_ambiguous_skip() -> None:
    with _AMBIGUOUS_LOCK:
        _AMBIGUOUS_COUNT.append(1)


def _ambiguous_skip_count() -> int:
    with _AMBIGUOUS_LOCK:
        return len(_AMBIGUOUS_COUNT)


def _warn_if_every_rating_was_ambiguous(office_id: str, attempted: int) -> None:
    skipped = _ambiguous_skip_count()

    if attempted > 1 and skipped == attempted:
        logger.warning(
            "All %d rating(s) for office %s were skipped on the ambiguous 500 from the ratings "
            "values endpoint.",
            attempted,
            office_id,
        )


def _handle_missing_rating(error: Exception, rating_id: str, office_id: str, window: str) -> bool:
    if cda_errors.is_no_data(error):
        logger.debug("No rating curve for %s in office %s%s; nothing staged.", rating_id, office_id, window)
        _tally.record(_NO_CURVE, rating_id)
        return True

    if cda_errors.is_ambiguous_rating_failure(error):
        _record_ambiguous_skip()
        logger.warning(
            "No rating curve for %s in office %s%s; nothing staged.",
            rating_id,
            office_id,
            window,
        )
        _tally.record(_NO_CURVE_INFERRED, rating_id)
        return True

    return False


def stage_ratings(
    office_id: str,
    ratings: Iterable[RatingConfig],
    default_start: str | None,
    default_end: str | None,
) -> None:
    ratings = list(ratings)
    work_items = _build_rating_work_items(office_id, ratings, default_start, default_end)
    if not work_items:
        logger.debug("No ratings configured for office %s; nothing to extract.", office_id)
        return

    tally = _start_batch()
    _reset_ambiguous_skips()
    started = time.monotonic()
    threading_util.execute_tasks(_download_one_rating, work_items, label=_label, tally=tally)
    _warn_if_every_rating_was_ambiguous(office_id, len(work_items))
    log_util.outcome(
        logger,
        action="Staged",
        noun="rating",
        total=len(work_items),
        tally=tally,
        office_id=office_id,
        elapsed=time.monotonic() - started,
    )


def publish_staged_ratings(
    office_id: str,
    ratings: Iterable[RatingConfig],
    default_start: str | None,
    default_end: str | None,
) -> None:
    ratings = list(ratings)
    work_items = _build_rating_work_items(office_id, ratings, default_start, default_end)
    if not work_items:
        logger.debug("No ratings configured for office %s; nothing to load.", office_id)
        return

    tally = _start_batch()
    started = time.monotonic()
    threading_util.execute_tasks(_upload_one_rating, work_items, label=_label, tally=tally)
    log_util.outcome(
        logger,
        action="Published",
        noun="rating",
        total=len(work_items),
        tally=tally,
        office_id=office_id,
        elapsed=time.monotonic() - started,
    )


def _download_one_rating(work_item: list[object]) -> None:
    office_id = work_item[0]
    rating_id = work_item[1]
    begin = work_item[2]
    end = work_item[3]
    por = work_item[4]

    if por:
        logger.info("Extracting rating XML (POR) for %s in office %s", rating_id, office_id)
        try:
            with cda_errors.ratings_request():
                rating_xml = cwms.get_ratings_xml(rating_id, office_id)
        except Exception as error:
            if not _handle_missing_rating(error, rating_id, office_id, ""):
                raise

            return

        filesystem_store.write_xml(_xml_text(rating_xml), office_id, RATINGS_FOLDER, rating_id)
        return

    logger.info(
        "Extracting rating XML %s for office %s [%s]",
        rating_id,
        office_id,
        log_util.window(begin, end),
    )
    try:
        with cda_errors.ratings_request():
            rating_xml = cwms.get_ratings_xml(rating_id, office_id, begin=begin, end=end)
    except Exception as error:
        if not _handle_missing_rating(
            error, rating_id, office_id, f" between {log_util.window(begin, end)}"
        ):
            raise

        return

    filesystem_store.write_xml(_xml_text(rating_xml), office_id, RATINGS_FOLDER, rating_id)


def _upload_one_rating(work_item: list[object]) -> None:
    office_id = work_item[0]
    rating_id = work_item[1]
    rating_xml = filesystem_store.read_xml(office_id, RATINGS_FOLDER, rating_id)
    if not rating_xml:
        raise FileNotFoundError(
            "No staged rating data found."
        )

    cwms.api.post(
        "ratings",
        rating_xml,
        {"store-template": True, "fail-if-exists": False},
        api_version=102,
    )


def _xml_text(xml_value: Any) -> str:
    if isinstance(xml_value, bytes):
        return xml_value.decode("utf-8")

    return str(xml_value)


def _build_rating_work_items(
    office_id: str,
    ratings: Iterable[RatingConfig],
    default_start: str | None,
    default_end: str | None,
) -> list[list[object]]:
    work_items: list[list[object]] = []

    for rating in ratings:
        rating_id = rating.id
        por = rating.period_of_record
        begin = None if por else _parse_timestamp(rating.start_time or default_start, "start")
        end = None if por else _parse_timestamp(rating.end_time or default_end, "end")
        work_items.append([office_id, rating_id, begin, end, por])

    return work_items


def _parse_timestamp(value: str | None, label: str) -> datetime:
    if value is None:
        raise ValueError(f"Missing {label} time for rating processing.")

    normalized = value.strip()
    if normalized.lower() == "now":
        return datetime.now()

    try:
        return datetime.fromisoformat(normalized)
    except ValueError as exc:
        try:
            return datetime.strptime(normalized, "%Y-%m-%d")
        except ValueError:
            raise ValueError(f"Invalid {label} time '{value}'. Use ISO-8601 or YYYY-MM-DD.") from exc


__all__ = ["publish_staged_ratings", "stage_ratings"]
