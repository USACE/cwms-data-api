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
import pytest

import cwms
import utils.threading_util as threading_util
import rating
from config import RatingConfig


def test_stage_ratings_por(mocker):
    mock_execute = mocker.patch("utils.threading_util.execute_tasks")
    ratings = [RatingConfig(id="SWT.EUFA.Stage;Flow.Standard.Production", enabled=True, raw={"por": True})]

    rating.stage_ratings("SWT", ratings, "2026-01-01", "2026-01-02")

    assert mock_execute.call_count == 1
    assert mock_execute.call_args_list[0].args[0] == rating._download_one_rating
    work_item = mock_execute.call_args_list[0].args[1][0]
    assert work_item[4] is True


def test_rating_config_requires_a_literal_id():
    # There is no longer an "id is None, resolve it from a source" path: ids
    # arrive already resolved, from cda-expander. An entry without one is a
    # config error caught at parse time rather than something the pipeline
    # skips at run time.
    with pytest.raises(KeyError):
        RatingConfig.from_dict({"por": True})


def test_download_one_rating_por(mocker):
    mock_write_json = mocker.patch("utils.filesystem_store.write_json")
    mock_get_ratings_xml = mocker.patch("cwms.get_ratings_xml", return_value="<ratings>xml</ratings>")

    rating._download_one_rating(["SWT", "SWT.EUFA.Stage;Flow.Standard.Production", None, None, True])

    mock_get_ratings_xml.assert_called_once_with("SWT.EUFA.Stage;Flow.Standard.Production", "SWT")
    mock_write_json.assert_called_once_with(
        {"xml": "<ratings>xml</ratings>"},
        "SWT",
        "Ratings",
        "SWT.EUFA.Stage;Flow.Standard.Production.por",
    )


def test_upload_one_rating_uses_xml_only(mocker):
    mock_store_rating = mocker.patch("cwms.store_rating")
    mocker.patch("utils.filesystem_store.read_json", return_value={"xml": "<ratings>xml</ratings>"})

    rating._upload_one_rating(["SWT", "SWT.EUFA.Stage;Flow.Standard.Production", None, None, True])

    mock_store_rating.assert_called_once_with("<ratings>xml</ratings>", store_template=True)


def _api_error(status_code: int, body: str = ""):
    import requests

    response = requests.Response()
    response.status_code = status_code
    response.reason = "Not Found" if status_code == 404 else "Internal Server Error"
    response.url = "https://cda.test/cwms-data/x"
    response._content = body.encode()

    return cwms.api.ApiError(response)


def test_missing_rating_curve_is_not_a_failure(mocker, caplog):
    """
    A resolved rating id that a project has no curve for is ordinary now that a
    whole association category is applied to every project. It must not fail the
    item, because execute_tasks turns a hard failure into an aborted run.
    """
    import logging
    caplog.set_level(logging.DEBUG)
    mocker.patch("cwms.get_ratings_xml", side_effect=_api_error(404, '{"message":"Not found."}'))
    mock_write = mocker.patch("utils.filesystem_store.write_json")

    begin = rating._parse_timestamp("2026-07-01", "start")
    end = rating._parse_timestamp("2026-07-15", "end")

    rating._download_one_rating(["SWT", "EUFA.Elev;Area.Linear.Production", begin, end, False])

    assert "nothing staged" in caplog.text
    mock_write.assert_not_called()


def test_missing_por_rating_curve_is_not_a_failure(mocker, caplog):
    import logging
    caplog.set_level(logging.DEBUG)
    mocker.patch("cwms.get_ratings_xml", side_effect=_api_error(404, '{"message":"Not found."}'))
    mock_write = mocker.patch("utils.filesystem_store.write_json")

    rating._download_one_rating(["SWT", "EUFA.Elev;Stor.Linear.Production", None, None, True])

    assert "nothing staged" in caplog.text
    mock_write.assert_not_called()


def test_rating_server_errors_still_propagate(mocker):
    mocker.patch("cwms.get_ratings_xml", side_effect=_api_error(500, '{"message":"Database Error"}'))
    mocker.patch("utils.filesystem_store.write_json")

    with pytest.raises(cwms.api.ApiError):
        rating._download_one_rating(["SWT", "EUFA.Elev;Area.Linear.Production", None, None, True])


AMBIGUOUS_500 = '{"message":"Failed to process request to retrieve RatingSet"}'


def test_ambiguous_500_is_treated_as_a_missing_rating(mocker, caplog):
    """
    CDA answers 500 rather than 404 for a rating that does not exist:
    RatingController.getOne catches RatingException and maps it to 500. The
    rating *spec* endpoint returns 404 correctly, but the values endpoint does
    not, so a missing rating has to be inferred from the 500.
    """
    import logging
    caplog.set_level(logging.WARNING)
    mocker.patch("cwms.get_ratings_xml", side_effect=_api_error(500, AMBIGUOUS_500))
    mock_write = mocker.patch("utils.filesystem_store.write_json")

    rating._download_one_rating(["SWT", "EUFA.Elev;Area.Linear.Production", None, None, True])

    mock_write.assert_not_called()
    assert "no rating curve" in caplog.text.lower()


def test_ambiguous_500_is_reported_at_warning_not_info(mocker, caplog):
    """
    Unlike a true 404 this is a guess, so it must be louder - the same 500 is
    also produced by genuine processing failures.
    """
    import logging
    caplog.set_level(logging.DEBUG)
    mocker.patch("cwms.get_ratings_xml", side_effect=_api_error(500, AMBIGUOUS_500))
    mocker.patch("utils.filesystem_store.write_json")

    rating._download_one_rating(["SWT", "EUFA.Elev;Area.Linear.Production", None, None, True])

    levels = {r.levelname for r in caplog.records if "no rating curve" in r.getMessage().lower()}
    assert levels == {"WARNING"}


def test_an_unrelated_500_still_fails(mocker):
    """
    Only CDA's specific RatingSet message is treated as missing data. Any other
    500 - an outage, a pool problem - must still fail the item.
    """
    mocker.patch(
        "cwms.get_ratings_xml",
        side_effect=_api_error(500, '{"message":"Database Error"}'),
    )
    mocker.patch("utils.filesystem_store.write_json")

    with pytest.raises(cwms.api.ApiError):
        rating._download_one_rating(["SWT", "EUFA.Elev;Area.Linear.Production", None, None, True])


def test_every_rating_failing_ambiguously_is_flagged(mocker, caplog):
    """
    One project lacking a rating curve is ordinary. Every rating in the batch
    failing this way looks far more like an unwell instance, and because the 500
    is indistinguishable from "missing" the run would otherwise report success
    having staged nothing.
    """
    import logging
    caplog.set_level(logging.WARNING)
    mocker.patch("cwms.get_ratings_xml", side_effect=_api_error(500, AMBIGUOUS_500))
    mocker.patch("utils.filesystem_store.write_json")
    threading_util.init_executor(2)

    rating.stage_ratings(
        "SWT",
        [
            RatingConfig(id="EUFA.Elev;Area.Linear.Production", enabled=True, raw={"por": True}),
            RatingConfig(id="EUFA.Elev;Stor.Linear.Production", enabled=True, raw={"por": True}),
        ],
        "2026-07-01",
        "2026-07-15",
    )

    assert "more consistent with the service being unwell" in caplog.text


def test_a_partial_ambiguous_batch_is_not_flagged(mocker, caplog):
    """
    Some ratings missing and others present is the normal picture, so it must not
    trip the service-health warning.
    """
    import logging
    caplog.set_level(logging.WARNING)

    def maybe(rating_id, office_id, begin=None, end=None):
        if "Elev;Area" in rating_id:
            raise _api_error(500, AMBIGUOUS_500)
        return "<ratings/>"

    mocker.patch("cwms.get_ratings_xml", side_effect=maybe)
    mocker.patch("utils.filesystem_store.write_json")
    threading_util.init_executor(2)

    rating.stage_ratings(
        "SWT",
        [
            RatingConfig(id="EUFA.Elev;Area.Linear.Production", enabled=True, raw={"por": True}),
            RatingConfig(id="EUFA.Elev;Stor.Linear.Production", enabled=True, raw={"por": True}),
        ],
        "2026-07-01",
        "2026-07-15",
    )

    assert "more consistent with the service being unwell" not in caplog.text


def test_a_single_ambiguous_rating_is_not_flagged(mocker, caplog):
    """A one-item batch carries no signal, so it should not raise suspicion."""
    import logging
    caplog.set_level(logging.WARNING)
    mocker.patch("cwms.get_ratings_xml", side_effect=_api_error(500, AMBIGUOUS_500))
    mocker.patch("utils.filesystem_store.write_json")
    threading_util.init_executor(2)

    rating.stage_ratings(
        "SWT",
        [RatingConfig(id="EUFA.Elev;Area.Linear.Production", enabled=True, raw={"por": True})],
        "2026-07-01",
        "2026-07-15",
    )

    assert "more consistent with the service being unwell" not in caplog.text


def test_the_ambiguous_counter_resets_between_batches(mocker, caplog):
    import logging
    caplog.set_level(logging.WARNING)
    mocker.patch("cwms.get_ratings_xml", side_effect=_api_error(500, AMBIGUOUS_500))
    mocker.patch("utils.filesystem_store.write_json")
    threading_util.init_executor(2)

    items = [RatingConfig(id="EUFA.Elev;Area.Linear.Production", enabled=True, raw={"por": True})]
    rating.stage_ratings("SWT", items, "2026-07-01", "2026-07-15")
    rating.stage_ratings("SWT", items, "2026-07-01", "2026-07-15")

    # Two one-item batches, not one two-item batch, so still no warning.
    assert "more consistent with the service being unwell" not in caplog.text


def test_ratings_are_stored_with_their_template(mocker):
    """
    store_template must be True. With False, CDA strips <rating-template> out of
    the XML (RatingController.removeTemplate) before storing, leaving a
    rating-spec that references a template the request no longer carries. A
    destination that has never seen that template answers 500 "Database Error" -
    which is what a first publish to a fresh CDA hit.

    True is also cwms-python's default and CDA's documented default; this was the
    one place the pipeline opted out.
    """
    mock_store = mocker.patch("cwms.store_rating")
    mocker.patch(
        "utils.filesystem_store.read_json",
        return_value={"xml": "<ratings><rating-template/><rating-spec/></ratings>"},
    )

    rating._upload_one_rating(["SWT", "EUFA.Elev;Area.Linear.Production", None, None, True])

    assert mock_store.call_args.kwargs["store_template"] is True


def test_the_windowed_path_also_stores_the_template(mocker):
    mock_store = mocker.patch("cwms.store_rating")
    mocker.patch("utils.filesystem_store.read_json", return_value={"xml": "<ratings/>"})

    begin = rating._parse_timestamp("2026-07-01", "start")
    end = rating._parse_timestamp("2026-07-15", "end")

    rating._upload_one_rating(["SWT", "EUFA.Elev;Area.Linear.Production", begin, end, False])

    assert mock_store.call_args.kwargs["store_template"] is True


def test_nothing_configured_is_not_a_warning(caplog):
    import logging
    caplog.set_level(logging.DEBUG)

    rating.stage_ratings("SWT", [], "2026-06-01", "2026-08-03")
    rating.publish_staged_ratings("SWT", [], "2026-06-01", "2026-08-03")

    assert not [r for r in caplog.records if r.levelno >= logging.WARNING]


def test_the_inferred_missing_rating_says_it_is_a_guess(caplog):
    """
    This 500 is indistinguishable from a genuine processing failure, so a reader
    who takes "no rating curve" at face value will not know to check. That was the
    one thing the line did not say.
    """
    import logging
    caplog.set_level(logging.WARNING)
    error = _api_error(500, AMBIGUOUS_500)

    assert rating._handle_missing_rating(error, "EUFA.Stage;Flow.Standard.Production", "SWT", "") is True

    message = caplog.text

    assert "guess" in message
    assert "500" in message
    assert "RatingController" not in message
