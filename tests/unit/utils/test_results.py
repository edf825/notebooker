from mock import sentinel, patch, MagicMock

from man.notebooker import constants
import man.notebooker.utils.results as results


def test_get_latest_job_results():
    serializer = MagicMock()
    serializer.get_latest_job_id_for_name_and_params.return_value = sentinel.latest_job_id
    with patch('man.notebooker.utils.results._get_job_results', return_value=sentinel.result) as get_results:
        result = results.get_latest_job_results(
            sentinel.report_name,
            sentinel.report_params,
            serializer,
            sentinel.retrying,
            sentinel.ignore_cache
        )
    assert result == sentinel.result
    get_results.assert_called_once_with(
        sentinel.latest_job_id,
        sentinel.report_name,
        serializer,
        sentinel.retrying,
        sentinel.ignore_cache
    )
    serializer.get_latest_job_id_for_name_and_params.assert_called_once_with(
        sentinel.report_name,
        sentinel.report_params
    )


def test_missing_latest_job_results():
    serializer = MagicMock()
    serializer.get_latest_job_id_for_name_and_params.return_value = None
    with patch('man.notebooker.utils.results._get_job_results') as get_results:
        result = results.get_latest_job_results(
            sentinel.report_name,
            sentinel.report_params,
            serializer,
            sentinel.retrying,
            sentinel.ignore_cache
        )
    get_results.assert_not_called()
    assert isinstance(result, constants.NotebookResultError)
    assert result.report_name == sentinel.report_name
    assert result.job_id is None
    assert result.overrides == sentinel.report_params