import json
import logging

import pytest
from click.testing import CliRunner

from waterbodies.cli.surface_area_change.generate_tasks import generate_tasks

_log = logging.getLogger(__name__)


@pytest.fixture
def runner():
    return CliRunner(echo_stdin=True)


def test_generate_tasks_cli_backlog_processing(runner, capsys: pytest.CaptureFixture):
    expected_result = [
        {
            "solar_day": "2016-04-05",
            "tile_id_x": 199,
            "tile_id_y": 35,
            "task_datasets_ids": [
                "180340f9-b365-506b-b561-153af5b1490d",
                "9b916e21-2229-5121-8333-0a8b3736d440",
            ],
        }
    ]

    run_type = "backlog-processing"
    temporal_range = "2016-04-05--P1D"
    historical_extent_rasters_directory = "tests/data/"
    args = [
        "--verbose",
        f"--temporal-range={temporal_range}",
        f"--run-type={run_type}",
        f"--historical-extent-rasters-directory={historical_extent_rasters_directory}",
    ]

    with capsys.disabled() as disabled:  # noqa F841
        result = runner.invoke(generate_tasks, args=args, catch_exceptions=True)

    result_output = json.loads(result.output.split("\n")[-1])

    assert result_output[0]["solar_day"] == expected_result[0]["solar_day"]
    assert result_output[0]["tile_id_x"] == expected_result[0]["tile_id_x"]
    assert result_output[0]["tile_id_y"] == expected_result[0]["tile_id_y"]
    assert sorted(result_output[0]["task_datasets_ids"]) == sorted(
        expected_result[0]["task_datasets_ids"]
    )
