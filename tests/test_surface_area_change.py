import json
import logging
import os

import pytest
from click.testing import CliRunner

from waterbodies.cli.surface_area_change.generate_tasks import generate_tasks
from waterbodies.cli.surface_area_change.process_task import process_task

_log = logging.getLogger(__name__)


@pytest.fixture
def runner():
    return CliRunner(echo_stdin=True)


@pytest.fixture
def reference_task():
    task = {
        "solar_day": "2016-04-05",
        "tile_id_x": 199,
        "tile_id_y": 35,
        "task_datasets_ids": [
            "180340f9-b365-506b-b561-153af5b1490d",
            "9b916e21-2229-5121-8333-0a8b3736d440",
        ],
    }
    return task


@pytest.fixture
def set_up_test_env():
    os.environ["TestingMode"] = "True"


def test_generate_tasks_cli_backlog_processing(
    runner, reference_task, capsys: pytest.CaptureFixture
):
    expected_result = [reference_task]
    run_type = "backlog-processing"
    temporal_range = "2016-04-05--P1D"
    historical_extent_rasters_directory = "tests/data/historical_extent_rasters_directory"
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


@pytest.mark.skip(reason="Cant figure out how inputs are passed")
def test_process_tasks_cli_backlog_processing(
    set_up_test_env, reference_task, runner, capsys: pytest.CaptureFixture
):

    run_type = "backlog-processing"
    solar_day = reference_task["solar_day"]
    tile_id_x = reference_task["tile_id_x"]
    tile_id_y = reference_task["tile_id_y"]
    task_datasets_ids = 'reference_task["task_datasets_ids"]'
    historical_extent_rasters_directory = "tests/data/historical_extent_rasters_directory"

    args = [
        "--verbose",
        f"--run-type={run_type}",
        f"--solar-day={solar_day}",
        f"--tile-id-x={tile_id_x}",
        f"--tile-id-y={tile_id_y}",
        f"--task-datasets-ids='{task_datasets_ids}'",
        f"--historical-extent-rasters-directory={historical_extent_rasters_directory}",
        "--overwrite",
    ]
    with capsys.disabled() as disabled:  # noqa F841
        result = runner.invoke(process_task, args=args, catch_exceptions=True)

    _log.info(result)
