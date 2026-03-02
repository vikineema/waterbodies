import json
import logging
import os
import subprocess
from pathlib import Path

import pandas as pd
import pytest
from click.testing import CliRunner

from waterbodies.cli.surface_area_change.generate_tasks import generate_tasks
from waterbodies.cli.surface_area_change.process_tasks import process_tasks
from waterbodies.db import get_test_waterbodies_engine
from waterbodies.io import get_filesystem

TEST_DATA_DIR = Path(__file__).parent / "data"
WATERBODIES_S3_URI = "s3://deafrica-services/waterbodies"
WATERBODIES_LATEST_VERSION = "v0.0.3"
OUTPUT_DIR = Path("/tmp/output")

_log = logging.getLogger(__name__)


@pytest.fixture
def runner():
    return CliRunner(echo_stdin=True)


@pytest.fixture
def reference_task():
    task = {
        "solar_day": "2016-04-05",
        "tile_index_x": 199,
        "tile_index_y": 35,
        "task_datasets_ids": [
            "180340f9-b365-506b-b561-153af5b1490d",
            "9b916e21-2229-5121-8333-0a8b3736d440",
        ],
    }
    return task


def test_generate_tasks_cli_backlog_processing(
    runner, reference_task, capsys: pytest.CaptureFixture
):
    expected_result = [reference_task]
    run_type = "backlog-processing"
    temporal_range = "2016-04-05--P1D"
    historical_extent_rasters_directory = str(TEST_DATA_DIR / "historical_extent_rasters_directory")
    max_parallel_steps = 7000
    args = [
        "--verbose",
        f"--temporal-range={temporal_range}",
        f"--run-type={run_type}",
        f"--historical-extent-rasters-directory={historical_extent_rasters_directory}",
        f"--max-parallel-steps={max_parallel_steps}",
    ]

    with capsys.disabled() as disabled:  # noqa F841
        result = runner.invoke(generate_tasks, args=args, catch_exceptions=True)

    assert result.exit_code == 0

    bash_command = "cat /tmp/tasks_chunks | jq '.[0]'"
    task_list_str = subprocess.check_output(bash_command, shell=True, universal_newlines=True)
    task_list = json.loads(task_list_str)

    assert task_list[0]["solar_day"] == expected_result[0]["solar_day"]
    assert task_list[0]["tile_index_x"] == expected_result[0]["tile_index_x"]
    assert task_list[0]["tile_index_y"] == expected_result[0]["tile_index_y"]
    assert sorted(task_list[0]["task_datasets_ids"]) == sorted(
        expected_result[0]["task_datasets_ids"]
    )


def test_process_tasks_cli_backlog_processing(runner, capsys: pytest.CaptureFixture):
    os.environ["TestingMode"] = "True"

    run_type = "backlog-processing"
    tasks_list_file = str(TEST_DATA_DIR / "surface_area_change" / "tasks_chunks")
    historical_extent_rasters_directory = str(TEST_DATA_DIR / "historical_extent_rasters_directory")

    args = [
        "--verbose",
        f"--run-type={run_type}",
        f"--tasks-list-file={tasks_list_file}",
        f"--historical-extent-rasters-directory={historical_extent_rasters_directory}",
        "--overwrite",
    ]
    with capsys.disabled() as disabled:  # noqa F841
        result = runner.invoke(process_tasks, args=args, catch_exceptions=True)

    _log.info(result)

    assert result.exit_code == 0

    sql_query = "SELECT * FROM waterbodies_observations"
    engine = get_test_waterbodies_engine()
    produced = pd.read_sql(con=engine, sql=sql_query)

    expected = pd.read_parquet(
        TEST_DATA_DIR / "surface_area_change" / "waterbodies_observations.parquet"
    )
    pd.testing.assert_frame_equal(produced, expected)

    # Clean up
    fs = get_filesystem(engine.url.database, anon=True)
    # Outputs of generate_tasks
    fs.rm("/tmp/tasks_chunks")
    fs.rm("/tmp/tasks_chunks_count")
    # Outputs of process_tasks
    fs.rm(engine.url.database, recursive=True)
