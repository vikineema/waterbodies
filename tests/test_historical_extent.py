import json
import logging
import os
import subprocess
from pathlib import Path

import geopandas as gpd
import pytest
from click.testing import CliRunner
from geopandas import testing as gpd_testing

from waterbodies.cli.historical_extent.generate_tasks import generate_tasks
from waterbodies.cli.historical_extent.process_polygons import process_polygons
from waterbodies.cli.historical_extent.process_tasks import process_tasks
from waterbodies.db import get_test_waterbodies_engine
from waterbodies.historical_extent import load_waterbodies_from_db
from waterbodies.io import check_file_exists, get_filesystem

TEST_DATA_DIR = Path(__file__).parent / "data"
WATERBODIES_S3_URI = "s3://deafrica-services/waterbodies"
WATERBODIES_LATEST_VERSION = "v0.0.3"
OUTPUT_DIR = Path("/tmp/output")

_log = logging.getLogger(__name__)


@pytest.fixture
def runner():
    return CliRunner(echo_stdin=True)


@pytest.fixture
def reference_tasks():
    tasks = [
        {
            "tile_index_x": 210,
            "tile_index_y": 76,
            "task_datasets_ids": ["328a32d7-d85a-5dc8-b795-d119fc30c1ad"],
        },
        {
            "tile_index_x": 211,
            "tile_index_y": 77,
            "task_datasets_ids": ["d17a6f00-a198-5725-a089-4279cd321463"],
        },
        {
            "tile_index_x": 211,
            "tile_index_y": 76,
            "task_datasets_ids": ["475d22b9-f889-5d4e-b6fa-15ae72bf5fdb"],
        },
    ]

    return tasks


def test_generate_tasks_cli(runner, reference_tasks, capsys: pytest.CaptureFixture):
    max_parallel_steps = 1

    args = [
        f"--max-parallel-steps={max_parallel_steps}",
    ]
    with capsys.disabled() as disabled:  # noqa F841
        result = runner.invoke(generate_tasks, args=args, catch_exceptions=True)

    assert result.exit_code == 0

    bash_command = "cat /tmp/tasks_chunks | jq '.[0]'"
    task_list_str = subprocess.check_output(bash_command, shell=True, universal_newlines=True)
    task_list = json.loads(task_list_str)

    assert len(task_list) == len(reference_tasks)
    for task in task_list:
        assert task in reference_tasks


def test_process_tasks_cli(runner, capsys: pytest.CaptureFixture):
    tasks_list_file = "/tmp/tasks_chunks"
    land_sea_mask_rasters_directory = (
        f"{WATERBODIES_S3_URI}/{WATERBODIES_LATEST_VERSION}/hydrosheds_v1_1_land_mask/"
    )
    output_directory = str(OUTPUT_DIR)
    args = [
        "--verbose",
        f"--tasks-list-file={tasks_list_file}",
        f"--land-sea-mask-rasters-directory={land_sea_mask_rasters_directory}",
        f"--output-directory={output_directory}",
        "--no-overwrite",
    ]
    with capsys.disabled() as disabled:  # noqa F841
        result = runner.invoke(process_tasks, args=args, catch_exceptions=True)

    assert result.exit_code == 0

    # Actual files are produced
    assert check_file_exists(os.path.join(output_directory, "waterbodies_x210_y076.parquet"))
    assert check_file_exists(os.path.join(output_directory, "waterbodies_x211_y076.parquet"))

    tile_to_check = "waterbodies_x210_y076.parquet"
    produced = gpd.read_parquet(os.path.join(output_directory, tile_to_check))
    expected = gpd.read_parquet(TEST_DATA_DIR / "historical_extent" / "per_tile" / tile_to_check)

    gpd_testing.assert_geodataframe_equal(produced, expected)

    # Clean up
    fs = get_filesystem(output_directory, anon=True)
    # Outputs of generate_tasks
    fs.rm("/tmp/tasks_chunks", recursive=True)
    fs.rm("/tmp/tasks_chunks_count", recursive=True)
    # Outputs of process_tasks
    fs.rm(output_directory, recursive=True)


def test_process_polygons_cli(runner, capsys: pytest.CaptureFixture):
    # Note that the waterbody with the uuid kxvn5n709s in the expected output parquet
    # file, are meant to be two waterbodies as shown in the Waterbodies Historical Extent
    # v0.0.3. TODO: Need to figure out what has changed that these two are not being
    # segmented during the process_tasks or process_polygons steps into two seperate
    # waterbodies kxvjcx58ey (Lake Edward) and s8j2nbrhwp (Lake George).

    os.environ["TestingMode"] = "True"

    polygons_directory = str(TEST_DATA_DIR / "historical_extent" / "per_tile")
    args = [
        "--verbose",
        f"--polygons-directory={polygons_directory}",
    ]
    with capsys.disabled() as disabled:  # noqa F841
        result = runner.invoke(process_polygons, args=args, catch_exceptions=True)

    assert result.exit_code == 0

    engine = get_test_waterbodies_engine()
    produced = load_waterbodies_from_db(engine)

    expected = gpd.read_parquet(
        TEST_DATA_DIR / "historical_extent" / "processed_waterbodies.parquet"
    )

    gpd_testing.assert_geodataframe_equal(produced, expected)

    # Clean up for historical extent tests
    fs = get_filesystem(engine.url.database, anon=True)
    fs.rm(engine.url.database, recursive=True)
