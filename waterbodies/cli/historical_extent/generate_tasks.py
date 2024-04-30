import json
import logging
import os

import click
import numpy as np
from datacube import Datacube

from waterbodies.hopper import create_tasks_from_datasets
from waterbodies.io import check_directory_exists, get_filesystem
from waterbodies.logs import logging_setup
from waterbodies.text import format_task


@click.command(
    name="generate-tasks", help="Generate historical extent tasks to run.", no_args_is_help=True
)
@click.option("-v", "--verbose", default=1, count=True)
@click.option(
    "--max-parallel-steps",
    default=7000,
    type=int,
    help="Maximum number of parallel steps to have in the workflow.",
)
def generate_tasks(
    verbose,
    max_parallel_steps,
):
    logging_setup(verbose)
    _log = logging.getLogger(__name__)

    dc = Datacube(app="generate-tasks")

    # Find all the WOfS All Time Summaries datasets
    dc_query = dict(product="wofs_ls_summary_alltime")
    datasets = dc.find_datasets(**dc_query)
    _log.info(f"Found {len(datasets)} datasets matching the query {dc_query}")

    tasks = create_tasks_from_datasets(
        datasets=datasets, tile_index_filter=None, bin_solar_day=False
    )
    tasks = [format_task(task) for task in tasks]
    sorted_tasks = sorted(tasks, key=lambda x: x["tile_index_x"])
    _log.info(f"Total number of tasks: {len(sorted_tasks)}")

    task_chunks = np.array_split(np.array(sorted_tasks), max_parallel_steps)
    task_chunks = [chunk.tolist() for chunk in task_chunks]
    task_chunks = list(filter(None, task_chunks))
    task_chunks_count = str(len(task_chunks))
    _log.info(f"{len(sorted_tasks)} tasks chunked into {task_chunks_count} chunks")
    task_chunks_json_array = json.dumps(task_chunks)

    tasks_directory = "/tmp/"
    tasks_output_file = os.path.join(tasks_directory, "tasks_chunks")
    tasks_count_file = os.path.join(tasks_directory, "tasks_chunks_count")

    fs = get_filesystem(path=tasks_directory)

    if not check_directory_exists(path=tasks_directory):
        fs.mkdirs(path=tasks_directory, exist_ok=True)
        _log.info(f"Created directory {tasks_directory}")

    with fs.open(tasks_output_file, "w") as file:
        file.write(task_chunks_json_array)
    _log.info(f"Tasks chunks written to {tasks_output_file}")

    with fs.open(tasks_count_file, "w") as file:
        file.write(task_chunks_count)
    _log.info(f"Tasks chunks count written to {tasks_count_file}")
