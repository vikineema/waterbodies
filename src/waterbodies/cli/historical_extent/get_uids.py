import json
import logging
import os

import click
import numpy as np
from sqlalchemy import select
from sqlalchemy.orm import sessionmaker

from waterbodies.db import get_waterbodies_engine
from waterbodies.historical_extent import create_waterbodies_historical_extent_table
from waterbodies.io import check_directory_exists, get_filesystem
from waterbodies.logs import logging_setup


@click.command(
    name="get-waterbodies-uids",
    help="Get all the waterbody uids for which to update the dynamic attributes",
    no_args_is_help=True,
)
@click.option("-v", "--verbose", default=1, count=True)
@click.option(
    "--max-parallel-steps",
    default=7000,
    type=int,
    help="Maximum number of parallel steps to have in the workflow.",
)
def get_waterbodies_uids(verbose, max_parallel_steps):
    logging_setup(verbose)
    _log = logging.getLogger(__name__)

    engine = get_waterbodies_engine()

    Session = sessionmaker(bind=engine)

    table = create_waterbodies_historical_extent_table(engine=engine)

    with Session.begin() as session:
        uids = session.scalars(select(table.c["uid"])).all()
        _log.info(f"Found {len(uids)} polygon UIDs in the {table.name} table")

    uids_chunks = np.array_split(np.array(uids), max_parallel_steps)
    uids_chunks = [chunk.tolist() for chunk in uids_chunks]
    uids_chunks = list(filter(None, uids_chunks))
    uids_chunks_count = str(len(uids_chunks))
    _log.info(f"{len(uids)} polygon UIDs chunked into {uids_chunks_count} chunks")
    uids_chunks_json_array = json.dumps(uids_chunks)

    output_directory = "/tmp/"
    uids_output_file = os.path.join(output_directory, "uids_chunks")
    uids_count_file = os.path.join(output_directory, "uids_chunks_count")

    fs = get_filesystem(path=output_directory)

    if not check_directory_exists(path=output_directory):
        fs.mkdirs(path=output_directory, exist_ok=True)
        _log.info(f"Created directory {output_directory}")

    with fs.open(uids_output_file, "w") as file:
        file.write(uids_chunks_json_array)
    _log.info(f"UIDs chunks written to {uids_output_file}")

    with fs.open(uids_count_file, "w") as file:
        file.write(uids_chunks_count)
    _log.info(f"UIDs chunks count written to {uids_count_file}")
