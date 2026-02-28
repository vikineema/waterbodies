import json
import logging
import os
from datetime import datetime
from itertools import chain

import click
from sqlalchemy import exists, select, update
from sqlalchemy.orm import sessionmaker

from waterbodies.db import get_waterbodies_engine
from waterbodies.historical_extent import (
    create_waterbodies_historical_extent_table,
    get_waterbody_timeseries,
)
from waterbodies.io import check_directory_exists, get_filesystem
from waterbodies.logs import logging_setup
from waterbodies.utils import TaskFailedError


@click.command(
    name="update-dynamic-attrs",
    help="Update the dynamic attributes of a list of waterbodies.",
    no_args_is_help=True,
)
@click.option("-v", "--verbose", default=1, count=True)
@click.option(
    "--uids-list-file",
    type=str,
    help="Path to the text file containing the list of waterbody uids.",
)
@click.option(
    "--start-date",
    type=str,
    default="1984-01-01",
    show_default=True,
    help=("Start date of the time range to query a waterbody's timeseries for. "),
)
def update_dynamic_attrs(verbose, uids_list_file, start_date):
    logging_setup(verbose)
    _log = logging.getLogger(__name__)

    engine = get_waterbodies_engine()

    Session = sessionmaker(bind=engine)

    table = create_waterbodies_historical_extent_table(engine=engine)

    end_date = datetime.now().strftime("%Y-%m-%d")

    # The date when attributes in the database were last updated
    last_attrs_update_date = datetime.today().strftime("%Y-%m-%d")

    fs = get_filesystem(path=uids_list_file, anon=True)
    with fs.open(uids_list_file) as file:
        content = file.read()
        decoded_content = content.decode()
        uids = json.loads(decoded_content)

    # In case file contains list of lists
    if all(isinstance(item, list) for item in uids):
        uids = list(chain(*uids))
    else:
        pass

    failed_uids = []
    for idx, uid in enumerate(uids):
        _log.info(f"Updating dynamic attributes for waterbody: {uid}   {idx + 1}/{len(uids)}")

        try:
            # Check the uid provided exists in the waterbodies_historical_extent table.
            with Session.begin() as session:
                # Query to check if the specific uid exists in the table
                exists_query = select(exists().where(table.c.uid == uid))
                exists_result = session.execute(exists_query).scalar()

            if not exists_result:
                e = ValueError(
                    f"UID {uid} does not exist in the waterbodies_historical_extent table"
                )
                _log.error(e)
                raise e

            _log.info(
                f"Loading timeseries for waterbody {uid} for the time range "
                f"{start_date} to {end_date} ..."
            )
            timeseries = get_waterbody_timeseries(
                engine=engine, uid=uid, start_date=start_date, end_date=end_date
            ).sort_values(by="date")

            if timeseries.empty:
                _log.info(
                    f"No observations available for waterbody {uid} "
                    f"for the time range {start_date} to {end_date}"
                )
                update_statement = (
                    update(table)
                    .where(table.c.uid == uid)
                    .values(last_attrs_update_date=last_attrs_update_date)
                )
            else:
                valid_timeseries = timeseries[
                    (timeseries["percent_observed"] > 85) & (timeseries["percent_invalid"] < 5)
                ].sort_values(by="date")

                if valid_timeseries.empty:
                    _log.info(
                        f"No valid observations available for waterbody {uid} "
                        f"for the time range {start_date} to {end_date}"
                    )
                    update_statement = (
                        update(table)
                        .where(table.c.uid == uid)
                        .values(last_attrs_update_date=last_attrs_update_date)
                    )
                else:
                    _log.info(f"Updating last valid wet observation for waterbody {uid}")

                    # Most recent date the waterbody was observed
                    last_obs_date = timeseries.date.iloc[-1].strftime("%Y-%m-%d")

                    # Most recent date a valid wet observation was recorded for the waterbody.
                    last_valid_obs_date = valid_timeseries.date.iloc[-1].strftime("%Y-%m-%d")
                    # Most recent valid wet observation for the waterbody (%)
                    last_valid_obs = valid_timeseries.percent_wet.iloc[-1]

                    # Update the attributes
                    update_statement = (
                        update(table)
                        .where(table.c.uid == uid)
                        .values(
                            dict(
                                last_obs_date=last_obs_date,
                                last_valid_obs_date=last_valid_obs_date,
                                last_valid_obs=last_valid_obs,
                                last_attrs_update_date=last_attrs_update_date,
                            )
                        )
                    )

            with Session.begin() as session:
                session.execute(update_statement)
        except Exception as error:
            _log.exception(error)
            _log.error(f"Failed to update dynamic attributes for waterbody {uid}")
            failed_uids.append(uid)

    if failed_uids:
        failed_uids_json_array = json.dumps(failed_uids)

        output_directory = "/tmp/"
        failed_uids_output_file = os.path.join(output_directory, "failed_uids")

        fs = get_filesystem(path=output_directory, anon=False)

        if not check_directory_exists(path=output_directory):
            fs.mkdirs(path=output_directory, exist_ok=True)
            _log.info(f"Created directory {output_directory}")

        with fs.open(failed_uids_output_file, "a") as file:
            file.write(failed_uids_json_array + "\n")
        _log.info(f"Failed uids written to {failed_uids_output_file}")

        raise TaskFailedError(
            f"Dynamic attributes for {len(failed_uids)} \
                              waterbodies failed to update."
        )
