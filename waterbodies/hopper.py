import datetime

from datacube import Datacube
from datacube.model import Dataset


def find_datasets_by_creation_date(
    product: str,
    start_date: datetime.date,
    end_date: datetime.date,
    dc: Datacube = None,
) -> list[Dataset]:

    if dc is None:
        dc = Datacube()

    assert end_date >= start_date

    creation_time_range = (start_date, end_date)

    datasets = dc.find_datasets(product=[product], creation_time=creation_time_range)

    return datasets
