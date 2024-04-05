import os
import re


def get_tile_id_tuple_from_str(string_: str) -> tuple[int, int]:
    """
    Get the tile id (x,y) from a string.

    Parameters
    ----------
    string_ : str
        String to search for a tile id

    Returns
    -------
    tuple[int, int]
        Found tile id (x,y).
    """
    x_id_pattern = re.compile(r"x\d{3}")
    y_id_pattern = re.compile(r"y\d{3}")

    tile_id_x_str = re.search(x_id_pattern, string_).group(0)
    tile_id_y_str = re.search(y_id_pattern, string_).group(0)

    tile_id_x = int(tile_id_x_str.lstrip("x"))
    tile_id_y = int(tile_id_y_str.lstrip("y"))

    tile_id = (tile_id_x, tile_id_y)

    return tile_id


def get_tile_id_str_from_tuple(tile_id_tuple: tuple[int, int]) -> str:

    tile_id_x, tile_id_y = tile_id_tuple

    tile_id_str = f"x{tile_id_x:03d}_y{tile_id_y:03d}"

    return tile_id_str


def get_tile_id_tuple_from_filename(file_path: str) -> tuple[int, int]:
    """
    Search for a tile id in the base name of a file.

    Parameters
    ----------
    file_path : str
        File path to search tile id in.

    Returns
    -------
    tuple[int, int]
        Found tile id (x,y).
    """
    file_name = os.path.splitext(os.path.basename(file_path))[0]

    tile_id = get_tile_id_tuple_from_str(file_name)

    return tile_id


def get_task_id_str_from_tuple(task_id_tuple: tuple[str, int, int]) -> str:

    solar_day, tile_id_x, tile_id_y = task_id_tuple

    task_id_str = f"{solar_day}/x{tile_id_x:03d}/y{tile_id_y:03d}"

    return task_id_str


def get_solar_day_from_string(string_: str) -> str:
    date_pattern = re.compile(r"(\d{4}-\d{2}-\d{2})")

    solar_day = re.search(date_pattern, string_).group(0)

    return solar_day


def get_task_id_tuple_from_str(task_id_str: str) -> tuple[str, int, int]:
    tile_id_x, tile_id_y = get_tile_id_tuple_from_str(task_id_str)

    solar_day = get_solar_day_from_string(task_id_str)

    task_id_tuple = (solar_day, tile_id_x, tile_id_y)

    return task_id_tuple


def format_task(task: dict[tuple[str, int, int], list[str]]) -> dict:
    """
    Format task.

    Parameters
    ----------
    task : dict[tuple[str, int, int], list[str]]
        Task to format

    Returns
    -------
    dict
        Task in the correct output format.
    """

    assert len(task) == 1
    task_id, task_datasets_ids = next(iter(task.items()))

    solar_day, tile_id_x, tile_id_y = task_id

    task = dict(
        solar_day=solar_day,
        tile_id_x=tile_id_x,
        tile_id_y=tile_id_y,
        task_datasets_ids=task_datasets_ids,
    )

    return task
