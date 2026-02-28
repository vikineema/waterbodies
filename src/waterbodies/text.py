import os
import re


def get_tile_index_tuple_from_str(string_: str) -> tuple[int, int]:
    """
    Get the tile index (x,y) from a string.

    Parameters
    ----------
    string_ : str
        String to search for a tile index

    Returns
    -------
    tuple[int, int]
        Found tile index (x,y).
    """
    x_pattern = re.compile(r"x\d{3}")
    y_pattern = re.compile(r"y\d{3}")

    tile_index_x_str = re.search(x_pattern, string_).group(0)
    tile_index_y_str = re.search(y_pattern, string_).group(0)

    tile_index_x = int(tile_index_x_str.lstrip("x"))
    tile_index_y = int(tile_index_y_str.lstrip("y"))

    tile_index = (tile_index_x, tile_index_y)

    return tile_index


def get_tile_index_str_from_tuple(tile_index_tuple: tuple[int, int]) -> str:
    """
    Convert a tile index tuple into the tile index string format.

    Parameters
    ----------
    tile_index_tuple : tuple[int, int]
        Tile index tuple to convert to string.

    Returns
    -------
    str
        Tile index in string format.
    """

    tile_index_x, tile_index_y = tile_index_tuple

    tile_index_str = f"x{tile_index_x:03d}_y{tile_index_y:03d}"

    return tile_index_str


def get_tile_index_tuple_from_filename(file_path: str) -> tuple[int, int]:
    """
    Search for a tile index in the base name of a file.

    Parameters
    ----------
    file_path : str
        File path to search tile index in.

    Returns
    -------
    tuple[int, int]
        Found tile index (x,y).
    """
    file_name = os.path.splitext(os.path.basename(file_path))[0]

    tile_id = get_tile_index_tuple_from_str(file_name)

    return tile_id


def get_task_id_str_from_tuple(task_id_tuple: tuple[str, int, int]) -> str:
    solar_day, tile_index_x, tile_index_y = task_id_tuple

    task_id_str = f"{solar_day}/x{tile_index_x:03d}/y{tile_index_y:03d}"

    return task_id_str


def get_solar_day_from_string(string_: str) -> str:
    date_pattern = re.compile(r"(\d{4}-\d{2}-\d{2})")

    solar_day = re.search(date_pattern, string_).group(0)

    return solar_day


def get_task_id_tuple_from_str(task_id_str: str) -> tuple[str, int, int]:
    tile_index_x, tile_index_y = get_tile_index_tuple_from_str(task_id_str)

    solar_day = get_solar_day_from_string(task_id_str)

    task_id_tuple = (solar_day, tile_index_x, tile_index_y)

    return task_id_tuple


def format_task(
    task: dict[tuple[str, int, int], list[str]] | dict[tuple[int, int], list[str]]
) -> dict:
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

    if len(task_id) == 3:
        solar_day, tile_index_x, tile_index_y = task_id

        task = dict(
            solar_day=solar_day,
            tile_index_x=tile_index_x,
            tile_index_y=tile_index_y,
            task_datasets_ids=task_datasets_ids,
        )
    elif len(task_id) == 2:
        tile_index_x, tile_index_y = task_id

        task = dict(
            tile_index_x=tile_index_x,
            tile_index_y=tile_index_y,
            task_datasets_ids=task_datasets_ids,
        )
    return task
