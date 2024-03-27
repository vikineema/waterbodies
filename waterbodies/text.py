import os
import re


def parse_tile_id_from_str(string_: str) -> tuple[int]:
    """
    Get x and y id of a tile from a string.

    Parameters
    ----------
    string_ : str
        String to search for a tile id

    Returns
    -------
    tuple[int]
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


def parse_tile_id_from_filename(file_path: str) -> tuple[int]:
    """
    Search for a tile id in the base name of a file.

    Parameters
    ----------
    file_path : str
        File path to search tile id in.

    Returns
    -------
    tuple[int]
        Found tile id (x,y).
    """
    file_name = os.path.splitext(os.path.basename(file_path))[0]

    tile_id = parse_tile_id_from_str(string_=file_name)
    return tile_id
