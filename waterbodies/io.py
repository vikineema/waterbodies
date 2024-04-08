import logging
import os
import re
from pathlib import Path

import fsspec
import geopandas as gpd
import s3fs
from dotenv import load_dotenv
from fsspec.implementations.local import LocalFileSystem
from s3fs.core import S3FileSystem

_log = logging.getLogger(__name__)


def is_s3_path(path: str) -> bool:
    return path.startswith("s3://")


def get_filesystem(
    path: str,
) -> S3FileSystem | LocalFileSystem:
    if is_s3_path(path=path):
        # TODO Need to test which works for the entire repository
        # anon=True use anonymous connection (public buckets only).
        # anon=False uses the key/secret given, or boto’s credential resolver
        # (client_kwargs, environment, variables, config files, EC2 IAM server, in that order)
        fs = s3fs.S3FileSystem(anon=False)
    else:
        fs = fsspec.filesystem("file")
    return fs


def check_file_exists(path: str) -> bool:
    fs = get_filesystem(path=path)
    if fs.exists(path) and fs.isfile(path):
        return True
    else:
        return False


def check_directory_exists(path: str) -> bool:
    fs = get_filesystem(path=path)
    if fs.exists(path) and fs.isdir(path):
        return True
    else:
        return False


def check_file_extension(path: str, accepted_file_extensions: list[str]) -> bool:
    _, file_extension = os.path.splitext(path)
    if file_extension.lower() in accepted_file_extensions:
        return True
    else:
        return False


def is_parquet(path: str) -> bool:
    accepted_parquet_extensions = [".pq", ".parquet"]
    return check_file_extension(path=path, accepted_file_extensions=accepted_parquet_extensions)


def is_geotiff(path: str) -> bool:
    accepted_geotiff_extensions = [".tif", ".tiff", ".gtiff"]
    return check_file_extension(path=path, accepted_file_extensions=accepted_geotiff_extensions)


def load_vector_file(path: str) -> gpd.GeoDataFrame:
    if is_parquet(path=path):
        gdf = gpd.read_parquet(path, filesystem=get_filesystem(path=path))
    else:
        gdf = gpd.read_file(path)
    return gdf


def find_geotiff_files(directory_path: str, file_name_pattern: str = ".*") -> list[str]:

    file_name_pattern = re.compile(file_name_pattern)

    fs = get_filesystem(path=directory_path)

    geotiff_file_paths = []

    for root, dirs, files in fs.walk(directory_path):
        for file_name in files:
            if is_geotiff(path=file_name):
                if re.search(file_name_pattern, file_name):
                    geotiff_file_paths.append(os.path.join(root, file_name))
                else:
                    continue
            else:
                continue

    if is_s3_path(path=directory_path):
        geotiff_file_paths = [f"s3://{file}" for file in geotiff_file_paths]

    return geotiff_file_paths


def is_sandbox_env() -> bool:
    """
    Check if running on the Analysis Sandbox

    Returns
    -------
    bool
        True if on Sandbox
    """
    return bool(os.environ.get("JUPYTERHUB_USER", None))


def check_waterbodies_db_credentials_exist():
    return bool(os.environ.get("WATERBODIES_DB_USER", None))


def setup_sandbox_env(dotenv_path: str = os.path.join(str(Path.home()), ".env")):
    """
    Load the .env file to set up the waterbodies database
    credentials on the Analysis Sandbox.

    Parameters
    ----------
    dotenv_path : str, optional
        Absolute or relative path to .env file, by default os.path.join(str(Path.home()), ".env")
    """
    if is_sandbox_env():
        if not check_waterbodies_db_credentials_exist():
            check_dotenv = load_dotenv(dotenv_path=dotenv_path, verbose=True, override=True)
            if not check_dotenv:
                # Check if the file does not exist
                if not check_file_exists(dotenv_path):
                    e = FileNotFoundError(f"{dotenv_path} does NOT exist!")
                    _log.exception(e)
                    raise e
                else:
                    e = ValueError(f"No variables found in {dotenv_path}")
                    _log.exception(e)
                    raise e
            else:
                if not check_waterbodies_db_credentials_exist():
                    raise ValueError(f"Waterbodies database credentials not in {dotenv_path}")
