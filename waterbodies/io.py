import logging
import os
import re

import fsspec
import geopandas as gpd
import s3fs
from fsspec.implementations.local import LocalFileSystem
from s3fs.core import S3FileSystem

_log = logging.getLogger(__name__)


def is_s3_path(path: str) -> bool:
    return path.startswith("s3://")


def get_filesystem(
    path: str,
    anon: bool = True,
) -> S3FileSystem | LocalFileSystem:
    if is_s3_path(path=path):
        fs = s3fs.S3FileSystem(anon=anon, s3_additional_kwargs={"ACL": "bucket-owner-full-control"})
    else:
        fs = fsspec.filesystem("file")
    return fs


def check_file_exists(path: str) -> bool:
    fs = get_filesystem(path=path, anon=True)
    if fs.exists(path) and fs.isfile(path):
        return True
    else:
        return False


def check_directory_exists(path: str) -> bool:
    fs = get_filesystem(path=path, anon=True)
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
        gdf = gpd.read_parquet(path, filesystem=get_filesystem(path=path, anon=True))
    else:
        gdf = gpd.read_file(path)
    return gdf


def find_geotiff_files(directory_path: str, file_name_pattern: str = ".*") -> list[str]:
    file_name_pattern = re.compile(file_name_pattern)

    fs = get_filesystem(path=directory_path, anon=True)

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


def find_parquet_files(directory_path: str, file_name_pattern: str = ".*") -> list[str]:
    file_name_pattern = re.compile(file_name_pattern)

    fs = get_filesystem(path=directory_path, anon=True)

    parquet_file_paths = []

    for root, dirs, files in fs.walk(directory_path):
        for file_name in files:
            if is_parquet(path=file_name):
                if re.search(file_name_pattern, file_name):
                    parquet_file_paths.append(os.path.join(root, file_name))
                else:
                    continue
            else:
                continue

    if is_s3_path(path=directory_path):
        parquet_file_paths = [f"s3://{file}" for file in parquet_file_paths]

    return parquet_file_paths
