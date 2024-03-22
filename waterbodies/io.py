import os

import fsspec
import geopandas as gpd
import s3fs
from fsspec.implementations.local import LocalFileSystem
from s3fs.core import S3FileSystem


def is_s3_file(path: str) -> bool:
    return path.startswith("s3://")


def get_filesystem(
    path: str,
) -> S3FileSystem | LocalFileSystem:
    if is_s3_file(path=path):
        # TODO Need to test which works for the entire repository
        # anon=True use anonymous connection (public buckets only).
        # anon=False uses the key/secret given, or boto’s credential resolver
        # (client_kwargs, environment, variables, config files, EC2 IAM server, in that order)
        fs = s3fs.S3FileSystem(anon=True)
    else:
        fs = fsspec.filesystem("file")
    return fs


def check_file_exists(path: str) -> bool:
    fs = get_filesystem(path=path)
    if fs.exists(path) and fs.isfile(path):
        return True
    else:
        return False


def is_parquet(path: str) -> bool:
    _, file_extension = os.path.splitext(path)

    known_parquet_extensions = [".pq", ".parquet"]
    if file_extension.lower() in known_parquet_extensions:
        return True
    else:
        return False


def load_vector_file(path: str) -> gpd.GeoDataFrame:
    if is_parquet(path=path):
        gdf = gpd.read_parquet(path, filesystem=get_filesystem(path=path))
    else:
        gdf = gpd.read_file(path)
    return gdf
