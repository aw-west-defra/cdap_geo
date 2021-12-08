"""Module level fixtures."""
from pathlib import Path
from types import MappingProxyType
from typing import Tuple

from _pytest.tmpdir import TempPathFactory
from geopandas import GeoDataFrame
from pyspark.sql.types import (
    BinaryType,
    DataType,
    LongType,
    StringType,
    StructField,
    StructType,
)
from pytest import fixture
from shapely.geometry import Point

from esa_geo_utils.io._pyspark import OGR_TO_SPARK


@fixture
def first_layer() -> GeoDataFrame:
    """First dummy layer."""
    return GeoDataFrame(
        data={
            "id": [0, 1],
            "category": ["A", "B"],
            "geometry": [Point(0, 0), Point(1, 0)],
        },
        crs="EPSG:27700",
    )


@fixture
def second_layer() -> GeoDataFrame:
    """Second dummy layer."""
    return GeoDataFrame(
        data={
            "id": [0, 1],
            "category": ["C", "D"],
            "geometry": [Point(1, 1), Point(0, 1)],
        },
        crs="EPSG:27700",
    )


@fixture
def directory_path(
    tmp_path_factory: TempPathFactory,
) -> Path:
    """Pytest temporary directory as Path object."""
    return tmp_path_factory.getbasetemp()


@fixture
def fileGDB_path(
    directory_path: Path,
    first_layer: GeoDataFrame,
    second_layer: GeoDataFrame,
) -> str:
    """Writes dummy layers to FileGDB and returns path as string."""
    path = directory_path / "data_source.gdb"

    path_as_string = str(path)

    first_layer.to_file(
        filename=path_as_string,
        index=False,
        layer="first",
    )

    second_layer.to_file(
        filename=path_as_string,
        index=False,
        layer="second",
    )

    return path_as_string


@fixture
def fileGDB_schema() -> StructType:
    """Schema for dummy FileGDB."""
    return StructType(
        [
            StructField("id", LongType()),
            StructField("category", StringType()),
            StructField("geometry", BinaryType()),
        ]
    )


@fixture
def fileGDB_schema_field_details() -> Tuple[Tuple[str, DataType], ...]:
    """Field details from dummy FileGDB schema."""
    return (
        ("id", LongType()),
        ("category", StringType()),
        ("geometry", BinaryType()),
    )


@fixture
def ogr_to_spark_mapping() -> MappingProxyType:
    """OGR to Spark data type mapping."""
    return OGR_TO_SPARK
