"""Module level fixtures."""
from pathlib import Path
from types import MappingProxyType
from typing import Tuple

from _pytest.tmpdir import TempPathFactory
from geopandas import GeoDataFrame
from pandas import DataFrame as PandasDataFrame
from pandas import Int64Dtype, Series, StringDtype
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
from shapely.geometry.base import BaseGeometry

from esa_geo_utils.io import OGR_TO_SPARK


@fixture
def layer_column_names() -> Tuple[str, ...]:
    """Column names shared by both dummy layers."""
    return ("id", "category", "geometry")


@fixture
def layer_column_names_missing_column(
    layer_column_names: Tuple[str, ...]
) -> Tuple[str, ...]:
    """Shared column names but missing `id` column."""
    return tuple(name for name in layer_column_names if name != "id")


@fixture
def layer_column_names_additional_column(
    layer_column_names: Tuple[str, ...]
) -> Tuple[str, ...]:
    """Shared column names but with extra column name."""
    return layer_column_names + ("additional",)


@fixture
def first_layer_first_row() -> Tuple[int, str, BaseGeometry]:
    """First row of first dummy layers."""
    return (0, "A", Point(0, 0))


@fixture
def first_layer_second_row() -> Tuple[int, str, BaseGeometry]:
    """Second row of first dummy layers."""
    return (1, "B", Point(1, 0))


@fixture
def first_layer_gdf(
    layer_column_names: Tuple[str, ...],
    first_layer_first_row: Tuple[int, str, BaseGeometry],
    first_layer_second_row: Tuple[int, str, BaseGeometry],
) -> GeoDataFrame:
    """First dummy layer."""
    return GeoDataFrame(
        data=(
            first_layer_first_row,
            first_layer_second_row,
        ),
        columns=layer_column_names,
        crs="EPSG:27700",
    ).astype(
        {
            "id": Int64Dtype(),
            "category": StringDtype(),
        },
    )


@fixture
def first_layer_pdf(
    first_layer_gdf: GeoDataFrame,
) -> PandasDataFrame:
    """First dummy layer as pdf with wkb geometry column."""
    first_layer_gdf["geometry"] = first_layer_gdf["geometry"].to_wkb()
    return PandasDataFrame(
        first_layer_gdf,
    )


@fixture
def first_layer_pdf_with_missing_column(
    first_layer_pdf: PandasDataFrame,
) -> PandasDataFrame:
    """First layer pdf with missing 'id' column."""
    return first_layer_pdf.drop(
        columns=["id"],
    )


@fixture
def first_layer_pdf_with_additional_column(
    first_layer_pdf: PandasDataFrame,
) -> PandasDataFrame:
    """First layer pdf with additional 'id' column."""
    return first_layer_pdf.assign(
        additional=Series(),
    )


@fixture
def second_layer_first_row() -> Tuple[int, str, BaseGeometry]:
    """First row of second dummy layers."""
    return (0, "C", Point(1, 1))


@fixture
def second_layer_second_row() -> Tuple[int, str, BaseGeometry]:
    """Second row of second dummy layers."""
    return (1, "D", Point(0, 1))


@fixture
def second_layer_gdf(
    layer_column_names: Tuple[str, ...],
    second_layer_first_row: Tuple[int, str, BaseGeometry],
    second_layer_second_row: Tuple[int, str, BaseGeometry],
) -> GeoDataFrame:
    """Second dummy layer."""
    return GeoDataFrame(
        data=(
            second_layer_first_row,
            second_layer_second_row,
        ),
        columns=layer_column_names,
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
    first_layer_gdf: GeoDataFrame,
    second_layer_gdf: GeoDataFrame,
) -> str:
    """Writes dummy layers to FileGDB and returns path as string."""
    path = directory_path / "data_source.gdb"

    path_as_string = str(path)

    first_layer_gdf.to_file(
        filename=path_as_string,
        index=False,
        layer="first",
    )

    second_layer_gdf.to_file(
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
