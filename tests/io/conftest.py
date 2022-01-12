"""Module level fixtures."""
from pathlib import Path
from types import MappingProxyType
from typing import List, Tuple

from _pytest.tmpdir import TempPathFactory
from geopandas import GeoDataFrame, GeoSeries
from numpy import int64, object0
from osgeo.ogr import DataSource, Open
from pandas import DataFrame as PandasDataFrame
from pandas import Series
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import Row, SparkSession
from pyspark.sql.types import (
    BinaryType,
    DataType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)
from pytest import fixture
from shapely.geometry import Point
from shapely.geometry.base import BaseGeometry

from esa_geo_utils.io import OGR_TO_SPARK, SPARK_TO_PANDAS
from esa_geo_utils.io._types import Chunks


@fixture
def layer_column_names() -> Tuple[str, ...]:
    """Column names shared by both dummy layers."""
    return ("id", "category", "geometry")


@fixture
def layer_column_names_missing_column(
    layer_column_names: Tuple[str, ...]
) -> Tuple[str, ...]:
    """Shared column names but missing `id` column."""
    return tuple(name for name in layer_column_names if name != "category")


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
            "id": int64,
            "category": object0,
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
def first_layer_pdf_first_row(
    first_layer_pdf: PandasDataFrame,
) -> PandasDataFrame:
    """Just the first row of the first layer PDF."""
    return first_layer_pdf.loc[[0]]


@fixture
def first_layer_pdf_with_additional_column(
    first_layer_pdf: PandasDataFrame,
) -> PandasDataFrame:
    """First layer pdf with additional 'id' column."""
    return first_layer_pdf.assign(
        additional=Series(),
    )


@fixture
def first_layer_pdf_with_wrong_types(
    first_layer_pdf: PandasDataFrame,
) -> PandasDataFrame:
    """First layer pdf but all types are object."""
    return first_layer_pdf.astype(object0)


@fixture
def first_layer_pdf_with_missing_column(
    first_layer_pdf: PandasDataFrame,
) -> PandasDataFrame:
    """First layer pdf with missing 'category' column."""
    return first_layer_pdf.drop(
        columns=["category"],
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
def erroneous_file_path() -> str:
    """."""
    return "/erroneous/file/path"


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
def fileGDB_wrong_types_path(
    directory_path: Path,
    first_layer_pdf_with_wrong_types: PandasDataFrame,
    second_layer_gdf: GeoDataFrame,
) -> str:
    """Writes dummy layers to FileGDB and returns path as string."""
    path = directory_path / "data_source_wrong_types.gdb"

    path_as_string = str(path)

    first_layer_gdf = GeoDataFrame(
        data=first_layer_pdf_with_wrong_types,
        geometry=GeoSeries.from_wkb(first_layer_pdf_with_wrong_types["geometry"]),
        crs="EPSG:27700",
    )

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
def fileGDB_data_source(
    fileGDB_path: str,
) -> DataSource:
    """DataSource for FileGDB."""  # noqa: D403
    return Open(fileGDB_path)


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


@fixture
def spark_to_pandas_mapping() -> MappingProxyType:
    """Spark to Pandas data type mapping."""
    return SPARK_TO_PANDAS


@fixture
def expected_single_chunk() -> Chunks:
    """FileGDB as single chunk."""  # noqa: D403
    return ((0, 3),)


@fixture
def expected_sequence_containing_single_chunk(
    expected_single_chunk: Chunks,
) -> Tuple[Chunks, ...]:
    """Sequence containing FileGDB as single chunk."""
    return (expected_single_chunk,)


@fixture
def expected_multiple_chunks() -> Chunks:
    """FileGDB as two chunks."""  # noqa: D403
    return ((0, 1), (1, 3))


@fixture
def expected_sequence_containing_multiple_chunks(
    expected_multiple_chunks: Chunks,
) -> Tuple[Chunks, ...]:
    """Sequence containing FileGDB as two chunks."""
    return (expected_multiple_chunks,)


@fixture
def expected_sequence_of_chunks(
    expected_single_chunk: Chunks,
    expected_multiple_chunks: Chunks,
) -> Tuple[Chunks, ...]:
    """Sequence containing FileGDB as single chunk and FileGDB as two chunks."""
    return (expected_single_chunk, expected_multiple_chunks)


@fixture
def spark_context() -> SparkSession:
    """Local Spark context."""
    return (
        SparkSession.builder.master(
            "local",
        )
        .appName(
            "Test context",
        )
        .getOrCreate()
    )


@fixture
def expected_paths_sdf(
    spark_context: SparkSession,
    fileGDB_path: str,
) -> SparkDataFrame:
    """Spark DataFrame of FileGDB path."""
    return spark_context.createDataFrame(
        data=((fileGDB_path,),),
        schema="path: string",
    )


@fixture
def expected_single_chunk_sdf(
    spark_context: SparkSession,
    fileGDB_path: str,
) -> SparkDataFrame:
    """Spark DataFrame of FileGDB as single chunk."""
    return spark_context.createDataFrame(
        data=(((fileGDB_path, "first", 0, 0, 3),)),
        schema=StructType(
            [
                StructField("path", StringType()),
                StructField("layer_name", StringType()),
                StructField("id", LongType(), False),
                StructField("start", IntegerType()),
                StructField("stop", IntegerType()),
            ],
        ),
    )


@fixture
def expected_multiple_chunks_sdf(
    spark_context: SparkSession,
    fileGDB_path: str,
) -> SparkDataFrame:
    """Spark DataFrame of FileGDB as two chunks."""
    return spark_context.createDataFrame(
        data=(
            (
                Row(
                    path=fileGDB_path,
                    layer_name="first",
                    id=0,
                    start=0,
                    stop=1,
                ),
                Row(
                    path=fileGDB_path,
                    layer_name="first",
                    id=1,
                    start=1,
                    stop=3,
                ),
            )
        ),
        schema=StructType(
            [
                StructField("path", StringType()),
                StructField("layer_name", StringType()),
                StructField("id", LongType(), False),
                StructField("start", IntegerType()),
                StructField("stop", IntegerType()),
            ],
        ),
    )


@fixture
def expected_null_data_frame(
    layer_column_names: Tuple[str, ...],
) -> PandasDataFrame:
    """Empty PDF with correct column names and dtypes."""
    return PandasDataFrame(columns=layer_column_names).astype(
        {
            "id": int64,
            "category": object0,
            "geometry": object0,
        },
    )


@fixture
def expected_parallel_reader_for_files() -> Tuple[List[str], int]:
    """Expected source code for _generate_parallel_reader_for_files."""
    return (
        [
            "    def _(pdf: PandasDataFrame) -> PandasDataFrame:\n",
            '        """Returns a pandas_udf compatible version of _pdf_from_vector_file."""\n',  # noqa: B950
            "        return _pdf_from_vector_file(\n",
            '            path=str(pdf["path"][0]),\n',
            "            layer_identifier=layer_identifier,\n",
            "            geom_field_name=geom_field_name,\n",
            "            coerce_to_schema=coerce_to_schema,\n",
            "            schema=schema,\n",
            "            spark_to_pandas_type_map=spark_to_pandas_type_map,\n",
            "        )\n",
        ],
        318,
    )


@fixture
def expected_parallel_reader_for_chunks() -> Tuple[List[str], int]:
    """Expected source code for _generate_parallel_reader_for_chunks."""
    return (
        [
            "    def _(pdf: PandasDataFrame) -> PandasDataFrame:\n",
            '        """Returns a pandas_udf compatible version of _pdf_from_vector_file_chunk."""\n',  # noqa: B950
            "        return _pdf_from_vector_file_chunk(\n",
            '            path=str(pdf["path"][0]),\n',
            '            layer_name=str(pdf["layer_name"][0]),\n',
            '            start=int(pdf["start"][0]),\n',
            '            stop=int(pdf["stop"][0]),\n',
            "            geom_field_name=geom_field_name,\n",
            "            coerce_to_schema=coerce_to_schema,\n",
            "            schema=schema,\n",
            "            spark_to_pandas_type_map=spark_to_pandas_type_map,\n",
            "        )\n",
        ],
        340,
    )
