"""Input / output functions."""

from types import MappingProxyType
from typing import Optional, Union

from numpy import float32, object0
from pandas import Int32Dtype, Int64Dtype, StringDtype
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    ArrayType,
    BinaryType,
    FloatType,
    IntegerType,
    LongType,
    StringType,
    StructType,
)

from esa_geo_utils.io._create_initial_df import (
    _add_vsi_prefix,
    _create_initial_df,
    _get_data_sources,
    _get_feature_counts,
    _get_layer_names,
    _get_paths,
    _get_sequence_of_chunks,
    _get_total_chunks,
)
from esa_geo_utils.io._create_schema import _create_schema
from esa_geo_utils.io._generate_parallel_reader import _generate_parallel_reader

# See:
# https://gdal.org/python/index.html
# https://spark.apache.org/docs/latest/sql-ref-datatypes.html
# https://pandas.pydata.org/docs/user_guide/basics.html#dtypes
# https://numpy.org/doc/stable/reference/arrays.dtypes.html

OGR_TO_SPARK = MappingProxyType(
    {
        "Binary": BinaryType(),
        "Date": StringType(),
        "DateTime": StringType(),
        "Integer": IntegerType(),
        "IntegerList": ArrayType(IntegerType()),
        "Integer64": LongType(),
        "Integer64List": ArrayType(LongType()),
        "Real": FloatType(),
        "RealList": ArrayType(FloatType()),
        "String": StringType(),
        "StringList": ArrayType(StringType()),
        "Time": StringType(),
        "WideString": StringType(),
        "WideStringList": ArrayType(StringType()),
    }
)

SPARK_TO_PANDAS = MappingProxyType(
    {
        ArrayType(FloatType()): object0,
        ArrayType(IntegerType()): object0,
        ArrayType(LongType()): object0,
        ArrayType(StringType()): object0,
        BinaryType(): object0,
        FloatType(): float32,
        IntegerType(): Int32Dtype(),
        LongType(): Int64Dtype(),
        StringType(): StringDtype(),
    }
)


def _spark_df_from_vector_files(
    directory: str,
    ogr_to_spark_type_map: MappingProxyType = OGR_TO_SPARK,
    spark: SparkSession = SparkSession._activeSession,
    suffix: str = "*",
    ideal_chunk_size: int = 3_000_000,
    geom_field_name: str = "geometry",
    geom_field_type: str = "Binary",
    coerce_to_schema: bool = False,
    spark_to_pandas_type_map: MappingProxyType = SPARK_TO_PANDAS,
    vsi_prefix: Optional[str] = None,
    schema: StructType = None,
    layer_identifier: Optional[Union[str, int]] = None,
) -> SparkDataFrame:
    """Given a folder of vector files, returns a Spark DataFrame.

    If a valid SQL statement is provided (and the vector file supports SQL
    queries), the results will be returned as a layer:

    Example:
        >>> gdf = spark_df_from_vector_files(
                directory="/path/to/vector/files",
                sql="SELECT * FROM layer_name LIMIT 100"
            )

    Else if a layer name or index is provided, that layer will be returned:

    Example:
        >>> gdf = geodataframe_from_ogr(
                path="/path/to/vector/files",
                layer="layer_name"
            )
        >>> gdf = geodataframe_from_ogr(
                path="/path/to/vector/files",
                layer=1
            )

    Else if neither is provided, the 0th layer will be returned:

    Example:
        >>> gdf = geodataframe_from_ogr(
                path="/path/to/vector/files",
            )

    Args:
        directory (str): [description]
        ogr_to_spark_type_map (MappingProxyType): [description]. Defaults
            to OGR_TO_SPARK.
        spark (SparkSession): [description]. Defaults to
            SparkSession._activeSession.
        suffix (str): [description]. Defaults to "*".
        ideal_chunk_size (int): [description]. Defaults to 3_000_000.
        geom_field_name (str): [description]. Defaults to "geometry".
        geom_field_type (str): [description]. Defaults to "Binary".
        coerce_to_schema (bool): [description]. Defaults to False.
        spark_to_pandas_type_map (MappingProxyType): [description]. Defaults
            to SPARK_TO_PANDAS.
        vsi_prefix (str, optional): [description]. Defaults to None.
        schema (StructType): [description]. Defaults to None.
        layer_identifier (str, optional): [description]. Defaults to None.

    Returns:
        SparkDataFrame: [description]
    """
    paths = _get_paths(
        directory=directory,
        suffix=suffix,
    )

    if vsi_prefix:
        paths = _add_vsi_prefix(
            paths=paths,
            vsi_prefix=vsi_prefix,
        )

    data_sources = _get_data_sources(paths)

    layer_names = _get_layer_names(
        data_sources=data_sources,
        layer_identifier=layer_identifier,
    )

    feature_counts = _get_feature_counts(
        data_sources=data_sources,
        layer_names=layer_names,
    )

    sequence_of_chunks = _get_sequence_of_chunks(
        feature_counts=feature_counts,
        ideal_chunk_size=ideal_chunk_size,
    )

    total_chunks = _get_total_chunks(sequence_of_chunks)

    df = _create_initial_df(
        spark=spark,
        paths=paths,
        layer_names=layer_names,
        sequence_of_chunks=sequence_of_chunks,
    )

    _schema = (
        schema
        if schema
        else _create_schema(
            data_source=data_sources[0],
            layer_name=layer_names[0],
            ogr_to_spark_type_map=ogr_to_spark_type_map,
            geom_field_name=geom_field_name,
            geom_field_type=geom_field_type,
        )
    )

    parallel_read = _generate_parallel_reader(
        geom_field_name=geom_field_name,
        coerce_to_schema=coerce_to_schema,
        spark_to_pandas_type_map=spark_to_pandas_type_map,
        schema=_schema,
    )

    spark.conf.set("spark.sql.shuffle.partitions", total_chunks)

    return (
        df.repartition(total_chunks, "chunk_id")
        .groupby("chunk_id")
        .applyInPandas(parallel_read, _schema)
    )
