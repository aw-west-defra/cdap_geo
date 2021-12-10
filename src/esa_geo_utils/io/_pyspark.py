from itertools import compress
from os import listdir
from os.path import join
from types import MappingProxyType
from typing import Any, Callable, Generator, Optional, Tuple, Union

from more_itertools import pairwise
from numpy import float32, int32, int64, object0, str0
from osgeo.ogr import DataSource, Feature, GetFieldTypeName, Layer, Open
from pandas import DataFrame as PandasDataFrame
from pandas import Series
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import Row, SparkSession
from pyspark.sql.functions import col, explode, lit, monotonically_increasing_id, udf
from pyspark.sql.types import (
    ArrayType,
    BinaryType,
    FloatType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

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
        IntegerType(): int32,
        LongType(): int64,
        StringType(): str0,
    }
)

# Create Spark DataFrame


def _get_paths(directory: str, suffix: str) -> Tuple[str, ...]:
    """Returns full paths for all files in a directory, with the given suffix."""
    paths = listdir(directory)
    return tuple(join(directory, path) for path in paths if path.endswith(suffix))


def _add_vsi_prefix(paths: Tuple[str, ...], vsi_prefix: str) -> Tuple[str, ...]:
    """Adds GDAL virtual file system prefix to the paths."""
    return tuple(vsi_prefix + "/" + path for path in paths)


def _create_paths_df(spark: SparkSession, paths: Tuple[str, ...]) -> SparkDataFrame:
    """Given a list of full paths, returns a DataFrame of those paths."""
    rows = [Row(path=path) for path in paths]
    return spark.createDataFrame(rows)


def _get_layer_names(data_source: DataSource) -> Tuple[str, ...]:
    """Given a OGR DataSource, returns a sequence of layers."""
    return tuple(
        data_source.GetLayer(index).GetName()
        for index in range(data_source.GetLayerCount())
    )


def _get_layer_name(
    data_source: DataSource,
    layer: Optional[Union[str, int]],
) -> Layer:
    """Returns the given layer name, name at index, or name of 0th layer."""
    layers = _get_layer_names(
        data_source=data_source,
    )

    if isinstance(layer, str):
        if layer not in layers:
            raise ValueError(f"Expecting one of {layers} but received {layer}.")
        else:
            _layer = layer
    elif isinstance(layer, int):
        _layer = layers[layer]
    elif not layer:
        _layer = layers[0]

    return _layer


@udf(returnType=StringType())
def _get_layer_name_udf(path: str, layer: str) -> str:
    data_source = Open(path)
    return _get_layer_name(data_source, layer=layer)


def _get_feature_count(layer: Layer) -> int:
    feat_count = layer.GetFeatureCount()
    if feat_count == -1:
        feat_count = layer.GetFeatureCount(force=True)
    return feat_count


def _get_layer(
    data_source: DataSource,
    layer: Optional[Union[str, int]],
    start: Optional[int],
    stop: Optional[int],
) -> Layer:
    """Returns a GDAL Layer from a SQL statement, name or index, or 0th layer."""
    _layer = _get_layer_name(
        data_source=data_source,
        layer=layer,
    )

    # ! "S608: Possible SQL injection vector through string-based query
    # ! construction" is turned off here because `_get_layer_name` will
    # ! raise an error if the user supplied layer doesn't exist and
    # ! `start` and `stop` are generated within the function.
    if isinstance(start, int) and isinstance(stop, int):
        sql = f"SELECT * from {_layer} WHERE FID >= {start} AND FID < {stop}"  # noqa: S608, B950
    else:
        sql = f"SELECT * from {_layer}"  # noqa: S608

    return data_source.ExecuteSQL(sql)


@udf(returnType=IntegerType())
def _get_feature_count_udf(path: str, layer: str) -> int:
    data_source = Open(path)
    _layer = _get_layer(
        data_source=data_source,
        layer=layer,
        start=None,
        stop=None,
    )
    return _get_feature_count(_layer)


def _create_feature_count_df(
    paths_df: SparkDataFrame,
    layer: Optional[str],
) -> SparkDataFrame:
    return paths_df.withColumn(
        "layer",
        _get_layer_name_udf("path", lit(layer)),
    ).withColumn(
        "feature_count",
        _get_feature_count_udf("path", "layer"),
    )


@udf(returnType=ArrayType(ArrayType(IntegerType())))
def _get_ranges_udf(
    feature_count: int, ideal_chunk_size: int
) -> Tuple[Tuple[int, int], ...]:
    exclusive_range = range(0, feature_count, ideal_chunk_size)
    inclusive_range = tuple(exclusive_range) + (feature_count + 1,)
    range_pairs = pairwise(inclusive_range)
    return tuple(range_pairs)


def _create_spark_df(
    spark: SparkSession,
    paths: Tuple[str, ...],
    ideal_chunk_size: int,
    layer: Optional[str],
) -> SparkDataFrame:
    paths_df = _create_paths_df(spark=spark, paths=paths)

    feature_count_df = _create_feature_count_df(
        paths_df=paths_df,
        layer=layer,
    )

    ranges_df = feature_count_df.withColumn(
        "ranges",
        _get_ranges_udf("feature_count", lit(ideal_chunk_size)),
    )

    return (
        ranges_df.withColumn(
            "ranges",
            explode("ranges"),
        )
        .withColumn("id", monotonically_increasing_id())
        .withColumn("start", col("ranges")[0])
        .withColumn("stop", col("ranges")[1])
        .drop("ranges")
    )


def _get_property_names(layer: Layer) -> Tuple[Any, ...]:
    """Given a GDAL Layer, return the non-geometry field names."""
    layer_definition = layer.GetLayerDefn()
    return tuple(
        layer_definition.GetFieldDefn(index).GetName()
        for index in range(layer_definition.GetFieldCount())
    )


def _get_property_types(layer: Layer) -> Tuple[Any, ...]:
    """Given a GDAL Layer, return the non-geometry field types."""
    layer_definition = layer.GetLayerDefn()
    type_codes = tuple(
        layer_definition.GetFieldDefn(index).GetType()
        for index in range(layer_definition.GetFieldCount())
    )
    return tuple(GetFieldTypeName(type_code) for type_code in type_codes)


def _get_feature_schema(
    layer: Layer,
    ogr_to_spark_type_map: MappingProxyType,
    geom_field_name: str,
    geom_field_type: str,
) -> StructType:
    """Given a GDAL Layer and a data type mapping, return a PySpark DataFrame schema."""
    property_names = _get_property_names(layer=layer)
    property_types = _get_property_types(layer=layer)
    property_struct_fields = [
        StructField(field_name, ogr_to_spark_type_map[field_type])
        for field_name, field_type in zip(property_names, property_types)
    ]
    geometry_struct_field = [
        StructField(geom_field_name, ogr_to_spark_type_map[geom_field_type])
    ]
    return StructType(property_struct_fields + geometry_struct_field)


def _create_schema(
    paths: Tuple[Any, ...],
    geom_field_name: str,
    geom_field_type: str,
    ogr_to_spark_type_map: MappingProxyType,
    layer: Optional[Union[str, int]],
) -> StructType:
    """Returns a schema for a given layer in the first file in a list of file paths."""
    data_source = Open(paths[0])
    _layer = _get_layer(
        data_source=data_source,
        layer=layer,
        start=None,
        stop=None,
    )
    return _get_feature_schema(
        layer=_layer,
        ogr_to_spark_type_map=ogr_to_spark_type_map,
        geom_field_name=geom_field_name,
        geom_field_type=geom_field_type,
    )


def _get_properties(feature: Feature) -> Tuple:
    """Given a GDAL Feature, return the non-geometry fields."""
    return tuple(field for field in feature)


def _get_geometry(feature: Feature) -> Tuple:
    """Given a GDAL Feature, return the geometry fields."""
    return tuple([feature.GetGeometryRef().ExportToWkb()])


def _get_features(layer: Layer) -> Generator:
    """Given a GDAL Layer, return the all fields."""
    return ((_get_properties(feature) + _get_geometry(feature)) for feature in layer)


def _get_fields(schema: StructType) -> Tuple:
    """Returns fields from schema."""
    return tuple((field.name, field.dataType) for field in schema.fields)


def _get_field_names_and_columns_names(
    pdf: PandasDataFrame, schema_fields: Tuple
) -> Tuple:
    """Returns field names from fields and columns from DataFrame."""
    schema_field_names = tuple(field[0] for field in schema_fields)
    column_names = tuple(column for column in pdf.columns)
    return schema_field_names, column_names


def _get_missing_fields_and_additional_columns(
    schema_field_names: Tuple, column_names: Tuple
) -> Tuple:
    """Returns tuples of missing fields and additional columns."""
    missing_fields = tuple(
        field_name not in column_names for field_name in schema_field_names
    )
    additional_columns = tuple(
        column not in schema_field_names for column in column_names
    )
    return missing_fields, additional_columns


def _drop_additional_columns(
    pdf: PandasDataFrame,
    column_names: Tuple,
    additional_columns: Tuple,
) -> PandasDataFrame:
    """Removes additional columns from pandas DataFrame."""
    to_drop = list(compress(column_names, additional_columns))
    return pdf.drop(columns=to_drop)


def _add_missing_fields(
    pdf: PandasDataFrame,
    schema_fields: Tuple,
    missing_fields: Tuple,
    spark_to_pandas_type_map: MappingProxyType,
    schema_field_names: Tuple,
) -> PandasDataFrame:
    """Adds missing fields to pandas DataFrame."""
    to_add = compress(schema_fields, missing_fields)
    missing_field_series = tuple(
        Series(name=field[0], dtype=spark_to_pandas_type_map[field[1]])
        for field in to_add
    )
    pdf_plus_missing_fields = pdf.append(missing_field_series)
    reindexed_pdf = pdf_plus_missing_fields.reindex(columns=schema_field_names)
    return reindexed_pdf


def _coerce_columns_to_schema(
    pdf: PandasDataFrame,
    schema_fields: Tuple,
    spark_to_pandas_type_map: MappingProxyType,
) -> PandasDataFrame:
    """Adds missing fields or removes additional columns to match schema."""
    schema_field_names, column_names = _get_field_names_and_columns_names(
        pdf=pdf,
        schema_fields=schema_fields,
    )

    if column_names == schema_field_names:
        return pdf
    else:
        missing_fields, additional_columns = _get_missing_fields_and_additional_columns(
            schema_field_names=schema_field_names,
            column_names=column_names,
        )
        if any(missing_fields) and any(additional_columns):
            pdf_minus_additional_columns = _drop_additional_columns(
                pdf=pdf,
                column_names=column_names,
                additional_columns=additional_columns,
            )
            return _add_missing_fields(
                pdf=pdf_minus_additional_columns,
                schema_fields=schema_fields,
                missing_fields=missing_fields,
                spark_to_pandas_type_map=spark_to_pandas_type_map,
                schema_field_names=schema_field_names,
            )
        elif any(missing_fields):
            return _add_missing_fields(
                pdf=pdf,
                schema_fields=schema_fields,
                missing_fields=missing_fields,
                spark_to_pandas_type_map=spark_to_pandas_type_map,
                schema_field_names=schema_field_names,
            )
        else:
            return _drop_additional_columns(
                pdf=pdf,
                column_names=column_names,
                additional_columns=additional_columns,
            )


def _coerce_types_to_schema(
    pdf: PandasDataFrame,
    schema_fields: Tuple,
    spark_to_pandas_type_map: MappingProxyType,
) -> PandasDataFrame:
    for column_name, data_type in schema_fields:
        pdf[column_name] = pdf[column_name].astype(spark_to_pandas_type_map[data_type])
    return pdf


def _null_data_frame_from_schema(schema: StructType) -> PandasDataFrame:
    """Generates an empty DataFrame that fits the schema."""
    return PandasDataFrame(
        data={
            field.name: Series(dtype=SPARK_TO_PANDAS[field.dataType])
            for field in schema.fields
        }
    )


def _vector_file_to_pdf(
    path: str,
    start: int,
    stop: int,
    layer: Optional[Union[str, int]],
    geom_field_name: str,
    coerce_to_schema: bool,
    schema: StructType,
    spark_to_pandas_type_map: MappingProxyType,
) -> PandasDataFrame:
    """Given a file path and layer, returns a pandas DataFrame."""
    data_source = Open(path)
    if data_source is None:
        return _null_data_frame_from_schema(schema=schema)
    _layer = _get_layer(
        data_source=data_source,
        layer=layer,
        start=start,
        stop=stop,
    )
    if _layer is None:
        return _null_data_frame_from_schema(schema=schema)
    features_generator = _get_features(layer=_layer)
    feature_names = _get_property_names(layer=_layer) + tuple([geom_field_name])
    pdf = PandasDataFrame(data=features_generator, columns=feature_names)
    if pdf is None:
        return _null_data_frame_from_schema(schema=schema)
    if coerce_to_schema:
        schema_fields = _get_fields(schema=schema)
        coerced_pdf = _coerce_columns_to_schema(
            pdf=pdf,
            schema_fields=schema_fields,
            spark_to_pandas_type_map=spark_to_pandas_type_map,
        )
        if coerced_pdf is None:
            return _null_data_frame_from_schema(schema=schema)
        else:
            return _coerce_types_to_schema(
                pdf=coerced_pdf,
                schema_fields=schema_fields,
                spark_to_pandas_type_map=spark_to_pandas_type_map,
            )
    else:
        return pdf


def _parallel_read_generator(
    geom_field_name: str,
    coerce_to_schema: bool,
    schema: StructType,
    spark_to_pandas_type_map: MappingProxyType,
) -> Callable:
    """Adds arbitrary key word arguments to the wrapped function."""

    def _(pdf: PandasDataFrame) -> PandasDataFrame:
        """Returns a the pandas_udf compatible version of _vector_file_to_pdf."""
        return _vector_file_to_pdf(
            path=pdf["path"][0],
            layer=pdf["layer"][0],
            start=pdf["start"][0],
            stop=pdf["stop"][0],
            geom_field_name=geom_field_name,
            coerce_to_schema=coerce_to_schema,
            schema=schema,
            spark_to_pandas_type_map=spark_to_pandas_type_map,
        )

    return _


# def _set_geometry_column_metadata(
#     df: DataFrame,
#     geometry_column_name: str,
#     layer: Layer,
# ) -> None:
#     df.schema[geometry_column_name].metadata = {
#         "crs": layer.GetSpatialRef().ExportToWkt(),
#         "encoding": "WKT",
#         "bbox": layer.GetExtent(),
#     }


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
    layer: Optional[str] = None,
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
        ideal_chunk_size (int): [description]. Defaults to 5_000_000.
        geom_field_name (str): [description]. Defaults to "geometry".
        geom_field_type (str): [description]. Defaults to "WKB".
        coerce_to_schema (bool): [description]. Defaults to False.
        spark_to_pandas_type_map (MappingProxyType): [description]. Defaults
            to SPARK_TO_PANDAS.
        vsi_prefix (str, optional): [description]. Defaults to None.
        schema (StructType): [description]. Defaults to None.
        layer (str, optional): [description]. Defaults to None.

    Returns:
        SparkDataFrame: [description]
    """
    paths = _get_paths(directory=directory, suffix=suffix)

    if vsi_prefix:
        paths = _add_vsi_prefix(paths=paths, vsi_prefix=vsi_prefix)

    df = _create_spark_df(
        spark=spark,
        paths=paths,
        ideal_chunk_size=ideal_chunk_size,
        layer=layer,
    )

    _schema = (
        schema
        if schema
        else _create_schema(
            paths=paths,
            layer=layer,
            ogr_to_spark_type_map=ogr_to_spark_type_map,
            geom_field_name=geom_field_name,
            geom_field_type=geom_field_type,
        )
    )

    parallel_read = _parallel_read_generator(
        geom_field_name=geom_field_name,
        coerce_to_schema=coerce_to_schema,
        spark_to_pandas_type_map=spark_to_pandas_type_map,
        schema=schema,
    )

    num_of_partitions = df.count()

    spark.conf.set("spark.sql.shuffle.partitions", num_of_partitions)

    return (
        df.repartition(num_of_partitions, col("id"))
        .groupby("id")
        .applyInPandas(parallel_read, _schema)
    )
