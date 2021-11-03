from os import listdir
from os.path import join
from types import MappingProxyType
from typing import Any, Callable, Dict, Generator, Optional, Tuple, Union

from osgeo.ogr import DataSource, Feature, GetFieldTypeName, Layer, Open
from pandas import DataFrame as PandasDataFrame
from pandas import Series
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import Row, SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import (
    ArrayType,
    BinaryType,
    FloatType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

OGR_TO_SPARK = MappingProxyType(
    {
        "String": StringType(),
        "StringList": ArrayType(StringType()),
        "Integer": IntegerType(),
        "DateTime": StringType(),
        "Real": FloatType(),
        "WKB": BinaryType(),
    }
)

SPARK_TO_PANDAS = MappingProxyType(
    {
        ArrayType(StringType()): "object",
        StringType(): "str",
        IntegerType(): "int",
        FloatType(): "float",
        BinaryType(): "object",
    }
)


def _get_layer(
    data_source: DataSource,
    sql: Optional[str],
    layer: Optional[Union[str, int]],
    sql_kwargs: Optional[Dict[str, str]],
) -> Layer:
    """Returns a GDAL Layer from a SQL statement, name or index, or 0th layer."""
    if sql:
        return data_source.ExecuteSQL(sql, **sql_kwargs)
    elif layer:
        return data_source.GetLayer(layer)
    else:
        return data_source.GetLayer()


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
    sql: Optional[str],
    sql_kwargs: Optional[Dict[str, str]],
) -> StructType:
    """Returns a schema for a given layer in the first file in a list of file paths."""
    data_source = Open(paths[0])
    _layer = _get_layer(
        data_source=data_source, sql=sql, layer=layer, sql_kwargs=sql_kwargs
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
    properties_generator = (_get_properties(feature=feature) for feature in layer)
    geometry_generator = (_get_geometry(feature=feature) for feature in layer)
    return (
        properties + geometry
        for properties, geometry in zip(properties_generator, geometry_generator)
    )


def _coerce_to_schema(
    pdf: PandasDataFrame,
    schema: StructType,
    spark_to_pandas_type_map: MappingProxyType,
) -> PandasDataFrame:
    schema_fields = tuple((field.name, field.dataType) for field in schema.fields)

    schema_field_names = tuple(field[0] for field in schema_fields)
    column_names = tuple(column for column in pdf.columns)

    if column_names == schema_field_names:
        return pdf

    additional_columns = tuple(
        column for column in column_names if column not in schema_field_names
    )
    missing_fields = tuple(
        field for field in schema_fields if field[0] not in column_names
    )

    if len(additional_columns) > 0:
        pdf_minus_additional_columns = pdf.drop(columns=additional_columns)
        return pdf_minus_additional_columns

    if len(missing_fields) > 0:
        missing_field_series = tuple(
            Series(name=field[0], dtype=spark_to_pandas_type_map[field[1]])
            for field in missing_fields
        )
        pdf_plus_missing_fields = pdf.append(missing_field_series)
        reindexed_pdf = pdf_plus_missing_fields.reindex(columns=schema_field_names)
        return reindexed_pdf


def _vector_file_to_pdf(
    path: str,
    sql: Optional[str],
    layer: Optional[Union[str, int]],
    geom_field_name: str,
    coerce_to_schema: bool,
    schema: StructType,
    spark_to_pandas_type_map: MappingProxyType,
    sql_kwargs: Optional[Dict[str, str]],
) -> PandasDataFrame:
    """Given a file path and layer, returns a pandas DataFrame."""
    data_source = Open(path)
    _layer = _get_layer(
        data_source=data_source,
        sql=sql,
        layer=layer,
        sql_kwargs=sql_kwargs,
    )
    features_generator = _get_features(layer=_layer)
    feature_names = _get_property_names(layer=_layer) + tuple([geom_field_name])
    pdf = PandasDataFrame(data=features_generator, columns=feature_names)
    if coerce_to_schema:
        coerced_pdf = _coerce_to_schema(
            pdf=pdf,
            schema=schema,
            spark_to_pandas_type_map=spark_to_pandas_type_map,
        )
        return coerced_pdf
    else:
        return pdf


def _get_paths(directory: str, suffix: str) -> Tuple[Any, ...]:
    """Returns full paths for all files in a directory, with the given suffix."""
    paths = listdir(directory)
    return tuple(join(directory, path) for path in paths if path.endswith(suffix))


def _add_vsi_prefix(paths: Tuple[Any, ...], vsi_prefix: str) -> Tuple[Any, ...]:
    """Adds GDAL virtual file system prefix to the paths."""
    return tuple(vsi_prefix + "/" + path for path in paths)


def _create_paths_df(spark: SparkSession, paths: Tuple[Any, ...]) -> SparkDataFrame:
    """Given a list of full paths, returns a DataFrame of those paths."""
    rows = [Row(path=path) for path in paths]
    return spark.createDataFrame(rows)


def _parallel_read_generator(
    sql: Optional[str],
    layer: Optional[Union[str, int]],
    geom_field_name: str,
    coerce_to_schema: bool,
    schema: StructType,
    spark_to_pandas_type_map: MappingProxyType,
    sql_kwargs: Optional[Dict[str, str]],
) -> Callable:
    """Adds arbitrary key word arguments to the wrapped function."""

    def _(pdf: PandasDataFrame) -> PandasDataFrame:
        """Returns a the pandas_udf compatible version of _vector_file_to_pdf."""
        return _vector_file_to_pdf(
            path=pdf["path"][0],
            sql=sql,
            layer=layer,
            geom_field_name=geom_field_name,
            coerce_to_schema=coerce_to_schema,
            schema=schema,
            spark_to_pandas_type_map=spark_to_pandas_type_map,
            sql_kwargs=sql_kwargs,
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
    geom_field_name: str = "geometry",
    geom_field_type: str = "WKB",
    coerce_to_schema: bool = False,
    spark_to_pandas_type_map: MappingProxyType = SPARK_TO_PANDAS,
    vsi_prefix: Optional[str] = None,
    schema: StructType = None,
    layer: Optional[str] = None,
    sql: Optional[str] = None,
    sql_kwargs: Optional[Dict[str, str]] = None,
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
        geom_field_name (str): [description]. Defaults to "geometry".
        geom_field_type (str): [description]. Defaults to "WKB".
        coerce_to_schema (bool): [description]. Defaults to False.
        spark_to_pandas_type_map (MappingProxyType): [description]. Defaults
            to SPARK_TO_PANDAS.
        vsi_prefix (str, optional): [description]. Defaults to None.
        schema (StructType): [description]. Defaults to None.
        layer (str, optional): [description]. Defaults to None.
        sql (str, optional): [description]. Defaults to None.
        sql_kwargs (Dict[str, str], optional): [description]. Defaults to None.

    Returns:
        SparkDataFrame: [description]
    """
    paths = _get_paths(directory=directory, suffix=suffix)

    if vsi_prefix:
        paths = _add_vsi_prefix(paths=paths, vsi_prefix=vsi_prefix)

    num_of_files = len(paths)

    spark.conf.set("spark.sql.shuffle.partitions", num_of_files)

    paths_df = _create_paths_df(spark=spark, paths=paths)

    _schema = (
        schema
        if schema
        else _create_schema(
            paths=paths,
            layer=layer,
            sql=sql,
            ogr_to_spark_type_map=ogr_to_spark_type_map,
            geom_field_name=geom_field_name,
            geom_field_type=geom_field_type,
            sql_kwargs=sql_kwargs,
        )
    )

    parallel_read = _parallel_read_generator(
        sql=sql,
        layer=layer,
        geom_field_name=geom_field_name,
        coerce_to_schema=coerce_to_schema,
        spark_to_pandas_type_map=spark_to_pandas_type_map,
        schema=schema,
        sql_kwargs=sql_kwargs,
    )

    return (
        paths_df.repartition(num_of_files, col("path"))
        .groupby("path")
        .applyInPandas(parallel_read, _schema)
    )
