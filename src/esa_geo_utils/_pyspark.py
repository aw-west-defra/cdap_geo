from os import listdir
from os.path import join
from typing import Any, Callable, Dict, Generator, Optional, Tuple, Union

from osgeo.ogr import DataSource, Feature, GetFieldTypeName, Layer, Open
from pandas import DataFrame as PandasDataFrame
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import Row, SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import DataType, StructField, StructType


def _get_layer(
    data_source: DataSource,
    sql: Optional[str],
    layer: Optional[Union[str, int]],
    **kwargs: Optional[str]
) -> Layer:
    """Returns a GDAL Layer from a SQL statement, name or index, or 0th layer."""
    if sql:
        return data_source.ExecuteSQL(sql, **kwargs)
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
    data_type_map: Dict[str, DataType],
    geom_field_name: str,
    geom_field_type: str,
) -> StructType:
    """Given a GDAL Layer and a data type mapping, return a PySpark DataFrame schema."""
    property_names = _get_property_names(layer=layer)
    property_types = _get_property_types(layer=layer)
    property_struct_fields = [
        StructField(name, data_type_map[type]())
        for name, type in zip(property_names, property_types)
    ]
    geometry_struct_field = [
        StructField(geom_field_name, data_type_map[geom_field_type]())
    ]
    return StructType(property_struct_fields + geometry_struct_field)


def _create_schema(
    paths: Tuple[Any, ...],
    data_type_map: Dict[str, DataType],
    geom_field_name: str,
    geom_field_type: str,
    layer: Optional[Union[str, int]],
    sql: Optional[str],
    **kwargs: Optional[str]
) -> StructType:
    """Returns a schema for a given layer in the first file in a list of file paths."""
    data_source = Open(paths[0])
    _layer = _get_layer(data_source=data_source, sql=sql, layer=layer, **kwargs)
    return _get_feature_schema(
        layer=_layer,
        data_type_map=data_type_map,
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


def _gdal_to_pdf(
    path: str,
    sql: Optional[str],
    layer: Optional[Union[str, int]],
    geom_field_name: str,
    **kwargs: Optional[str]
) -> PandasDataFrame:
    """Given a file path and layer, returns a pandas DataFrame."""
    data_source = Open(path)
    _layer = _get_layer(data_source=data_source, sql=sql, layer=layer, **kwargs)
    features_generator = _get_features(layer=_layer)
    feature_names = _get_property_names(layer=_layer) + tuple([geom_field_name])
    return PandasDataFrame(data=features_generator, columns=feature_names)


def _get_paths(directory: str, suffix: str) -> Tuple[Any, ...]:
    """Returns full paths for all files in a directory, with the given suffix."""
    paths = listdir(directory)
    return tuple(join(directory, path) for path in paths if path.endswith(suffix))


def _create_paths_df(spark: SparkSession, paths: Tuple[Any, ...]) -> SparkDataFrame:
    """Given a list of full paths, returns a DataFrame of those paths."""
    rows = [Row(path=path) for path in paths]
    return spark.createDataFrame(rows)


def _parallel_read_generator(
    sql: Optional[str],
    layer: Optional[Union[str, int]],
    geom_field_name: str,
    **kwargs: Optional[str]
) -> Callable:
    """Adds arbitrary key word arguments to the wrapped function."""

    def _(pdf: PandasDataFrame) -> PandasDataFrame:
        """Returns a the pandas_udf compatible version of _gdal_to_pdf."""
        return _gdal_to_pdf(
            path=pdf["path"][0],
            sql=sql,
            layer=layer,
            geom_field_name=geom_field_name,
            **kwargs,
        )

    return _


def gdal_to_dataframe(
    directory: str,
    data_type_map: Dict[str, DataType],
    spark: SparkSession = SparkSession._activeSession,
    suffix: str = "*",
    geom_field_name: str = "geometry",
    geom_field_type: str = "WKB",
    layer: Optional[str] = None,
    sql: Optional[str] = None,
    **kwargs: Optional[str]
) -> SparkDataFrame:
    """Given a folder of vector files, returns a Spark DataFrame."""
    paths = _get_paths(directory=directory, suffix=suffix)

    num_of_files = len(paths)

    spark.conf.set("spark.sql.shuffle.partitions", num_of_files)

    paths_df = _create_paths_df(spark=spark, paths=paths)

    schema = _create_schema(
        paths=paths,
        layer=layer,
        sql=sql,
        data_type_map=data_type_map,
        geom_field_name=geom_field_name,
        geom_field_type=geom_field_type,
        **kwargs,
    )

    parallel_read = _parallel_read_generator(
        sql=sql,
        layer=layer,
        geom_field_name=geom_field_name,
    )

    return (
        paths_df.repartition(num_of_files, col("path"))
        .groupby("path")
        .applyInPandas(parallel_read, schema)
    )
