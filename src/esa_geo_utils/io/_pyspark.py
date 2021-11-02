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
        StructField(field_name, data_type_map[field_type])
        for field_name, field_type in zip(property_names, property_types)
    ]
    geometry_struct_field = [
        StructField(geom_field_name, data_type_map[geom_field_type])
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


def _vector_file_to_pdf(
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
    **kwargs: Optional[str]
) -> Callable:
    """Adds arbitrary key word arguments to the wrapped function."""

    def _(pdf: PandasDataFrame) -> PandasDataFrame:
        """Returns a the pandas_udf compatible version of _vector_file_to_pdf."""
        return _vector_file_to_pdf(
            path=pdf["path"][0],
            sql=sql,
            layer=layer,
            geom_field_name=geom_field_name,
            **kwargs,
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
    data_type_map: Dict[str, DataType],
    spark: SparkSession = SparkSession._activeSession,
    suffix: str = "*",
    geom_field_name: str = "geometry",
    geom_field_type: str = "WKB",
    vsi_prefix: Optional[str] = None,
    layer: Optional[str] = None,
    sql: Optional[str] = None,
    **kwargs: Optional[str]
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
        data_type_map (Dict[str, DataType]): [description]
        spark (SparkSession): [description]. Defaults to SparkSession._activeSession.
        suffix (str): [description]. Defaults to "*".
        geom_field_name (str): [description]. Defaults to "geometry".
        geom_field_type (str): [description]. Defaults to "WKB".
        vsi_prefix (str, optional): [description]. Defaults to None.
        layer (str, optional): [description]. Defaults to None.
        sql (str, optional): [description]. Defaults to None.
        **kwargs (str, optional): [description].

    Returns:
        SparkDataFrame: [description]
    """
    paths = _get_paths(directory=directory, suffix=suffix)

    if vsi_prefix:
        paths = _add_vsi_prefix(paths=paths, vsi_prefix=vsi_prefix)

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
        **kwargs,
    )

    return (
        paths_df.repartition(num_of_files, col("path"))
        .groupby("path")
        .applyInPandas(parallel_read, schema)
    )
