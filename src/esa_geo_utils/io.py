"""Input / output functions."""

from collections import defaultdict
from glob import glob
from itertools import chain, product
from json import dumps
from os.path import basename
from typing import (
    Any,
    DefaultDict,
    Dict,
    Generator,
    Iterator,
    List,
    Optional,
    Tuple,
    Union,
)
from xml.etree.ElementTree import (  # noqa: S405 - N/A to user-created XML
    Element,
    ElementTree,
    SubElement,
)

from geopandas import GeoDataFrame, GeoSeries
from osgeo.ogr import DataSource, Feature, Layer, Open
from pyspark.sql import DataFrame, SparkSession


def _get_layer(
    data_source: DataSource,
    sql: Optional[str],
    layer: Optional[Union[str, int]],
    **kwargs: str
) -> Layer:
    """Returns a layer from a SQL statement, name or index, or 0th layer."""
    if sql:
        return data_source.ExecuteSQL(sql, **kwargs)
    elif layer:
        return data_source.GetLayer(layer)
    else:
        return data_source.GetLayer()


def _ogr_feature_to_json_feature(feature: Feature, geometry_column_name: str) -> str:
    feature_dictionary = {
        feature.GetFieldDefnRef(index).GetName(): feature.GetField(index)
        for index in range(feature.GetFieldCount())
    }
    feature_dictionary[geometry_column_name] = feature.GetGeomFieldRef(
        0
    ).ExportToIsoWkt()
    return dumps(feature_dictionary)


def _properties_from_feature(feature: Feature) -> Dict[str, Any]:
    return {
        feature.GetFieldDefnRef(index).GetName(): feature.GetField(index)
        for index in range(feature.GetFieldCount())
    }


def _properties_from_layer(layer: Layer) -> Generator[Dict[str, Any], None, None]:
    return (_properties_from_feature(feature) for feature in layer)


def _geometry_from_layer(layer: Layer) -> GeoSeries:
    wkbs = [feature.GetGeomFieldRef(0).ExportToWkb() for feature in layer]
    return GeoSeries.from_wkb(wkbs)


def list_layers(path: str) -> List[str]:
    """Given a path to an OGR file, returns a list of layers.

    Example:
       >>> list_layers("/path/to/ogr/file")
       ["layer0", "layer1", "layer2"]

    Args:
        path (str): path to an OGR file.

    Returns:
        List[str]: a list of layers.
    """
    data_source = Open(path)
    return [
        data_source.GetLayer(index).GetName()
        for index in range(data_source.GetLayerCount())
    ]


def geodataframe_from_ogr(
    path: str,
    layer: Optional[Union[str, int]] = None,
    sql: Optional[str] = None,
    **kwargs: str
) -> GeoDataFrame:
    """Given a path to an OGR file, returns a layer as a `GeoDataFrame`_.

    If a valid SQL statement is provided (and the OGR file supports SQL
    queries), the results will be returned as a layer:

    Example:
        >>> gdf = geodataframe_from_ogr(
                path="/path/to/ogr/file",
                sql="SELECT * FROM layer_name LIMIT 100"
            )

    Else if a layer name or index is provided, that layer will be returned:

    Example:
        >>> gdf = geodataframe_from_ogr(
                path="/path/to/ogr/file",
                layer="layer_name"
            )
        >>> gdf = geodataframe_from_ogr(
                path="/path/to/ogr/file",
                layer=1
            )

    Else if neither is provided, the 0th layer will be returned:

    Example:
        >>> gdf = geodataframe_from_ogr(
                path="/path/to/ogr/file"
            )

    Args:
        path (str): path to an OGR file.
        layer (Union[str, int], optional): a layer name or layer index.
            Defaults to None.
        sql (str, optional): [description]. Defaults to None.
        kwargs: [description].

    Returns:
        GeoDataFrame: [description]

    .. _GeoDataFrame:
        https://geopandas.org/docs/reference/geodataframe.html
    """
    data_source: DataSource = Open(path)
    _layer: Layer = _get_layer(data_source=data_source, layer=layer, sql=sql, **kwargs)
    properties: Generator[Dict[str, Any], None, None] = _properties_from_layer(
        layer=_layer
    )
    geometry: GeoSeries = _geometry_from_layer(layer=_layer)
    crs: str = _layer.GetSpatialRef().ExportToWkt()
    return GeoDataFrame(properties, geometry=geometry, crs=crs)


def _create_path_layer_tuples(paths: List[str]) -> Iterator[Tuple[str, str]]:
    return chain.from_iterable([product([path], list_layers(path)) for path in paths])


def _create_paths_by_layer_dict(
    path_layer_tuples: Iterator[Tuple[str, str]]
) -> DefaultDict[str, List[str]]:
    paths_by_layer = defaultdict(list)
    for path, layer in path_layer_tuples:
        paths_by_layer[layer].append(path)
    return paths_by_layer


def _create_vrt_xml(paths_by_layer: DefaultDict[str, List[str]]) -> ElementTree:
    data_source = Element("OGRVRTDataSource")

    for layer, paths in paths_by_layer.items():
        union_layer = SubElement(data_source, "OGRVRTUnionLayer", {"name": layer})
        layers = [
            SubElement(
                union_layer, "OGRVRTLayer", {"name": basename(path).split(".")[0]}
            )
            for path in paths
        ]

        source_elements = [SubElement(layer, "SrcDataSource") for layer in layers]

        for index, source_element in enumerate(source_elements):
            source_element.text = paths[index]

        layer_elements = [SubElement(layer, "SrcLayer") for layer in layers]

        for layer_element in layer_elements:
            layer_element.text = layer

    return ElementTree(element=data_source)


def vrt_from_ogr(path: str) -> ElementTree:
    """# TODO: public function docstring."""
    paths = glob(path)
    path_layer_tuples = _create_path_layer_tuples(paths)
    paths_by_layer_dict = _create_paths_by_layer_dict(path_layer_tuples)
    return _create_vrt_xml(paths_by_layer_dict)


def _set_geometry_column_metadata(
    df: DataFrame,
    geometry_column_name: str,
    layer: Layer,
) -> None:
    df.schema[geometry_column_name].metadata = {
        "crs": layer.GetSpatialRef().ExportToWkt(),
        "encoding": "WKT",
        "bbox": layer.GetExtent(),
    }


def spark_dataframe_from_ogr(
    path: str,
    spark: SparkSession = SparkSession._activeSession,
    layer: Union[str, int] = None,
    sql: str = None,
    geometry_column_name: str = "geometry",
    **kwargs: str
) -> DataFrame:
    """# TODO: public function docstring."""
    data_source = Open(path)
    _layer = _get_layer(data_source=data_source, layer=layer, sql=sql, **kwargs)
    features_generator = (
        _ogr_feature_to_json_feature(
            feature=feature, geometry_column_name=geometry_column_name
        )
        for feature in _layer
    )
    features_rdd = spark.sparkContext.parallelize(features_generator)
    df = spark.read.json(features_rdd)
    _set_geometry_column_metadata(
        df=df, geometry_column_name=geometry_column_name, layer=_layer
    )
    return df
