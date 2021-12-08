"""Tests for _pyspark module."""
from pathlib import Path
from types import MappingProxyType
from typing import Optional, Union

import pytest
from osgeo.ogr import Layer, Open
from pyspark.sql.types import BinaryType, LongType, StringType, StructField, StructType
from shapely.geometry import Point
from shapely.wkb import loads

from esa_geo_utils.io._pyspark import (
    _add_vsi_prefix,
    _create_schema,
    _get_feature_schema,
    _get_features,
    _get_geometry,
    _get_layer,
    _get_paths,
    _get_properties,
    _get_property_names,
    _get_property_types,
)


def test__get_paths(directory_path: Path, fileGDB_path: str) -> None:
    """Returns collection of FileGDB file paths."""
    paths = _get_paths(directory=str(directory_path), suffix="gdb")
    assert paths == (fileGDB_path,)


@pytest.mark.parametrize(
    argnames="vsi_prefix",
    argvalues=["/vsigzip/", "vsigzip", "/vsigzip", "vsigzip/"],
    ids=[
        "Wrapped by slashes",
        "No slashes",
        "Prefixed with slash",
        "Postfixed with slash",
    ],
)
def test__add_vsi_prefix(fileGDB_path: str, vsi_prefix: str) -> None:
    """VSI prefix is prepended to paths."""
    _paths = (fileGDB_path,)
    prefixed_paths = _add_vsi_prefix(paths=_paths, vsi_prefix=vsi_prefix)
    assert prefixed_paths == ("/" + vsi_prefix.strip("/") + "/" + fileGDB_path,)


_get_layer_PARAMETER_NAMES = [
    "layer",
    "start",
    "stop",
    "expected_layer_name",
    "expected_feature_count",
]

_get_layer_PARAMETER_VALUES = [
    (None, None, None, "second", 2),
    (0, None, None, "second", 2),
    ("first", None, None, "first", 2),
]

_get_layer_PARAMETER_IDS = [
    "No arguments",
    "Layer by index",
    "Layer by name",
]


@pytest.mark.parametrize(
    argnames=_get_layer_PARAMETER_NAMES,
    argvalues=_get_layer_PARAMETER_VALUES,
    ids=_get_layer_PARAMETER_IDS,
)
def test__get_layer(
    fileGDB_path: str,
    layer: Optional[Union[str, int]],
    start: int,
    stop: int,
    expected_layer_name: str,
    expected_feature_count: int,
) -> None:
    """Returns expected layer name and feature count for each method."""
    data_source = Open(fileGDB_path)

    _layer: Layer = _get_layer(
        data_source=data_source,
        layer=layer,
        start=start,
        stop=stop,
    )

    layer_name = _layer.GetName()

    feature_count = _layer.GetFeatureCount()

    assert (layer_name == expected_layer_name) & (
        feature_count == expected_feature_count
    )


def test__get_property_names(fileGDB_path: str) -> None:
    """Returns expected non-geometry field names."""
    data_source = Open(fileGDB_path)
    layer = data_source.GetLayer()
    property_names = _get_property_names(layer=layer)
    assert property_names == ("id", "category")


def test__get_property_types(fileGDB_path: str) -> None:
    """Returns expected non-geometry field types."""
    data_source = Open(fileGDB_path)
    layer = data_source.GetLayer()
    property_types = _get_property_types(layer=layer)
    assert property_types == ("Integer64", "String")


def test__get_feature_schema(
    fileGDB_path: str, ogr_to_spark_mapping: MappingProxyType
) -> None:
    """Returns expected Spark schema."""
    data_source = Open(fileGDB_path)
    layer = data_source.GetLayer()
    schema = _get_feature_schema(
        layer=layer,
        ogr_to_spark_type_map=ogr_to_spark_mapping,
        geom_field_name="geometry",
        geom_field_type="Binary",
    )
    assert schema == StructType(
        [
            StructField("id", LongType()),
            StructField("category", StringType()),
            StructField("geometry", BinaryType()),
        ]
    )


_create_schema_PARAMETER_NAMES = [
    "layer",
]

_create_schema_PARAMETER_VALUES = [
    (None,),
    (0,),
    ("first",),
]

_create_schema_PARAMETER_IDS = [
    "No arguments",
    "Layer by index",
    "Layer by name",
]


@pytest.mark.parametrize(
    argnames=_create_schema_PARAMETER_NAMES,
    argvalues=_create_schema_PARAMETER_VALUES,
    ids=_create_schema_PARAMETER_IDS,
)
def test__create_schema(
    fileGDB_path: str,
    ogr_to_spark_mapping: MappingProxyType,
    layer: Optional[Union[str, int]],
) -> None:
    """Returns expected Spark schema regardless of `_get_layer` function used."""
    schema = _create_schema(
        paths=tuple([fileGDB_path]),
        geom_field_name="geometry",
        geom_field_type="Binary",
        ogr_to_spark_type_map=ogr_to_spark_mapping,
        layer=layer,
    )
    assert schema == StructType(
        [
            StructField("id", LongType()),
            StructField("category", StringType()),
            StructField("geometry", BinaryType()),
        ]
    )


def test__get_properties(fileGDB_path: str) -> None:
    """Properties from 0th row from 0th layer."""
    data_source = Open(fileGDB_path)
    layer = data_source.GetLayer()
    feature = layer.GetFeature(0)
    properties = _get_properties(feature)
    assert properties == (0, "C")


def test__get_geometry(fileGDB_path: str) -> None:
    """Geometry from 0th row from 0th layer."""
    data_source = Open(fileGDB_path)
    layer = data_source.GetLayer()
    feature = layer.GetFeature(0)
    geometry = _get_geometry(feature)
    shapely_object = loads(bytes(geometry[0]))
    assert shapely_object == Point(1, 1)


def test__get_features(fileGDB_path: str) -> None:
    """All fields from the 0th row of the 0th layer."""
    data_source = Open(fileGDB_path)
    layer = data_source.GetLayer()
    features_generator = _get_features(layer)
    *properties, geometry = next(features_generator)
    shapely_object = loads(bytes(geometry))
    assert tuple(properties) == (0, "C")
    assert shapely_object == Point(1, 1)
