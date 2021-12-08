"""Tests for _pyspark module."""
from contextlib import nullcontext as does_not_raise
from pathlib import Path
from types import MappingProxyType
from typing import ContextManager, Optional, Union

import pytest
from osgeo.ogr import Layer, Open
from pyspark.sql.types import StructType
from pytest import raises
from shapely.geometry import Point
from shapely.wkb import loads

from esa_geo_utils.io._pyspark import (
    _add_vsi_prefix,
    _create_schema,
    _get_feature_count,
    _get_feature_schema,
    _get_features,
    _get_geometry,
    _get_layer,
    _get_layer_name,
    _get_layer_names,
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
    argnames=[
        "vsi_prefix",
    ],
    argvalues=[
        ("/vsigzip/",),
        ("vsigzip",),
        ("/vsigzip",),
        ("vsigzip/",),
    ],
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


def test__get_layer_names(fileGDB_path: str) -> None:
    """Returns layer names from dummy FileGDB."""
    data_source = Open(fileGDB_path)
    layer_names = _get_layer_names(
        data_source=data_source,
    )
    assert layer_names == ("second", "first")


@pytest.mark.parametrize(
    argnames=[
        "layer",
        "expected_layer_name",
        "expected_exception",
    ],
    argvalues=[
        ("first", "first", does_not_raise()),
        ("third", None, raises(ValueError)),
        (0, "second", does_not_raise()),
        (None, "second", does_not_raise()),
    ],
    ids=[
        "Valid layer name",
        "Invalid layer name",
        "Index",
        "None",
    ],
)
def test__get_layer_name(
    fileGDB_path: str,
    layer: Optional[Union[str, int]],
    expected_layer_name: Optional[str],
    expected_exception: ContextManager,
) -> None:
    """Returns given layer."""
    data_source = Open(fileGDB_path)
    with expected_exception:
        layer_name = _get_layer_name(
            data_source=data_source,
            layer=layer,
        )
        assert layer_name == expected_layer_name


def test__get_feature_count(fileGDB_path: str) -> None:
    """0th layer in dummy FileGDB has 2 features."""
    data_source = Open(fileGDB_path)
    layer = data_source.GetLayer()
    feat_count = _get_feature_count(layer=layer)
    assert feat_count == 2


@pytest.mark.parametrize(
    argnames=[
        "layer",
        "start",
        "stop",
        "expected_layer_name",
        "expected_feature_count",
    ],
    argvalues=[
        (None, None, None, "second", 2),
        (0, None, None, "second", 2),
        ("first", None, None, "first", 2),
        ("second", 0, 1, "second", 1),
    ],
    ids=[
        "No arguments",
        "Layer by index",
        "Layer by name",
        "Layer chunk",
    ],
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
    fileGDB_path: str,
    fileGDB_schema: StructType,
    ogr_to_spark_mapping: MappingProxyType,
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
    assert schema == fileGDB_schema


@pytest.mark.parametrize(
    argnames=[
        "layer",
    ],
    argvalues=[
        (None,),
        (0,),
        ("first",),
    ],
    ids=[
        "No arguments",
        "Layer by index",
        "Layer by name",
    ],
)
def test__create_schema(
    fileGDB_path: str,
    fileGDB_schema: StructType,
    ogr_to_spark_mapping: MappingProxyType,
    layer: Optional[Union[str, int]],
) -> None:
    """Returns expected Spark schema regardless of `_get_layer` function used."""
    schema = _create_schema(
        paths=(fileGDB_path,),
        geom_field_name="geometry",
        geom_field_type="Binary",
        ogr_to_spark_type_map=ogr_to_spark_mapping,
        layer=layer,
    )
    assert schema == fileGDB_schema


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


# def test__get_fields(fileGDB_schema: StructType) -> None:
#     pass
