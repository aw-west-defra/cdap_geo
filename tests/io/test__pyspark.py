"""Tests for _pyspark module."""
from pathlib import Path
from types import MappingProxyType
from typing import Dict, Optional, Union

import pytest
from osgeo.ogr import Layer, Open
from pyspark.sql.types import BinaryType, LongType, StringType, StructField, StructType

from esa_geo_utils.io._pyspark import (
    _create_schema,
    _get_feature_schema,
    _get_layer,
    _get_paths,
    _get_property_names,
    _get_property_types,
)

_get_layer_PARAMETER_NAMES = [
    "sql",
    "sql_kwargs",
    "layer",
    "expected_layer_name",
    "expected_feature_count",
]

_get_layer_PARAMETER_VALUES = [
    (None, None, None, "second", 2),
    (None, None, 0, "second", 2),
    (None, None, "first", "first", 2),
    ("SELECT * FROM first WHERE id = 1", None, None, "first", 1),
]

_get_layer_PARAMETER_IDS = [
    "No arguments",
    "Layer by index",
    "Layer by name",
    "Layer by SQL",
]


@pytest.mark.parametrize(
    argnames=_get_layer_PARAMETER_NAMES,
    argvalues=_get_layer_PARAMETER_VALUES,
    ids=_get_layer_PARAMETER_IDS,
)
def test__get_layer(
    fileGDB_path: str,
    sql: Optional[str],
    sql_kwargs: Optional[Dict[str, str]],
    layer: Optional[Union[str, int]],
    expected_layer_name: str,
    expected_feature_count: int,
) -> None:
    """Returns expected layer name and feature count for each method."""
    data_source = Open(fileGDB_path)

    _layer: Layer = _get_layer(
        data_source=data_source,
        sql=sql,
        layer=layer,
        sql_kwargs=sql_kwargs,
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


def test__get_feature_schema(fileGDB_path: str, OGR_TO_SPARK: MappingProxyType) -> None:
    """Returns expected Spark schema."""
    data_source = Open(fileGDB_path)
    layer = data_source.GetLayer()
    schema = _get_feature_schema(
        layer=layer,
        ogr_to_spark_type_map=OGR_TO_SPARK,
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


def test__get_paths(directory_path: Path, fileGDB_path: str) -> None:
    """Returns collection of FileGDB file paths."""
    paths = _get_paths(directory=str(directory_path), suffix="gdb")
    assert paths == tuple([fileGDB_path])


_create_schema_PARAMETER_NAMES = [
    "sql",
    "sql_kwargs",
    "layer",
]

_create_schema_PARAMETER_VALUES = [
    (None, None, None),
    (None, None, 0),
    (None, None, "first"),
    ("SELECT * FROM first WHERE id = 1", None, None),
]

_create_schema_PARAMETER_IDS = [
    "No arguments",
    "Layer by index",
    "Layer by name",
    "Layer by SQL",
]


@pytest.mark.parametrize(
    argnames=_create_schema_PARAMETER_NAMES,
    argvalues=_create_schema_PARAMETER_VALUES,
    ids=_create_schema_PARAMETER_IDS,
)
def test__create_schema(
    fileGDB_path: str,
    ogr_to_spark_mapping: MappingProxyType,
    sql: Optional[str],
    sql_kwargs: Optional[Dict[str, str]],
    layer: Optional[Union[str, int]],
) -> None:
    """Returns expected Spark schema regardless of `_get_layer` function used."""
    schema = _create_schema(
        paths=tuple([fileGDB_path]),
        geom_field_name="geometry",
        geom_field_type="Binary",
        ogr_to_spark_type_map=ogr_to_spark_mapping,
        layer=layer,
        sql=sql,
        sql_kwargs=sql_kwargs,
    )
    assert schema == StructType(
        [
            StructField("id", LongType()),
            StructField("category", StringType()),
            StructField("geometry", BinaryType()),
        ]
    )
