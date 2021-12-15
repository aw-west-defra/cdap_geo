"""Tests for _pyspark module."""
from typing import Optional, Union

import pytest
from osgeo.ogr import Layer, Open

from esa_geo_utils.io._pyspark import _get_layer

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
