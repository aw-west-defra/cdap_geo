"""Tests for _pyspark module."""
from typing import Optional, Union

import pytest
from osgeo.ogr import Layer, Open

from esa_geo_utils._pyspark import _get_layer

PARAMETERS = [
    (None, None, "second", 2),
    (None, 0, "second", 2),
    (None, "first", "first", 2),
    ("SELECT * FROM first WHERE id LIKE 'first_id'", None, "first", 1),
]


@pytest.mark.parametrize(
    "sql,layer,expected_layer_name,expected_feature_count",
    PARAMETERS,
    ids=[
        "No arguments",
        "Layer by index",
        "Layer by name",
        "Layer by SQL",
    ],
)
def test__get_layer(
    fileGDB_path: str,
    sql: Optional[str],
    layer: Optional[Union[str, int]],
    expected_layer_name: str,
    expected_feature_count: int,
) -> None:
    """Returns expected layer name and feature count for each method."""
    data_source = Open(fileGDB_path)

    _layer: Layer = _get_layer(data_source=data_source, sql=sql, layer=layer)

    layer_name = _layer.GetName()

    feature_count = _layer.GetFeatureCount()

    assert (layer_name == expected_layer_name) & (
        feature_count == expected_feature_count
    )
