"""Tests for _pyspark module."""
from typing import Dict, Optional, Union

import pytest
from osgeo.ogr import Layer, Open

from esa_geo_utils.io._pyspark import _get_layer

PARAMETERS = [
    (None, None, None, "second", 2),
    (None, None, 0, "second", 2),
    (None, None, "first", "first", 2),
    ("SELECT * FROM first WHERE id LIKE 'first_id'", None, None, "first", 1),
]


@pytest.mark.parametrize(
    "sql,sql_kwargs,layer,expected_layer_name,expected_feature_count",
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
    sql_kwargs: Optional[Dict[str, str]],
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
