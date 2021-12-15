"""Tests for _pyspark module."""
from contextlib import nullcontext as does_not_raise
from pathlib import Path
from typing import ContextManager, Optional, Union

import pytest
from osgeo.ogr import Open
from pytest import raises

from esa_geo_utils.io._create_initial_df import (
    _add_vsi_prefix,
    _get_data_source_layer_names,
    _get_feature_count,
    _get_layer_name,
    _get_paths,
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


def test__get_data_source_layer_names(fileGDB_path: str) -> None:
    """Returns layer names from dummy FileGDB."""
    data_source = Open(fileGDB_path)
    layer_names = _get_data_source_layer_names(
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
        (2, None, raises(ValueError)),
        (None, "second", does_not_raise()),
    ],
    ids=[
        "Valid layer name",
        "Invalid layer name",
        "Valid layer index",
        "Invalid layer index",
        "None",
    ],
)
def test__get_layer_name(
    fileGDB_path: str,
    layer_identifier: Optional[Union[str, int]],
    expected_layer_name: Optional[str],
    expected_exception: ContextManager,
) -> None:
    """Returns given layer."""
    data_source = Open(fileGDB_path)
    with expected_exception:
        layer_name = _get_layer_name(
            layer_identifier=layer_identifier,
            data_source=data_source,
        )
        assert layer_name == expected_layer_name


def test__get_feature_count(fileGDB_path: str) -> None:
    """0th layer in dummy FileGDB has 2 features."""
    data_source = Open(fileGDB_path)
    feat_count = _get_feature_count(
        data_source=data_source,
        layer_name="first",
    )
    assert feat_count == 2
