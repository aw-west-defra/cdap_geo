"""Tests for _indexing module."""
from typing import Union, Sequence
from shapely.geometry import MultiPoint

import pytest

from esa_geo_utils.indexing._indexing import _coords_to_bng, _bng_multipoint

PARAMETERS_COORDS = [
    (91794, 10403, 100000, "SV"),
    (181069, 32592, 100000, "SW"),
    (529912, 179176, 100000, "TQ"),
    (327106, 1024213, 100000, "HY"),
    (91794, 10403, 1000, "SV9110"),
    (181069, 32592, 1000, "SW8132"),
    (529912, 179176, 1000, "TQ2979"),
    (327106, 1024213, 1000, "HY2724"),
    (91794, 10403, 1, "SV9179410403"),
    (181069, 32592, 1, "SW8106932592"),
    (529912, 179176, 1, "TQ2991279176"),
    (327106, 1024213, 1, "HY2710624213"),
    (91794.0, 10403.0, 100000, "SV"),
    (181069.0, 32592.0, 100000, "SW"),
    (529912.0, 179176.0, 100000, "TQ"),
    (327106.0, 1024213.0, 100000, "HY"),
    (91794.0, 10403.0, 1000, "SV9110"),
    (181069.0, 32592.0, 1000, "SW8132"),
    (529912.0, 179176.0, 1000, "TQ2979"),
    (327106.0, 1024213.0, 1000, "HY2724"),
    (91794.0, 10403.0, 1, "SV9179410403"),
    (181069.0, 32592.0, 1, "SW8106932592"),
    (529912.0, 179176.0, 1, "TQ2991279176"),
    (327106.0, 1024213.0, 1, "HY2710624213"),
]


@pytest.mark.parametrize(
    "eastings,northings,resolution,expected_bng",
    PARAMETERS_COORDS,
    ids=[
        "Scilly_100km_int",
        "Cornwall_100km_int",
        "London_100km_int",
        "Orkney_100km_int",
        "Scilly_1km_int",
        "Cornwall_1km_int",
        "London_1km_int",
        "Orkney_1km_int",
        "Scilly_1m_int",
        "Cornwall_1m_int",
        "London_1m_int",
        "Orkney_1m_int",
        "Scilly_100km_float",
        "Cornwall_100km_float",
        "London_100km_float",
        "Orkney_100km_float",
        "Scilly_1km_float",
        "Cornwall_1km_float",
        "London_1km_float",
        "Orkney_1km_float",
        "Scilly_1m_float",
        "Cornwall_1m_float",
        "London_1m_float",
        "Orkney_1m_float",
    ],
)
def test__coords_to_bng(
    eastings: Union[int, float],
    northings: Union[int, float],
    resolution: int,
    expected_bng: str,
) -> None:
    """Returns expected British National Grid reference."""
    assert expected_bng == _coords_to_bng(eastings, northings, resolution)


PARAMETERS_MULTIPOINTS = [
    (MultiPoint([(91794, 10403),
                 (181069, 32592),
                 (529912, 179176),
                 (327106, 1024213)]),
     100_000,
     ["SV", "SW", "TQ", "HY"]),
    (MultiPoint([(91794, 10403),
                 (181069, 32592),
                 (529912, 179176),
                 (327106, 1024213)]),
     1_000,
     ["SV9110", "SW8132", "TQ2979", "HY2724"]),
    (MultiPoint([(91794.0, 10403.0),
                 (181069.0, 32592.0),
                 (529912.0, 179176.0),
                 (327106.0, 1024213.0)]),
     1,
     ["SV9179410403", "SW8106932592",
     "TQ2991279176", "HY2710624213"]),
]


@pytest.mark.parametrize(
    "geometry,resolution,expected_bng",
    PARAMETERS_MULTIPOINTS,
    ids=[
        "multipoint_100km",
        "multipoint_1km",
        "multipoint_1m",
    ],
)
def test__bng_multipoint(
    geometry: MultiPoint,
    resolution: int,
    expected_bng: Sequence[str],
) -> None:
    """returns set of expected British National Grid references"""
    assert expected_bng.sort() == _bng_multipoint(geometry, resolution).sort()
