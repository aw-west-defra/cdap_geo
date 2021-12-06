"""Tests for _indexing module."""
from typing import Union, Sequence
from shapely.geometry import (
    Point,
    MultiPoint,
    LineString,
    MultiLineString,
    Polygon,
    MultiPolygon)

import pytest

from esa_geo_utils.indexing._indexing import (
    _coords_to_bng,
    _bng_point,
    _bng_multipoint,
    _bng_geom_bounding_box,
    _bng_multigeom_bounding_box)

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


PARAMETERS_POINTS = [
    (Point(535000, 181000),
     1_000,
     ['TQ3581', 'TQ3580', 'TQ3481', 'TQ3480']),
    (Point(535000, 181100),
     1_000,
     ['TQ3581', 'TQ3481']),
    (Point(535000, 181100),
     100,
     ['TQ350811', 'TQ350810', 'TQ349811', 'TQ349810']),
    (Point(535200, 181100),
     1_000,
     ['TQ3581']),
    (Point(535200, 181100),
     100,
     ['TQ352811', 'TQ352810', 'TQ351811', 'TQ351810']),
]


@pytest.mark.parametrize(
    "geometry,resolution,expected_bng",
    PARAMETERS_POINTS,
    ids=[
        "point_vertex_1km",
        "point_edge_1km",
        "point_vertex_100m",
        "point_1km",
        "point_vertex_100m"
    ],
)
def test__bng_point(
    geometry: Point,
    resolution: int,
    expected_bng: Sequence[str],
) -> None:
    """Returns set of expected British National Grid references."""
    assert expected_bng.sort() == _bng_point(geometry, resolution, pad=1).sort()


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
    (MultiPoint([(92000, 10000),
                 (181000, 33000),
                 (529912, 179176),
                 (327106, 1024213)]),
     1_000,
     ['SV9209', 'HY2724', 'SV9210', 'SV9109', 'SW8132',
      'SW8133', 'SW8033', 'SW8032', 'TQ2979', 'SV9110']),
]


@pytest.mark.parametrize(
    "geometry,resolution,expected_bng",
    PARAMETERS_MULTIPOINTS,
    ids=[
        "multipoint_100km",
        "multipoint_1km",
        "multipoint_1m",
        "multipoint_vertex_1km",
    ],
)
def test__bng_multipoint(
    geometry: MultiPoint,
    resolution: int,
    expected_bng: Sequence[str],
) -> None:
    """Returns set of expected British National Grid references."""
    assert expected_bng.sort() == _bng_multipoint(geometry, resolution, pad=1).sort()


PARAMETERS_POLYGONS = [
    (Polygon([(528111, 180999), (528777, 182005), (529816, 181977),
              (529990, 180040), (528878, 180755), (528111, 180999)]),
     1000,
     ['TQ2880', 'TQ2881', 'TQ2882', 'TQ2980', 'TQ2981', 'TQ2982']),
    (Polygon([(528111, 180999), (528777, 182005), (529816, 181977),
              (529990, 180040), (528878, 180755), (528111, 180999)],
             [[(528750, 181241), (529251, 181755), (529742, 181667),
               (529511, 180765), (528750, 181241)]]),
     1000,
     ['TQ2880', 'TQ2881', 'TQ2882', 'TQ2980', 'TQ2981', 'TQ2982']),
    (Polygon([(528511.0, 180999.0), (528777.0, 182005.0), (529816.0, 181977.0),
              (529850.0, 180600.0), (528878.0, 180755.0), (528511.0, 180999.0)]),
     100,
     ['TQ285806', 'TQ285807', 'TQ285808', 'TQ285809', 'TQ285810', 'TQ285811',
      'TQ285812', 'TQ285813', 'TQ285814', 'TQ285815', 'TQ285816', 'TQ285817',
      'TQ285818', 'TQ285819', 'TQ285820', 'TQ286806', 'TQ286807', 'TQ286808',
      'TQ286809', 'TQ286810', 'TQ286811', 'TQ286812', 'TQ286813', 'TQ286814',
      'TQ286815', 'TQ286816', 'TQ286817', 'TQ286818', 'TQ286819', 'TQ286820',
      'TQ287806', 'TQ287807', 'TQ287808', 'TQ287809', 'TQ287810', 'TQ287811',
      'TQ287812', 'TQ287813', 'TQ287814', 'TQ287815', 'TQ287816', 'TQ287817',
      'TQ287818', 'TQ287819', 'TQ287820', 'TQ288806', 'TQ288807', 'TQ288808',
      'TQ288809', 'TQ288810', 'TQ288811', 'TQ288812', 'TQ288813', 'TQ288814',
      'TQ288815', 'TQ288816', 'TQ288817', 'TQ288818', 'TQ288819', 'TQ288820',
      'TQ289806', 'TQ289807', 'TQ289808', 'TQ289809', 'TQ289810', 'TQ289811',
      'TQ289812', 'TQ289813', 'TQ289814', 'TQ289815', 'TQ289816', 'TQ289817',
      'TQ289818', 'TQ289819', 'TQ289820', 'TQ290806', 'TQ290807', 'TQ290808',
      'TQ290809', 'TQ290810', 'TQ290811', 'TQ290812', 'TQ290813', 'TQ290814',
      'TQ290815', 'TQ290816', 'TQ290817', 'TQ290818', 'TQ290819', 'TQ290820',
      'TQ291806', 'TQ291807', 'TQ291808', 'TQ291809', 'TQ291810', 'TQ291811',
      'TQ291812', 'TQ291813', 'TQ291814', 'TQ291815', 'TQ291816', 'TQ291817',
      'TQ291818', 'TQ291819', 'TQ291820', 'TQ292806', 'TQ292807', 'TQ292808',
      'TQ292809', 'TQ292810', 'TQ292811', 'TQ292812', 'TQ292813', 'TQ292814',
      'TQ292815', 'TQ292816', 'TQ292817', 'TQ292818', 'TQ292819', 'TQ292820',
      'TQ293806', 'TQ293807', 'TQ293808', 'TQ293809', 'TQ293810', 'TQ293811',
      'TQ293812', 'TQ293813', 'TQ293814', 'TQ293815', 'TQ293816', 'TQ293817',
      'TQ293818', 'TQ293819', 'TQ293820', 'TQ294806', 'TQ294807', 'TQ294808',
      'TQ294809', 'TQ294810', 'TQ294811', 'TQ294812', 'TQ294813', 'TQ294814',
      'TQ294815', 'TQ294816', 'TQ294817', 'TQ294818', 'TQ294819', 'TQ294820',
      'TQ295806', 'TQ295807', 'TQ295808', 'TQ295809', 'TQ295810', 'TQ295811',
      'TQ295812', 'TQ295813', 'TQ295814', 'TQ295815', 'TQ295816', 'TQ295817',
      'TQ295818', 'TQ295819', 'TQ295820', 'TQ296806', 'TQ296807', 'TQ296808',
      'TQ296809', 'TQ296810', 'TQ296811', 'TQ296812', 'TQ296813', 'TQ296814',
      'TQ296815', 'TQ296816', 'TQ296817', 'TQ296818', 'TQ296819', 'TQ296820',
      'TQ297806', 'TQ297807', 'TQ297808', 'TQ297809', 'TQ297810', 'TQ297811',
      'TQ297812', 'TQ297813', 'TQ297814', 'TQ297815', 'TQ297816', 'TQ297817',
      'TQ297818', 'TQ297819', 'TQ297820', 'TQ298806', 'TQ298807', 'TQ298808',
      'TQ298809', 'TQ298810', 'TQ298811', 'TQ298812', 'TQ298813', 'TQ298814',
      'TQ298815', 'TQ298816', 'TQ298817', 'TQ298818', 'TQ298819', 'TQ298820']),
    (Polygon([(528511.0, 180999.0), (528777.0, 182005.0), (529816.0, 181977.0),
              (529850.0, 180600.0), (528878.0, 180755.0), (528511.0, 180999.0)],
             [[(528750.0, 181241.0), (529251.0, 181755.0), (529742.0, 181667.0),
               (529511.0, 180765.0), (528750.0, 181241.0)]]),
     100,
     ['TQ285806', 'TQ285807', 'TQ285808', 'TQ285809', 'TQ285810', 'TQ285811',
      'TQ285812', 'TQ285813', 'TQ285814', 'TQ285815', 'TQ285816', 'TQ285817',
      'TQ285818', 'TQ285819', 'TQ285820', 'TQ286806', 'TQ286807', 'TQ286808',
      'TQ286809', 'TQ286810', 'TQ286811', 'TQ286812', 'TQ286813', 'TQ286814',
      'TQ286815', 'TQ286816', 'TQ286817', 'TQ286818', 'TQ286819', 'TQ286820',
      'TQ287806', 'TQ287807', 'TQ287808', 'TQ287809', 'TQ287810', 'TQ287811',
      'TQ287812', 'TQ287813', 'TQ287814', 'TQ287815', 'TQ287816', 'TQ287817',
      'TQ287818', 'TQ287819', 'TQ287820', 'TQ288806', 'TQ288807', 'TQ288808',
      'TQ288809', 'TQ288810', 'TQ288811', 'TQ288812', 'TQ288813', 'TQ288814',
      'TQ288815', 'TQ288816', 'TQ288817', 'TQ288818', 'TQ288819', 'TQ288820',
      'TQ289806', 'TQ289807', 'TQ289808', 'TQ289809', 'TQ289810', 'TQ289811',
      'TQ289812', 'TQ289813', 'TQ289814', 'TQ289815', 'TQ289816', 'TQ289817',
      'TQ289818', 'TQ289819', 'TQ289820', 'TQ290806', 'TQ290807', 'TQ290808',
      'TQ290809', 'TQ290810', 'TQ290811', 'TQ290812', 'TQ290813', 'TQ290814',
      'TQ290815', 'TQ290816', 'TQ290817', 'TQ290818', 'TQ290819', 'TQ290820',
      'TQ291806', 'TQ291807', 'TQ291808', 'TQ291809', 'TQ291810', 'TQ291811',
      'TQ291812', 'TQ291813', 'TQ291814', 'TQ291815', 'TQ291816', 'TQ291817',
      'TQ291818', 'TQ291819', 'TQ291820', 'TQ292806', 'TQ292807', 'TQ292808',
      'TQ292809', 'TQ292810', 'TQ292811', 'TQ292812', 'TQ292813', 'TQ292814',
      'TQ292815', 'TQ292816', 'TQ292817', 'TQ292818', 'TQ292819', 'TQ292820',
      'TQ293806', 'TQ293807', 'TQ293808', 'TQ293809', 'TQ293810', 'TQ293811',
      'TQ293812', 'TQ293813', 'TQ293814', 'TQ293815', 'TQ293816', 'TQ293817',
      'TQ293818', 'TQ293819', 'TQ293820', 'TQ294806', 'TQ294807', 'TQ294808',
      'TQ294809', 'TQ294810', 'TQ294811', 'TQ294812', 'TQ294813', 'TQ294814',
      'TQ294815', 'TQ294816', 'TQ294817', 'TQ294818', 'TQ294819', 'TQ294820',
      'TQ295806', 'TQ295807', 'TQ295808', 'TQ295809', 'TQ295810', 'TQ295811',
      'TQ295812', 'TQ295813', 'TQ295814', 'TQ295815', 'TQ295816', 'TQ295817',
      'TQ295818', 'TQ295819', 'TQ295820', 'TQ296806', 'TQ296807', 'TQ296808',
      'TQ296809', 'TQ296810', 'TQ296811', 'TQ296812', 'TQ296813', 'TQ296814',
      'TQ296815', 'TQ296816', 'TQ296817', 'TQ296818', 'TQ296819', 'TQ296820',
      'TQ297806', 'TQ297807', 'TQ297808', 'TQ297809', 'TQ297810', 'TQ297811',
      'TQ297812', 'TQ297813', 'TQ297814', 'TQ297815', 'TQ297816', 'TQ297817',
      'TQ297818', 'TQ297819', 'TQ297820', 'TQ298806', 'TQ298807', 'TQ298808',
      'TQ298809', 'TQ298810', 'TQ298811', 'TQ298812', 'TQ298813', 'TQ298814',
      'TQ298815', 'TQ298816', 'TQ298817', 'TQ298818', 'TQ298819', 'TQ298820']),
]

PARAMETERS_LINESTRINGS = [
    (LineString([(529000, 185000), (530500, 185200),
                 (531111, 184990), (532000, 185000)]),
     1_000,
     ['TQ2884', 'TQ2885', 'TQ2984', 'TQ2985', 'TQ3084',
      'TQ3085', 'TQ3184', 'TQ3185', 'TQ3284', 'TQ3285']),
    (LineString([(532000, 185000), (532099, 185099)]),
     100,
     ['TQ319849', 'TQ319850', 'TQ320849', 'TQ320850']),
]


@pytest.mark.parametrize(
    "geometry,resolution,expected_bng",
    PARAMETERS_POLYGONS + PARAMETERS_LINESTRINGS,
    ids=[
        "polygon_1km",
        "polygon_hole_1km",
        "polygon_100m",
        "polygon_hole_100m",
        "linestring_1km",
        "linestring_100m",
    ],
)
def test__bng_geom_bounding_box(
    geometry: Union[LineString, Polygon],
    resolution: int,
    expected_bng: Sequence[str]
) -> None:
    """Returns set of expected British National Grid references."""
    assert (expected_bng.sort()
            == _bng_geom_bounding_box(geometry, resolution, pad=1).sort())


PARAMETERS_MULTIPOLYGONS = [
    (MultiPolygon([Polygon([(529999, 179999), (531000, 183000),
                            (531500, 185000), (529000, 185500),
                            (527499, 181000), (528500, 180500),
                            (528750, 182000), (529750, 183000),
                            (529500, 179500)]),
                   Polygon([(530600, 181000), (531000, 182000),
                            (531800, 181500), (531200, 180000),
                            (530600, 181000)])]),
     1_000,
     ['TQ2782', 'TQ2883', 'TQ2884', 'TQ2880', 'TQ3080', 'TQ2780', 'TQ3184',
      'TQ2982', 'TQ3081', 'TQ3182', 'TQ2981', 'TQ3085', 'TQ2779', 'TQ2979',
      'TQ2885', 'TQ3180', 'TQ3183', 'TQ3079', 'TQ2985', 'TQ3084', 'TQ3179',
      'TQ2784', 'TQ3083', 'TQ3082', 'TQ3185', 'TQ2783', 'TQ2980', 'TQ2785',
      'TQ2781', 'TQ2881', 'TQ2882', 'TQ2879', 'TQ2984', 'TQ3181', 'TQ2983']),
    (MultiPolygon([Polygon([(529999, 179999), (531000, 183000),
                            (531500, 185000), (529000, 185500),
                            (527499, 181000), (528500, 180500),
                            (528750, 182000), (529750, 183000),
                            (529500, 179500)]),
                   Polygon([(530600, 181000), (531000, 182000),
                            (532200, 181500), (531200, 180000),
                            (530600, 181000)])]),
     1_000,
     ['TQ2782', 'TQ2883', 'TQ2884', 'TQ2880', 'TQ3282', 'TQ3080', 'TQ2780',
      'TQ3184', 'TQ2982', 'TQ3081', 'TQ3182', 'TQ3279', 'TQ2981', 'TQ3085',
      'TQ2779', 'TQ2979', 'TQ2885', 'TQ3281', 'TQ3180', 'TQ3183', 'TQ3079',
      'TQ2985', 'TQ3084', 'TQ3179', 'TQ2784', 'TQ3083', 'TQ3082', 'TQ3185',
      'TQ2783', 'TQ2980', 'TQ2785', 'TQ2781', 'TQ2881', 'TQ2882', 'TQ2879',
      'TQ2984', 'TQ3181', 'TQ2983', 'TQ3280']),
    (MultiPolygon([Polygon([(529999, 179999), (531000, 183000),
                            (531500, 185000), (529000, 185500),
                            (527499, 181000), (528500, 180500),
                            (528750, 182000), (529750, 183000),
                            (529500, 179500)]),
                   Polygon([(533600, 181000), (534000, 182000),
                            (535200, 181500), (534200, 180000),
                            (533600, 181000)])]),
     1_000,
     ['TQ2782', 'TQ2883', 'TQ2884', 'TQ2880', 'TQ3580', 'TQ3080', 'TQ2780',
      'TQ3184', 'TQ3382', 'TQ2982', 'TQ3479', 'TQ3480', 'TQ3081', 'TQ3182',
      'TQ3482', 'TQ3579', 'TQ2981', 'TQ3380', 'TQ3085', 'TQ3481', 'TQ3381',
      'TQ2779', 'TQ2979', 'TQ2885', 'TQ3180', 'TQ3183', 'TQ3582', 'TQ3079',
      'TQ2985', 'TQ3084', 'TQ3179', 'TQ2784', 'TQ3083', 'TQ3082', 'TQ3185',
      'TQ2783', 'TQ2980', 'TQ2785', 'TQ2781', 'TQ2881', 'TQ2882', 'TQ2879',
      'TQ3581', 'TQ2984', 'TQ3181', 'TQ3379', 'TQ2983'])
]

PARAMETERS_MULTILINESTRINGS = [
    (MultiLineString([LineString([(529000, 185000), (530500, 185200),
                                  (531111, 184990), (531990, 184950)]),
                      LineString([(531990, 184950), (532099, 185099)])]),
     1_000,
     ['TQ2884', 'TQ2885', 'TQ2984', 'TQ2985', 'TQ3084', 'TQ3085', 'TQ3184',
      'TQ3185', 'TQ3284', 'TQ3285']),
    (MultiLineString([LineString([(529000, 185000), (530500, 185200),
                                  (531111, 184990), (531990, 184950)]),
                      LineString([(530990, 181950), (531099, 182099)])]),
     1_000,
     ['TQ3082', 'TQ2885', 'TQ2884', 'TQ3182', 'TQ3084', 'TQ2985', 'TQ2984',
      'TQ3184', 'TQ3085', 'TQ3181', 'TQ3081', 'TQ3185'])
]


@pytest.mark.parametrize(
    "geometry,resolution,expected_bng",
    PARAMETERS_MULTIPOLYGONS + PARAMETERS_MULTILINESTRINGS,
    ids=[
        "multipoly_simple_1km",
        "multipoly_merge_1km",
        "multipoly_separate_1km",
        "multiline_complex_1km",
        "multiline_simple_1km"
    ],
)
def test__bng_multigeom_bounding_box(
    geometry: Union[MultiLineString, MultiPolygon],
    resolution: int,
    expected_bng: Sequence[str]
) -> None:
    """Returns set of expected British National Grid references."""
    assert (expected_bng.sort()
            == _bng_multigeom_bounding_box(geometry, resolution, pad=1).sort())
