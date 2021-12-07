"""Module level fixtures."""
from _pytest.tmpdir import TempPathFactory
from geopandas import GeoDataFrame
from pytest import fixture
from shapely.geometry import Point


@fixture
def first_layer() -> GeoDataFrame:
    """First dummy layer."""
    return GeoDataFrame(
        data={
            "id": ["first_id", "second_id"],
            "geometry": [Point(0, 0), Point(1, 0)],
        },
        crs="EPSG:27700",
    )


@fixture
def second_layer() -> GeoDataFrame:
    """Second dummy layer."""
    return GeoDataFrame(
        data={
            "id": ["first_id", "second_id"],
            "geometry": [Point(1, 1), Point(0, 1)],
        },
        crs="EPSG:27700",
    )


@fixture
def fileGDB_path(
    tmp_path_factory: TempPathFactory,
    first_layer: GeoDataFrame,
    second_layer: GeoDataFrame,
) -> str:
    """Writes dummy layers to FileGDB and returns path as string."""
    path = tmp_path_factory.getbasetemp() / "data_source.gdb"

    path_as_string = str(path)

    first_layer.to_file(
        filename=path_as_string,
        index=False,
        layer="first",
    )

    second_layer.to_file(
        filename=path_as_string,
        index=False,
        layer="second",
    )

    return path_as_string
