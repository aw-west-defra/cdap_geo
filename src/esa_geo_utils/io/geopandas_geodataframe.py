from typing import Union

from geopandas import GeoDataFrame, read_file
from osgeo.ogr import Open

from .common import _get_layer, _layer_to_geojson


def ogr_to_geodataframe(path: str, layer: Union[str, int] = None, sql: str = None, **kwargs) -> GeoDataFrame:
  data_source = Open(path)
  _layer = _get_layer(data_source=data_source, layer=layer, sql=sql, **kwargs)
  geojson = _layer_to_geojson(layer=_layer)
  crs_wkt = _layer.GetSpatialRef().ExportToWkt()
  return read_file(geojson, crs_wkt=crs_wkt)