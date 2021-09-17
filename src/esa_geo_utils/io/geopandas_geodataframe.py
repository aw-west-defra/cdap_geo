from typing import Union

from geopandas import GeoDataFrame
from osgeo.ogr import Open

from .common import _get_layer, _properties_from_layer, _geometry_from_layer


def geodataframe_from_ogr(path: str, layer: Union[str, int] = None, sql: str = None, **kwargs) -> GeoDataFrame:
  data_source = Open(path)
  layer = _get_layer(data_source=data_source, layer=layer, sql=sql, **kwargs)
  properties = _properties_from_layer(layer=layer)
  geometry = _geometry_from_layer(layer=layer)
  crs = layer.GetSpatialRef().ExportToWkt()
  return GeoDataFrame(properties, geometry=geometry, crs=crs)