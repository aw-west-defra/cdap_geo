from json import dumps
from typing import List, Optional, Union

from geojson import Feature as geojsonFeature
from geojson import FeatureCollection
from osgeo.ogr import DataSource
from osgeo.ogr import Feature as ogrFeature
from osgeo.ogr import Layer, Open


def _get_layer(data_source: DataSource, sql: Optional[str], layer: Optional[Union[str, int]], **kwargs) -> Layer:
  if sql:
    return data_source.ExecuteSQL(sql, **kwargs)
  elif layer:
    return data_source.GetLayer(layer)
  else: 
    return data_source.GetLayer()


def _ogr_feature_to_json_feature(feature: ogrFeature, geometry_column_name: str) -> str:
  feature_dictionary = {feature.GetFieldDefnRef(index).GetName(): feature.GetField(index) for index in range(feature.GetFieldCount())}
  feature_dictionary[geometry_column_name] = feature.GetGeomFieldRef(0).ExportToIsoWkt()
  return dumps(feature_dictionary)


def _ogr_feature_to_geojson_feature(feature: ogrFeature) -> geojsonFeature:
  feature_dictionary = feature.ExportToJson(as_object=True)
  return geojsonFeature(**feature_dictionary)


def _layer_to_geojson(layer: Layer) -> str:
  features_list = [_ogr_feature_to_geojson_feature(feature) for feature in layer]
  feature_collection = FeatureCollection(features_list)
  return dumps(feature_collection)


def list_layers(path: str) -> List[str]:
  data_source = Open(path)
  return [data_source.GetLayer(index).GetName() for index in range(data_source.GetLayerCount())]
