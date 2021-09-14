from typing import Union

from osgeo.ogr import Layer, Open
from pyspark.sql import DataFrame, SparkSession

from .common import _get_layer, _ogr_feature_to_json_feature


def _set_geometry_column_metadata(df: DataFrame, geometry_column_name: str, layer: Layer, ) -> None:
  df.schema[geometry_column_name].metadata = {
    "crs": layer.GetSpatialRef().ExportToWkt(),
    "encoding": "WKT",
    "bbox": layer.GetExtent()
  }

def ogr_to_spark_dataframe(path: str, spark: SparkSession = SparkSession._activeSession, layer: Union[str, int] = None, sql: str = None, geometry_column_name: str = "geometry", **kwargs) -> DataFrame:
  data_source = Open(path)
  _layer = _get_layer(data_source=data_source, layer=layer, sql=sql, **kwargs)
  features_generator = (_ogr_feature_to_json_feature(feature=feature, geometry_column_name=geometry_column_name) for feature in _layer)
  features_rdd = spark.sparkContext.parallelize(features_generator)
  df = spark.read.json(features_rdd)
  _set_geometry_column_metadata(df=df, geometry_column_name=geometry_column_name, layer=_layer)
  return df