sc
from pyspark.sql import functions as F, types as T
from cdap_geo_utils import __version__
from cdap_geo_utils.utils import wkb, sdf_memsize
from geopandas.io.arrow import _encode_metadata
from pyarrow import parquet
# Typing
from typing import Union
from pyspark.sql.dataframe import DataFrame as SparkDataFrame
from pandas import DataFrame as PandasDataFrame
from geopandas import GeoDataFrame, GeoSeries
from shapely.geometry.base import BaseGeometry
DataFrame = Union[SparkDataFrame, PandasDataFrame, GeoDataFrame]
Geometry = Union[GeoDataFrame, GeoSeries, BaseGeometry]


# GeoParquet-ify
def geoparquetify(
  path: str,
  geometry_column: str = 'geometry',
  crs: Union[int, str] = None,
  encoding: str = 'WKB',
) -> None:
  '''Hack spark parquet to interoperate with geoparquet standard'''
  _get_bounds = F.udf(
    lambda column: wkb(column).bounds,
    returnType = T.ArrayType(T.DoubleType()),
  )

  data = spark.read.parquet(path)
  data = data.withColumn('bounds', _get_bounds('geometry'))

  # Generate minx, miny, maxx, maxy
  bounds = (data
    .select(
      F.col('bounds')[0].alias('minx'),
      F.col('bounds')[1].alias('miny'),
      F.col('bounds')[2].alias('maxx'),
      F.col('bounds')[3].alias('maxy'),
    ).agg({
      'minx': 'min',
      'miny': 'min',
      'maxx': 'max',
      'maxy': 'max'
    }).collect()[0]
  )
  bbox = [
    bounds['min(minx)'],
    bounds['min(miny)'],
    bounds['max(maxx)'],
    bounds['max(maxy)'],
  ]

  # Create metadata dictionary
  geo_metadata = {
    'primary_column': geometry_column,
    'columns': {geometry_column: {
      'crs': crs,
      'encoding': encoding,
      'bbox': bbox,
    }},
    'schema_version': '0.1.0',
    'creator': {'library': 'cdap_geo_utils', 'version': __version__},
  }
  
  # 0th part of the parquet file.
  for part in dbutils.fs.ls(path):  # TODO: not dbutils
    if 'part-00000' in part.name:
      part0_path = part.path.replace('dbfs:/', '/dbfs/')
      break

  part = parquet.read_table(part0_path)
  metadata = part.schema.metadata
  metadata.update({b'geo': _encode_metadata(geo_metadata)})
  part = part.replace_schema_metadata(metadata)
  parquet.write_table(part, part0_path)


# AutoPartition
def sdf_autopartition(sdf: SparkDataFrame, column: str = 'geometry', inplace: bool = False) -> SparkDataFrame:
  jobs_cap = 100_000
  numPartitions = (
    round(sdf.count() / 1e6),
    round(sdf_memsize(sdf) / 1024**2),
    sc.defaultParallelism * 1.5,
  )
  numPartitions = [min(r, jobs_cap) for r in numPartitions]
  if max(numPartitions) <= sdf.rdd.getNumPartitions():
    return sdf
  print(f'\tRepartitioning: From {sdf.rdd.getNumPartitions()}, To max{numPartitions})')
  sdf_repartitioned = sdf.repartition(max(numPartitions), column)
  if inplace:
    sdf = sdf_repartitioned
  return sdf_repartitioned


# Write GeoParquet
def sdf_write_geoparquet(
  sdf: SparkDataFrame,
  path: str,
  # write.parquet
  mode: str = None,
  partitionBy: Union[str, list] = None,
  compression: str = None,
  # autopartition
  autopartition: bool = False,
  inplace: bool = False,
  # geoparquetify
  geometry_column: str = 'geometry',
  crs: Union[int, str] = None,
  encoding: str = 'WKB',
) -> None:
  if autopartition:
    sdf_autopartition(sdf, geometry_column, inplace)
  sdf.write.parquet(path, mode, partitionBy, compression)
  geoparquetify(path, geometry_column, crs, encoding)
