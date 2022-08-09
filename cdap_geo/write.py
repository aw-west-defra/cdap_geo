from . import __version__
from .typing import *
from .utils import spark, wkb, sdf_memsize
from . import bounds
from geopandas.io.arrow import _encode_metadata
from pyspark.sql import functions as F, types as T
from pyarrow import parquet
from os import listdir


# GeoParquet-ify
def geoparquetify(
  path: str,
  geometry_column: str = 'geometry',
  crs: Union[int, str] = None,
  encoding: str = 'WKB',
) -> None:
  '''
  Hack spark parquet to interoperate with geoparquet standard
  by Dan Lewis
  '''

  # Verify Path
  if not path.endswith('/'):
    path += '/'
  if path.startswith('/dbfs/'):
    root = path
    path = path.replace('/dbfs/', 'dbfs:/')
  elif path.startswith('dbfs:/'):
    root = path.replace('dbfs:/', '/dbfs/')

  # Bounds
  bbox = spark.read.parquet(path) \
    .withColumn(
      'bounds', bounds(geometry_column)
    ).select(
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
  bbox = [
    bbox['min(minx)'],
    bbox['min(miny)'],
    bbox['max(maxx)'],
    bbox['max(maxy)'],
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
    'creator': {'library': 'cdap_geo', 'version': __version__},
  }
  
  # 0th part of the parquet file.
  for file in listdir(root):
    if 'part-00000' in file:
      part0_path = root + file
      break

  part = parquet.read_table(part0_path)
  metadata = part.schema.metadata
  metadata.update({b'geo': _encode_metadata(geo_metadata)})
  part = part.replace_schema_metadata(metadata)
  parquet.write_table(part, part0_path)


# AutoPartition
def sdf_autopartition(sdf: SparkDataFrame, partitionBy: str = None, inplace: bool = False,
    count_ratio: float = 1e-6, mem_ratio: float = 1/1024**2, thead_ratio: float = 1.5) -> SparkDataFrame:
  jobs_cap = 100_000
  numPartitions = (
    round(sdf.rdd.countApprox(800, .8) * count_ratio),
    round(sdf_memsize(sdf) * mem_ratio),
    round(spark.sparkContext.defaultParallelism * thead_ratio),
  )
  numPartitions = [min(r, jobs_cap) for r in numPartitions]
  if max(numPartitions) <= sdf.rdd.getNumPartitions():
    return sdf
  print(f'\tRepartitioning: From {sdf.rdd.getNumPartitions()}, To max{numPartitions}')
  sdf_repartitioned = sdf.repartition(max(numPartitions), partitionBy)
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
  count_ratio: float = 1e-6,
  mem_ratio: float = 1/1024**2,
  thead_ratio: float = 1.5,
  # geoparquetify
  geometry_column: str = 'geometry',
  crs: Union[int, str] = None,
  encoding: str = 'WKB',
) -> None:
  if autopartition:
    sdf_autopartition(sdf, partitionBy, inplace, count_ratio, mem_ratio, thead_ratio)
  sdf.write.parquet(path, mode, partitionBy, compression)
  geoparquetify(path, geometry_column, crs, encoding)
