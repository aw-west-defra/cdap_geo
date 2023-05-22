from . import __version__
from .typing import *
from .utils import spark, wkb, sdf_memsize
from .functions import bounds
from geopandas import read_file
from geopandas.io.arrow import _encode_metadata, _geopandas_to_arrow
from pyspark.sql import functions as F, types as T
from pyarrow import parquet
from os import listdir, path
from glob import glob


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
  if path.startswith('/dbfs/'):
    path = path.replace('/dbfs/', 'dbfs:/')
  if autopartition:
    sdf_autopartition(sdf, partitionBy, inplace, count_ratio, mem_ratio, thead_ratio)
  sdf.write.parquet(path, mode, partitionBy, compression)
  geoparquetify(path, geometry_column, crs, encoding)

  
# Distributed Convert to Parquet
def writer_gpd(f, name):
  df = read_file(f)
  table = _geopandas_to_arrow(df)
  table = table.rename_columns(f'{col}-{table[col].type}' for col in table.column_names)
  parquet.write_to_dataset(table, path_out, basename_template=name)
  return path.join(path_out, name.format(i=1))

@F.udf(T.StringType())
def get_name(f):
  return path.splitext(path.basename(f))[0]+'-{i}'

def distributed_to_parquet(path:str, path_out:str, writer) -> SparkDataFrame:
  '''Read a collection of files using spark and save them as parquet.
  Requires a writer.
  '''
  _writer = F.udf(writer, T.StringType())
  files = glob(path)
  df = (PandasDataFrame({'filepath': files})
    .pipe(spark.createDataFrame)
    .repartition(len(files), 'filepath')
    .withColumn('name', get_name('filepath'))
    .withColumn('filepath_out', _writer('filepath', 'name'))
  )
  return spark.read.option('spark.sql.parquet.mergeSchema', True).parquet(path_out.replace('/dbfs/', 'dbfs:/'))

def distributed_to_geoparquet(*args) -> SparkDataFrame:
  '''Read many vector files and save as geoparquet.
  Using the writer derived from gpd.read_file.
  Columns are renamed to fit their type so schemas can be merged.
  '''
  return distributed_to_parquet(*args, writer=writer_gpd)

def merged_column_names(columns:list, splitter:str='-') -> list:
  '''Merge columns where they have been renamed col-type
  Currently supports:
  - lonecol, which just renames
  - listcol, merges list<item: type> and type, both into the former
  - floatcol, merges double and int64, both into the former
  - else, multiple columns with different types keep their type
  
  WIP: methodology can definitely be improved, suggestions welcome.
  '''
  lonecol = '{0}-{1} AS {0}'.format
  listcol = 'CASE WHEN (`{0}-list<item: {1}>` IS NOT NULL) THEN `{0}-list<item: {1}>` ELSE ARRAY(`{0}-{1}`) END AS `{0}`'.format
  floatcol = 'CASE WHEN (`{0}-double` IS NOT NULL) THEN `{0}-double` ELSE CAST(`{0}-int64` AS FLOAT) END AS `{0}`'.format

  dict_columns = {}
  for s in columns:
    col, typ = s.split(splitter)
    if col not in dict_columns:
      dict_columns[col] = []
    dict_columns[col].append(typ)
  dict_columns

  expr_columns = []
  for col, typs in cols_dict.items():
    if len(typs) == 1:
      expr_columns.append(lonecol(col, typs[0]))
    elif len(typs) == 2:
      if 'int64' in typs and 'double' in typs:
        expr_columns.append(floatcol(col))
      elif f'list<item: {typ[0]}>'==typ[1] or f'list<item: {typ[1]}>'==typ[0]:
        typ = sorted(typs, key=len)[0]
        expr_columns.append(listcol(col, typ))
      else:
        expr_columns.extend(['-'.join([col, typ]) for typ in typs])
    else:
      expr_columns.extend(['-'.join([col, typ]) for typ in typs])
  expr_columns
  return expr_columns

def merge_columns(df:SparkDataFrame):
  '''Apply merged_column_names
  '''
  return df.select(F.expr(col) for col in merged_column_names(df.columns))
