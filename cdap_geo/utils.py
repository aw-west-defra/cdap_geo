from .typing import *
import os
from inspect import currentframe
from shapely import wkb as wkb_io
from pyspark.serializers import AutoBatchedSerializer, PickleSerializer
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F


# Define SparkSession and SparkContext
spark = SparkSession.getActiveSession()


# Get the Variable Name
def get_var_name(var, f_back: int = 1):
  _vars = currentframe()
  for _ in range(f_back):
    _vars = _vars.f_back
  for k, v in _vars.f_locals.items():
    if v is var:
      return k


# SparkDataFrame column udf to wkb
def wkb(data):
  return wkb_io.loads(bytes(data))

def wkbs(data):
  return GeoSeries.from_wkb(data)


# File/Dir Size
def get_tree_size(path: str) -> int:
  '''Return sum of sizes of files within folder and subfolders (tree) in bytes.  From PEP 471'''
  total = 0
  for entry in os.scandir(path):
    if entry.is_dir(follow_symlinks=False):
      total += get_tree_size(entry.path)
    else:
      total += entry.stat(follow_symlinks=False).st_size
  return total

def get_size(path: str) -> int:
  '''Return file or tree size in bytes.  Also supports DBFS.'''
  if path.startswith('dbfs:/'):
    path = path.replace('dbfs:/', '/dbfs/')
  if os.path.isfile(path):
    size = os.path.getsize(path)
  elif os.path.isdir(path):
    size = get_tree_size(path)
  return size


# Force Execute
def sdf_force_execute(df: SparkDataFrame):
  df.write.format('noop').mode('overwrite').save()
  return df


# SparkDataFrame Statistics
def sdf_memsize(sdf: SparkDataFrame) -> int:
  rdd = sdf.rdd._reserialize(AutoBatchedSerializer(PickleSerializer()))
  JavaObj = rdd.ctx._jvm.org.apache.spark.mllib.api.python.SerDe.pythonToJava(rdd._jrdd, True)
  return spark._jvm.org.apache.spark.util.SizeEstimator.estimate(JavaObj)

def sdf_print_stats(sdf: SparkDataFrame, name: str = None, f_back: int = 2) -> SparkDataFrame:
  if name is None:
    name = get_var_name(sdf, f_back)
  Count = sdf.count()
  Size = sdf_memsize(sdf)
  Parts = sdf.rdd.getNumPartitions()
  print(f'{name}:  Count={Count},  Size={Size},  Parts={Parts}')
  return sdf


# Maximum of Group
def sdf_groupmax(df, group, maximise):
  return df \
    .withColumn(
      'max',
      F.max(maximise).over(Window.partitionBy(group))
    ) \
    .filter(F.col(maximise) == F.col('max')) \
    .drop('max')
