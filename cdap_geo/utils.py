from .typing import *
import os
from inspect import currentframe
from shapely import wkb as wkb_io
from pyspark.serializers import AutoBatchedSerializer, PickleSerializer
from pyspark.sql import SparkSession


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

def _get_size(path: str) -> int:
  '''Return file or tree size in bytes'''
  if os.path.isfile(path):
    return os.path.getsize(path)
  elif os.path.isdir(path):
    return get_tree_size(path)

def get_size(path: str) -> int:
  '''Support DBFS'''
  if path.startswith('dbfs:/'):
    path = path.replace('dbfs:/', '/dbfs/')
  return _get_size(path)


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
  stats = (name, sdf.count(), sdf_memsize(sdf), sdf.rdd.getNumPartitions())
  print('{}:  Count:{},  Size:{},  Partitions:{}'.format(*stats))
  return sdf
