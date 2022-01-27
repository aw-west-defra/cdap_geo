import os
from inspect import currentframe
from shapely import wkb as wkb_io
from pyspark import SparkDataFrame
from pyspark.serializers import AutoBatchedSerializer, PickleSerializer


# Get the Variable Name
def get_var_name(var):
  _vars = currentframe().f_back.f_back.f_locals
  for k, v in _vars.items():
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
SparkDataFrame.execute = sdf_force_execute


# SparkDataFrame Statistics
def sdf_memsize(sdf: SparkDataFrame) -> int:
  rdd = sdf.rdd._reserialize(AutoBatchedSerializer(PickleSerializer()))
  JavaObj = rdd.ctx._jvm.org.apache.spark.mllib.api.python.SerDe.pythonToJava(rdd._jrdd, True)
  return spark._jvm.org.apache.spark.util.SizeEstimator.estimate(JavaObj)
SparkDataFrame.memsize = sdf_memsize

def sdf_print_stats(sdf: SparkDataFrame):
  stats = (get_var_name(sdf), sdf.count(), sdf.memsize(), sdf.rdd.getNumPartitions())
  print('{}:  Count:{},  Size:{},  Partitions:{}'.format(*stats))
  return sdf
SparkDataFrame.stats = sdf_print_stats
