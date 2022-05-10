from ._indexing import calculate_bng_index
from pyspark.sql import functions as F, types as T


def bng_index(column, resolution):
  '''Spatial Indexing
  currying resolution ∈ (1, 10, 100, 1_000, 10_000, 100_000)
  '''
  @F.udf(returnType=T.ArrayType(T.StringType()))
  def _bng_index(column):
    return calculate_bng_index(column, resolution=resolution, how='intersects')
  return _bng_index(column)


def bng_join(left, right, resolution):
  left = ( left
    .withColumn('index_left', F.monotonically_increasing_id()) )
  right = ( right
    .withColumnRenamed('geometry', 'geometry_right')
    .withColumn('index_right', F.monotonically_increasing_id()) )
  left_index = ( left
    .withColumn('index_spatial', bng_index('geometry', resolution))
    .withColumn('index_spatial', F.explode('index_spatial'))
    .select('index_left', 'index_spatial') )
  right_index = ( right
    .withColumn('index_spatial', bng_index('geometry_right', resolution))
    .withColumn('index_spatial', F.explode('index_spatial'))
    .select('index_right', 'index_spatial') )
  sdf = ( SparkDataFrame
    .join(left_index, right_index, on='index_spatial')
    .drop('index_spatial').distinct()
    .join(left, on='index_left')
    .join(right, on='index_right')
    .drop('index_left', 'index_right') )
  return sdf
