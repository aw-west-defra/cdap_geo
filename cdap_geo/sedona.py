from pyspark.sql import functions as F, types as T
from .utils import spark
from .typing import *
# https://sedona.apache.org/api/sql/Overview/


def st_register():
  from sedona.register import SedonaRegistrator
  SedonaRegistrator.registerAll(spark)


def st_load(col:str='geometry', force2d:bool=True, simplify:bool=True, precision:int=None) -> SparkSeries:
  '''Read and clean WKB (binary type) into Sedona (udt type)
  '''
  geom = f'ST_GeomFromWKB(HEX({col}))'
  if force2d:
    geom = f'ST_Force_2D({geom})'
  if simplify:
    geom = f'ST_SimplifyPreserveTopology({geom}, 0)'
  if precision is not None:
    geom = f'ST_PrecisionReduce({geom}, {precision})'
  return F.expr(f'''
    CASE WHEN ({col} IS NULL)
      THEN ST_GeomFromText("Point EMPTY")
      ELSE {geom}
    END
  ''')


def st_dump(col:str='geometry') -> SparkSeries:
  '''Reverse to st_load, convert from sedona to wkb
  '''
  return F.expr(f'ST_AsBinary({col})')


def st_explode(col:str='geometry', maxVerticies:int=256) -> SparkSeries:
  '''Explode geometries to optimise
  http://blog.cleverelephant.ca/2019/11/subdivide.html
  Do not ST_Dump first: https://en.wikipedia.org/wiki/Even%E2%80%93odd_rule
  '''
  return F.expr(f'ST_SubDivideExplode({col}, {maxVerticies})')


def st_group(df:SparkDataFrame, key:str, col:str='geometry') -> SparkDataFrame:
  '''Reverse explode with a groupby key and simplify
  '''
  return (df
    .groupBy(key)
    .agg(
      F.expr(f'ST_SimplifyPreserveTopology(ST_Union_Aggr({col}), 0)').alias(col)
    )
  )


def st_intersects(df0:SparkDataFrame, df1:SparkDataFrame) -> SparkDataFrame:
  df0.createOrReplaceTempView('df0')
  df1.createOrReplaceTempView('df1')
  return spark.sql('SELECT df0.* FROM df0, df1 WHERE ST_Intersects(df0.geometry, df1.geometry)')


def st_intersection(df0:SparkDataFrame, df1:SparkDataFrame) -> SparkDataFrame:
  df0.createOrReplaceTempView('df0')
  df1.createOrReplaceTempView('df1')
  df2 = spark.sql('SELECT df0.*, ST_Intersection(df0.geometry, df1.geometry) as geometry_2 FROM df0, df1')
  # SQL doesn't overwrite columns just has 2 with the same name
  return df2.drop('geometry').withColumnRenamed('geometry_2', 'geometry')


def st_join(df_left:SparkDataFrame, df_right:SparkDataFrame, lsuffix='_left', rsuffix='_right', from_wkb=False) -> SparkDataFrame:
  df_left = df_left.withColumnRenamed('geometry', 'geometry'+lsuffix)
  df_right = df_right.withColumnRenamed('geometry', 'geometry'+rsuffix)

  df_left.createOrReplaceTempView('left')
  df_right.createOrReplaceTempView('right')

  geometry_left = f'left.geometry{lsuffix}'
  geometry_right = f'right.geometry{rsuffix}'
  if from_wkb:
    geometry_left = f'ST_GeomFromWKB(hex( {geometry_left} ))'
    geometry_right = f'ST_GeomFromWKB(hex( {geometry_right} ))'

  df = spark.sql(f'''
    SELECT left.*, right.*
    FROM left, right
    WHERE ST_Intersects({geometry_left}, {geometry_right})
  ''')
  
  spark.sql('DROP TABLE left')
  spark.sql('DROP TABLE right')
  return df
