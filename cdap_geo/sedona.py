from pyspark.sql import functions as F, types as T
from .utils import spark
from .typing import *
from re import search
# https://sedona.apache.org/api/sql/Overview/


def st_register():
  from sedona.register import SedonaRegistrator
  SedonaRegistrator.registerAll(spark)

  
def st(fn:str, off:bool=False):
  '''Fix Sedona Functions
  Sedona ST_xxx does not function for null and invalid geoms
  It can also return invalid geometries
  '''
  args = [x.strip() for x in search(r'\((.*)\)', fn).group(1).split(',')]
  arg0, arg1 = args[0], args[1] if 1<len(args) and not args[1].isnumeric() else 'not null'
  off = arg1!='not null' if off else off  # Check arg1 is geometry
  ans0, ans1 = arg1 if off else 'ST_GeomFromText("Point EMPTY")', arg0
  return F.expr(f'''CASE
    WHEN ({arg0} IS NULL) THEN
      {ans0}
    WHEN ({arg1} IS NULL) THEN
      {ans1}
    ELSE
      ST_MakeValid({fn})
  END''')


def st_valid(col:str):
  null = 'ST_GeomFromText("Point EMPTY")'
  return F.expr(f'CASE WHEN ({col} IS NULL) THEN {null} ELSE ST_MakeValid({col}) END')


def st_load(col:str='geometry', force2d:bool=True, simplify:float=0, precision:float=None) -> SparkSeries:
  '''Read and clean WKB (binary type) into Sedona (udt type)
  '''
  geom = f'ST_MakeValid(ST_GeomFromWKB(HEX({col})))'
  if force2d:
    geom = f'ST_Force_2D({geom})'
  if simplify is not None:
    geom = f'ST_SimplifyPreserveTopology({geom}, {simplify})'
  if precision is not None:
    geom = f'ST_PrecisionReduce({geom}, {precision})'
  return st_valid(geom)


def st_buffer(col:str, buf:float, tol:float=1e-6):
  return F.expr(f'ST_MakeValid(ST_Buffer(ST_MakeValid(ST_Buffer({col}, {tol})), {buf}-{tol}))')


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


def st_explode2(col:str='geometry') -> SparkSeries:
  '''Explode geometry collections
  '''
  return F.explode(F.expr(f'ST_Dump({col})'))


def st_group(df:SparkDataFrame, key:str, col:str='geometry', simplify:float=0) -> SparkDataFrame:
  '''Reverse explode with a groupby key and simplify
  '''
  return (df
    .groupBy(key)
    .agg(
      F.expr(f'ST_SimplifyPreserveTopology(ST_Union_Aggr({col}), {simplify})').alias(col)
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
  
  for col in df_left.columns:
    if col in df_right.columns:
      df_left = df_left.withColumnRenamed(col, col+lsuffix)
      df_right = df_right.withColumnRenamed(col, col+rsuffix)

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
