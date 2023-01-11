from pyspark.sql import functions as F
from .utils import spark
from geopandas._compat import import_optional_dependency
# https://sedona.apache.org/api/sql/Overview/


def st_register():
  SedonaRegistrator = import_optional_dependency('sedona').register.SedonaRegistrator
  SedonaRegistrator.registerAll(spark)


def st_load(col, force2d=True, simplify=True):
  '''Read and clean WKB (binary type) into Sedona (udt type)
  '''
  geom = f'ST_GeomFromWKB(HEX({col}))'
  if force2d:
    geom = f'ST_Force_2D({geom})'
  if simplify:
    geom = f'ST_SimplifyPreserveTopology({geom}, 0)'
  return F.expr(f'''
    CASE WHEN ({col} IS NULL)
      THEN ST_GeomFromText("Point EMPTY")
      ELSE {geom}
    END
  ''')


def st_explode(df, col, maxVerticies:int=256):
  return (df
    .withColumn(col, F.explode(F.expr(f'ST_Dump({col})')))
    .withColumn(col, F.expr(f'ST_SubDivideExplode({col}, {maxVerticies})'))
  )


def st_dump(col):
  '''Reverse to st_load, convert from sedona to wkb
  '''
  return F.expr(f'ST_AsBinary({col})')


def st_intersects(df0, df1):
  df0.createOrReplaceTempView('df0')
  df1.createOrReplaceTempView('df1')
  return spark.sql('SELECT df0.* FROM df0, df1 WHERE ST_Intersects(df0.geometry, df1.geometry)')


def st_intersection(df0, df1):
  df0.createOrReplaceTempView('df0')
  df1.createOrReplaceTempView('df1')
  df2 = spark.sql('SELECT df0.*, ST_Intersection(df0.geometry, df1.geometry) as geometry_2 FROM df0, df1')
  # SQL doesn't overwrite columns just has 2 with the same name
  return df2.drop('geometry').withColumnRenamed('geometry_2', 'geometry')


def st_join(df_left, df_right, lsuffix='_left', rsuffix='_right', from_wkb=False):
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
