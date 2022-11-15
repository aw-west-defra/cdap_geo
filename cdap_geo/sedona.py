from geopandas._compat import import_optional_dependency

def register():
  sedona = import_optional_dependency('sedona')
  sedona.SedonaRegistrator.registerAll(spark)


def sedona_intersects(df0, df1):
  df0.createOrReplaceTempView('df0')
  df1.createOrReplaceTempView('df1')
  return spark.sql('SELECT df0.* FROM df0, df1 WHERE ST_Intersects(df0.geometry, df1.geometry)')


def sedona_intersection(df0, df1):
  df0.createOrReplaceTempView('df0')
  df1.createOrReplaceTempView('df1')
  df2 = spark.sql('SELECT df0.*, ST_Intersection(df0.geometry, df1.geometry) as geometry_2 FROM df0, df1')
  # SQL doesn't overwrite columns just has 2 with the same name
  return df2.drop('geometry').withColumnRenamed('geometry_2', 'geometry')


def st_intersects(df_left, df_right, lsuffix='_left', rsuffix='_right', from_wkb=False):
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
