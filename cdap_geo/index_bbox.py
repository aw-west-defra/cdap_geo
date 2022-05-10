from pyspark.sql import functions as F, types as T
from .functions import bounds, intersects_udf


# Bounding Box Join
def bbox_bounds(df, suffix):
  return df \
    .withColumn('bounds', bounds('geometry'+suffix)) \
    .withColumn('minx'+suffix, F.col('bounds')[0]) \
    .withColumn('miny'+suffix, F.col('bounds')[1]) \
    .withColumn('maxx'+suffix, F.col('bounds')[2]) \
    .withColumn('maxy'+suffix, F.col('bounds')[3]) \
    .select('index'+suffix, 'minx'+suffix, 'miny'+suffix, 'maxx'+suffix, 'maxy'+suffix)

def bbox_index(suffix, resolutions, limits):
  @F.udf(returnType=T.ArrayType(T.StringType()))
  def _bbox_index(minx, miny, maxx, maxy):
    bbox_in = lambda x, y:  minx<x<maxx+resolutions[0] and miny<y<maxy+resolutions[1]
    indexes = []
    for x in range(limits[0], limits[2], resolutions[0]):
      for y in range(limits[1], limits[3], resolutions[1]):
        if bbox_in(x, y):
          indexes.append(f'{x}-{y}')
    return indexes
  return _bbox_index(
    F.col('minx'+suffix),
    F.col('miny'+suffix),
    F.col('maxx'+suffix),
    F.col('maxy'+suffix),
  )

def bbox_join(left, right, lsuffix='', rsuffix='_right', resolutions=[100_000, 100_000], limits=[-500_000, -500_000, 1_500_000, 1_500_000], do_bbox=True):
  # Lookup Index
  left = left \
    .withColumnRenamed('geometry', 'geometry'+lsuffix) \
    .withColumn('index'+lsuffix, F.monotonically_increasing_id())
  right = right \
    .withColumnRenamed('geometry', 'geometry'+rsuffix) \
    .withColumn('index'+rsuffix, F.monotonically_increasing_id())
  # Bounds and Spatial Index
  l = bbox_bounds(left, lsuffix) \
    .withColumn('index_spatial', bbox_index(lsuffix, resolutions, limits)) \
    .withColumn('index_spatial', F.explode('index_spatial'))
  r = bbox_bounds(right, rsuffix) \
    .withColumn('index_spatial', bbox_index(rsuffix, resolutions, limits)) \
    .withColumn('index_spatial', F.explode('index_spatial'))
  # Spatial Index Filter
  df = l.join(r, on='index_spatial') \
    .drop('index_spatial').distinct()
  # BBox Filter
  if do_bbox:
    df = df.filter(
        ~ ( (F.col('minx'+lsuffix) > F.col('maxx'+rsuffix))  # east of
        | (F.col('miny'+lsuffix) > F.col('maxy'+rsuffix))  # north of
        | (F.col('maxx'+lsuffix) < F.col('minx'+rsuffix))  # west of
        | (F.col('maxy'+lsuffix) < F.col('miny'+rsuffix)) )  # south of
      )
  df = df.drop(
      'minx'+lsuffix, 'miny'+lsuffix, 'maxx'+lsuffix, 'maxy'+lsuffix,
      'minx'+rsuffix, 'miny'+rsuffix, 'maxx'+rsuffix, 'maxy'+rsuffix,
    )
  # Geometry Filter
  df = df \
    .join(left, on='index'+lsuffix) \
    .join(right, on='index'+rsuffix) \
    .drop('index'+lsuffix, 'index'+rsuffix)
  return df

def bbox_intersects(left, right):
  sdf = bbox_join(left, right) \
    .filter(intersects_udf('geometry', 'geometry_right'))
  return sdf
