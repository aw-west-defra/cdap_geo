import geohash2 as geohash


def overlap_gen(str0, str1, /,*):
  for chr0, chr1 in zip(str0, str1):
    if chr0 != chr1:
      break
    yield chr0
    

def overlap(str0, str1, /,*):
  return ''.join(overlap_gen(str0, str1))


def encode_box(xmin, ymin, xmax, ymax, /,*, invert=True):
  if invert:  # For shapely bounds
    xmin, ymin, xmax, ymax = ymin, xmin, ymax, xmax    
  return overlap(
    geohash.encode(xmin, ymin),
    geohash.encode(xmax, ymax),
  )


def gdf_geohash(df, /,*, inplace=False):
  if not inplace:
    df = df.copy()
  df['geohash'] = df.apply(lambda df: encode_box(*df.geometry.bounds), axis=1)
  return df
