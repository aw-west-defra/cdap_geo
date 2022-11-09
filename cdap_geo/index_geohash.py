from geohash2 import encode


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
    encode(xmin, ymin),
    encode(xmax, ymax),
  )


def gdf_geohash(df, /,*, column='geohash', inplace=False):
  if not inplace:
    df = df.copy()
  df[column] = df.apply(lambda df: encode_box(*df.geometry.to_crs('WGS84').bounds), axis=1)
  return df
