from .typing import *
from .utils import spark
from pyspark.sql import functions as F, types as T


def SparkDataFrame_to_SedonaDataFrame(df: SparkDataFrame):
  return df.withColumn('geometry', F.expr('ST_GeomFromWKB(hex(geometry))'))

def SedonaDataFrame_to_SparkDataFrame(df: SparkDataFrame):
  try:
    sdf = df.withColumn('geometry', F.expr('ST_AsBinary(geometry)'))
  except e:  # TODO: limit to correct error
    print('\tSedona missing ST_AsBinary, using fallback slow conversion from UDT to WKT to WKB.')
    sdf = df.withColumn('geometry', F.expr('ST_AsText(geometry)'))
    @F.pandas_udf(returnType=T.BinaryType())
    def wkt2wkb_pudf(col: Series) -> Series:
      return GeoSeries.from_wkt(col).to_wkb()
    sdf = sdf.withColumn('geometry', wkt2wkb_pudf('geometry'))
  return sdf

def SparkDataFrame_to_GeoDataFrame(df: SparkDataFrame, crs: int = 27700):
  pdf = df.toPandas()
  return GeoDataFrame(pdf, geometry=GeoSeries.from_wkb(pdf['geometry'], crs=crs), crs=crs)

def GeoDataFrame_to_PandasDataFrame(gdf: GeoDataFrame):
  return gdf.to_wkb()

def GeoDataFrame_to_SparkDataFrame(gdf: GeoDataFrame):
  return gdf \
    .pipe(GeoDataFrame_to_PandasDataFrame) \
    .pipe(spark.createDataFrame)

def GeoSeries_to_GeoDataFrame(ds: GeoSeries, crs: Union[int, str] = None):
  if hasattr(ds, 'crs'):
    if crs is None:
      crs = ds.crs
    elif crs != ds.crs:
      print(f'\tChanging CRS, provided: {crs} != {ds.crs} :GeoSeries')
  return GeoDataFrame({'geometry':ds}, crs=crs)

def BaseGeometry_to_GeoDataFrame(g, crs):
  return GeoSeries(g).to_GeoDataFrame(crs=crs)



def to_gdf(x:Union[SparkDataFrame, Geometry], crs:Union[int,str]=None) -> GeoDataFrame:
  if isinstance(x, GeoDataFrame):
    gdf = x
  elif isinstance(x, SparkDataFrame):
    gdf = SparkDataFrame_to_GeoDataFrame(x, crs=crs)
  elif isinstance(x, GeoSeries):
    gdf = GeoSeries_to_GeoDataFrame(x, crs=crs)
  elif isinstance(x, GeoSeries):
    gdf = BaseGeometry_to_GeoDataFrame(x, crs=crs)
  else:
    raise TypeError('Unknown type: ', type(x))
  return gdf

def to_sdf(x:Union[SparkDataFrame, Geometry], crs:Union[int,str]=None) -> SparkDataFrame:
  if isinstance(x, SparkDataFrame):
    sdf = x
  elif isinstance(x, GeoDataFrame):
    sdf = GeoDataFrame_to_SparkDataFrame(x)
  else:
    sdf = to_sdf(to_gdf(x, sdf))
  return sdf
