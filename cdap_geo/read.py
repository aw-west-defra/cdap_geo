from .utils import spark
from typing import Union
from struct import unpack
from pyspark.sql import functions as F, types as T
from fiona import listlayers



GeoPackageDialect_scala = '''
import org.apache.spark.sql.jdbc.{JdbcDialect, JdbcDialects}
import org.apache.spark.sql.types._

object GeoPackageDialect extends JdbcDialect {
  override def canHandle(url: String): Boolean = url.startsWith("jdbc:sqlite")
  
  override def getCatalystType(
    sqlType: Int,
    typeName: String, 
    size: Int, 
    md: MetadataBuilder
  ): Option[DataType] = typeName match {
    case "BOOLEAN" => Some(BooleanType)
    case "TINYINT" => Some(ByteType)
    case "SMALLINT" => Some(ShortType)
    case "MEDIUMINT" => Some(IntegerType)
    case "INT" | "INTEGER" => Some(LongType)
    case "FLOAT" => Some(FloatType)
    case "DOUBLE" | "REAL" => Some(DoubleType)
    case "TEXT" => Some(StringType)
    case "BLOB" => Some(BinaryType)
    case "GEOMETRY" | "POINT" | "LINESTRING" | "POLYGON" | "MULTIPOINT" | "MULTILINESTRING" |
      "MULTIPOLYGON" | "GEOMETRYCOLLECTION" | "CIRCULARSTRING" | "COMPOUNDCURVE" |
      "CURVEPOLYGON" | "MULTICURVE" | "MULTISURFACE" | "CURVE" | "SURFACE" => Some(BinaryType)
    case "DATE" => Some(DateType)
    case "DATETIME" => Some(StringType) 
  }
}

JdbcDialects.registerDialect(GeoPackageDialect)
'''


gpb_return_schema = T.StructType([
  T.StructField('magic', T.StringType()),
  T.StructField('version', T.IntegerType()),
  T.StructField('flags', T.StringType()),
  T.StructField('srs_id', T.IntegerType()),
  T.StructField('envelope', T.ArrayType(T.DoubleType()))
])

gpb_unpacking_schema = lambda unpacked:  {
  'magic': unpacked[0].decode('ascii') + unpacked[1].decode('ascii'),
  'version': unpacked[2],
  'flags': format(unpacked[3], 'b').zfill(8),
  'srs_id': unpacked[4],
  'envelope': [unpacked[5], unpacked[6], unpacked[7], unpacked[8]]
}


@F.udf(returnType=gpb_return_schema)
def unpack_gpb_header(byte_array: bytearray) -> T.StructType:
  return gpb_unpacking_schema(unpack('ccBBidddd', byte_array))


def _read_gpkg(filepath, layer):
  sdf = spark.read \
    .format('jdbc') \
    .option('url', f'jdbc:sqlite:{filepath}') \
    .option('dbtable', layer) \
    .load()
  return sdf


def read_gpkg(filepath: str, layer: Union[str, int] = None):
  ''' Read GeoPackage into Spark
  requires scala: GeoPackageDialect
  '''
  HEADER_LENGTH = 40
  split_head = f'SUBSTRING(geom, 0, {HEADER_LENGTH})'
  split_wkb = f'SUBSTRING(geom, {HEADER_LENGTH}+1, LENGTH(geom)-{HEADER_LENGTH})'
  
  if layer is None:
    layer = 0
  if isinstance(layer, int):
    layer = listlayers(filepath)[layer]
  
  try:
    sdf = _read_gpkg(filepath, layer)
  except:
    print('Please run this scala command:\n\n\n%scala'+GeoPackageDialect_scala)
  
  sdf = sdf \
    .withColumn('gpd_header', unpack_gpb_header(F.expr(split_head))) \
    .withColumn('geometry', F.expr(split_wkb)) \
    .drop('geom')

  return sdf
