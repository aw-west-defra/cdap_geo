from .typing import *
from .utils import spark, wkb
from pyspark.sql import functions as F, types as T
from typing import Union


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



def read_gpkg(filepath: str, layer: Union[str, int] = None):
  ''' Read GeoPackage into Spark
  requires scala: GeoPackageDialect
  '''
  HEADER_LENGTH = 40
  
  _read_gpkg = lambda filepath, layer:  spark.read.format('jdbc').option('url', f'jdbc:sqlite:{filepath}').option('dbtable', layer).load()
  _sdfcol_tolist = lambda sdf, col:  sdf.toPandas()[col].tolist()
  _listlayers_gpkg = lambda filepath: _sdfcol_tolist(_read_gpkg(filepath, 'gpkg_contents'), 'table_name')
  _gpd2wkb = lambda header_length:  F.expr(f'SUBSTRING(geometry, {header_length}+1, LENGTH(geometry)-{header_length}) AS geometry')
  
  if layer is None:
    layer = 0
  if isinstance(layer, int):
    layers = _listlayers_gpkg(filepath)
    layer = layers[layer]
    if 1 < len(layers):
      print(f'\tSelecting: {layer} from {layers}')
  
  sdf = _read_gpkg(filepath, layer) \
    .withColumnRenamed('geom', 'geometry') \
    .withColumn('geometry', _gpd2wkb(HEADER_LENGTH)) \

  return sdf
