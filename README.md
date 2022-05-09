# Geospatial Functions for CDAP
Spark and GeoPandas in a small package.  
Convert, save geoparquet, join, and some UDFs.  
[PySpark](https://spark.apache.org/docs/3.1.1/api/python/reference/)  
[GeoPandas](https://geopandas.org/en/stable/docs/reference.html)  
[Shapely](https://shapely.readthedocs.io/en/latest/manual.html)  
[PySpark Vector Files](https://github.com/Defra-Data-Science-Centre-of-Excellence/pyspark-vector-files) for loading large files.  
*There's more functions for using [Sedona](https://sedona.apache.org/api/sql/Overview/) or just Shapely geometries, but you'll have to read code for them.*


## Install
```sh
%pip install git+https://github.com/aw-west-defra/cdap_geo.git
```


## Example
```py
import geopandas as gpd
from cdap_geo import (
  # Convert
  to_sdf,            # lots of things -> SparkDataFrame
  to_gdf,            # lots of things -> SparkDataFrame
  # UDFs
  area,              # calculate Area -> float
  buffer,            # calculate Buffer -> wkb
  bounds,            # calculate Bound -> 4 floats
  intersects,        # calculate Intersects -> boolean
  intersection,      # calculate Intersection -> wkb
  # Spatial Join
  join,              # spatially Join two SparkDataFrames -> SparkDataFrame
  # Write
  write_geoparquet,  # Save with metadata -> None
)

other = gpd.read_file('other.geojson')
other = other.to_crs(epsg=27700)  # be careful
other = to_sdf(other)

dataset = spark.read.parquet('dataset.parquet')

smaller_dataset = cdap_geo.join(dataset, other) \
  .drop('geometry_right') \
  .withColumn('geometry', cdap_geo.buffer('geometry')) \
  .withColumn('buffered_area', cdap_geo.area('geometry')) \
  .drop('geometry')

out_file = './path/to/output.parquet'
write_geoparquet(smaller_dataset, out_file, crs=27700)

gpd.read_parquet(out_file).plot()
```
