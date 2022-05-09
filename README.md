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
other = to_sdf(other) \
  .select('geometry')  # It's good practice to drop unused columns, or they'll be duplicated.

df_input = spark.read.parquet('input.parquet') \  #
  .select('geometry')

# Join
df_intersects = join(df_input, other)  # rsuffix='_right'

# Only keep the intersecting geometry
df_intersection = df_intersects \
  .withColumn('geometry', intersection('geometry', 'geometry_right')) \
  .drop('geometry_right')  # remember to drop unused data as early as possible.

# Buffer that intersection and calculate the area
df_buffered = df_intersection \
  .withColumn('geometry', buffer('geometry')) \
  .withColumn('area', area('geometry'))

# Calculate the boundaries of the buffered geometry
df_with_bounds = df_buffered \
  .withColumn('bounds', bounds('geometry')) \
  .withColumn('minx', F.col('bounds')[0]) \
  .withColumn('miny', F.col('bounds')[1]) \
  .withColumn('maxx', F.col('bounds')[2]) \
  .withColumn('maxy', F.col('bounds')[3]) \
  .drop('bounds')

# Save
out_file = 'output.parquet'
write_geoparquet(df_with_bounds, out_file, crs=27700)

# Plot
gpd.read_parquet(out_file).plot()
```
