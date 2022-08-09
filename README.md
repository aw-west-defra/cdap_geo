# Geospatial Functions for CDAP
Spark and GeoPandas in a small package.  
Convert, save geoparquet, join, and some UDFs.  
*There's more functions for using [Sedona](https://sedona.apache.org/api/sql/Overview/) or just Shapely geometries, but you'll have to read code for them.*


## Install
In Databricks notebook using:
```sh
%pip install git+https://github.com/aw-west-defra/cdap_geo.git
```


## Documentation
This is definitely a work in progress.
```py
from cdap_geo import (
  # Convert
  to_sdf,            # lots of things -> SparkDataFrame
  to_gdf,            # lots of things -> SparkDataFrame
  # UDFs
  area,              # calculate Area -> T.FloatType()
  buffer,            # calculate Buffer -> wkb = T.BinaryType()
  crs,               # convert Coordinate Reference System -> wkb = T.BinaryType()
  bounds,            # calculate Bound -> T.ArrayType([T.FloatType()]*4)
  intersects,        # calculate Intersects -> T.BooleanType()
  intersection,      # calculate Intersection -> wkb = T.BinaryType()
  # Spatial Join
  join,              # spatially Join two SparkDataFrames -> SparkDataFrame
  bng,               # calculate spatial British National Grid index -> T.ArrayType(T.StringType())
  # Read
  read_gpkg          # quickly read GeoPackage -> SparkDataFrame
  ingest             # read dataset folder -> GeoParquet dataset folder with BNG and CRS
  # Write
  write_geoparquet,  # Save with metadata -> None
)
```


## Requirements
[PySpark](https://spark.apache.org/docs/3.1.1/api/python/reference/)  
[GeoPandas](https://geopandas.org/en/stable/docs/reference.html)  
[Shapely](https://shapely.readthedocs.io/en/latest/manual.html)  
Optional:  [PySpark Vector Files](https://github.com/Defra-Data-Science-Centre-of-Excellence/pyspark-vector-files) for loading large files.  
Included:  [BNG Indexer](https://github.com/Defra-Data-Science-Centre-of-Excellence/bng-indexer) for a standard indexing method.  Required for `bng` function.  


## Example
Import libraries, and load data.
```py
import geopandas as gpd
import cdap_geo

other = gpd.read_file('other.geojson')
other = other.to_crs(epsg=27700)  # be careful
other = cdap_geo.to_sdf(other) \
  .select('geometry')  # It's good practice to drop unused columns.

df_input = spark.read.parquet('input.parquet') \
  .select('geometry')  # This will speed up tasks and require less RAM.
```
Spatially join two data together.
```py
df_intersects = cdap_geo.join(df_input, other)  # rsuffix='_right'
```
Only keep the intersecting geometry.
```py
df_intersection = df_intersects \
  .withColumn('geometry', cdap_geo.intersection('geometry', 'geometry_right')) \
  .drop('geometry_right')  # remember to drop unused data as early as possible.
```
Buffer that intersection and calculate the area.
```py
df_buffered = df_intersection \
  .withColumn('geometry', cdap_geo.buffer('geometry')) \
  .withColumn('area', cdap_geo.area('geometry'))
```
Calculate the boundaries of the buffered geometry.  
And move the boundaries to their own columns.
```py
df_with_bounds = df_buffered \
  .withColumn('bounds', cdap_geo.bounds('geometry')) \
  .withColumn('minx', F.col('bounds')[0]) \
  .withColumn('miny', F.col('bounds')[1]) \
  .withColumn('maxx', F.col('bounds')[2]) \
  .withColumn('maxy', F.col('bounds')[3]) \
  .drop('bounds')
```
*Earlier we used `cdap_geo.join` which calculates its own spatial index and bounding box optimisation for a spatial join.  But it is better to apply a spatial index during data ingestion.*  
Add British Nation Grid index for future joining with other datasets.
```py
df_out = df_with_bounds \
  .withColumn('bng', cdap_geo.bng('geometry'))
```
Save to geoparquet and reload the smaller dataframe using GeoPandas to plot it.  
```py
out_file = 'output.parquet'
cdap_geo.write_geoparquet(df_out, out_file, crs=27700)
if cdap_geo.utils.get_size(out_file) < 1e9:
  gpd.read_parquet(out_file).plot()
```
*But we used a secret function `cdap_geo.utils.get_size` find out how large the parquet is, and only load and plot if it's <1GB.*
