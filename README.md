# Geospatial Functions for CDAP

## Install
```sh
%pip install git+https://github.com/aw-west-defra/cdap_geo
```

## Example
```py
import geopandas as gpd
from cdap_geo.convert import GeoDataFrame_to_SparkDataFrame
from cdap_geo.intersect import (
  index_intersects as intersects,
  index_intersection as intersection,
)
from cdap_geo.write import sdf_write_geoparquet

other = gpd.read_file('other.geojson')
other = other.to_crs(epsg=27700)  # be careful
other = GeoDataFrame_to_SparkDataFrame(other)
dataset = spark.read.parquet('dataset.parquet')

resolution = 10_000  # ∈ (1, 10, 100, 1_000, 10_000, 100_000)

smaller_dataset = intersects(dataset, other, resolution)

sdf_write_geoparquet(smaller_dataset, './path/to/output.parquet', crs=27700)
```


## SubModules

### Typing
Define the shared classes in Spark, Pandas, and GeoPandas, which are DataFrames, Geometries
These are using for type-setting throughout the module.
```py
DataFrame = Union[SparkDataFrame, PandasDataFrame, GeoDataFrame]
Geometry = Union[GeoDataFrame, GeoSeries, BaseGeometry]
```

### Utils
Contains some useful functions reused throughout the module.
```py
spark, sc
get_var_name
wkb
get_tree_size, get_size
sdf_force_execute, sdf_memsize, sdf_print_stats
sdf_groupmax
```

### Convert
Convert between the shared classes defined in typing.
```py
SparkDataFrame_to_SedonaDataFrame
SedonaDataFrame_to_SparkDataFrame
SparkDataFrame_to_GeoDataFrame
GeoDataFrame_to_SparkDataFrame
GeoSeries_to_GeoDataFrame
BaseGeometry_to_GeoDataFrame
```

### Write
Ouput a Spark dataframe as geoparquet.
```py
geoparquetify
sdf_autopartition
sdf_write_geoparquet
```

### Intersect
There is currently four methods for intersecting, UDFs, Sedona, Indexed UDFs, and most recently Bounding Box UDFs.
- UDFs
- Sedona
- Indexed UDFs
- Bounding Box UDFs
```py
area, unary_union, buffer, bounds
gdf_intersects, gdf_intersection
gdf_intersects_udf, gdf_intersects_pudf
sedona_intersects, sedona_intersection
intersects_udf, intersects_pudf, intersection_udf, intersection_pudf
index_join, index_intersects, index_intersection
bbox_join, bbox_intersects, bbox_intersection
```
