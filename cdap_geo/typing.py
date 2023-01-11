from typing import Union, Callable
from pyspark.sql.dataframe import DataFrame as SparkDataFrame
from pyspark.sql.column import Column as SparkSeries
from pandas import DataFrame as PandasDataFrame, Series
from geopandas import GeoDataFrame, GeoSeries
from shapely.geometry.base import BaseGeometry

DataFrame = Union[SparkDataFrame, PandasDataFrame, GeoDataFrame]
Series = Union[SparkSeries, Series, GeoSeries]
Geometry = Union[GeoDataFrame, GeoSeries, BaseGeometry]
