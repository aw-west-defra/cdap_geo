from typing import Union
from pyspark.sql.dataframe import DataFrame as SparkDataFrame
from pandas import DataFrame as PandasDataFrame, Series
from geopandas import GeoDataFrame, GeoSeries
from shapely.geometry.base import BaseGeometry
DataFrame = Union[SparkDataFrame, PandasDataFrame, GeoDataFrame]
Geometry = Union[GeoDataFrame, GeoSeries, BaseGeometry]
