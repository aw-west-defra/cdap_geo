import os
from pyspark_vector_files import read_vector_files
from fiona import listlayers
from . import write_geoparquet
from . import bng



def ingest(path, suffix, path_to, **kwargs):
  if not path_to.endswith('/'):
    path_to += '/'

  for layer in listlayer(path):
    path_to += layer+'.parquet'
    sdf = read_vector_files(
        path = path,
        suffix = suffix,
        layer_identifier = layer,
        **kwargs
    ) \
      .withColumn('bng', bng('geometry', resolution=100_000))
    write_geoparquet(sdf, path_to)
