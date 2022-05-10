from cdap_geo import __version__
from setuptools import setup, find_packages

setup(
  name = 'cdap_geo',
  version = __version__,
  description = 'Geospatial functions for CDAP.',
  long_description = open('README.md').read(),
  author = 'Andrew West',
  url = 'https://github.com/aw-west-defra/cdap_geo',
  license = 'Crown copywrite',
  python_requires = '>=3',
  packages = find_packages(),
  install_requires = [
    'pyspark >=3',
    'geopandas >=0.8',
  ],
  extras_require = {
    'sedona >=1',
    'pyspark_vector_files =0.1',
    'bng_indexer @ git+https://github.com/Defra-Data-Science-Centre-of-Excellence/bng-indexer =0.1',
  },
)
