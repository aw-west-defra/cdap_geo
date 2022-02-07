from cdap_geo import __version__
from setuptools import setup

setup(
  name = 'cdap_geo',
  version = __version__,
  description = 'Geospatial functions for CDAP.',
  long_description = open('README.md').read(),
  author = 'Andrew West',
  url = 'https://github.com/aw-west-defra/cdap_geo',
  license = 'Crown copywrite',
  install_requires = [
    'python >=3.7',
    'GDAL >=3',
    'pyspark >=3',
    'git+https://github.com/shapely/shapely.git@main',
    'geopandas >=0.8',
    'git+https://github.com/Defra-Data-Science-Centre-of-Excellence/esa_geo_utils.git@develop',
  ],
  extras_require = [
    'sedona >=1'
  ]
)
