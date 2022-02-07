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
  package_dir = {'': 'src'},
  install_requires = [
    'python >=3.7',
    'GDAL >=3',
    'pyspark >=3',
    # 'shapely @ git+ssh://git@github.com/shapely/shapely.git',
    'geopandas >=0.8',
    # 'esa_geo_utils @ git+ssh://git@github.com/Defra-Data-Science-Centre-of-Excellence/esa_geo_utils.git',
  ],
  extras_require = {
    'sedona': ['sedona >=1'],
  },
)
