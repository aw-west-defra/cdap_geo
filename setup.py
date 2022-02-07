from setuptools import setup, find_packages

setup(
    name = 'cdap_geo',
    version = '0.1',
    description = 'Geospatial functions for CDAP.',
    long_description = open('README.md').read(),
    author = 'Andrew West',
    url = 'https://github.com/aw-west-defra/cdap_geo',
    license = None,#open('LICENSE').read(),
    packages = find_packages(exclude=('tests', 'docs'))
)