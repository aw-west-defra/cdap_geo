from requests import get
from geopandas import read_file
from pandas import concat
from .convert import GeoDataFrame_to_PandasDataFrame, GeoDataFrame_to_SparkDataFrame
from .typing import PandasDataFrame
from .utils import spark


def paths_arcgis(f, batch):
  a = '/arcgis/rest/services/'
  b = '/FeatureServer/0/query?'
  f0, f1 = f.split(b)
  name = f0.split(a)[1]
  f0 += b
  f_count = f0 + 'where=1%3D1&returnCountOnly=true&f=json'

  count = get(f_count).json()['count']
  
  paths = []
  for l in range(1, count, batch):
    u = min(l + batch, count)
    r = 'objectIds=' + ','.join( str(x) for x in range(l, u) ) + '&'
    path = f0 + r + f1
    paths.append(path)
  return paths


def parallel_reader(pdf):
  path = str(pdf['path'][0])
  return read_file(path) \
    .pipe(GeoDataFrame_to_PandasDataFrame)


def sdf_read_arcgis(f, batch=200):
  paths = paths_arcgis(f, batch)
  schema = read_file(paths[0]) \
    .pipe(GeoDataFrame_to_SparkDataFrame) \
    .schema
  df = PandasDataFrame({'path': paths}) \
    .pipe(spark.createDataFrame) \
    .repartition(len(paths)) \
    .groupBy('path') \
    .applyInPandas(parallel_reader, schema)
  if '&returnGeometry=false&' in f:
    df = df.drop('geometry')
  return df


def gdf_read_arcgis(f, batch=200):
  paths = paths_arcgis(f, batch)
  df = concat(read_file(f) for f in paths)
  if '&returnGeometry=false&' in f:
    df = df.drop(columns=['geometry'])
  return df


def gdf_rename(df, rename):
  return df.rename(columns=rename)[[*rename.values(), 'geometry']]



remote = {
  # Defra Magic
  'moorland': {
    'link': 'https://magic.defra.gov.uk/Datasets/Zip_files/magmoor_shp.zip',
    'rename': {'NAME': 'Moor'},
    'read': read_file,
  },
  'lfa': {
    'link': 'https://magic.defra.gov.uk/Datasets/Zip_files/maglfa_shp.zip',
    'rename': {'REF_CODE': 'LFA'},
    'read': read_file,
  },

  # ONS
  'region': {
    'link': 'https://services1.arcgis.com/ESMARspQHYMw9BZ9/arcgis/rest/services/Regions_December_2021_EN_BFC/FeatureServer/0/query?where=1%3D1&outFields=RGN21NM&outSR=27700&f=json',
    'rename': {'RGN21NM': 'Region'},
    'read': gdf_read_arcgis,
  },
  'county': {
    'link': 'https://services1.arcgis.com/ESMARspQHYMw9BZ9/arcgis/rest/services/Counties_and_Unitary_Authorities_December_2021_UK_BFC/FeatureServer/0/query?where=1%3D1&outFields=CTYUA21NM&outSR=27700&f=json',
    'rename': {'CTYUA21NM': 'County'},
    'read': read_file,
    'read': gdf_read_arcgis,
  },
  'district': {
    'link': 'https://services1.arcgis.com/ESMARspQHYMw9BZ9/arcgis/rest/services/Local_Authority_Districts_December_2021_GB_BFC/FeatureServer/0/query?where=1%3D1&outFields=LAD21NM&outSR=27700&f=json',
    'rename': {'LAD21NM': 'District'},
    'read': gdf_read_arcgis,
  },
  'ward': {
    'link': 'https://services1.arcgis.com/ESMARspQHYMw9BZ9/arcgis/rest/services/Wards_DEC_2021_UK_BFC_V2/FeatureServer/0/query?where=1%3D1&outFields=WD21NM&outSR=27700&f=json',
    'rename': {'WD21NM': 'Ward'},
    'read': gdf_read_arcgis,
  },
  # Natural England
  'npark': {
    'link': 'https://services.arcgis.com/JJzESW51TqeY9uat/arcgis/rest/services/National_Parks_England/FeatureServer/0/query?where=1%3D1&outFields=NAME&outSR=27700&f=json',
    'rename': {'NAME': 'NPark'},
    'read': gdf_read_arcgis,
  },
  'aonb': {
    'link': 'https://services.arcgis.com/JJzESW51TqeY9uat/arcgis/rest/services/Areas_of_Outstanding_Natural_Beauty_England/FeatureServer/0/query?where=1%3D1&outFields=NAME&outSR=27700&f=json',
    'rename': {'NAME': 'AONB'},
    'read': gdf_read_arcgis,
  },
  'sssi': {
    'link': 'https://services.arcgis.com/JJzESW51TqeY9uat/arcgis/rest/services/SSSI_England/FeatureServer/0/query?where=1%3D1&outFields=SSSI_NAME,STATUS&outSR=27700&f=json',
    'rename': {'SSSI_NAME': 'SSSI', 'STATUS': 'SSSI Status'},
    'read': gdf_read_arcgis,
  },
  'nca': {
    'link': 'https://services.arcgis.com/JJzESW51TqeY9uat/arcgis/rest/services/National_Character_Areas_England/FeatureServer/0/query?where=1%3D1&outFields=NCA_Name,NANAME&outSR=27700&f=json',
    'rename': {'NANAME': 'NCA Group', 'NCA_Name': 'NCA'},
    'read': gdf_read_arcgis,
  },
  
  'spa': {
    'link': 'https://services.arcgis.com/JJzESW51TqeY9uat/arcgis/rest/services/Special_Protection_Areas_England/FeatureServer/0/query?where=1%3D1&outFields=SPA_NAME,STATUS&outSR=27700&f=json',
    'rename': {'SPA_NAME': 'SPA', 'STATUS': 'SPA Status'},
    'read': gdf_read_arcgis,
  },
  'sac': {
    'link': 'https://services.arcgis.com/JJzESW51TqeY9uat/arcgis/rest/services/Special_Areas_of_Conservation_England/FeatureServer/0/query?where=1%3D1&outFields=SAC_NAME,STATUS&outSR=27700&f=json',
    'rename': {'SAC_NAME': 'SAC', 'STATUS': 'SAC Status'},
    'read': gdf_read_arcgis,
  },
  'class': {
    'link': 'https://services.arcgis.com/JJzESW51TqeY9uat/arcgis/rest/services/National_Habitat_Networks_England/FeatureServer/0/query?where=1%3D1&outFields=Class&outSR=27700&f=json',
    'rename': {'Class': 'Habitat Class'},
    'read': gdf_read_arcgis,
  },

  'ramsar': {
    'link': 'https://services.arcgis.com/JJzESW51TqeY9uat/arcgis/rest/services/Ramsar_England/FeatureServer/0/query?where=1%3D1&outFields=NAME,STATUS&outSR=27700&f=json',
    'rename': {'NAME': 'RAMSAR', 'STATUS': 'RAMSAR Status'},
    'read': gdf_read_arcgis,
  },
  'peat': {
    'link': 'https://services.arcgis.com/JJzESW51TqeY9uat/arcgis/rest/services/Peaty_Soils_Location_England/FeatureServer/0/query?where=1%3D1&outFields=PCLASSDESC&outSR=27700&f=json',
    'rename': {'PCLASSDESC': 'Peat Class'},
    'read': gdf_read_arcgis,
  },
  'acl': {
    'link': 'https://services.arcgis.com/JJzESW51TqeY9uat/arcgis/rest/services/Provisional Agricultural Land Classification (ALC) (England)/FeatureServer/0/query?where=1%3D1&outFields=GEOGEXT,ALC_GRADE&outSR=27700&f=json',
    'rename': {'GEOGEXT': 'ALC', 'ALC_GRADE': 'ACL Grade'},
    'read': gdf_read_arcgis,
  },
  'trail': {
    'link': 'https://services.arcgis.com/JJzESW51TqeY9uat/arcgis/rest/services/National_Trails_England/FeatureServer/0/query?where=1%3D1&outFields=Name&outSR=27700&f=json',
    'rename': {'Name': 'Trail'},
    'read': gdf_read_arcgis,
  },

  'crow_commons': {
    'link': 'https://services.arcgis.com/JJzESW51TqeY9uat/arcgis/rest/services/CRoW_Act_2000_Section_4_Conclusive_Registered_Common_Land/FeatureServer/0/query?where=1%3D1&outFields=MAPSTATUS,AUTHORITY,NAME&outSR=27700&f=json',
    'rename': {'AUTHORITY': 'Common Group', 'NAME': 'Common', 'MAPSTATUS': 'Common Status'},
    'read': gdf_read_arcgis,
  },
}

list_remotes = list(remote.keys())

def gdf_read_remote(name):
  if name in remote:
    f = remote[name]['link']
    r = remote[name]['rename']
    df = remote[name]['read'](f)
    if isinstance(r, dict):
      df = gdf_rename(df, r)
  return df
