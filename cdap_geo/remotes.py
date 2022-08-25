from requests import get
from geopandas import read_file
from pandas import concat


def gpd_read_arcgis(f, limit=200):
  a = '/arcgis/rest/services/'
  b = '/FeatureServer/0/query?'
  f0, f1 = f.split(b)
  name = f0.split(a)[1]
  f0 += b
  f_count = f0 + 'where=1%3D1&returnCountOnly=true&f=json'

  count = get(f_count).json()['count']
  if count < limit:  # Single File
    df = read_file(f)
  else:  # Multiple Files
    dfs = []
    for l in range(1, count, limit):
      u = min(l + limit, count)
      r = 'objectIds=' + ','.join( str(x) for x in range(l, u) ) + '&'
      f_part = f0 + r + f1
      dfs.append( read_file(f_part) )
    df = concat(dfs)
  if '&returnGeometry=false&' in f:
    df = df.drop(columns=['geometry'])
  return df


def gpd_rename(df, rename):
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
    'read': gpd_read_arcgis,
  },
  'county': {
    'link': 'https://services1.arcgis.com/ESMARspQHYMw9BZ9/arcgis/rest/services/Counties_and_Unitary_Authorities_December_2021_UK_BFC/FeatureServer/0/query?where=1%3D1&outFields=CTYUA21NM&outSR=27700&f=json',
    'rename': {'CTYUA21NM': 'County'},
    'read': read_file,
    'read': gpd_read_arcgis,
  },
  'district': {
    'link': 'https://services1.arcgis.com/ESMARspQHYMw9BZ9/arcgis/rest/services/Local_Authority_Districts_December_2021_GB_BFC/FeatureServer/0/query?where=1%3D1&outFields=LAD21NM&outSR=27700&f=json',
    'rename': {'LAD21NM': 'District'},
    'read': gpd_read_arcgis,
  },
  'ward': {
    'link': 'https://services1.arcgis.com/ESMARspQHYMw9BZ9/arcgis/rest/services/Wards_DEC_2021_UK_BFC_V2/FeatureServer/0/query?where=1%3D1&outFields=WD21NM&outSR=27700&f=json',
    'rename': {'WD21NM': 'Ward'},
    'read': gpd_read_arcgis,
  },
  # Natural England
  'npark': {
    'link': 'https://services.arcgis.com/JJzESW51TqeY9uat/arcgis/rest/services/National_Parks_England/FeatureServer/0/query?where=1%3D1&outFields=NAME&outSR=27700&f=json',
    'rename': {'NAME': 'NPark'},
    'read': gpd_read_arcgis,
  },
  'aonb': {
    'link': 'https://services.arcgis.com/JJzESW51TqeY9uat/arcgis/rest/services/Areas_of_Outstanding_Natural_Beauty_England/FeatureServer/0/query?where=1%3D1&outFields=NAME&outSR=27700&f=json',
    'rename': {'NAME': 'AONB'},
    'read': gpd_read_arcgis,
  },
  'sssi': {
    'link': 'https://services.arcgis.com/JJzESW51TqeY9uat/arcgis/rest/services/SSSI_England/FeatureServer/0/query?where=1%3D1&outFields=SSSI_NAME,STATUS&outSR=27700&f=json',
    'rename': {'SSSI_NAME': 'SSSI', 'STATUS': 'SSSI Status'},
    'read': gpd_read_arcgis,
  },
  'nca': {
    'link': 'https://services.arcgis.com/JJzESW51TqeY9uat/arcgis/rest/services/National_Character_Areas_England/FeatureServer/0/query?where=1%3D1&outFields=NCA_Name,NANAME&outSR=27700&f=json',
    'rename': {'NANAME': 'NCA Group', 'NCA_Name': 'NCA'},
    'read': gpd_read_arcgis,
  },
  
  'spa': {
    'link': 'https://services.arcgis.com/JJzESW51TqeY9uat/arcgis/rest/services/Special_Protection_Areas_England/FeatureServer/0/query?where=1%3D1&outFields=SPA_NAME,STATUS&outSR=27700&f=json',
    'rename': {'SPA_NAME': 'SPA', 'STATUS': 'SPA Status'},
    'read': gpd_read_arcgis,
  },
  'sac': {
    'link': 'https://services.arcgis.com/JJzESW51TqeY9uat/arcgis/rest/services/Special_Areas_of_Conservation_England/FeatureServer/0/query?where=1%3D1&outFields=SAC_NAME,STATUS&outSR=27700&f=json',
    'rename': {'SAC_NAME': 'SAC', 'STATUS': 'SAC Status'},
    'read': gpd_read_arcgis,
  },
  'class': {
    'link': 'https://services.arcgis.com/JJzESW51TqeY9uat/arcgis/rest/services/National_Habitat_Networks_England/FeatureServer/0/query?where=1%3D1&outFields=Class&outSR=27700&f=json',
    'rename': {'Class': 'Habitat Class'},
    'read': gpd_read_arcgis,
  },

  'ramsar': {
    'link': 'https://services.arcgis.com/JJzESW51TqeY9uat/arcgis/rest/services/Ramsar_England/FeatureServer/0/query?where=1%3D1&outFields=NAME,STATUS&outSR=27700&f=json',
    'rename': {'NAME': 'RAMSAR', 'STATUS': 'RAMSAR Status'},
    'read': gpd_read_arcgis,
  },
  'peat': {
    'link': 'https://services.arcgis.com/JJzESW51TqeY9uat/arcgis/rest/services/Peaty_Soils_Location_England/FeatureServer/0/query?where=1%3D1&outFields=PCLASSDESC&outSR=27700&f=json',
    'rename': {'PCLASSDESC': 'Peat Class'},
    'read': gpd_read_arcgis,
  },
  'acl': {
    'link': 'https://services.arcgis.com/JJzESW51TqeY9uat/arcgis/rest/services/Provisional Agricultural Land Classification (ALC) (England)/FeatureServer/0/query?where=1%3D1&outFields=GEOGEXT,ALC_GRADE&outSR=27700&f=json',
    'rename': {'GEOGEXT': 'ALC', 'ALC_GRADE': 'ACL Grade'},
    'read': gpd_read_arcgis,
  },
  'trail': {
    'link': 'https://services.arcgis.com/JJzESW51TqeY9uat/arcgis/rest/services/National_Trails_England/FeatureServer/0/query?where=1%3D1&outFields=Name&outSR=27700&f=json',
    'rename': {'Name': 'Trail'},
    'read': gpd_read_arcgis,
  },

  'crow_commons': {
    'link': 'https://services.arcgis.com/JJzESW51TqeY9uat/arcgis/rest/services/CRoW_Act_2000_Section_4_Conclusive_Registered_Common_Land/FeatureServer/0/query?where=1%3D1&outFields=MAPSTATUS,AUTHORITY,NAME&outSR=27700&f=json',
    'rename': {'AUTHORITY': 'Common Group', 'NAME': 'Common', 'MAPSTATUS': 'Common Status'},
    'read': gpd_read_arcgis,
  },
}

list_remotes = list(remote.keys())

def gpd_read_remote(name):
  if name in remote:
    f = remote[name]['link']
    r = remote[name]['rename']
    df = remote[name]['read'](f)
    if isinstance(r, dict):
      df = gpd_rename(df, r)
  return df
