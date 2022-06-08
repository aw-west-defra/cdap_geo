from requests import get
from geopandas import read_file

known_arcgis = {
  # ONS
  'region': 'https://services1.arcgis.com/ESMARspQHYMw9BZ9/arcgis/rest/services/Regions_December_2021_EN_BFC/FeatureServer/0/query?where=1%3D1&outFields=RGN21NM&outSR=27700&f=json',
  'county': 'https://services1.arcgis.com/ESMARspQHYMw9BZ9/arcgis/rest/services/Counties_and_Unitary_Authorities_December_2021_UK_BFC/FeatureServer/0/query?where=1%3D1&outFields=CTYUA21NM&outSR=27700&f=json',
  'district': 'https://services1.arcgis.com/ESMARspQHYMw9BZ9/arcgis/rest/services/Local_Authority_Districts_December_2021_GB_BFC/FeatureServer/0/query?where=1%3D1&outFields=LAD21NM&outSR=27700&f=json',
  'ward': 'https://services1.arcgis.com/ESMARspQHYMw9BZ9/arcgis/rest/services/Wards_DEC_2021_UK_BFC_V2/FeatureServer/0/query?where=1%3D1&outFields=WD21NM&outSR=27700&f=json',
  # Natural England
  'npark': 'https://services.arcgis.com/JJzESW51TqeY9uat/arcgis/rest/services/National_Parks_England/FeatureServer/0/query?where=1%3D1&outFields=NAME&outSR=27700&f=json',
  'aonb': 'https://services.arcgis.com/JJzESW51TqeY9uat/arcgis/rest/services/Areas_of_Outstanding_Natural_Beauty_England/FeatureServer/0/query?where=1%3D1&outFields=NAME&outSR=27700&f=json',
  'sssi': 'https://services.arcgis.com/JJzESW51TqeY9uat/arcgis/rest/services/SSSI_England/FeatureServer/0/query?where=1%3D1&outFields=SSSI_NAME,STATUS&outSR=27700&f=json',
  'spa': 'https://services.arcgis.com/JJzESW51TqeY9uat/arcgis/rest/services/Special_Protection_Areas_England/FeatureServer/0/query?where=1%3D1&outFields=SPA_NAME,STATUS&outSR=27700&f=json',
  'sac': 'https://services.arcgis.com/JJzESW51TqeY9uat/arcgis/rest/services/Special_Areas_of_Conservation_England/FeatureServer/0/query?where=1%3D1&outFields=SAC_NAME,STATUS&outSR=27700&f=json',
  'class': 'https://services.arcgis.com/JJzESW51TqeY9uat/arcgis/rest/services/National_Habitat_Networks_England/FeatureServer/0/query?where=1%3D1&outFields=Class&outSR=27700&f=json',

  'ramsar': 'https://services.arcgis.com/JJzESW51TqeY9uat/arcgis/rest/services/Ramsar_England/FeatureServer/0/query?where=1%3D1&outFields=NAME,STATUS&outSR=27700&f=json',
  'peat': 'https://services.arcgis.com/JJzESW51TqeY9uat/arcgis/rest/services/Peaty_Soils_Location_England/FeatureServer/0/query?where=1%3D1&outFields=PCLASSDESC&outSR=27700&f=json',
  'grade': 'https://services.arcgis.com/JJzESW51TqeY9uat/arcgis/rest/services/Provisional Agricultural Land Classification (ALC) (England)/FeatureServer/0/query?where=1%3D1&outFields=GEOGEXT,ALC_GRADE&outSR=27700&f=json',
  'trail': 'https://services.arcgis.com/JJzESW51TqeY9uat/arcgis/rest/services/National_Trails_England/FeatureServer/0/query?where=1%3D1&outFields=Name&outSR=27700&f=json',
  'moorland': 'https://services.arcgis.com/JJzESW51TqeY9uat/arcgis/rest/services/Moorland_Change_Map_England_2020_2021/FeatureServer/0/query?where=1%3D1&outFields=LOCATION&outSR=27700&f=json',

  'crow_open_country': 'https://services.arcgis.com/JJzESW51TqeY9uat/arcgis/rest/services/CRoW_Act_2000_Section_4_Conclusive_Open_Country/FeatureServer/0/query?where=1%3D1&outFields=COLLECTION,MAPPINGARE,MAPSTATUS,DATESIGNED,CACHAIR,ISSUEDATE&outSR=27700&f=json',
  'crow_commons': 'https://services.arcgis.com/JJzESW51TqeY9uat/arcgis/rest/services/CRoW_Act_2000_Section_4_Conclusive_Registered_Common_Land/FeatureServer/0/query?where=1%3D1&outFields=UNIQUEID,MAPPINGARE,MAPSTATUS,AUTHORITY,CL_NUMBER,NAME,DATESIGNED,CACHAIR&outSR=27700&f=json'
}

def read_arcgis(f, limit=1000):
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
    return NotImplementedError(f'{name} has {count:,} rows > {limit:,} limit.  Multi-read solution coming...')
    df = []
    for l in range(1, count, limit):
      u = min(l + limit, count)
      r = 'objectIds=' + ','.join( str(x) for x in range(l, u) ) + '&'
      f_part = f0 + r + f1
      df.append( read_file(f_part) )
  return df, count
