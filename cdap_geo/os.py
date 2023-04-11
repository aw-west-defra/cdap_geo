import pandas as pd
from osdatahub import DataPackageDownload


def dl_packages(key:str):
  return pd.json_normalize(
    DataPackageDownload.all_products(key),
    'versions', record_prefix = 'version-',
    meta = ['id', 'name', 'url', 'createdOn', 'productId', 'productName'],
  )

def dl_packages_tidied(key:str, latest:bool=True, full:bool=True):
  cols_old = ['id', 'version-id', 'version-createdOn', 'version-supplyType', 'version-reason', 'version-format', 'productId', 'productName', 'name']
  cols_new = ['id_product', 'id_version', 'createdOn', 'supplyType', 'reason', 'format', 'productId', 'product', 'name']
  df = (dl_packages(key)
    [cols_old]
    .rename(columns=dict(zip(cols_old, cols_new)))
    .sort_values(['id_product', 'id_version'], ascending=False)
  )
  if latest:
    df = df.groupby('id_product').first().reset_index()
  if full:
    df = df.query('supplyType == "Full"')
  return df

