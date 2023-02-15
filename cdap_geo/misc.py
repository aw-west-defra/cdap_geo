from numpy import arange
from shapely.geometry import box
from .typing import GeoDataFrame


def split_grid(bbox:tuple, splits:int) -> GeoDataFrame:
  '''Split a box into a dataframe of equally divided boxes.
  Will created splits^2 rows.
  '''
  x0, y0, x1, y1 = bbox
  dx, dy = (x1-x0)/splits, (y1-y0)/splits
  X, Y = arange(x0, x1+dx, dx), arange(y0, y1+dy, dy)
  return GeoDataFrame({'geometry': [
    box(xmin, ymin, xmax, ymax)
    for (xmin, xmax) in zip(X[:-1], X[1:])
    for (ymin, ymax) in zip(Y[:-1], Y[1:])
  ]})


if __name__ is '__main__':
  df = split_grid([0,6,4,10], 4)
  df.boundary.plot(ax=df.plot(alpha=.3)).axis('off')
