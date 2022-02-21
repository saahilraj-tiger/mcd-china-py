import pandas as pd
import numpy as np

from MarketDimensions import*


FIELDREPORT=pd.read_sas(f"{peRep}")
POST_GeoDropDownlist=pd.read_sas(f"{file['cntr1']}_POST_GeoDropDownlist_VA.sas7dbat")
POST_GeoDropDownlist.merge(FIELDREPORT,POST_GeoDropDownlist,left_on='market',right_on='geoname')

# ------- saahil----- Reports------