import pandas as pd
import mysql.connector
from GetData_market_Old import*

levels = pd.DataFrame({'rank': [1, 2, 3], 'geolevel': ['market', 'zone', 'field']})
levels.sort_values(by=['rank'])
levels['obsnum'] = range(0, 0+len(levels))
levels.sort_values(by=rank)
print(levels)

# **** this is for level 2 queries  ****/
level2 = pd.DataFrame({'rank': [1], 'typelevel': ['Price_Tier_Strategy']})
level2['obsnum'] = range(1, len(level2)+1)
level2.sort_values(by=rank)

lvlcnt = levels[levels['geolevel'] != ' '].count()

lvllist  = levels[levels['geolevel'] != ' '].sort_values(by=['rank'])[['geolevel']]

lvllist2  = levels[levels['geolevel'] != ' '].sort_values(by=['rank'])[['geolevel']]

lvllistid  = levels[levels['geolevel'] != ' '].sort_values(by=['rank'])[['geolevel']]

lvllistlast1=levels[levels['rank'] == lvlcnt]['geolevel']
lvllistlast2 =levels[levels['rank'] == lvlcnt]levels['geolevel']

fieldlth = regions['field'].str.len().max()
catglth = iteminfo['NEWCATEGORY'].str.len().max()

lvl2cnt = level2[level2['typelevel'] != ' '].count()

lvl2list = level2[level2['typelevel '] != ' '].sort_values(by=['rank'])[['typelevel']]

fldfrmt = fieldlth

catfrmt = catglth

print(lvlcnt, lvllist, fieldlth, lvllistid,lvllistlast1, lvllistlast2, lvl2cnt, lvl2list)
