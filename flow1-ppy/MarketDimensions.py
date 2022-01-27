import pandas as pd
import mysql.connector
from Getdata_market import *

levels=pd.DataFrame({'rank':[1,2,3],'geolevel':['market','zone','field']})
levels.sort_values(by=['rank'])
levels['obsnum']=range(0,0+len(levels))
levels.sort_values(by=rank)
print(levels)

# **** this is for level 2 queries  ****/
level2=pd.DataFrame({'rank':[1],'typelevel':['Price_Tier_Strategy']})
level2['obsnum']=range(1,len(level2)+1)

level2.sort_values(by=rank)

global lvlcnt,lvllist,fieldlth, lvllistid, lvllistlast1, lvllistlast2, lvl2cnt, lvl2list


levels=levels[levels['geolevel']== ' ']
levels.sort_values(by=['rank'])
lvlcnt=len(levels.columns)

levels=levels[levels['geolevel']== ' ']
levels.sort_values(by=['rank'])					# separated by (,)
lvllist=levels['geolevel']

levels=levels[levels['geolevel']== ' ']
levels.sort_values(by=['rank'])					# separated by (" ")
lvllist2=levels['geolevel']


levels=levels[levels['geolevel']== ' ']
levels.sort_values(by=['rank'])					# separated by (" ")
lvllistid=levels['geolevel'].str.strip()


# select 'a.'||compress(geolevel), 'b.'||compress(geolevel) into: lvllistlast1, :lvllistlast2
# 	from &mylib3..levels
# 	where rank eq &lvlcnt.;
levels[levels['rank']==lvlcnt]					# Doubt for above query 😑
lvllistlast1=levels['geolevel']
lvllistlast2=levels['geolevel']

fieldlth=regions['field'].str.len().max()
print(fieldlth)

catglth=iteminfo['NEWCATEGORY'].str.len().max()
print(catglth)


level2=level2[level2['typelevel ']!= ' ' ]
lvl2cnt=len(level2[level2.columns])


level2=level2[level2['typelevel ']!= ' ' ]
level2.sort_values(by=['rank'])
lvl2list=level2[level2['typelevel']]


fldfrmt=fieldlth.strip()

catfrmt=catglth.strip()

print( lvlcnt,lvllist,fieldlth, lvllistid,lvllistlast1, lvllistlast2, lvl2cnt, lvl2list)