import pandas as pd
import os
import shutil
import glob

from datetime import date
import mysql.connector
from param_file import *

with open(r'C:\Users\gopala.chennu\Desktop\Current\vs\Codes\master.yml', 'r') as doc:
    file = yaml.load(doc, Loader=yaml.FullLoader)

tday=pd.to_datetime(file['round1start']).date().day().zfill(2)
tmonth=pd.to_datetime(file['round1start']).date().month().zfill(2)
tyear=pd.to_datetime(file['round1start']).date().year().zfill(4)
pday=pd.to_datetime(file['round1pre']).date().day().zfill(2)
pmonth=pd.to_datetime(file['round1pre']).date().month().zfill(2)
pyear=pd.to_datetime(file['round1pre']).date().year().zfill(4)

print(tday,tmonth,tyear)
print()
print(pday,pmonth,pyear)
print()

round1str=str(tyear+"-"+tmonth+"-"+tday)
print(round1str)

round1pre=str(pyear+"-"+pmonth+"-"+pday)
print(round1pre)

#select WK_END_THU_ID_NU into: startweek from connection to redshift
WK_END_THU=pd.read_sql('''
		select *
		from RMDW_Tables.WK_END_THU
		where DATE %(round1str)s between wk_end_thu_beg_dt and wk_end_thu_end_dt
		order by WK_END_THU_ID_NU;


''',redshift,params={'round1str':file['round1str']})

startweek=WK_END_THU['WK_END_THU_ID_NU']

#select * from connection to redshift
weekenddates1a =pd.read_sql('''
        select *, wk_end_thu_end_yr_nu-%(yrback)s as lastyear
		from RMDW_Tables.WK_END_THU
		where (WK_END_THU_ID_NU between %(startweek)s-%(burn)s-%(weeks)s+1 and (%(startweek)s-%(burn)s))
			or (WK_END_THU_ID_NU between ( %(startweek)s+1) and %(startweek)s+%(weeks)s)
		order by WK_END_THU_ID_NU
''',redshift,params={'yrback':file['yrback','startweek':startweek,'burn':file['burn'],'weeks':file['weeks']]})

weekenddates1=pd.DataFrame(weekenddates1a)

weekenddates1.loc[weekenddates1['WK_END_THU_ID_NU ']>=startweek,'period']='CY'

weekenddates1.loc[weekenddates1['WK_END_THU_ID_NU ']<startweek,'period']='Pre_CY'

minlast=weekenddates1['lastyear'].min()
maxlast=weekenddates1['lastyear'].max()

print(minlast,maxlast)


#connect to redshift(dsn=gdap user=&reduser. pwd="&redpass.");
weekenddates2=pd.read_sql('''
        select *
		from RMDW_Tables.WK_END_THU
		where wk_end_thu_end_yr_nu between %(minlast)s and %(maxlast)s
		order by WK_END_THU_ID_NU;
''',redshift,params={'minlast':minlast,'maxlast':maxlast})

weekenddates1[weekenddates1['wk_end_thu_wk_nu']==53 ]=52
weekenddates1c=weekenddates1

weekenddates3=pd.read_sql('''
select a.*, b.period, b.lastyear
from weekenddates2 a join weekenddates1c b on (a.wk_end_thu_wk_nu=b.wk_end_thu_wk_nu and a.wk_end_thu_end_yr_nu=b.lastyear);
''')

weekenddates=weekenddates3.append(weekenddates1)

weekenddates.loc[(weekenddates['lastyear']==weekenddates['wk_end_thu_end_yr_nu'] & weekenddates['period']=='CY'),'period']='LY'

weekenddates.loc[(weekenddates['lastyear']==weekenddates['wk_end_thu_end_yr_nu'] & weekenddates['period']=='Pre_CY'),'period']='Pre_LY'

weekenddates.drop(['lastyear'],axis=1)

#/*** APPEND 208 WEEKS/16 QUARTERS/4 YEARS OF WEEKLY PMIX AND GUEST COUNTS TOGETHER ***/

def loop2():
    weekenddates4=weekenddates.drop(['load_dw_audt_ts','updt_dw_audt_ts','dw_file_id'],inplace=True)
    weekenddates4['obsnum']=range(1,len(weekenddates4)+1)

    numweeks=len(weekenddates4.columns)

    if glob.glob(f'{lisa}wkly_pmix1.*'):
        os.remove(f'{lisa}wkly_pmix1.*')
    if glob.glob(f'{lisa}gstcnts1.*'):
        os.remove(f'{lisa}gstcnts1.*')
    if glob.glob(f'{lisa}wkly_pmix.*'):
        os.remove(f'{lisa}wkly_pmix.*')
    if glob.glob(f'{lisa}gstcnts.*'):
        os.remove(f'{lisa}gstcnts.*')

    # for loop doubt





    # pending





    wkly_pmix1=pd.DataFrame(f"{mylib1}wkly_pmix1")
    append_data1=wkly_pmix1.append(wkly_pmix)       #base is wkly_pmix1

    gstcnts1=pd.DataFrame(f"{mylib1}gstcnts")
    append_data2=gstcnts1.append(gstcnts1)

loop2()

#connect to redshift(dsn=gdap user=&reduser. pwd="&redpass.");
PriceSnap1=pd.read_sql('''
        select * from connection to redshift
		(select a.MCD_GBAL_LCAT_ID_NU, a.SLD_MENU_ITM_ID,
			a.prc_eff_dt, a.prc_end_dt, a.sld_menu_itm_actv_fl, a.frnt_cter_itm_prc_am
		from RMDW_Tables.LCAT_MENU_ITM_PRC_SNAP a
		where a.terr_cd=%(terr)s and (a.prc_eff_dt between DATE %(pre1str)s and DATE %(round1str)s+(%(weekacp)s*7))
			and frnt_cter_itm_prc_am is not null and frnt_cter_itm_prc_am >0
		order by a.MCD_GBAL_LCAT_ID_NU, a.SLD_MENU_ITM_ID, a.prc_eff_dt
		);

''',redshift,params={'terr':file['terr'],'pre1str':file['pre1str'],'round1str':file['round1str'],'weekacp':file['weekacp']})

# /*** get all prices during the preround that ended during the post round - basically the prices that changed***/

OLDPriceSnap1=pd.read_sql('''
        select * from connection to redshift
		(select a.MCD_GBAL_LCAT_ID_NU, a.SLD_MENU_ITM_ID,
			a.prc_eff_dt, a.prc_end_dt, a.sld_menu_itm_actv_fl, a.frnt_cter_itm_prc_am
		from RMDW_Tables.LCAT_MENU_ITM_PRC_SNAP a
		where a.terr_cd=%(terr)s. and (a.prc_eff_dt < DATE %(pre1str)s)
			and (a.prc_end_dt between DATE %(pre1str)s-1 and DATE %(round1str)s+(%(weekacp)s*7))
			and frnt_cter_itm_prc_am is not null and frnt_cter_itm_prc_am >0
		order by a.MCD_GBAL_LCAT_ID_NU, a.SLD_MENU_ITM_ID, a.prc_eff_dt
		);

''',redshift,params={'terr':file['terr'],'pre1str':file['pre1str'],'round1str':file['round1str'],'weekacp':file['weekacp']})

PriceSnap2=PriceSnap1.append(OLDPriceSnap1)
PriceSnap2.loc[PriceSnap2['prc_end_dt '].notna(),'prc_end_dt']='31DEC2099'

PriceSnap3=pd.read_sql('''select MCD_GBAL_LCAT_ID_NU, SLD_MENU_ITM_ID,
			min(prc_eff_dt) as prc_eff_dt , max(prc_end_dt) as prc_end_dt , frnt_cter_itm_prc_am
	from PriceSnap2
	group by  MCD_GBAL_LCAT_ID_NU, SLD_MENU_ITM_ID, frnt_cter_itm_prc_am;

''')


# PriceSnap3=PriceSnap2
# PriceSnap3.groupby(['MCD_GBAL_LCAT_ID_NU', 'SLD_MENU_ITM_ID', 'frnt_cter_itm_prc_am']).agg({'prc_eff_dt':np.min(),'(prc_end_dt) ':np.max()})

# price_recs_round=f'{mylib3}{price_recs_round}'+file['round']
# PriceSnap3['rcom_prc']=price_recs_round['rcom_prc']

PriceSnap4a =pd.read_sql('''select a.*,  b.rcom_prc
	from PriceSnap3 a left join price_recs_round&round. b on (a.MCD_GBAL_LCAT_ID_NU=b.MCD_GBAL_LCAT_ID_NU and a.sld_menu_itm_id=b.sld_menu_itm_id)
	where prc_eff_dt >= "%(round1pre)s"

''',inter1,params={'round1pre':file['round1pre']})

PriceSnap4ab=pd.read_sql('''
select a.MCD_GBAL_LCAT_ID_NU, a.sld_menu_itm_id, a.frnt_cter_itm_prc_am as oldPrice,  b.rcom_prc
	from PriceSnap3 a left join &mylib3..price_recs_round&round. b on (a.MCD_GBAL_LCAT_ID_NU=b.MCD_GBAL_LCAT_ID_NU and a.sld_menu_itm_id=b.sld_menu_itm_id)
	where prc_eff_dt < "&round1pre"d

''')






