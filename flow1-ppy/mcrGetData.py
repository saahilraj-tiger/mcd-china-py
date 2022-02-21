import pandas as pd
from datetime import date
import mysql.connector
from param_file import *


with open(r'C:\Users\gopala.chennu\Desktop\Current\vs\Codes\master.yml', 'r') as doc:
    file = yaml.load(doc, Loader=yaml.FullLoader)

tday = pd.to_datetime(round1start).date().day().zfill(2)
tmonth = pd.to_datetime(round1start).date().month().zfill(2)
tyear = pd.to_datetime(round1start).date().year().zfill(4)
pday = pd.to_datetime(round1pre).date().day().zfill(2)
pmonth = pd.to_datetime(round1pre).date().month().zfill(2)
pyear = pd.to_datetime(round1pre).date().year().zfill(4)

print(tday, tmonth, tyear)
print()
print(pday, pmonth, pyear)
print()


round1str = str(tyear+"-"+tmonth+"-"+tday)
print(round1str)

round1pre = str(pyear+"-"+pmonth+"-"+pday)
print(round1pre)

WE_END_THU = pd.read_sas(f"{RMDW_Tables}WE_END_THU")
dates = (
    (WE_END_THU[round1str] >= WE_END_THU['wk_end_thu_beg_dt'])
    &(WE_END_THU[round1str] <= WE_END_THU['wk_end_thu_end_dt'])
    )
WE_END_THU = WE_END_THU.loc[dates]
WE_END_THU.WK_END_THU_ID_NU.sort_values(by=['WK_END_THU_ID_NU'])
print(WE_END_THU)

startweek = WE_END_THU['WK_END_THU_ID_NU ']


# for COVID Set pre year 2 back
weekenddates1a=WE_END_THU.loc[
    ((WK_END_THU['WK_END_THU_ID_NU']>=startweek-f"{file['burn']}"-f"{file['weeks']}"+1)
    &(WK_END_THU['WK_END_THU_ID_NU']<=startweek-f"{file['burn']}"))
    |((WK_END_THU['WK_END_THU_ID_NU']>=startweek+1)
    &(WK_END_THU['WK_END_THU_ID_NU']<=startweek+f"{file['weeks']}"))
].sort_values(by=['WK_END_THU_ID_NU'])
print(weekenddates1a)


weekenddates1a['period'] =np.where(
    weekenddates1a['WK_END_THU_ID_NU']>startweek,
    'CY','Pre_CY'
)
weekenddates1=weekenddates1a

minlast = weekenddates1.lastyear.min()
maxlast = weekenddates1.lastyear.max()
print(minlast, maxlast)

weekenddates2=(
    weekenddates1.loc[(weekenddates1['wk_end_thu_end_yr_nu '] >= minlast)
    & (weekenddates1['wk_end_thu_end_yr_nu '] <= maxlast)]
)
weekenddates1.sort_values(by='WK_END_THU_ID_NU')
print(weekenddates1)

weekenddates1.loc[weekenddates1['wk_end_thu_wk_nu']==53, 'wk_end_thu_wk_nu'] = 52
weekenddates1c=weekenddates1
print(weekenddates1c)

weekenddates3 = pd.merge(
    weekenddates2, weekenddates1c, how='inner',
    left_on=['wk_end_thu_wk_nu', 'wk_end_thu_end_yr_nu'],
    right_on=['wk_end_thu_wk_nu', 'lastyear']
)
print(weekenddates3)

weekenddates = pd.concat([weekenddates3, weekenddates1], axis=1)
weekenddates['period']=np.where(
    (weekenddates['lastyear']==weekenddates['wk_end_thu_end_yr_nu'])
    & (weekenddates['period']=='CY'),
    'LY'
)
weekenddates['period']=np.where(
    (weekenddates['lastyear']==weekenddates['wk_end_thu_end_yr_nu'])
    & (weekenddates['period']=='Pre_CY'),
    'Pre_LY'
)
weekenddates.drop(['lastyear'], axis=1)
print(weekenddates)

#  APPEND 208 WEEKS/16 QUARTERS/4 YEARS OF WEEKLY PMIX AND GUEST COUNTS TOGETHER ***/


def loop2():
    weekenddates4 = weekenddates
    weekenddates4.drop(['load_dw_audt_ts',' updt_dw_audt_ts dw_file_id'], axis=1)
    weekenddates4['obsnum'] = range(0, len(weekenddates4))
    print(weekenddates4)

    numweeks = len(weekenddates4.columns)

    for i in range(numweeks):
        weeknum = weekenddates4['wk_end_thu_wk_nu'].str.strip()
        yearnum = weekenddates4['wk_end_thu_end_yr_nu'].str.strip()
        weekid = weekenddates4['WK_END_THU_ID_NU'].str.strip()
        quarter = weekenddates4['wk_end_thu_cal_qtr_id_nu'].str.strip()
        monthnum = weekenddates['wk_end_thu_end_mo_nu'].str.strip()
        period = weekenddates['period'].str.strip()

        #    AL A CARTE AND COMBO

        wkly_pmix = pd.DataFrame(f"{dlpmix}/dly_Pmix_{weekid}")
        week = weeknum
        year = yearnum
        quarter = quarter
        month = monthnum
        period = period

        gstcnts = pd.DataFrame(f"{gcpmix}/wkly_guest_cnt_{weekid}")
        week = weeknum
        year = yearnum
        quarter = quarter
        month = monthnum
        period = period

        wkly_pmix1 = pd.DataFrame(f"{mylib1}wkly_pmix1")
        append_data1 = wkly_pmix1.append(wkly_pmix,ignore_index=True)

        gstcnts1 = pd.DataFrame(f"{mylib1}gstcnts")
        append_data2 = gstcnts1.append(gstcnts1)

       #  return append_data1,append_data2


#    what are the date ranges represented by market
#    get all prices after the operators see the recommendations up to a number of weeks later
# connect to redshift(dsn=gdap user=&reduser. pwd="&redpass.");


PriceSnap1 = pd.read_sas(f"{RMDW_Tables}LCAT_MENU_ITM_PRC_SNAP")
PriceSnap1 = PriceSnap1[(PriceSnap1['terr_cd'] == terr) &
                                                ((PriceSnap1['prc_end_dt'] >= pre1str)
                                                & (PriceSnap1['prc_end_dt'] <= round1str+int(f"{weekacp}")*7)) &
                                                ((PriceSnap1['frnt_cter_itm_prc_am'] > 0)
                                                & (PriceSnap1['frnt_cter_itm_prc_am'].notnull()))]
PriceSnap1.sort_values(by=['MCD_GBAL_LCAT_ID_NU', 'SLD_MENU_ITM_ID', 'prc_eff_dt'])

print(PriceSnap1)
#/*** get all prices during the preround that ended during the post round - basically the prices that changed***/
OLDPriceSnap1 = PriceSnap1[(
    (PriceSnap1['terr_cd'] == terr)
    & (PriceSnap1['prc_eff_dt'] < pre1str)
    & ((PriceSnap1['prc_end_dt'] >= pre1str-1) & (PriceSnap1['prc_end_dt'] <= round1str+int(f"{weekacp}")*7))
    & ((PriceSnap1['frnt_cter_itm_prc_am'] > 0) & (PriceSnap1['frnt_cter_itm_prc_am'].notnull()))
)]

OLDPriceSnap1.sort_values(by=['MCD_GBAL_LCAT_ID_NU', 'SLD_MENU_ITM_ID', 'prc_eff_dt'])
print(OLDPriceSnap1)


PriceSnap2 =PriceSnap1.append(OLDPriceSnap1,ignore_index=True)
PriceSnap2['prc_end_dt'] = np.where(PriceSnap2['prc_end_dt '].isna(), "31DEC2099",np.NaN)

PriceSnap3 = PriceSnap2
PriceSnap3.groupby(['MCD_GBAL_LCAT_ID_NU', 'SLD_MENU_ITM_ID', 'frnt_cter_itm_prc_am']).agg(
        {'prc_eff_dt': np.min(), 'prc_end_dt ': np.max()}
    )

price_recs_round = f"{mylib3}{price_recs_round}"+file['round']
PriceSnap3['rcom_prc'] = price_recs_round['rcom_prc']
PriceSnap4a = pd.merge(PriceSnap3, price_recs_round, how='left', on=['MCD_GBAL_LCAT_ID_NU', 'MCD_GBAL_LCAT_ID_NU'])
PriceSnap4a[PriceSnap4a['prc_eff_dt'] > file['round1pre']]
print(PriceSnap4a)

PriceSnap4b = pd.merge(PriceSnap3, price_recs_round, on=['MCD_GBAL_LCAT_ID_NU', 'sld_menu_itm_id'])
PriceSnap4b =PriceSnap4b[PriceSnap4b['prc_eff_dt '] > file['round1pre']]
PriceSnap4b[['MCD_GBAL_LCAT_ID_NU', 'sld_menu_itm_id','frnt_cter_itm_prc_am', 'rcom_prc']]
PriceSnap4b.rename(columns={'frnt_cter_itm_prc_am': 'oldPrice'})
print(PriceSnap4b)

PriceSnap4a['accepted']=np.where(
    ~(PriceSnap4a['rcom_prc'].isna())
    & ( PriceSnap4a['rcom_prc'] == PriceSnap4a['frnt_cter_itm_prc_am']),
    1,0
)
PriceSnap4a['partial']=np.where(
    ~(PriceSnap4a['rcom_prc'].isna())
    & ( PriceSnap4a['accepted'] ==0)
    & (round(abs(round(PriceSnap4a['rcom_prc'],2)-round(PriceSnap4a['frnt_cter_itm_prc_am'],2))),2)<=file['recdiff'],
    1,0
)
PriceSnap4a['accepted']=np.where(
    (PriceSnap4a['rcom_prc'].isna()),
    0,np.nan
)
PriceSnap4a['partial']=np.where(
    (PriceSnap4a['rcom_prc'].isna()),
    0,np.nan
)
PriceSnap5=PriceSnap4a.sort_values(
    by=['MCD_GBAL_LCAT_ID_NU ',
        'sld_menu_itm_id ',
        'accepted ',
        'partial ',
        'prc_end_dt'], ascending=[True, False, False, False, True]
,inplace=True)

PriceSnap6=PriceSnap5.groupby(['MCD_GBAL_LCAT_ID_NU ','sld_menu_itm_id ','accepted ','partial ','prc_end_dt']).agg(
    sld_menu_itm_id=pd.NamedAgg(column='sld_menu_itm_id',aggfunc='first')
)

PriceSnap7 = pd.merge(PriceSnap6, PriceSnap4b, on=['MCD_GBAL_LCAT_ID_NU', 'sld_menu_itm_id'])
PriceSnap7['Old Menu Price']=PriceSnap7['oldPrice'].copy()

PriceSnap7['increase']=np.where(
    round(PriceSnap7['frnt_cter_itm_prc_am'],2)>round(PriceSnap7['oldPrice'],2),
    1,np.nan
)
PriceSnap7['nochange']=np.where(
    round(PriceSnap7['frnt_cter_itm_prc_am'],2)>round(PriceSnap7['oldPrice'],2),
    1,np.nan
)
PriceSnap7['decrease']=np.where(
    round(PriceSnap7['frnt_cter_itm_prc_am'],2)>round(PriceSnap7['oldPrice'],2),
    1,np.nan
)

PriceSnap = PriceSnap7.groupby(['increase' 'nochange' 'decrease']).value_counts()


#    /*** Get Menu Board Price for price taken ***/

LCAT_MENU_ITM_PRC_SNAP = pd.read_sas(f"{RMDW_Tables}LCAT_MENU_ITM_PRC_SNAP")
PriceSnapMB1 = LCAT_MENU_ITM_PRC_SNAP[(
    (LCAT_MENU_ITM_PRC_SNAP['terr_cd'] == file['terr'])
    & (~LCAT_MENU_ITM_PRC_SNAP['frnt_cter_itm_prc_am'].isna())
    & (LCAT_MENU_ITM_PRC_SNAP['frnt_cter_itm_prc_am '] > 0)
)].sort_values(by=['MCD_GBAL_LCAT_ID_NU', 'SLD_MENU_ITM_ID', 'prc_eff_dt'], inplace=True)\
    [[
    'MCD_GBAL_LCAT_ID_NU',
    'SLD_MENU_ITM_ID',
    'prc_eff_dt',
    'prc_end_dt',
    'sld_menu_itm_actv_fl',
    'frnt_cter_itm_prc_am']]
PriceSnapMB= PriceSnapMB1.loc[PriceSnapMB1['prc_end_dt'].isna(),'prc_end_dt'] ='31DEC2099'

wkly_pmix1 = pd.read_sas(f"{lisa}/wkly_pmix1.sas7dbat")
lastyearstrs = (
    wkly_pmix1[wkly_pmix1['period'] == 'LY']
    .groupby(['mcd_gbal_lcat_id_nu'])['WK_END_THU_ID_NU']
    .count()
    .unique()
    .reset_index()
    .sort_values(by=['mcd_gbal_lcat_id_nu'])
)
lastyearstrs.rename(columns={'WK_END_THU_ID_NU': 'weekcnt1'})

thisyearstrs=(
    wkly_pmix1[wkly_pmix1['period']=='Pre_CY']
    .groupby(['mcd_gbal_lcat_id_nu'])['WK_END_THU_ID_NU']
    .count()
    .unique()
    .reset_index()
    .sort_values(by['mcd_gbal_lcat_id_nu'])
)
thisyearstrs.rename(columns={'WK_END_THU_ID_NU':'weekcnt2'})

thisyearstrs2=(
    wkly_pmix1[wkly_pmix1['period']=='CY']
    .groupby(['mcd_gbal_lcat_id_nu'])['WK_END_THU_ID_NU']
    .count()
    .unique()
    .reset_index()
    .sort_values(by['mcd_gbal_lcat_id_nu'])
)
thisyearstrs2.rename(columns={'WK_END_THU_ID_NU':'weekcnt3'})

storelst=(
    pd.merge(lastyearstrs,thisyearstrs,on='mcd_gbal_lcat_id_nu',how='inner')
    .merge(thisyearstrs2,left_on='mcd_gbal_lcat_id_nu',right_on='mcd_gbal_lcat_id_nu',how='inner')
)

gstcnts1 = pd.DataFrame(f"{lisa}/gstcnts1")
gstcnts = pd.merge(gstcnts1, storelst, on=['mcd_gbal_lcat_id_nu'])
gstcnts.rename(columns={'InStore': 'delivery'})
print(gstcnts)

# /*** Join price data into Sales data ***/

wkly_pmixMB = (
    pd.merge(wkly_pmix1, storelst, on=['mcd_gbal_lcat_id_nu'])
    .merge(PriceSnapMB, how='left', left_on=['mcd_gbal_lcat_id_nu','SLD_MENU_ITM_ID'],right_on=['MCD_GBAL_LCAT_ID_NU','SLD_MENU_ITM_ID '])
).query('cal_dt >= prc_eff_dt and cal_dt <= prc_end_dt')


# /***** Aggregating Daily data to weekly ****/
wkly_pmixMB.groupby(
    ['WK_END_DT',
    'WK_END_THU_ID_NU',
    'terr_cd',
    'mcd_gbal_lcat_id_nu',
    'store_nu',
    'sld_menu_itm_id',
    'sld_menu_itm_na',
    'itm_category',
    'period']
).agg(
    alacarte_unit=pd.NamedAgg(column='alacarte_units',aggfunc='sum')
    combo_units=pd.NamedAgg(column='combo_units',aggfunc='sum')
    total_units=pd.NamedAgg(column='total_units',aggfunc='sum')
    net_sales_lcl=pd.NamedAgg(column='net_sales_lcl',aggfunc='sum')
    gross_sales_lcl=pd.NamedAgg(column='gross_sales_lcl',aggfunc='sum')
    total_cost_lcl=pd.NamedAgg(column='total_cost_lcl',aggfunc='sum')
)
wkly_pmixMB['Menu_sales']=wkly_pmixMB['frnt_cter_itm_prc_am']*wkly_pmixMB['alacarte_units']
