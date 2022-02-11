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

# db=mysql.connector.connect(host=" ",username=" ", passwd=" ",database=" ")
# cursor=db.cursor()

# connect to redshift(dsn=gdap user=&reduser. pwd="&redpass.");

# -------- Connect to AWS Redshift connection then change again (Arkit sir)--------


WE_END_THU = pd.DataFrame(RMDW_Tables.WE_END_THU)
wk_end_thu_beg_dt = ' '
wk_end_thu_end_dt = ' '
dates = ((WE_END_THU['DATE'] >= wk_end_thu_beg_dt) &
         (WE_END_THU['DATE'] <= wk_end_thu_end_dt))
WE_END_THU = WE_END_THU.loc['dates']
WE_END_THU.WK_END_THU_ID_NU.sort_values()
print(WE_END_THU)

startweek = WE_END_THU['WK_END_THU_ID_NU ']
startweek = pd.DataFrame(startweek)

# for COVID Set pre year 2 back

cursor.execute(
    """
    CREATE TABLE weekenddates1a as
	select * from connection to redshift
		(select *, wk_end_thu_end_yr_nu-&yrback. as lastyear
		from RMDW_Tables.WK_END_THU
		where (WK_END_THU_ID_NU between &startweek.-&burn.-&weeks.+1 and (&startweek.-&burn.))
			or (WK_END_THU_ID_NU between (&startweek.+1) and &startweek.+&weeks.)
		order by WK_END_THU_ID_NU
		)
    """
)
WK_END_THU = pd.DataFrame(RMDW_Tables.WK_END_THU)
# where (WK_END_THU_ID_NU between &startweek.-&burn.-&weeks.+1 and (&startweek.-&burn.))
# or (WK_END_THU_ID_NU between (&startweek.+1) and &startweek.+&weeks.)                                 # DOubt for above case 👈👈👈
WE_END_THU.WK_END_THU_ID_NU.sort_values()

weekenddates1a = WK_END_THU
print(weekenddates1a)

weekenddates1 = pd.DataFrame(weekenddates1a)
weekenddates1['period'] = ['CY' if WK_END_THU_ID_NU >= startweek else "Pre_CY"]

minlast = weekenddates1.lastyear.min()
maxlast = weekenddates1.lastyear.max()
print(minlast, maxlast)


weekenddates2 = pd.DataFrame(RMDW_Tables.WK_END_THU)
weekenddates1[(weekenddates1['wk_end_thu_end_yr_nu '] >= minlast)
              & (weekenddates1['wk_end_thu_end_yr_nu '] <= maxlast)]
weekenddates1.sort_values(by='WK_END_THU_ID_NU')
print(weekenddates1)

weekenddates1c = pd.DataFrame(weekenddates1)
weekenddates1c.loc[53, 'wk_end_thu_wk_nu'] = 52
print(weekenddates1c)

weekenddates3 = pd.merge(weekenddates2, weekenddates1c, how='inner', left_on=[
                         'wk_end_thu_wk_nu', 'wk_end_thu_end_yr_nu'], right_on=['wk_end_thu_wk_nu', 'lastyear'])
print(weekenddates3)

weekenddates = pd.concat([weekenddates3, weekenddates1], axis=1)
weekenddates = weekenddates[weekenddates['lastyear']
                            == weekenddates['wk_end_thu_end_yr_nu']]
weekenddates['period'] = weekenddates['period'].apply(
    lambda x: "LY" if x == "CY" else ("Pre_LY" if x == "Pre_CY" else x))
weekenddates.drop(['lastyear'], axis=1)
print(weekenddates)

#  APPEND 208 WEEKS/16 QUARTERS/4 YEARS OF WEEKLY PMIX AND GUEST COUNTS TOGETHER ***/


def loop2():
    weekenddates4 = weekenddates
    weekenddates4.drop('load_dw_audt_ts',
                       ' updt_dw_audt_ts dw_file_id', axis=1)
    weekenddates4['obsnum'] = range(0, len(weekenddates4))
    print(weekenddates4)

    numweeks = len(weekenddates4.columns)

    # proc datasets lib=&mylib1.;
    # 	delete wkly_pmix1 gstcnts1 wkly_pmix gstcnts;       --- ⚡ NOT Understand----

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
        quarter = quarter         # Creating Columns with series Doubt 😢
        month = monthnum
        period = period

        gstcnts = pd.DataFrame(f"{gcpmix}/wkly_guest_cnt_{weekid}")
        week = weeknum
        year = yearnum
        quarter = quarter           # Creating Columns ith series Doubt 🖐🖐
        month = monthnum
        period = period

        wkly_pmix1 = pd.DataFrame(f"{mylib1}wkly_pmix1")
        append_data1 = wkly_pmix1.append(wkly_pmix)  # base is wkly_pmix1

        gstcnts1 = pd.DataFrame(f"{mylib1}gstcnts")
        append_data2 = gstcnts1.append(gstcnts1)

       #  return append_data1,append_data2           #if need it


#    what are the date ranges represented by market
#    get all prices after the operators see the recommendations up to a number of weeks later


# connect to redshift(dsn=gdap user=&reduser. pwd="&redpass.");


LCAT_MENU_ITM_PRC_SNAP = pd.DataFrame(RMDW_Tables.LCAT_MENU_ITM_PRC_SNAP)
LCAT_MENU_ITM_PRC_SNAP = LCAT_MENU_ITM_PRC_SNAP[(LCAT_MENU_ITM_PRC_SNAP['terr_cd'] == terr) &
                                                ((LCAT_MENU_ITM_PRC_SNAP['prc_end_dt'] >= pre1str) & LCAT_MENU_ITM_PRC_SNAP['prc_end_dt'] <= round1str+int(f"{weekacp}")*7) &
                                                ((LCAT_MENU_ITM_PRC_SNAP['frnt_cter_itm_prc_am'] > 0) & LCAT_MENU_ITM_PRC_SNAP['frnt_cter_itm_prc_am'].notnull())]
LCAT_MENU_ITM_PRC_SNAP.sort_values(
    by=['MCD_GBAL_LCAT_ID_NU', 'SLD_MENU_ITM_ID', 'prc_eff_dt'])

PriceSnap1 = LCAT_MENU_ITM_PRC_SNAP[['MCD_GBAL_LCAT_ID_NU', 'SLD_MENU_ITM_ID',
                                     'prc_eff_dt', 'prc_end_dt', 'sld_menu_itm_actv_fl', 'frnt_cter_itm_prc_am']]

print(PriceSnap1)


# connect to redshift(dsn=gdap user=&reduser. pwd="&redpass.");

LCAT_MENU_ITM_PRC_SNAP = pd.DataFrame(RMDW_Tables.LCAT_MENU_ITM_PRC_SNAP)
LCAT_MENU_ITM_PRC_SNAP = LCAT_MENU_ITM_PRC_SNAP[(LCAT_MENU_ITM_PRC_SNAP['terr_cd'] == terr) &
                                                (LCAT_MENU_ITM_PRC_SNAP['prc_eff_dt'] < pre1str) &
                                                ((LCAT_MENU_ITM_PRC_SNAP['prc_end_dt'] >= pre1str-1) & LCAT_MENU_ITM_PRC_SNAP['prc_end_dt'] <= round1str+int(f"{weekacp}")*7) &
                                                ((LCAT_MENU_ITM_PRC_SNAP['frnt_cter_itm_prc_am'] > 0) & LCAT_MENU_ITM_PRC_SNAP['frnt_cter_itm_prc_am'].notnull())]

LCAT_MENU_ITM_PRC_SNAP.sort_values(
    by=['MCD_GBAL_LCAT_ID_NU', 'SLD_MENU_ITM_ID', 'prc_eff_dt'])

OLDPriceSnap1 = LCAT_MENU_ITM_PRC_SNAP[['MCD_GBAL_LCAT_ID_NU', 'SLD_MENU_ITM_ID',
                                        'prc_eff_dt', 'prc_end_dt', 'sld_menu_itm_actv_fl', 'frnt_cter_itm_prc_am']]

print(OLDPriceSnap1)


PriceSnap2 = pd.concat(['PriceSnap1 ', 'OLDPriceSnap1'])
PriceSnap2['prc_end_dt '] = np.where(
    PriceSnap2['prc_end_dt '].isna(), "31DEC2099")

PriceSnap3 = PriceSnap2
PriceSnap3.groupby(['MCD_GBAL_LCAT_ID_NU', 'SLD_MENU_ITM_ID', 'frnt_cter_itm_prc_am']).agg(
    {'prc_eff_dt': np.min(), '(prc_end_dt) ': np.max()})

price_recs_round = f"{mylib3}{price_recs_round}"+file['round']
PriceSnap3['rcom_prc'] = price_recs_round['rcom_prc']
PriceSnap4a = pd.merge(PriceSnap3, price_recs_round, how='left', on=[
                       'MCD_GBAL_LCAT_ID_NU', 'MCD_GBAL_LCAT_ID_NU'])
PriceSnap4a[PriceSnap4a['prc_eff_dt '] > round1pre]
print(PriceSnap4a)

PriceSnap4b = pd.merge(PriceSnap3, price_recs_round, on=[
                       'MCD_GBAL_LCAT_ID_NU', 'sld_menu_itm_id'])
PriceSnap4b[PriceSnap4b['prc_eff_dt '] > round1pre]
PriceSnap4b[['MCD_GBAL_LCAT_ID_NU', 'sld_menu_itm_id',
             'frnt_cter_itm_prc_am', 'rcom_prc']]
PriceSnap4b.rename(columns={'frnt_cter_itm_prc_am': 'oldPrice'})
print(PriceSnap4b)


PriceSnap5 = PriceSnap4a['rcom_prc '].isna(
).PriceSnap4a['rcom_prc'] == frnt_cter_itm_prc_am
# if rcom_prc ne . then do;
# 		if rcom_prc = frnt_cter_itm_prc_am then accepted=1;
# 		else accepted=0;
# 		if accepted=0 and round(abs(round(rcom_prc,.01)-round(frnt_cter_itm_prc_am,.01)),.01) <= &recdiff. then partial=1;
# 		else partial=0;
# 	end;                                                ## DOubt 😥
# 	else do;
# 		accepted=0;
# 		partial=0;

# proc sort data=PriceSnap5;
# 	by MCD_GBAL_LCAT_ID_NU sld_menu_itm_id descending accepted descending partial descending prc_end_dt;


PriceSnap5.sort_values(
    by=['MCD_GBAL_LCAT_ID_NU ', 'sld_menu_itm_id'], ascending=False)

PriceSnap6 = PriceSnap5
print(PriceSnap5)
PriceSnap6.first()


PriceSnap7 = pd.merge(PriceSnap6, PriceSnap4b, on=[
                      'MCD_GBAL_LCAT_ID_NU', 'sld_menu_itm_id'])
PriceSnap6['Old Menu Price'] = PriceSnap4b['oldPrice']
print(PriceSnap7)

PriceSnap7['frnt_cter_itm_prc_am'] = np.where(PriceSnap7['frnt_cter_itm_prc_am'].apply(
    np.ceil) > (PriceSnap7['oldPrice'].apply(np.ceil)), increase=1)
# data &mylib3..PriceSnap;
# 	set PriceSnap7;
# 	if round(frnt_cter_itm_prc_am,.01) > round(oldPrice,.01) then increase=1;
# 	else if round(frnt_cter_itm_prc_am,.01) = round(oldPrice,.01) then nochange=1;
# 	else if round(frnt_cter_itm_prc_am,.01) < round(oldPrice,.01) then decrease=1;
# run;


PriceSnap = PriceSnap.groupby(
    ['increase' 'nochange' 'decrease']).value_counts()


#    /*** Get Menu Board Price for price taken ***/

LCAT_MENU_ITM_PRC_SNAP = pd.DataFrame(RMDW_Tables.LCAT_MENU_ITM_PRC_SNAP)
LCAT_MENU_ITM_PRC_SNAP = LCAT_MENU_ITM_PRC_SNAP[(LCAT_MENU_ITM_PRC_SNAP['terr_cd'] == terr) & (LCAT_MENU_ITM_PRC_SNAP['frnt_cter_itm_prc_am'].dropna())
                                                (LCAT_MENU_ITM_PRC_SNAP['frnt_cter_itm_prc_am '] > 0)]
LCAT_MENU_ITM_PRC_SNAP.sort_values(
    by=['MCD_GBAL_LCAT_ID_NU', 'SLD_MENU_ITM_ID', 'prc_eff_dt'], inplace=True)
LCAT_MENU_ITM_PRC_SNAP = LCAT_MENU_ITM_PRC_SNAP[[
    'MCD_GBAL_LCAT_ID_NU', 'SLD_MENU_ITM_ID', 'prc_eff_dt', 'prc_end_dt', 'sld_menu_itm_actv_fl', 'frnt_cter_itm_prc_am']]
PriceSnapMB1 = LCAT_MENU_ITM_PRC_SNAP


PriceSnapMB['prc_end_dt'] = np.where(
    PriceSnapMB1['prc_end_dt'].isna(), '31DEC2099')

wkly_pmix1 = pd.DataFrame(f"{mylib1}/wkly_pmix1")
wkly_pmix1['period'] = [wkly_pmix1['period'] == 'LY']
wkly_pmix1 = wkly_pmix1.sort_values(by=['mcd_gbal_lcat_id_nu'])
wkly_pmix1 = wkly_pmix1.groupby(['mcd_gbal_lcat_id_nu']).agg(
    {'WK_END_THU_ID_NU': np.size(), 'WK_END_THU_ID_NU': pd.Series.nunique})
wkly_pmix1.rename(columns={'WK_END_THU_ID_NU': 'weekcnt1'})
lastyearstrs = wkly_pmix1
print(lastyearstrs)

wkly_pmix1['period'] = [wkly_pmix1['period'] == 'Pre_CY']
wkly_pmix1 = wkly_pmix1.sort_values(by=['mcd_gbal_lcat_id_nu'])
wkly_pmix1 = wkly_pmix1.groupby(['mcd_gbal_lcat_id_nu']).agg(
    {'WK_END_THU_ID_NU': np.size(), 'WK_END_THU_ID_NU': pd.Series.nunique})
wkly_pmix1.rename(columns={'WK_END_THU_ID_NU': 'weekcnt1'})
thisyearstrs = wkly_pmix1
print(thisyearstrs)

wkly_pmix1['period'] = [wkly_pmix1['period'] == 'CY']
wkly_pmix1 = wkly_pmix1.sort_values(by=['mcd_gbal_lcat_id_nu'])
wkly_pmix1 = wkly_pmix1.groupby(['mcd_gbal_lcat_id_nu']).agg(
    {'WK_END_THU_ID_NU': np.size(), 'WK_END_THU_ID_NU': pd.Series.nunique})
wkly_pmix1.rename(columns={'WK_END_THU_ID_NU': 'weekcnt1'})
thisyearstrs2 = wkly_pmix1
print(thisyearstrs2)

storelst = pd.merge(lastyearstrs, thisyearstrs, how='inner', on=[
                    'mcd_gbal_lcat_id_nu']).merge(thisyearstrs2, how='inner', on=['mcd_gbal_lcat_id_nu'])
print(storelst)


gstcnts1 = pd.DataFrame(f"{mylib1}/gstcnts1")
gstcnts = pd.merge(gstcnts1, storelst, on=['mcd_gbal_lcat_id_nu'])
gstcnts.rename(columns={'InStore': 'delivery'})
print(gstcnts)

# /*** Join price data into Sales data ***/

wkly_pmixMB = pd.merge(wkly_pmix1, storelst, on=['mcd_gbal_lcat_id_nu', ''])\
    .merge(PriceSnapMB, how='left', on=['mcd_gbal_lcat_id_nu'])
wkly_pmixMB = wkly_pmixMB[(wkly_pmixMB['cal_dt'] >= prc_eff_dt) & (
    wkly_pmixMB['prc_end_dt'] <= prc_eff_dt)]
print(wkly_pmixMB)

# /***** Aggregating Daily data to weekly ****/

wkly_pmixMB['Menu_sales'] = \
    wkly_pmixMB.groupby(['WK_END_DT', 'WK_END_THU_ID_NU', 'terr_cd', 'mcd_gbal_lcat_id_nu', 'store_nu', 'sld_menu_itm_id', 'sld_menu_itm_na', 'itm_category', 'period'])\
    .agg({'alacarte_units': np.sum, 'combo_units': np.sum, 'total_units': np.sum, 'net_sales_lcl': np.sum, 'gross_sales_lcl': np.sum, 'total_cost_lcl': np.sum, 'frnt_cter_itm_prc_am': np.sum})

wkly_pmixMB['Menu_sales'] = \
    wkly_pmixMB[wkly_pmixMB['frnt_cter_itm_prc_am']
                * wkly_pmixMB['alacarte_units']]
