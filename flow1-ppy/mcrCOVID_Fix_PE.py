import pandas as pd
import numpy as np


from Getdata_market import *

print(lvlcnt,lvllist,fieldlth,lvllistis,lvllistlast1,lvllistlast2)

# **** COVID FIX BEGINS HERE
waterfallbse_round=pd.DataFrame()

if waterfallbse_round[waterfallbse_round['promo']!=1]:
    if waterfallbse_round['accepted']==1:
        PP_Mrgrec_items2=round(waterfallbse_round['avgNETprice_cy']-waterfallbse_round['avgNETprice_pre_CY'],2)*(waterfallbse_round['avgunits_cy'])
        PP_Slsrec_items2=round(waterfallbse_round['avgNETprice_cy ']-waterfallbse_round['avgNETprice_pre_CY'],2)*(waterfallbse_round['avgunits_cy'])
    elif waterfallbse_round['partial']==1:
        PP_Mrgrec_items_pric_dev2=round( waterfallbse_round['avgNETprice_cy'] - waterfallbse_round['avgNETprice_pre_CY'] ,2)*waterfallbse_round['avgunits_cy']
        PP_Slsrec_items_pric_dev2=round(waterfallbse_round['avgNETprice_cy'] - waterfallbse_round['avgNETprice_pre_CY'] ,.2)*waterfallbse_round['avgunits_cy']
    elif waterfallbse_round['addflag ']==1:
        PP_Mrgprice_outside_recs2=round(waterfallbse_round['avgNETprice_cy'] - waterfallbse_round['avgNETprice_pre_CY'] ,2)*waterfallbse_round['avgunits_cy']
        PP_Slsprice_outside_recs2=round(waterfallbse_round['avgNETprice_cy'] - waterfallbse_round['avgNETprice_pre_CY'] ,2)*waterfallbse_round['avgunits_cy']

measured_marginRecs=waterfallbse_round[['PP_Mrgrec_items2','PP_Mrgrec_items_pric_dev2']].sum(axis=1)

measured_salesRecs=waterfallbse_round[['PP_Slsrec_items2','PP_Slsrec_items_pric_dev2']].sum(axis=1)

measured_margin=waterfallbse_round[['PP_Mrgrec_items2','PP_Mrgrec_items_pric_dev2','PP_Mrgprice_outside_recs2']].sum(axis=1)

measured_sales=waterfallbse_round[['PP_Slsrec_items2','PP_Slsrec_items_pric_dev2','PP_Slsprice_outside_recs2']].sum(axis=1)


	# /**** COVID FIX ENDS HERE *****/
waterfallbse_round.sort_values(by=['LVLLIST2','mcd_gbal_lcat_id_nu ','store_nu ','delivery ','sld_menu_itm_id'])

#  reusable code to summarize the waterfall base to a restauarant level ***/

wap_cal_base =waterfallbse_round[['LVLLIST2','mcd_gbal_lcat_id_nu ','units_pre_cy ','units_ly ','oldprice ','newprice','prcg_engn_curr_prc ','rcom_prc','accepted ','partial ','PEstore ','AvgMenuPrice_CY ','AvgMenuPrice_Pre_CY']]


wap_cal_base['newprice']=wap_cal_base['newprice'].apply(lambda x: AvgMenuPrice_CY if x==np.NaN else x)


wap_cal_base['oldprice ']=wap_cal_base['oldprice '].apply(lambda x: AvgMenuPrice_Pre_CY if x==np.NaN else x)


wap_cal_base['rcom_prc  ']=wap_cal_base['rcom_prc  '].apply(lambda x: AvgMenuPrice_CY if x==np.NaN else x)


wap_cal_base['prcg_engn_curr_prc ']=wap_cal_base['prcg_engn_curr_prc '].apply(lambda x: AvgMenuPrice_Pre_CY if x==np.NaN else x)


# make it df for further porpose ---( Erase Later)
waterfallbse_rest2=wap_cal_base[wap_cal_base['newprice'].notna() & waterfallbse_round['prcg_engn_curr_prc '].notna() & waterfallbse_round['pestore']==1]
waterfallbse_rest2.groupby(['LVLLISTID','mcd_gbal_lcat_id_nu'])
waterfallbse_rest2['New_WAP']=waterfallbse_rest2['newprice']*waterfallbse_rest2['units_ly'].sum()/waterfallbse_rest2['units_ly']
waterfallbse_rest2['Old_WAP']=waterfallbse_rest2['oldprice']*waterfallbse_rest2['units_ly'].sum()/waterfallbse_rest2['units_ly']

def wfbse_rest3(wap_cal_base):
    waterfallbse_rest2.groupby(['LVLLISTID','mcd_gbal_lcat_id_nu'])
    wap_cal_base[wap_cal_base['rcom_prc'].notna() & wap_cal_base['prcg_engn_curr_prc '].notna() & wap_cal_base['pestore']==1]
    wap_cal_base['Rec_WAP']=wap_cal_base['rcom_prc']*wap_cal_base['units_ly'].sum()/wap_cal_base['units_ly']
    wap_cal_base['Prcg_eng_WAP']=wap_cal_base['prcg_engn_curr_prc']*waterfallbse_rest2['units_ly'].sum()/wap_cal_base['units_ly']
    return wap_cal_base

waterfallbse_rest3=wfbse_rest3(wap_cal_base)
print(waterfallbse_rest3)








