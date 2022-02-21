import pandas as pd
import numpy as np


from Getdata_market import *

print(lvlcnt,lvllist,fieldlth,lvllistis,lvllistlast1,lvllistlast2)

# **** COVID FIX BEGINS HERE
waterfallbse_round=pd.read_sas(f"{inter1}waterfallbase_round{file['round']}")
waterfallbse_round['PP_Mrgrec_items2']=np.where(
    (waterfallbse_round['promo']!=1)
    &(waterfallbse_round['accepted']==1),
    round(waterfallbse_round['avgNETprice_cy']-waterfallbse_round['avgNETprice_pre_CY'],2)
    *waterfallbse_round['avgunits_cy']
)
waterfallbse_round['PP_Slsrec_items2']=np.where(
    waterfallbse_round['PP_Mrgrec_items2']=np.where(
    (waterfallbse_round['promo']!=1)
    &(waterfallbse_round['accepted']==1),
    round(waterfallbse_round['avgNETprice_cy']-waterfallbse_round['avgNETprice_pre_CY'],2)
    *waterfallbse_round['avgunits_cy']
)
waterfallbse_round['measured_marginRecs']=(
    waterfallbse_round[['PP_Mrgrec_items2','PP_Mrgrec_items_pric_dev2']].sum()
)
waterfallbse_round['measured_salesRecs']=(
    waterfallbse_round[['PP_Slsrec_items2','PP_Slsrec_items_pric_dev2']].sum()
)
waterfallbse_round['measured_margin']=(
    waterfallbse_round[['PP_Mrgrec_items2','PP_Mrgrec_items_pric_dev2','PP_Mrgprice_outside_recs2']].sum()
)
waterfallbse_round['measured_sales']=waterfallbse_round[['PP_Slsrec_items2','PP_Slsrec_items_pric_dev2','PP_Slsprice_outside_recs2']].sum()
waterfallbse_round.sort_values(by=[file['LVLLIST2'] ,'mcd_gbal_lcat_id_nu', 'store_nu', 'delivery', 'sld_menu_itm_id'])

wap_cal_base=waterfallbse_round

wap_cal_base[:"newprice"]=np.where(
    wap_cal_base["newprice"].isna(),wap_cal_base['AvgMenuPrice_CY'],np.nan
)
wap_cal_base[:"oldprice"]=np.where(
    wap_cal_base["oldprice"].isna(),wap_cal_base['AvgMenuPrice_Pre_CY'],np.nan
)
wap_cal_base[:"rcom_prc"]=np.where(
    wap_cal_base["rcom_prc"].isna(),wap_cal_base['AvgMenuPrice_CY'],np.nan
)
wap_cal_base[:"prcg_engn_curr_prc "]=np.where(
    wap_cal_base["prcg_engn_curr_prc "].isna(),wap_cal_base['AvgMenuPrice_Pre_CY'],np.nan
)
Waterfallbase_rest2=(
    wap_cal_base[~(wap_cal_base['newprice'].isna())
    &~(wap_cal_base['oldprice'].isna())
    &(wap_cal_base['pestore']==1)]

).groupby([f"{file['lvllistid']}","mcd_gbal_lcat_id_nu"])
Waterfallbase_rest2['New_WAP']=(
    sum(wap_cal_base['newprice']*wap_cal_base['units_ly'])/sum(units_ly)
)
Waterfallbase_rest2['Old_WAP']=(
    sum(wap_cal_base['oldprice']*wap_cal_base['units_ly'])/sum(wap_cal_base['units_ly'])
)

Waterfallbase_rest3=(
    ~(wap_cal_base['rcom_prc '].isna())
    &(wap_cal_base['prcg_engn_curr_prc '].isna())
    &(wap_cal_base['pestore']==1)
).groupby(f"{lvllistid}",'mcd_gbal_lcat_id_nu')
Waterfallbase_rest3['Rec_WAP']=sum(wap_cal_base['rcom_prc']*wap_cal_base['units_ly'])/sum(wap_cal_base['units_ly'])
Waterfallbase_rest3['Prcg_eng_WAP']=sum(wap_cal_base['prcg_engn_curr_prc']*wap_cal_base['units_ly'])/sum(wap_cal_base['units_ly'])

Waterfallbase_rest1=(
    waterfallbse_round.merge(wkly_pmix_trans_REST,on=['mcd_gbal_lcat_id_nu'],how='left')
    .merge(Waterfallbase_rest2,on=['mcd_gbal_lcat_id_nu'],how='left')
    .merge(Waterfallbase_rest3,on=['mcd_gbal_lcat_id_nu'],how='left')
).groupby([f"{lvllistid}",
    'mcd_gbal_lcat_id_nu',
    'store_nu',
    'AvgGuestCount_CY',
    'AvgGuestCount_LY',
    'AvgGuestCount_Pre_CY',
    'avg_YOY_tot_GC',
    'gc_elastic',
    'InfluencePrcGC',
    'PEStore',
    'new_WAP',
    'old_WAP',
    'Rec_WAP',
    'Prcg_eng_WAP']).agg(
        est_Units=NamedAgg(column='est_Units',aggfunc='sum')
        est_margin=NamedAgg(column='est_margin',aggfunc='sum')

        low_est_margin=NamedAgg(column='low_est_margin',aggfunc='sum')
        up_est_margin=NamedAgg(column='up_est_margin',aggfunc='sum')
        est_sales=NamedAgg(column='est_sales',aggfunc='sum')
        low_est_sales=NamedAgg(column='low_est_sales',aggfunc='sum')
        up_est_sales=NamedAgg(column='up_est_sales',aggfunc='sum')

        rec_Units=NamedAgg(column='rec_Units',aggfunc='sum')
        rec_margin=NamedAgg(column='rec_margin',aggfunc='sum')
        low_rec_margin=NamedAgg(column='low_rec_margin',aggfunc='sum')
        up_rec_margin=NamedAgg(column='up_rec_margin',aggfunc='sum')
        rec_sales=NamedAgg(column='rec_sales',aggfunc='sum')
        low_rec_sales=NamedAgg(column='low_rec_sales',aggfunc='sum')
        up_rec_sales=NamedAgg(column='up_rec_sales',aggfunc='sum')

        measured_marginRecs=NamedAgg(column='measured_marginRecs',aggfunc='sum')
        measured_salesRecs=NamedAgg(column='measured_salesRecs',aggfunc='sum')
        measured_margin=NamedAgg(column='measured_margin',aggfunc='sum')
        measured_sales=NamedAgg(column='measured_sales',aggfunc='sum')
        GuestCountMrgImpact=NamedAgg(column='GuestCountMrgImpact',aggfunc='sum')
        GuestCountSlsImpact=NamedAgg(column='GuestCountSlsImpact',aggfunc='sum')

        RecAccept=NamedAgg(column='acptflag',aggfunc='sum')
        RecAdded=NamedAgg(column='addflag',aggfunc='sum')
        RecFlag=NamedAgg(column='recflag',aggfunc='sum')
        AvgUnits_LY=NamedAgg(column='AvgUnits_LY',aggfunc='sum')
        AvgUnits_Pre_CY=NamedAgg(column='AvgUnits_Pre_CY',aggfunc='sum')
        AvgUnits_CY=NamedAgg(column='AvgUnits_CY',aggfunc='sum')
        avgTotalsales_LY=NamedAgg(column='AvgSales_LY',aggfunc='sum')
        avgTotalsales_Pre_CY=NamedAgg(column='AvgSales_Pre_CY',aggfunc='sum')
        avgTotalsales_CY=NamedAgg(column='AvgSales_CY',aggfunc='sum')
        AvgTotalCost_LY=NamedAgg(column='AvgCost_LY',aggfunc='sum')
        AvgTotalCost_pre_CY=NamedAgg(column='AvgCost_pre_CY',aggfunc='sum')
        AvgTotalCost_CY=NamedAgg(column='AvgCost_CY',aggfunc='sum')
        act_Units=NamedAgg(column='act_Units',aggfunc='sum')
        act_Sales=NamedAgg(column='act_Sales',aggfunc='sum')
        act_Margin=NamedAgg(column='act_Margin',aggfunc='sum')
        pre_act_Units=NamedAgg(column='pre_act_Units',aggfunc='sum')
        pre_act_Sales=NamedAgg(column='pre_act_Sales',aggfunc='sum')
        pre_act_Margin=NamedAgg(column='pre_act_Margin',aggfunc='sum')

    )
Waterfallbase_rest1['Overall_PC_per_YOY']=(
    (sum((Waterfallbase_rest1['avgprice_cy']-Waterfallbase_rest1['avgprice_ly'])
    *Waterfallbase_rest1['avgunits_ly'])/sum(Waterfallbase_rest1['avgunits_ly'])
    )/(sum(Waterfallbase_rest1['avgprice_ly']*Waterfallbase_rest1['avgunits_ly'])/sum(Waterfallbase_rest1['avgunits_ly']))
)
Waterfallbase_rest1['menupricechgRecItems']=(
    (sum((Waterfallbase_rest1['avgprice_cy']-Waterfallbase_rest1['Non_Rec_Price'])*(waterfallbase_rest1['avgunits_ly']))/sum(waterfallbase_rest1['avgunits_ly']))/
    (sum(waterfallbase_rest1['avgprice_ly']*waterfallbase_rest1['avgunits_ly'])/sum(waterfallbase_rest1['avgunits_ly']))
)
# menu price change recommended items plus other adds
waterfallbase_rest1['menupricechgItems']=(sum((waterfallbase_rest1['avgprice_cy']*waterfallbase_rest1['Non_Rec_Price2'])*waterfallbase_rest1['avgunits_ly'])/sum(waterfallbase_rest1['avgunits_ly']))/
(sum(waterfallbase_rest1['avgprice_ly']*waterfallbase_rest1['avgunits_ly'])/sum(waterfallbase_rest1['avgunits_ly']))

waterfallbase_rest1['avg_YOY_tot_rest_price']=(
    (sum((waterfallbase_rest1['AvgSales_CY']/waterfallbase_rest1['avgunits_cy'])-sum(waterfallbase_rest1['avgSales_ly']/waterfallbase_rest1['AvgUnits_LY']))/sum(waterfallbase_rest1['avgSales_LY']/waterfallbase_rest1['avgUnits_LY']))
)
waterfallbase_rest1['Avg_YOY_tot_rest_sales']=(
    sum(waterfallbase_rest1['AvgSales_CY']-waterfallbase_rest1['AvgSales_LY'])/sum(waterfallbase_rest1['AvgSales_LY'])
)
waterfallbase_rest1['avg_YOY_rest_margin']=(
    (sum(waterfallbase_rest1['AvgSales_CY']-waterfallbase_rest1['AvgCost_CY'])-sum(waterfallbase_rest1['AvgSales_LY']-waterfallbase_rest1['AvgCost_LY']))/
    sum(waterfallbase_rest1['AvgSales_LY']-waterfallbase_rest1['AvgCost_LY'])
)
waterfallbase_rest1['avg_YOY_tot_GC']=(waterfallbase_rest1['AvgGuestCount_CY']-waterfallbase_rest1['AvgGuestCount_LY'])/waterfallbase_rest1['AvgGuestCount_LY']
waterfallbase_rest1['AvgTotalMargin_LY']=waterfallbase_rest1['AvgSales_LY']-waterfallbase_rest1['AvgCost_LY']
waterfallbase_rest1['AvgTotalMargin_pre_CY']=waterfallbase_rest1['AvgSales_Pre_CY']-waterfallbase_rest1['AvgCost_pre_CY']
waterfallbase_rest1['AvgTotalMargin_CY']=waterfallbase_rest1['AvgSales_CY']-waterfallbase_rest1['AvgCost_CY']

waterfallbase_Rest_round=Waterfallbase_rest1
waterfallbase_Rest_round['est_GC']=np.where(
    ~(waterfallbase_Rest_round['gc_elastic'].isna())
    &((~waterfallbase_Rest_round['New_WAP'].isin[np.nan,0]) &(~waterfallbase_Rest_round['old_WAP'].isin[np.nan,0])),
    (waterfallbase_Rest_round['gc_elastic']*(waterfallbase_Rest_round['New_WAP'].np.log())-waterfallbase_Rest_round['old_WAP'])
)
waterfallbase_Rest_round['low_est_GC']=np.where(
    ~(waterfallbase_Rest_round['gc_elastic'].isna())
    &((~waterfallbase_Rest_round['New_WAP'].isin[np.nan,0]) &(~waterfallbase_Rest_round['old_WAP'].isin[np.nan,0])),
    (1+file['range'])*waterfallbase_Rest_round['est_GC'],(1-file['range']*waterfallbase_Rest_round['est_GC'])
)
waterfallbase_Rest_round['act_GC']=np.where(
    ~(waterfallbase_Rest_round['gc_elastic'].isna())
    &((~waterfallbase_Rest_round['New_WAP'].isin[np.nan,0]) &(~waterfallbase_Rest_round['old_WAP'].isin[np.nan,0])),
    (waterfallbase_Rest_round['AvgGuestCount_CY']-waterfallbase_Rest_round['AvgGuestCount_LY'])/(waterfallbase_Rest_round['AvgGuestCount_LY'])
)
waterfallbase_Rest_round['pre_act_GC']=np.where(
    ~(waterfallbase_Rest_round['gc_elastic'].isna())
    &((~waterfallbase_Rest_round['New_WAP'].isin[np.nan,0]) &(~waterfallbase_Rest_round['old_WAP'].isin[np.nan,0])),
    (waterfallbase_Rest_round['avgGuestCount_Pre_CY']-waterfallbase_Rest_round['AvgGuestCount_LY'])/(waterfallbase_Rest_round['AvgGuestCount_LY'])
)
waterfallbase_Rest_round['rec_GC']=np.where(
    ~(waterfallbase_Rest_round['gc_elastic'].isna())
    &((~waterfallbase_Rest_round['Rec_WAP'].isin[np.nan,0]) &(~waterfallbase_Rest_round['prcg_eng_WAP'].isin[np.nan,0])),
    waterfallbase_Rest_round['gc_elastic']*(waterfallbase_Rest_round['Rec_WAP'].np.log()-waterfallbase_Rest_round['prcg_eng_WAP'].np.log())-1
)
waterfallbase_Rest_round['rec_GC']=np.where(
    ~(waterfallbase_Rest_round['gc_elastic'].isna())
    &((~waterfallbase_Rest_round['Rec_WAP'].isin[np.nan,0]) &(~waterfallbase_Rest_round['prcg_eng_WAP'].isin[np.nan,0])),
    waterfallbase_Rest_round['gc_elastic']*(waterfallbase_Rest_round['Rec_WAP'].np.log()-waterfallbase_Rest_round['prcg_eng_WAP'].np.log())-1
)
waterfallbase_Rest_round['low_rec_GC']=np.where(
    ~(waterfallbase_Rest_round['gc_elastic'].isna())
    &((~waterfallbase_Rest_round['Rec_WAP'].isin[np.nan,0]) &(~waterfallbase_Rest_round['prcg_eng_WAP'].isin[np.nan,0])),
    (1+file['range'])*waterfallbase_Rest_round['rec_GC'],(1-file['range']*waterfallbase_Rest_round['rec_GC'])

)
waterfallbase_Rest_round['up_est_GC']=np.where(
    ~(waterfallbase_Rest_round['gc_elastic'].isna())
    &((~waterfallbase_Rest_round['Rec_WAP'].isin[np.nan,0]) &(~waterfallbase_Rest_round['prcg_eng_WAP'].isin[np.nan,0])),
    (1-file['range'])*waterfallbase_Rest_round['rec_GC'],(1+file['range']*waterfallbase_Rest_round['rec_GC'])
)
waterfallbase_Rest_round['rec_act_GC']=np.where(
    ~(waterfallbase_Rest_round['gc_elastic'].isna())
    &((~waterfallbase_Rest_round['Rec_WAP'].isin[np.nan,0]) &(~waterfallbase_Rest_round['prcg_eng_WAP'].isin[np.nan,0])),
    waterfallbase_Rest_round['AvgGuestCount_CY']-waterfallbase_Rest_round['AvgGuestCount_LY']/waterfallbase_Rest_round['AvgGuestCount_LY']
)
waterfallbase_Rest_round['rec_pre_act_GC']=np.where(
    ~(waterfallbase_Rest_round['gc_elastic'].isna())
    &((~waterfallbase_Rest_round['Rec_WAP'].isin[np.nan,0]) &(~waterfallbase_Rest_round['prcg_eng_WAP'].isin[np.nan,0])),
    waterfallbase_Rest_round['avgGuestCount_Pre_CY']-waterfallbase_Rest_round['AvgGuestCount_LY']/waterfallbase_Rest_round['AvgGuestCount_LY']
)
waterfallbase_Rest_round['avg_measured_GCRecs']=np.where(
    ~(waterfallbase_Rest_round['menupricechgRecItems'].isin(np.nan,0)),
    (waterfallbase_Rest_round['AvgGuestCount_CY']-waterfallbase_Rest_round['AvgGuestCount_LY'])/
    (waterfallbase_Rest_round['AvgGuestCount_LY']*waterfallbase_Rest_round['menupricechgRecItems'])/
    (waterfallbase_Rest_round['Overall_PC_per_YOY']*waterfallbase_Rest_round['InfluencePrcGC']),
    np.nan
)
waterfallbase_Rest_round['avg_measured_GC']=np.where(
    ~(waterfallbase_Rest_round['menupricechgItems '].isin(np.nan,0)),
    (waterfallbase_Rest_round['AvgGuestCount_CY']-waterfallbase_Rest_round['AvgGuestCount_LY'])/
    (waterfallbase_Rest_round['AvgGuestCount_LY']*waterfallbase_Rest_round['menupricechgItems'])/
    (waterfallbase_Rest_round['Overall_PC_per_YOY']*waterfallbase_Rest_round['InfluencePrcGC']),
    np.nan
)
waterfallbase_Rest_round['est_marg_ach']=np.where(
    ~(waterfallbase_Rest_round['low_est_margin'].isna())
    & (waterfallbase_Rest_round['measured_marginRecs ']<=(waterfallbase_Rest_round['up_est_margin'])
    & (waterfallbase_Rest_round['measured_marginRecs'])>=waterfallbase_Rest_round['low_est_margin']),
    1,np.nan
)
waterfallbase_Rest_round['est_marg_ach_over']=np.where(
    ~(waterfallbase_Rest_round['low_est_margin'].isna())
    & (waterfallbase_Rest_round['measured_marginRecs'])>=waterfallbase_Rest_round['up_est_margin ']),
    1,np.nan
)
waterfallbase_Rest_round['est_sales_ach']=np.where(
    ~(waterfallbase_Rest_round['low_est_sales '].isna())
    & (waterfallbase_Rest_round['measured_salesRecs  ']<=(waterfallbase_Rest_round['up_est_sales'])
    & (waterfallbase_Rest_round['measured_marginRecs'])>=waterfallbase_Rest_round['low_est_sales']),
    1,np.nan
)
waterfallbase_Rest_round['est_sales_ach_over']=np.where(
    ~(waterfallbase_Rest_round['low_est_sales   '].isna())
    & (waterfallbase_Rest_round['measured_salesRecs '])>waterfallbase_Rest_round['up_est_sales']),
    1,np.nan
)
waterfallbase_Rest_round['est_GC_ach']=np.where(
    ~(waterfallbase_Rest_round['low_est_GC  '].isna())
    & (waterfallbase_Rest_round['avg_measured_GCRecs  ']<=(waterfallbase_Rest_round['up_est_GC '])
    & (waterfallbase_Rest_round['avg_measured_GCRecs '])>=waterfallbase_Rest_round['low_est_GC ']),
    1,np.nan
)
waterfallbase_Rest_round['est_GC_ach_over']=np.where(
    ~(waterfallbase_Rest_round['low_est_GC  '].isna())
    & (waterfallbase_Rest_round['avg_measured_GCRecs  ']>(waterfallbase_Rest_round['up_est_GC '])
        1,np.nan
)
waterfallbase_Rest_round['rec_marg_ach']=np.where(
    ~(waterfallbase_Rest_round['low_rec_margin   '].isna())
    & (waterfallbase_Rest_round['measured_marginRecs   ']<=(waterfallbase_Rest_round['up_rec_margin  '])
    & (waterfallbase_Rest_round['measured_marginRecs  '])>=waterfallbase_Rest_round['low_rec_margin  ']),
    1,np.nan
)
waterfallbase_Rest_round['rec_marg_ach_over']=np.where(
    ~(waterfallbase_Rest_round['low_rec_margin '].isna())
    & (waterfallbase_Rest_round['measured_marginRecs  ']>(waterfallbase_Rest_round['up_rec_margin  '])
        1,np.nan
)
waterfallbase_Rest_round['rec_sales_ach']=np.where(
    ~(waterfallbase_Rest_round['low_rec_sales   '].isna())
    & (waterfallbase_Rest_round['measured_salesRecs ']<=(waterfallbase_Rest_round['up_rec_sales   '])
    & (waterfallbase_Rest_round['measured_salesRecs '])>=waterfallbase_Rest_round['low_rec_sales ']),
    1,np.nan
)
waterfallbase_Rest_round['rec_sales_ach_over']=np.where(
    ~(waterfallbase_Rest_round['low_rec_sales'].isna())
    & (waterfallbase_Rest_round['measured_marginRecs  ']>(waterfallbase_Rest_round['up_rec_margin  '])
        1,np.nan
)
waterfallbase_Rest_round['rec_GC_ach']=np.where(
    ~(waterfallbase_Rest_round['low_rec_GC    '].isna())
    & (waterfallbase_Rest_round['avg_measured_GCRecs  ']<=(waterfallbase_Rest_round['up_rec_GC    '])
    & (waterfallbase_Rest_round['avg_measured_GCRecs  '])>=waterfallbase_Rest_round['low_rec_GC  ']),
    1,np.nan
)
waterfallbase_Rest_round['rec_GC_ach_over']=np.where(
    ~(waterfallbase_Rest_round['low_rec_GC '].isna())
    & (waterfallbase_Rest_round['avg_measured_GCRecs  ']>(waterfallbase_Rest_round['up_rec_GC'])
    1,np.nan
)
waterfallbase_Rest_round[['rec_GC_ach_over','rec_GC_ach', 'rec_sales_ach', 'rec_sales_ach_over', 'rec_marg_ach_over', 'rec_marg_ach',
		'est_GC_ach_over', 'est_GC_ach', 'est_sales_ach', 'est_sales_ach_over', 'est_marg_ach_over', 'est_marg_ach'
		]].fillna(0)
waterfallbase_Rest_round['RecAcceptRate']=waterfallbase_Rest_round['RecAccept']/waterfallbase_Rest_round['RecFlag']
waterfallbase_Rest_round['PerMarginChangeRecs']=waterfallbase_Rest_round['measured_marginRecs']/(waterfallbase_Rest_round['avgTotalsales_Pre_CY']-waterfallbase_Rest_round['AvgTotalCost_Pre_CY'])
waterfallbase_Rest_round['PerSalesChangeRecs']=waterfallbase_Rest_round['measured_salesRecs']/waterfallbase_Rest_round['avgTotalsales_Pre_CY']

waterfallbase_Rest_round['PerMarginChange']=waterfallbase_Rest_round['measured_margin']/(waterfallbase_Rest_round['avgTotalsales_Pre_CY']-waterfallbase_Rest_round['AvgTotalCost_Pre_CY'])
waterfallbase_Rest_round['PerSalesChange']=waterfallbase_Rest_round['measured_sales']/waterfallbase_Rest_round['avgTotalsales_Pre_CY']

waterfallbase_Rest_round['GuestCountPerChange']=(waterfallbase_Rest_round['AvgGuestCount_CY']/waterfallbase_Rest_round['AvgGuestCount_LY'])-1
waterfallbase_Rest_round['EstPerMarginChange']=waterfallbase_Rest_round['est_margin']/(waterfallbase_Rest_round['avgTotalsales_Pre_CY']-waterfallbase_Rest_round['AvgTotalCost_Pre_CY'])
waterfallbase_Rest_round['EstPerSalesChange']=waterfallbase_Rest_round['est_sales']/waterfallbase_Rest_round['avgTotalsales_Pre_CY']
waterfallbase_Rest_round['RecPerMarginChange']=waterfallbase_Rest_round['rec_margin']/(waterfallbase_Rest_round['avgTotalsales_Pre_CY']-waterfallbase_Rest_round['AvgTotalCost_Pre_CY'])
waterfallbase_Rest_round['RecPerSalesChange']=waterfallbase_Rest_round['rec_sales']/waterfallbase_Rest_round['avgTotalsales_Pre_CY']

waterfallbase_Rest_round['EST_per_new_units']=1-(waterfallbase_Rest_round['est_units']/waterfallbase_Rest_round['AvgUnits_Pre_CY'])
waterfallbase_Rest_round['REC_per_new_units']=1-(waterfallbase_Rest_round['rec_Units']/waterfallbase_Rest_round['AvgUnits_Pre_CY'])

total1=waterfallbase_Rest_round.copy()
total1.drop(['GuestCountMrgImpact', 'MenuChangeMrgImpact', 'MixMrgImpact', 'total2', 'total3', 'total4', 'total5', 'total6',
				'MI_SlsPrice', 'MI_Slscarryover_price_inc', 'MI_Slspromo', 'MI_Slsno_price_change',
				'PP_Mrgrec_items', 'PP_Mrgrec_items_pric_dev', 'PP_Mrgcarryover_price_inc', 'PP_Mrgpromo',
				'MI_MrgPrice', 'MI_Mrgcarryover_price_inc', 'MI_Mrgpromo', 'MI_Mrgno_price_change'
				],axis=1)
total2=waterfallbase_Rest_round.copy()
total2.drop(['GuestCountSlsImpact', 'MenuChangeSlsImpact', 'MixSlsImpact', 'total1', 'total3', 'total4', 'total5', 'total6',
				'PP_Slsrec_items', 'PP_Slsrec_items_pric_dev', 'PP_Slscarryover_price_inc', 'PP_Slspromo',
				'MI_SlsPrice', 'MI_Slscarryover_price_inc', 'MI_Slspromo', 'MI_Slsno_price_change',
				'MI_MrgPrice', 'MI_Mrgcarryover_price_inc', 'MI_Mrgpromo', 'MI_Mrgno_price_change'
				],axis=1)
total3=waterfallbase_Rest_round.copy()
total3.drop(['GuestCountMrgImpact', 'MenuChangeMrgImpact', 'MixMrgImpact', 'total1', 'total2', 'total4', 'total5', 'total6',
				'PP_Slsrec_items', 'PP_Slsrec_items_pric_dev', 'PP_Slscarryover_price_inc', 'PP_Slspromo',
				'PP_Mrgrec_items', 'PP_Mrgrec_items_pric_dev', 'PP_Mrgcarryover_price_inc', 'PP_Mrgpromo',
				'MI_MrgPrice', 'MI_Mrgcarryover_price_inc', 'MI_Mrgpromo', 'MI_Mrgno_price_change'
				],axis=1)
total4=waterfallbase_Rest_round.copy()
total4.drop(['GuestCountSlsImpact', 'MenuChangeSlsImpact', 'MixSlsImpact', 'total1', 'total2' ,'total3', 'total5' ,'total6',
				'PP_Slsrec_items', 'PP_Slsrec_items_pric_dev', 'PP_Slscarryover_price_inc', 'PP_Slspromo',
				'MI_SlsPrice', 'MI_Slscarryover_price_inc', 'MI_Slspromo', 'MI_Slsno_price_change',
				'MI_MrgPrice', 'MI_Mrgcarryover_price_inc', 'MI_Mrgpromo', 'MI_Mrgno_price_change',
				'PP_Mrgrec_items', 'PP_Mrgrec_items_pric_dev', 'PP_Mrgcarryover_price_inc', 'PP_Mrgpromo'
				],axis=1)
total5=waterfallbase_Rest_round.copy()
total5.drop(['GuestCountMrgImpact','MenuChangeMrgImpact', 'MixMrgImpact', 'total1', 'total2', 'total3', 'total4', 'total6',
				'MI_SlsPrice', 'MI_Slscarryover_price_inc', 'MI_Slspromo', 'MI_Slsno_price_change', 'FPCostChangeImpact',
				'MI_MrgPrice', 'MI_Mrgcarryover_price_inc', 'MI_Mrgpromo', 'MI_Mrgno_price_change',
				'PP_Mrgrec_items', 'PP_Mrgrec_items_pric_dev', 'PP_Mrgcarryover_price_inc', 'PP_Mrgpromo', 'PP_MrgPrice',
				'PP_Slsrec_items', 'PP_Mrgrec_items', 'PP_Slsrec_items_pric_dev', 'PP_Mrgrec_items_pric_dev',
				'MI_Slsrec_items', 'MI_Mrgrec_items', 'MI_Slsrec_items_pric_dev', 'MI_Mrgrec_items_pric_dev',
				'PP_SlsPrice', 'PP_Slscarryover_price_inc', 'PP_Slsprice_inc_outside_recs', 'PP_Slsprice_dec_outside_recs', 'PP_Slspromo',
				'MI_SlsPrice', 'MI_Slscarryover_price_inc', 'MI_Slsprice_inc_outside_recs', 'MI_Slsprice_dec_outside_recs', 'MI_Slspromo', 'MI_Slsno_price_change'
				])
total6=waterfallbase_Rest_round.copy()
total6.drop(['GuestCountSlsImpact', 'MenuChangeSlsImpact', 'MixSlsImpact', 'total1', 'total2', 'total3', 'total4', 'total5',
				'MI_SlsPrice', 'MI_Slscarryover_price_inc', 'MI_Slspromo', 'MI_Slsno_price_change',
				'MI_MrgPrice', 'MI_Mrgcarryover_price_inc', 'MI_Mrgpromo', 'MI_Mrgno_price_change', 'PP_MrgPrice',
				'PP_Mrgrec_items', 'PP_Mrgrec_items_pric_dev', 'PP_Mrgcarryover_price_inc', 'PP_Mrgpromo',
				'PP_Slsrec_items', 'PP_Mrgrec_items', 'PP_Slsrec_items_pric_dev', 'PP_Mrgrec_items_pric_dev',
				'MI_Slsrec_items', 'MI_Mrgrec_items', 'MI_Slsrec_items_pric_dev', 'MI_Mrgrec_items_pric_dev',
				'PP_SlsPrice', 'PP_Slscarryover_price_inc', 'PP_Slsprice_inc_outside_recs', 'PP_Slsprice_dec_outside_recs', 'PP_Slspromo',
				'MI_SlsPrice', 'MI_Slscarryover_price_inc', 'MI_Slsprice_inc_outside_recs', 'MI_Slsprice_dec_outside_recs', 'MI_Slspromo', 'MI_Slsno_price_change'
				],axis=1)
waterfallbase_Rest_round['PP_SlsPrice']=sum(waterfallbase_Rest_round['PP_Slsrec_items'],waterfallbase_Rest_round['PP_Slsrec_items_pric_dev'])
waterfallbase_Rest_round['PP_MrgPrice']=sum(waterfallbase_Rest_round['PP_Mrgrec_items'],waterfallbase_Rest_round['PP_Mrgrec_items_pric_dev'])
waterfallbase_Rest_round['MI_SlsPrice']=sum(waterfallbase_Rest_round['MI_Slsrec_items'],waterfallbase_Rest_round['MI_Slsrec_items_pric_dev'])
waterfallbase_Rest_round['MI_MrgPrice']=sum(waterfallbase_Rest_round['MI_Mrgrec_items'],waterfallbase_Rest_round['MI_Mrgrec_items_pric_dev'])

waterfallbase_Rest_round['total1']=sum(waterfallbase_Rest_round[['PP_SlsPrice', 'PP_Slsprice_inc_outside_recs', 'PP_Slsprice_dec_outside_recs', 'PP_Slscarryover_price_inc','PP_Slspromo']])
waterfallbase_Rest_round['total1']=np.where(
    (round(total1,2)!=round(waterfallbase_Rest_round['PurePriceImpact'],2)),waterfallbase_Rest_round['total1'],np.nan
)
waterfallbase_Rest_round['total2']=sum(waterfallbase_Rest_round[['PP_MrgPrice', 'PP_Mrgprice_inc_outside_recs', 'PP_Mrgprice_dec_outside_recs', 'PP_Mrgcarryover_price_inc', 'PP_Mrgpromo']])
waterfallbase_Rest_round['total2']=np.where(
    (round(total2,2)!=round(waterfallbase_Rest_round['PurePriceImpact'],2)),waterfallbase_Rest_round['total2'],np.nan
)
waterfallbase_Rest_round['total3']=sum(waterfallbase_Rest_round[['MI_SlsPrice', 'MI_Slsprice_inc_outside_recs', 'MI_Slsprice_dec_outside_recs', 'MI_Slscarryover_price_inc', 'MI_Slspromo' ,'MI_Slsno_price_change']])
waterfallbase_Rest_round['total3']=np.where(
    (round(total3,2)!=round(waterfallbase_Rest_round['MixSlsImpact'],2)),waterfallbase_Rest_round['total3'],np.nan
)
waterfallbase_Rest_round['total4']=sum(waterfallbase_Rest_round[['MI_MrgPrice', 'MI_Mrgprice_inc_outside_recs', 'MI_Mrgprice_dec_outside_recs', 'MI_Mrgcarryover_price_inc','MI_Mrgpromo', 'MI_Mrgno_price_change']])
waterfallbase_Rest_round['total4']=np.where(
    (round(total4,2)!=round(waterfallbase_Rest_round['MixMrgImpact'],2)),waterfallbase_Rest_round['total4'],np.nan
)
waterfallbase_Rest_round[['GuestCountSlsImpact', 'MenuChangeSlsImpact', 'MixSlsImpact', 'PurePriceImpact',
		'GuestCountMrgImpact', 'MenuChangeMrgImpact', 'MixMrgImpact', 'FPCostChangeImpact'
		]].fillna(0)
waterfallbase_Rest_round['total5']=sum(waterfallbase_Rest_round[['GuestCountSlsImpact', 'MenuChangeSlsImpact', 'MixSlsImpact', 'PurePriceImpact']])
waterfallbase_Rest_round['total5']=np.where(
    (round(total5,2)!=round(waterfallbase_Rest_round['SalesDelta'],2)),waterfallbase_Rest_round['total5'],np.nan
)
waterfallbase_Rest_round['total6']=sum(waterfallbase_Rest_round[['GuestCountMrgImpact', 'MenuChangeMrgImpact', 'MixMrgImpact', 'PurePriceImpact', 'FPCostChangeImpact']])
waterfallbase_Rest_round['total6']=np.where(
    (round(total6,2)!=round(waterfallbase_Rest_round['MrgDelta'],2)),waterfallbase_Rest_round['total6'],np.nan
)
waterfallbase_Rest_round[['mcd_gbal_lcat_id_nu', 'delivery', 'sld_menu_itm_id', 'sld_menu_itm_na', 'ScrCategory',
			'units_LY', 'units_CY', 'sales_LY', 'sales_CY',
			'AvgSales_CY', 'AvgSales_LY', 'AVGCost_CY', 'AVGCost_LY',
			'avgprice_ly', 'avgprice_Pre_CY', 'avgprice_cy',
			'avgNETprice_ly', 'avgNETprice_Pre_CY', 'avgNETprice_cy',
			'cost_LY', 'cost_CY', 'AvgCostItem_LY', 'AvgCostItem_CY',
			'CurrentRoundChange', 'CurrentRoundCarry',
			'Product_Family', 'Price_Tier_Strategy',
			'promo', 'SalesDelta', 'PurePriceImpact',
			'PP_Slsrec_items', 'PP_Mrgrec_items', 'PP_Slsrec_items_pric_dev', 'PP_Mrgrec_items_pric_dev',
			'MI_Slsrec_items', 'MI_Mrgrec_items', 'MI_Slsrec_items_pric_dev', 'MI_Mrgrec_items_pric_dev',
			'PP_SlsPrice', 'PP_Slscarryover_price_inc', 'PP_Slsprice_inc_outside_recs', 'PP_Slsprice_dec_outside_recs', 'PP_Slspromo',
			'MI_SlsPrice', 'MI_Slscarryover_price_inc', 'MI_Slsprice_inc_outside_recs', 'MI_Slsprice_dec_outside_recs', 'MI_Slspromo', 'MI_Slsno_price_change',
			'PP_MrgPrice', 'PP_Mrgcarryover_price_inc', 'PP_Slsprice_dec_outside_recs', 'PP_Slscarryover_price_inc' 'PP_Mrgpromo', 'accepted', 'partial',
			'MI_MrgPrice', 'MI_Mrgcarryover_price_inc', 'MI_Slsprice_dec_outside_recs', 'MI_Slscarryover_price_inc', 'MI_Mrgpromo', 'MI_Mrgno_price_change',
			'MrgDelta', 'FPCostChangeImpact', 'GuestCountSlsImpact', 'MenuChangeSlsImpact', 'MixSlsImpact'
			'GuestCountMrgImpact', 'MenuChangeMrgImpact', 'MixMrgImpact',
			'total1', 'total2', 'total3', 'total4', 'total5', 'total6'
	]]