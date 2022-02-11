from tkinter import W
import pandas as pd
import numpy as np
import math


from MarketDimensions import *

elasfile = pd.read_sas()
wkly_pmix_trans_item2 = pd.read_sas()
Cross_elst1 = pd.merge(elasfile, wkly_pmix_trans_item2, how='left', left_on=[
                       'mcd_gbal_lcat_id_nu', 'cross_elastic_item'], right_on=['mcd_gbal_lcat_id_nu', 'sld_menu_itm_id'])
Cross_elst1.sort_values(
    by=['mcd_gbal_lcat_id_nu', 'sld_menu_itm_id', 'cross_elastic_item'])
print(Cross_elst1)

Cross_elst = Cross_elst1
Cross_elst = Cross_elst.rename(columns={'final_elasticity': 'elasticity'})
Cross_elst.groupby(
    ['mcd_gbal_lcat_id_nu ', 'sld_menu_itm_id ', 'cross_elastic_item']).first()
Cross_elst.groupby(
    ['mcd_gbal_lcat_id_nu ', 'sld_menu_itm_id ', 'cross_elastic_item']).last()

print(Cross_elst)

Cross_elst.loc[((Cross_elst['prestore'] == 1) & ~(Cross_elst['elasticity '].isna())) & (~(Cross_elst['newprice '].isna()) & ~(
    Cross_elst['oldPrice '].isna())), 'val1'] = Cross_elst['elasticity']*(math.log(Cross_elst['newprice'])-math.log(Cross_elst['oldPrice']))
Cross_elst.loc[~((Cross_elst['prestore'] == 1) & ~(Cross_elst['elasticity '].isna())) & (
    ~(Cross_elst['newprice '].isna()) & ~(Cross_elst['oldPrice '].isna())), 'val1'] = np.Nan

Cross_elst.loc[((Cross_elst['pestore'] == 1) & ~(Cross_elst['elasticity'].isna())) & (~(Cross_elst['prcg_engn_curr_prc '].isna()) & ~(
    Cross_elst['rcom_prc'].isna())), 'val2'] = Cross_elst['elasticity']*(math.log(Cross_elst['rcom_prc']-math.log(Cross_elst['prcg_engn_curr_prc'])))
Cross_elst.loc[~(((Cross_elst['pestore'] == 1) & ~(Cross_elst['elasticity'].isna())) & (
    ~(Cross_elst['prcg_engn_curr_prc '].isna()) & ~(Cross_elst['rcom_prc'].isna()))), 'val2'] = np.NaN

Cross_elst.loc[((Cross_elst['pestore'] == 1) & ~(Cross_elst['elasticity'].isna())) & (~(Cross_elst['newprice'].isna()) & ~(
    Cross_elst['oldPrice'].isna())), 'val1'] = (Cross_elst['elasticity']*(math.log(Cross_elst['newprice'])-math.log(Cross_elst['oldPrice']))).sum()
Cross_elst.loc[((Cross_elst['pestore'] == 1) & ~(Cross_elst['elasticity'].isna())) & (~(Cross_elst['prcg_engn_curr_prc'].isna()) & ~(
    Cross_elst['rcom_prc'].isna())), 'val2'] = (Cross_elst['elasticity']*(math.log(Cross_elst['rcom_prc'])-math.log(Cross_elst['prcg_engn_curr_prc']))).sum()

Cross_elst.groupby(
    ['mcd_gbal_lcat_id_nu ', 'sld_menu_itm_id ', 'cross_elastic_item']).last()

Cross_elst.loc[~Cross_elst['val1'].isin(), 'elast_est'] = math.exp(
    Cross_elst['val1'])
Cross_elst.loc[~Cross_elst['val2'].isin(), 'elast_rec'] = math.exp(
    Cross_elst['val2'])


waterfallbse_elasticity = pd.merge(wkly_pmix_trans_item2, Cross_elst, how='left', on=[
                                   'mcd_gbal_lcat_id_nu', 'sld_menu_itm_id'])
waterfallbse_elasticity.sort_values(by=['mcd_gbal_lcat_id_nu'])
print(waterfallbse_elasticity)


def loopoutA(level):
    PERest_level = pd.read_sas(f"{inter1}PERest_{file[level]}{file['round']}")
    PERest_level = wkly_pmix_trans_item2[wkly_pmix_trans_item2['PEStore'] == 1]
    cnt = wkly_pmix_trans_item2.groupby(
        'level')['mcd_gbal_lcat_id_nu'].unique().size()
    PERest_level['cnt' > 1]
    print(PERest_level)


def loopgeo0():
	for i in range(1, lvlcnt+1):
        geolvl = levels.loc[levels.index == i].reset_index()
        loopoutA(geolvl)


waterfallbase_elasticity = pd.read_sas(f"{lisa}waterfallbse_elasticity")
waterfallbse_elasticity.sort_values(by=['mcd_gbal_lcat_id_nu', 'delivery'])

#  Do store item/store calculations
gstcntsDlry = pd.read_sas(f"{lisa}gstcntsDlry_trans")
waterfallbse_round = pd.merge(waterfallbse_elasticity, gstcntsDlry, how='left', on=[
                              'mcd_gbal_lcat_id_nu', 'delivery'])
waterfallbse_round.loc[waterfallbse_round['units_cy'].isna(), 'removeflag'] = 1
waterfallbse_round.loc[((waterfallbse_round['Sales_CY']/(waterfallbse_round['units_cy ']) < .05))
                        & ((waterfallbse_round['Sales_LY']/(waterfallbse_round['units_ly ']) < .05)), 'removeflag'] = 1
waterfallbse_round.loc[(waterfallbse_round['Sales_CY']/waterfallbse_round['units_cy '] < .05)
                        & (waterfallbse_round['units_cy '].isin([0, np.NaN])), 'removeflag'] = 1
waterfallbse_round.loc[(waterfallbse_round['Sales_LY'] < .05) & (
    waterfallbse_round['units_cy'].isin([0, np.NaN])), 'removeflag'] = 1
waterfallbse_round.loc[waterfallbse_round['removeflag'].isna(),
                                                             'removeflag'] = 0

waterfallbse_round[['avgunits_ly', 'avgunits_cy', 'avgunits_Pre_ly', 'avgunits_Pre_cy',
		'AvgSales_LY', 'AvgSales_CY', 'AvgSales_pre_LY', 'AvgSales_Pre_CY',
		'AvgCost_LY', 'AvgCost_CY', 'AvgCost_pre_LY', 'AvgCost_pre_CY',
		'units_ly', 'units_cy', 'units_Pre_ly', 'units_Pre_cy',
		'Sales_LY', 'Sales_CY', 'Sales_pre_LY', 'Sales_Pre_CY',
		'Cost_LY', 'Cost_CY', 'Cost_pre_LY', 'Cost_pre_CY']].fillna(0)

#  fix for seasonal or not available Pre CY but available LY ***/

waterfallbse_round.loc[(waterfallbse_round['avgNETprice_Pre_cy '].isna()) & (
    waterfallbse_round[~waterfallbse_round['avgNETprice_ly'].isin([0, np.NaN])]), ['CurrentRoundChange', 'CurrentRoundCarry']] = [0, 1]

# if it was available last year, current year, and current year pre

waterfallbse_round.loc[(waterfallbse_round[~waterfallbse_round['avgNETprice_cy '].isna()]) &
                    ~(waterfallbse_round[waterfallbse_round['avgNETprice_ly '].isna()) &
                    ~(waterfallbse_round[waterfallbse_round['avgNETprice_Pre_cy'].isna()]) &
                    (abs(round(
                        waterfallbse_round['avgNETprice_cy']-waterfallbse_round['avgNETprice_ly'], 1)))
                    ['CurrentRoundChange', 'CurrentRoundCarry']] = [(waterfallbse_round['avgNETprice_cy']-waterfallbse_round['avgNETprice_Pre_cy'])/(waterfallbse_round['avgNETprice_cy']-waterfallbse_round['avgNETprice_ly']).apply(np.ceil(5)), 1-CurrentRoundChange]

waterfallbse_round.loc[(waterfallbse_round[~waterfallbse_round['avgNETprice_cy '].isna()]) &
                    ~(waterfallbse_round[waterfallbse_round['avgNETprice_ly '].isna()) &
                    abs(round(waterfallbse_round['avgNETprice_cy']-waterfallbse_round['avgNETprice_Pre_cy'], 2) >= 0.01), ['CurrentRoundChange', 'CurrentRoundCarry']] = [1, 0]
waterfallbse_round.loc[~(waterfallbse_round['avgNETprice_cy'].isna()) &
                    ~(waterfallbse_round['avgNETprice_ly'].isna()) &
                    ~(waterfallbse_round['avgNETprice_Pre_cy'].isna()), ['CurrentRoundChange', 'CurrentRoundCarry']]= [0, 0]

waterfallbse_round.loc[~(waterfallbse_round['(newprice '].isna()) &
                    ~(waterfallbse_round['AvgPrice_Pre_CY '].isna()) &
                    (abs(round(waterfallbse_round['newprice']-waterfallbse_round['AvgPrice_Pre_CY'], 2))) > 0.01, ['CurrentRoundChange', 'CurrentRoundCarry']] = [0, 0]

waterfallbse_round.loc[(waterfallbse_round['avgNETprice_Pre_cy '].isna()) & (waterfallbse_round[~waterfallbse_round['avgNETprice_ly'].isin([0, np.NaN])]), ['CurrentRoundChange', 'CurrentRoundCarry']] = [0, 0]

waterfallbse_round.loc[~waterfallbse_round['AvgGuestCount_LY'].isin([0, np.NaN]), 'GuestCountPerChange']= (waterfallbse_round['AvgGuestCount_CY']/waterfallbse_round['AvgGuestCount_LY'])-1
waterfallbse_round.loc[~waterfallbse_round['AvgGuestCount_LY'].isin([0, np.NaN]), 'GuestCountMrgImpact'] = round((waterfallbse_round['AvgSales_LY']-waterfallbse_round['AvgCost_LY'])*GuestCountPerChange, 5)
waterfallbse_round.loc[~waterfallbse_round['AvgGuestCount_LY'].isin([0, np.NaN]), 'GuestCountSlsImpact'] = round(waterfallbse_round['GuestCountPerChange']*waterfallbse_round['AvgSales_LY'], 4)


#  with GC impact removed
waterfallbse_round.loc[(waterfallbse_round['units_ly '] > 0) & (waterfallbse_round['units_cy'] <= 0), ['MenuChangeMrgImpact', 'MenuChangeSlsImpact']] = [round(((waterfallbse_round['avgunits_ly']*-1)*(waterfallbse_round['AvgNETPrice_ly'] & waterfallbse_round['AvgCostItem_ly']))-GuestCountMrgImpact, 4), round(((waterfallbse_round['avgunits_ly']*-1)*(waterfallbse_round['AvgNETPrice_ly']))-GuestCountSlsImpact, 4)]

waterfallbse_round.loc[(waterfallbse_round['units_ly '] <= 0) & (waterfallbse_round['units_cy'] > 0), ['MenuChangeMrgImpact', 'MenuChangeSlsImpact']]= [round((waterfallbse_round['avgunits_cy']*(waterfallbse_round['AvgNETPrice_cy']-waterfallbse_elasticity['AvgCostItem_cy'])-GuestCountMrgImpact), 4), round((waterfallbse_round['avgunits_cy']*(waterfallbse_round['AvgNETPrice_cy'])-GuestCountSlsImpact), 4)]

waterfallbse_round['FPCostChangeImpact'] = round((waterfallbse_round['AvgCostItem_ly']-waterfallbse_round['AvgCostItem_cy'])*waterfallbse_round['avgunits_ly'], 4)

waterfallbse_round.loc[abs(round(waterfallbse_round['AvgNETPrice_cy']-waterfallbse_round['AvgNETPrice_ly'], 2)) > 0.01, 'PurePriceImpact'] = round(round(waterfallbse_round['AvgNETPrice_cy']-waterfallbse_round['AvgNETPrice_ly'], 4)*waterfallbse_round['avgunits_ly'], 4)

#  added as part of Price taken Calculation

waterfallbse_round.loc[round(waterfallbse_round['avgMenuprice_CY ']-waterfallbse_round['avgMenuprice_LY'], 4) >= 0.01, 'PurePriceImpactMB'] = round(round(waterfallbse_round['AvgNETPrice_cY']-waterfallbse_round['AvgNETPrice_Ly'], 4)*waterfallbse_round['avgunits_ly'], 4)



waterfallbse_round[['PurePriceImpact',
                    'MenuChangeSlsImpact',
                    'GuestCountSlsImpact',
                    'MenuChangeMrgImpact',
                    'GuestCountMrgImpact',
                    'FPCostChangeImpact'
		            ]].fillna(0)

waterfallbse_round[['PurePriceImpact',
                    'MenuChangeSlsImpact',
                    'GuestCountSlsImpact',
                    'MenuChangeMrgImpact',
                    'GuestCountMrgImpact',
                    'FPCostChangeImpact']].replace(to_replace=[0], value=[np.NaN])



#  residual

waterfallbse_round['SalesDelta'] = round(waterfallbse_round['AvgSales_CY']-waterfallbse_round['AvgSales_LY'], 4)
waterfallbse_round['MrgDelta'] = round((waterfallbse_round['AvgSales_CY']-waterfallbse_round['AvgCost_CY'])-(waterfallbse_round['AvgSales_LY']-waterfallbse_round['AvgCost_LY']), 4)

waterfallbse_round['MixMrgImpact'] = waterfallbse_round['MrgDelta']-(waterfallbse_round[['PurePriceImpact',
                        'MenuChangeSlsImpact',
                        'GuestCountSlsImpact',
		                'MenuChangeMrgImpact',
                        'GuestCountMrgImpact',
                        'FPCostChangeImpact']]).sum(axis=1)

waterfallbse_round['MixSlsImpact'] = round(waterfallbse_round['SalesDelta']-waterfallbse_round[['PurePriceImpact', 'GuestCountSlsImpact', 'MenuChangeSlsImpact']], 4)


waterfallbse_round[['PurePriceImpact',
        'MenuChangeSlsImpact',
        'GuestCountSlsImpact',
        'MixMrgImpact',
        'MenuChangeMrgImpact',
        'GuestCountMrgImpact',
        'FPCostChangeImpact',
        'MixSlsImpact']].replace([0], np.NaN)

waterfallbse_round.loc[(round(waterfallbse_round['avgNETprice_cy']-waterfallbse_round['avgNETprice_ly'], 2) > 0.01) &
(waterfallbse_round['promo'] != 1) & (waterfallbse_round['accepted']=1), ['PP_Mrgrec_items', 'PP_Slsrec_items', 'MI_Mrgrec_items', 'MI_Slsrec_items']]=
[(waterfallbse_round['PurePriceImpact']*waterfallbse_round['CurrentRoundChange']),
(waterfallbse_round['PurePriceImpact'] * \
 waterfallbse_round['CurrentRoundChange']),
(waterfallbse_round['MixMrgImpact']*waterfallbse_round['CurrentRoundChange']),
(waterfallbse_round['MixSlsImpact']*waterfallbse_round['CurrentRoundChange'])]

waterfallbse_round.loc[(round(waterfallbse_round['avgNETprice_cy']-waterfallbse_round['avgNETprice_ly'], 2) > 0.01) &
(waterfallbse_round['promo'] != 1) & (waterfallbse_round['partial']=1), ['PP_Mrgrec_items_pric_dev', 'PP_Slsrec_items_pric_dev', 'MI_Mrgrec_items_pric_dev', 'MI_Slsrec_items_pric_dev']]=
[(waterfallbse_round['PurePriceImpact']*waterfallbse_round['CurrentRoundChange']),
(waterfallbse_round['PurePriceImpact'] * \
 waterfallbse_round['CurrentRoundChange']),
(waterfallbse_round['MixMrgImpact']*waterfallbse_round['CurrentRoundChange']),
(waterfallbse_round['MixSlsImpact']*waterfallbse_round['CurrentRoundChange'])]

waterfallbse_round.loc[(round(waterfallbse_round['avgNETprice_cy']-waterfallbse_round['avgNETprice_ly'], 2) > 0.01) &
(waterfallbse_round['promo'] != 1) & (round(waterfallbse_round[('avgNETprice_cy ']-waterfallbse_round['avgNETprice_ly'], 2) > 0.01), ['PP_Mrgprice_inc_outside_recs', 'PP_Slsprice_inc_outside_recs', 'MI_Mrgprice_inc_outside_recs', 'MI_Slsprice_inc_outside_recs']]=[(waterfallbse_round['PurePriceImpact']*waterfallbse_round['CurrentRoundChange']),
(waterfallbse_round['PurePriceImpact'] * \
 waterfallbse_round['CurrentRoundChange']),
(waterfallbse_round['MixMrgImpact']*waterfallbse_round['CurrentRoundChange']),
(waterfallbse_round['MixSlsImpact']*waterfallbse_round['CurrentRoundChange'])]

waterfallbse_round.loc[(round(waterfallbse_round['avgNETprice_cy']-waterfallbse_round['avgNETprice_ly'], 2) > 0.01) &
(waterfallbse_round['promo'] != 1) & ~(waterfallbse_round['accepted']=1) & ~(waterfallbse_round['partial']=1) & ~(round(waterfallbse_round[('avgNETprice_cy ']-waterfallbse_round['avgNETprice_ly'], 2) > 0.01), ['PP_Mrgprice_dec_outside_recs', 'PP_Slsprice_dec_outside_recs', 'MI_Mrgprice_dec_outside_recs', 'MI_Slsprice_dec_outside_recs']]=[(waterfallbse_round['PurePriceImpact']*waterfallbse_round['CurrentRoundChange']),
(waterfallbse_round['PurePriceImpact'] * \
 waterfallbse_round['CurrentRoundChange']),
(waterfallbse_round['MixMrgImpact']*waterfallbse_round['CurrentRoundChange']),
(waterfallbse_round['MixSlsImpact']*waterfallbse_round['CurrentRoundChange'])]

waterfallbse_round.loc[(round(waterfallbse_round['avgNETprice_cy']-waterfallbse_round['avgNETprice_ly'], 2) > 0.01) &
(waterfallbse_round['promo'] != 1), ['PP_Mrgcarryover_price_inc', 'PP_Slscarryover_price_inc', 'MI_Mrgcarryover_price_inc', 'MI_Slscarryover_price_inc']]=[(waterfallbse_round['PurePriceImpact']*waterfallbse_round['CurrentRoundChange']),
(waterfallbse_round['PurePriceImpact'] * \
 waterfallbse_round['CurrentRoundChange']),
(waterfallbse_round['MixMrgImpact']*waterfallbse_round['CurrentRoundChange']),
(waterfallbse_round['MixSlsImpact']*waterfallbse_round['CurrentRoundChange'])]

waterfallbse_round.loc[(round(waterfallbse_round['avgNETprice_cy']-waterfallbse_round['avgNETprice_ly'], 2) > 0.01) &
(waterfallbse_round['promo'] == 1), ['PP_Mrgpromo', 'PP_Slspromo', 'MI_Mrgpromo', 'MI_Slspromo']]=[(waterfallbse_round['PurePriceImpact']),
(waterfallbse_round['PurePriceImpact']),
(waterfallbse_round['MixMrgImpact']),
(waterfallbse_round['MixSlsImpact'])]

waterfallbse_round.loc[(round(waterfallbse_round['avgNETprice_cy']-waterfallbse_round['avgNETprice_ly'], 2) > 0.01), [
                        'MI_Mrgno_price_change', 'MI_Slsno_price_change']]=[(waterfallbse_round['MixMrgImpact']), (waterfallbse_round['MixSlsImpact'])]

waterfallbse_round['measured_marginRecs']=waterfallbse_round[['PP_Mrgrec_items',
    'PP_Mrgrec_items_pric_dev', 'MI_Mrgrec_items', 'MI_Mrgrec_items_pric_dev']].sum()
waterfallbse_round['measured_salesRecs']=waterfallbse_round[['PP_Slsrec_items',
    'PP_Slsrec_items_pric_dev', 'MI_Slsrec_items', 'MI_Slsrec_items_pric_dev']].sum()

waterfallbse_round['measured_margin']=waterfallbse_round[['PP_Mrgrec_items', 'PP_Mrgrec_items_pric_dev', 'MI_Mrgrec_items', 'MI_Mrgrec_items_pric_dev', 'PP_Mrgprice_inc_outside_recs',
			'MI_Mrgprice_inc_outside_recs', 'PP_Mrgprice_dec_outside_recs', 'MI_Mrgprice_dec_outside_recs']].sum()

waterfallbse_round['measured_sales']=waterfallbse_round[['PP_Slsrec_items', 'PP_Slsrec_items_pric_dev', 'MI_Slsrec_items', 'MI_Slsrec_items_pric_dev', 'PP_Slsprice_inc_outside_recs',
			'MI_Slsprice_inc_outside_recs', 'PP_Slsprice_dec_outside_recs', 'MI_Slsprice_dec_outside_recs']].sum()

waterfallbse_round.loc[~waterfallbse_round['rcom_prc '].isna(
), 'Non_Rec_Price']=waterfallbse_round['avgprice_pre_cy']

waterfallbse_round.loc[waterfallbse_round['rcom_prc '].isna(
), 'Non_Rec_Price']=waterfallbse_round['avgprice_cy']

waterfallbse_round['est_units']=np.where(
        (waterfallbse_round['pestore'] == 1) &

        ~(waterfallbse_round['elast_est'].isna()) &
        (waterfallbse_round['promo'] != 1) &
        (waterfallbse_round[waterfallbse_round['accepted']
         == 1 | waterfallbse_round['partial'] == 1]),
        waterfallbse_round['avgunits_pre_cy']*waterfallbse_round['elast_est'],
        np.NaN
)

waterfallbse_round['est_Units_full']=np.where(
        (waterfallbse_round['pestore'] == 1) &

        ~(waterfallbse_round['elast_est'].isna()) &
        (waterfallbse_round['promo'] != 1) &
        (waterfallbse_round[waterfallbse_round['accepted']
         == 1 | waterfallbse_round['partial'] == 1]),
        waterfallbse_round['units_pre_cy']*waterfallbse_round['elast_est'],
        np.NaN
)

waterfallbse_round['est_margin']=np.where(
        (waterfallbse_round['pestore'] == 1) &

        ~(waterfallbse_round['elast_est'].isna()) &
        (waterfallbse_round['promo'] != 1) &
        (waterfallbse_round[waterfallbse_round['accepted']
         == 1 | waterfallbse_round['partial'] == 1]),
        (waterfallbse_round['est_Units']*(waterfallbse_round['avgNETprice_cy']-waterfallbse_round['AvgCostItem_pre_cy'])) -
        (waterfallbse_round['avgunits_pre_cy']*(
            waterfallbse_round['avgNETprice_pre_cy']-waterfallbse_round['AvgCostItem_pre_cy'])),
        np.NaN
)

waterfallbse_round['low_est_margin']=np.where(
        (waterfallbse_round['pestore'] == 1) &

        ~(waterfallbse_round['elast_est'].isna()) &
        (waterfallbse_round['promo'] != 1) &
        (waterfallbse_round[waterfallbse_round['accepted']
         == 1 | waterfallbse_round['partial'] == 1]),
        (1-file['range'])*waterfallbse_round['est_margin'],
        np.NaN
)

waterfallbse_round['up_est_margin']=np.where(
        (waterfallbse_round['pestore'] == 1) &

        ~(waterfallbse_round['elast_est'].isna()) &
        (waterfallbse_round['promo'] != 1) &
        (waterfallbse_round[waterfallbse_round['accepted']
         == 1 | waterfallbse_round['partial'] == 1]),
       (1+file['range'])*waterfallbse_round['est_margin'],
        np.NaN
)

waterfallbse_round['Est_sales']=np.where(
        (waterfallbse_round['pestore'] == 1) &

        ~(waterfallbse_round['elast_est'].isna()) &
        (waterfallbse_round['promo'] != 1) &
        (waterfallbse_round[waterfallbse_round['accepted']
         == 1 | waterfallbse_round['partial'] == 1]),
        (waterfallbse_round['est_Units']*waterfallbse_round['avgNETprice_cy'])-(
            waterfallbse_round['avgunits_pre_cy']*waterfallbse_round['avgNETprice_pre_cy']),
        np.NaN
)
waterfallbse_round['low_est_sales']=np.where(
        (waterfallbse_round['pestore'] == 1) &

        ~(waterfallbse_round['elast_est'].isna()) &
        (waterfallbse_round['promo'] != 1) &
        (waterfallbse_round[waterfallbse_round['accepted']
         == 1 | waterfallbse_round['partial'] == 1]),
       (1-file['range'])*waterfallbse_round['Est_sales'],
        np.NaN
)
waterfallbse_round['act_Units']=np.where(
        (waterfallbse_round['pestore'] == 1) &

        ~(waterfallbse_round['elast_est'].isna()) &
        (waterfallbse_round['promo'] != 1) &
        (waterfallbse_round[waterfallbse_round['accepted']
         == 1 | waterfallbse_round['partial'] == 1]),
       waterfallbse_round['AvgUnits_CY'],
        np.NaN
)
waterfallbse_round['act_Sales']=np.where(
        (waterfallbse_round['pestore'] == 1) &

        ~(waterfallbse_round['elast_est'].isna()) &
        (waterfallbse_round['promo'] != 1) &
        (waterfallbse_round[waterfallbse_round['accepted']
         == 1 | waterfallbse_round['partial'] == 1]),
       waterfallbse_round['AvgSales_CY'],
        np.NaN
)
waterfallbse_round['act_Margin']=np.where(
        (waterfallbse_round['pestore'] == 1) &

        ~(waterfallbse_round['elast_est'].isna()) &
        (waterfallbse_round['promo'] != 1) &
        (waterfallbse_round[waterfallbse_round['accepted']
         == 1 | waterfallbse_round['partial'] == 1]),
       waterfallbse_round['AvgSales_CY']-waterfallbse_round['AvgCost_CY'],
        np.NaN
)
waterfallbse_round['pre_act_Units']=np.where(
        (waterfallbse_round['pestore'] == 1) &

        ~(waterfallbse_round['elast_est'].isna()) &
        (waterfallbse_round['promo'] != 1) &
        (waterfallbse_round[waterfallbse_round['accepted']
         == 1 | waterfallbse_round['partial'] == 1]),
        waterfallbse_round['AvgUnits_Pre_CY'],
        np.NaN
)
waterfallbse_round['pre_act_Sales']=np.where(
        (waterfallbse_round['pestore'] == 1) &

        ~(waterfallbse_round['elast_est'].isna()) &
        (waterfallbse_round['promo'] != 1) &
        (waterfallbse_round[waterfallbse_round['accepted']
         == 1 | waterfallbse_round['partial'] == 1]),
        waterfallbse_round['AvgSales_Pre_CY'],
        np.NaN
)
waterfallbse_round['pre_act_Margin']=np.where(
        (waterfallbse_round['pestore'] == 1) &

        ~(waterfallbse_round['elast_est'].isna()) &
        (waterfallbse_round['promo'] != 1) &
        (waterfallbse_round[waterfallbse_round['accepted']
         == 1 | waterfallbse_round['partial'] == 1]),
        waterfallbse_round['AvgSales_Pre_CY'] - \
            waterfallbse_round['AvgCost_pre_CY'],
        np.NaN
)

waterfallbse_round['rec_Units']=np.where(
        (waterfallbse_round['pestore'] == 1) &
        ~(waterfallbse_round['elast_rec '].isna()) &
        ~(waterfallbse_round['prcg_engn_curr_net_prc '].isna()) &
        ~(waterfallbse_round['rcom_net_prc '].isna()),
        waterfallbse_round['avgunits_pre_cy']*waterfallbse_round['elast_rec'],
        np.NaN
)
waterfallbse_round['rec_Units_full']=np.where(
        (waterfallbse_round['pestore'] == 1) &
        ~(waterfallbse_round['elast_rec '].isna()) &
        ~(waterfallbse_round['prcg_engn_curr_net_prc '].isna()) &
        ~(waterfallbse_round['rcom_net_prc '].isna()),
        waterfallbse_round['units_pre_cy']*waterfallbse_round['elast_rec'],
        np.NaN
)
waterfallbse_round['rec_margin']=np.where(
        (waterfallbse_round['pestore'] == 1) &
        ~(waterfallbse_round['elast_rec '].isna()) &
        ~(waterfallbse_round['prcg_engn_curr_net_prc '].isna()) &
        ~(waterfallbse_round['rcom_net_prc '].isna()),
        (
            waterfallbse_round['rec_Units']*(waterfallbse_round['rcom_net_prc']-waterfallbse_round['AvgCostItem_pre_cy'])
        )-
        (
            waterfallbse_round['avgunits_pre_cy']*(waterfallbse_round['prcg_engn_curr_net_prc']-waterfallbse_round['AvgCostItem_pre_cy'])
        ),
        np.NaN
)
waterfallbse_round['low_rec_margin']=np.where(
        (waterfallbse_round['pestore'] == 1) &
        ~(waterfallbse_round['elast_rec '].isna()) &
        ~(waterfallbse_round['prcg_engn_curr_net_prc '].isna()) &
        ~(waterfallbse_round['rcom_net_prc '].isna()),
        (1-file['range'])*waterfallbse_round['rec_margin'],
        np.NaN
)
waterfallbse_round['up_rec_margin']=np.where(
        (waterfallbse_round['pestore'] == 1) &
        ~(waterfallbse_round['elast_rec '].isna()) &
        ~(waterfallbse_round['prcg_engn_curr_net_prc '].isna()) &
        ~(waterfallbse_round['rcom_net_prc '].isna()),
       (1+file['range'])*waterfallbse_round['rec_margin'],
        np.NaN
)

waterfallbse_round['rec_sales']=np.where(
        (waterfallbse_round['pestore'] == 1) &
        ~(waterfallbse_round['elast_rec '].isna()) &
        ~(waterfallbse_round['prcg_engn_curr_net_prc '].isna()) &
        ~(waterfallbse_round['rcom_net_prc '].isna()),
        (waterfallbse_round['rec_Units']*waterfallbse_round['rcom_net_prc'])-(
            waterfallbse_round['avgunits_pre_cy']*(waterfallbse_round['prcg_engn_curr_net_prc'])),
        np.NaN
)
waterfallbse_round['low_rec_sales']=np.where(
        (waterfallbse_round['pestore'] == 1) &
        ~(waterfallbse_round['elast_rec '].isna()) &
        ~(waterfallbse_round['prcg_engn_curr_net_prc '].isna()) &
        ~(waterfallbse_round['rcom_net_prc '].isna()),
       (1-file['range'])*waterfallbse_round['Rec_sales'],
        np.NaN
)
waterfallbse_round['up_rec_sales']=np.where(
        (waterfallbse_round['pestore'] == 1) &
        ~(waterfallbse_round['elast_rec '].isna()) &
        ~(waterfallbse_round['prcg_engn_curr_net_prc '].isna()) &
        ~(waterfallbse_round['rcom_net_prc '].isna()),
       (1+file['range'])*waterfallbse_round['rec_sales'],
        np.NaN
)
waterfallbse_round['recflag']=np.where(
    ~(waterfallbse_round['rcom_prc'].isna()), 1, 0
)
waterfallbse_round['acptflag']=np.where(
    ~(waterfallbse_round['rcom_prc'].isna())
    & (waterfallbse_round['accepted'] == 1) | waterfallbse_round['partial'] == 1, 1, 0
)
waterfallbse_round.loc[:'accepted']=np.where(
    waterfallbse_round['accepted'].isna(), 0, 1
)
waterfallbse_round.loc[:'partial']=np.where(
    waterfallbse_round['partial'].isna(), 0, 1
)

waterfallbse_round['outsiderec']=np.where(
    ~(waterfallbse_round['rcom_prc'].isna()),
    np.where(
        waterfallbse_round['newprice'] != waterfallbse_round['oldPrice']
        & (waterfallbse_round['accepted'].isin([np.NaN, 0]))
        & (waterfallbse_round['partial'].isin([np.NaN, 0])),
        1, np.nan
    ),
    0
)

waterfallbse_round['addflag']=np.where(
    (waterfallbse_round['rcom_prc'].isna()),
    np.where(
        waterfallbse_round['newprice'] != waterfallbse_round['oldPrice'], 1, 0
    ),
    np.nan

)
# added for implementation report GC from price for all price changes

waterfallbse_round['Non_Rec_Price2']=np.where(
    (~(waterfallbse_round['rcom_prc'].isna())
     | waterfallbse_round['addflag'] == 1),
    waterfallbse_round['avgprice_pre_cy'], waterfallbse_round['avgprice_cy']
)

waterfallbse_round['MENU_CY_Flag']=np.where(
    ~(waterfallbse_round['avgMenuprice_CY'].isna()), 1, 0
)
waterfallbse_round['MENU_LY_Flag']=np.where(
    ~(waterfallbse_round['avgMenuprice_LY'].isna()), 1, 0
)
waterfallbse_round['MENU_Pre_CY_Flag']=np.where(
    ~(waterfallbse_round['avgMenuprice_Pre_CY '].isna()), 1, 0
)
waterfallbse_round['MENU_Pre_LY_Flag']=np.where(
    ~(waterfallbse_round['avgMenuprice_Pre_LY'].isna()), 1, 0
)
waterfallbse_round['LY_Flag']=np.where(
    ~(waterfallbse_round['avgprice_LY'].isna()), 1, 0
)
waterfallbse_round['Pre_LY_Flag']=np.where(
    ~(waterfallbse_round['avgprice_Pre_LY'].isna()), 1, 0
)
waterfallbse_round['Pre_CY_Flag']=np.where(
    ~(waterfallbse_round['avgprice_Pre_CY'].isna()), 1, 0
)
waterfallbse_round['CY_Flag']=np.where(
    ~(waterfallbse_round['avgprice_CY'].isna()), 1, 0
)

waterfallbse_round.sort_values(
    by=['LVLLIST2', 'mcd_gbal_lcat_id_nu ', 'store_nu', 'delivery ', 'sld_menu_itm_id'])

#  Added below WAP calculation for GC Estimation calculation
wap_cal_base=waterfallbse_round[['LVLLIST2', 'mcd_gbal_lcat_id_nu ', 'units_pre_cy ', 'units_ly ', 'oldprice', 'newprice',
    'prcg_engn_curr_prc ', 'rcom_prc ', 'accepted', 'partial', 'PEstore', 'AvgMenuPrice_CY', 'AvgMenuPrice_Pre_CY']]

wap_cal_base.loc[:"newprice"]=np.where(
    wap_cal_base["newprice"].isna(), wap_cal_base['AvgMenuPrice_CY'], np.nan
)
wap_cal_base[:"oldprice"]=np.where(
    wap_cal_base["oldprice"].isna(
    ), wap_cal_base['AvgMenuPrice_Pre_CY'], np.nan
)
wap_cal_base[:"rcom_prc"]=np.where(
    wap_cal_base['rcom_prc'].isna(), wap_cal_base['AvgMenuPrice_CY'], np.nan
)
wap_cal_base[:"prcg_engn_curr_prc"]=np.where(
    wap_cal_base["prcg_engn_curr_prc"].isna(
    ), wap_cal_base["AvgMenuPrice_Pre_CY"]
)

Waterfallbase_rest2=wap_cal_base.copy()
Waterfallbase_rest2[(~Waterfallbase_rest2['newprice'].isna())
& (~Waterfallbase_rest2['oldprice'].isna())
& (Waterfallbase_rest2['pestore']==1)].groupby([f"{lvllistid}",Waterfallbase_rest2['mcd_gbal_lcat_id_nu']])
Waterfallbase_rest2['New_WAP']=sum(Waterfallbase_rest2['newprice']*Waterfallbase_rest2['units_ly'])/(Waterfallbase_rest2['units_ly'].sum())
Waterfallbase_rest2['Old_WAP']=sum(Waterfallbase_rest2['oldprice']*Waterfallbase_rest2['units_ly'])/(Waterfallbase_rest2['units_ly'].sum())

Waterfallbase_rest3=wap_cal_base.copy()
Waterfallbase_rest3=(
    Waterfallbase_rest3[(~Waterfallbase_rest3['rcom_prc'].isna())
    & (~Waterfallbase_rest3['prcg_engn_curr_prc'].isin())
    & (Waterfallbase_rest3['pestore']==1)]
    .groupby([f"{lvllistid}",mcd_gbal_lcat_id_nu])
)
Waterfallbase_rest3['Rec_WAP']=(
    sum(Waterfallbase_rest3['rcom_prc']*Waterfallbase_rest3['units_ly'])/
    sum(Waterfallbase_rest3['units_ly'])
)
Waterfallbase_rest3['Prcg_eng_WAP']=(
    sum(Waterfallbase_rest3['prcg_engn_curr_prc']*Waterfallbase_rest3['units_ly'])/
    sum(Waterfallbase_rest3['units_ly'])
)
# added for implementation report GC from price for all price changes
# average measured GC for recommended price change items

waterfallbase_rest1= (
    waterfallbse_round.merge(wkly_pmix_trans_REST,on='mcd_gbal_lcat_id_nu',how='left')
    .merge(waterfallbase_rest2,left_on='mcd_gbal_lcat_id_nu',how='left')
    .merge(waterfallbase_rest3, left_on='mcd_gbal_lcat_id_nu',how='left')
)
waterfallbase_rest1.groupby(waterfallbase_rest1['LVLLISTID'], waterfallbase_rest1['mcd_gbal_lcat_id_nu'],
                            waterfallbase_rest1['store_nu'], waterfallbase_rest1['AvgGuestCount_CY'],
                            waterfallbase_rest1['AvgGuestCount_LY'], waterfallbase_rest1['AvgGuestCount_Pre_CY'],
                            waterfallbase_rest1['avg_YOY_tot_GC'], waterfallbase_rest1[
                                'gc_elastic'], waterfallbase_rest1['InfluencePrcGC'],
		                    waterfallbase_rest1['PEStore'], waterfallbase_rest1['new_WAP'], waterfallbase_rest1['old_WAP'],
                            waterfallbase_rest1['Rec_WAP'], waterfallbase_rest1['Prcg_eng_WAP']).agg(
            est_Units=pd.NamedAgg(column='est_Units', aggfunc='sum'),
            est_margin=pd.NamedAgg(column='est_margin', aggfunc='sum'),
            low_est_margin=pd.NamedAgg(column='low_est_margin', aggfunc='sum'),
            up_est_margin=pd.NamedAgg(column='up_est_margin', aggfunc='sum'),
            est_sales=pd.NamedAgg(column='est_sales', aggfunc='sum'),
            low_est_sales=pd.NamedAgg(column='low_est_sales', aggfunc='sum'),
            up_est_sales=pd.NamedAgg(column='up_est_sales', aggfunc='sum'),

            rec_Units=pd.NamedAgg(column='rec_Units', aggfunc='sum'),
            rec_margin=pd.NamedAgg(column='rec_margin', aggfunc='sum'),
            low_rec_margin=pd.NamedAgg(column='low_rec_margin', aggfunc='sum'),
            up_rec_margin=pd.NamedAgg(column='up_rec_margin', aggfunc='sum'),
            rec_sales=pd.NamedAgg(column='rec_sales', aggfunc='sum'),
            low_rec_sales=pd.NamedAgg(column='low_rec_sales', aggfunc='sum'),
            up_rec_sales=pd.NamedAgg(column='up_rec_sales', aggfunc='sum'),

            measured_marginRecs=pd.NamedAgg(
                column='measured_marginRecs', aggfunc='sum'),
            measured_salesRecs=pd.NamedAgg(
                column='measured_salesRecs', aggfunc='sum'),
            measured_margin=pd.NamedAgg(
                column='measured_margin', aggfunc='sum'),
            measured_sales=pd.NamedAgg(column='measured_sales', aggfunc='sum'),
            GuestCountMrgImpact=pd.NamedAgg(
                column='GuestCountMrgImpact', aggfunc='sum'),
            GuestCountSlsImpact=pd.NamedAgg(
                column='GuestCountSlsImpact', aggfunc='sum'),

            RecAccept=pd.NamedAgg(column='acptflag', aggfunc='sum'),
            RecAdded=pd.NamedAgg(column='addflag', aggfunc='sum'),
            RecFlag=pd.NamedAgg(column='recflag', aggfunc='sum'),
            AvgUnits_LY=pd.NamedAgg(column='AvgUnits_LY', aggfunc='sum'),
            AvgUnits_Pre_CY=pd.NamedAgg(
                column='AvgUnits_Pre_CY', aggfunc='sum'),
            AvgUnits_CY=pd.NamedAgg(column='AvgUnits_CY', aggfunc='sum'),
            avgTotalsales_LY=pd.NamedAgg(column='AvgSales_LY', aggfunc='sum'),
            avgTotalsales_Pre_CY=pd.NamedAgg(
                column='AvgSales_Pre_CY', aggfunc='sum'),
            avgTotalsales_CY=pd.NamedAgg(column='AvgSales_CY', aggfunc='sum'),
            AvgTotalCost_LY=pd.NamedAgg(column='AvgCost_LY', aggfunc='sum'),
            AvgTotalCost_pre_CY=pd.NamedAgg(
                column='AvgCost_pre_CY', aggfunc='sum'),
            AvgTotalCost_CY=pd.NamedAgg(column='AvgCost_CY', aggfunc='sum'),
            AvgTotalMargin_LY=pd.NamedAgg(
                column=waterfallbase_rest1['AvgSales_LY']-waterfallbase_rest1['AvgCost_LY'], aggfunc='sum'),  # todo
            AvgTotalMargin_pre_CY=pd.NamedAgg(
                column=waterfallbase_rest1['AvgSales_Pre_CY']-waterfallbase_rest1['AvgCost_pre_CY'], aggfunc='sum'),
            AvgTotalMargin_CY=pd.NamedAgg(
                column=AvgSales_CY-waterfallbase_rest1['AvgCost_CY'], aggfunc='sum'),
            act_Units=pd.NamedAgg(column='act_Units', aggfunc='sum'),
            act_Sales=pd.NamedAgg(column='act_Sales', aggfunc='sum'),
            act_Margin=pd.NamedAgg(column='act_Margin', aggfunc='sum'),
            pre_act_Units=pd.NamedAgg(column='pre_act_Units', aggfunc='sum'),
            pre_act_Sales=pd.NamedAgg(column='pre_act_Sales', aggfunc='sum'),
            pre_act_Margin=pd.NamedAgg(column='pre_act_Margin', aggfunc='sum')

            )
waterfallbase_rest1['Overall_PC_per_YOY']=(
    sum((waterfallbase_rest1['avgprice_cy']-waterfallbase_rest1['avgprice_ly'])*waterfallbase_rest1['avgunits_ly'])/
    sum(waterfallbase_rest1['avgunits_ly']))/
    (sum(waterfallbase_rest1['avgprice_ly']*waterfallbase_rest1['avgunits_ly'])/
    sum(waterfallbase_rest1['avgunits_ly']))

waterfallbase_rest1['menupricechgRecItems']=(sum(
    (waterfallbase_rest1['avgprice_cy']-waterfallbase_rest1['Non_Rec_Price'])*waterfallbase_rest1['avgunits_ly'])/sum(
        waterfallbase_rest1['avgunits_ly']))/(sum(waterfallbase_rest1['avgprice_ly']*waterfallbase_rest1['avgunits_ly'])/sum(
            waterfallbase_rest1['avgunits_ly']))
            # menu price change recommended items plus other adds
waterfallbase_rest1['menupricechgItems']=(sum(
    (waterfallbase_rest1['avgprice_cy']-waterfallbase_rest1['Non_Rec_Price2'])*waterfallbase_rest1['avgunits_ly'])/sum(
        waterfallbase_rest1['avgunits_ly']))/(sum(
    waterfallbase_rest1['avgprice_ly']*waterfallbase_rest1['avgunits_ly'])/sum(waterfallbase_rest1['avgunits_ly']))
waterfallbase_rest1['AvgGuestCount_Pre_CY']=waterfallbse_round['AvgGuestCount_Pre_CY']
waterfallbase_rest1['AvgGuestCount_CY']=waterfallbse_round['AvgGuestCount_CY']
waterfallbase_rest1['AvgGuestCount_LY']=waterfallbse_round['AvgGuestCount_LY']
waterfallbase_rest1['avg_YOY_tot_rest_price']=(sum(
    waterfallbase_rest1['AvgSales_CY']/waterfallbase_rest1['avgunits_cy'])-sum(
        waterfallbase_rest1['avgSales_ly']/waterfallbase_rest1['AvgUnits_LY']))/sum(
            waterfallbase_rest1['avgSales_LY']/waterfallbase_rest1['avgUnits_LY'])
waterfallbase_rest1['Avg_YOY_tot_rest_sales']=sum(
    waterfallbase_rest1['AvgSales_CY']-waterfallbase_rest1['AvgSales_LY'])/sum(waterfallbase_rest1['AvgSales_LY'])
waterfallbase_rest1['avg_YOY_rest_margin']=(sum(waterfallbase_rest1['AvgSales_CY']-waterfallbase_rest1['AvgCost_CY'])-sum(
    waterfallbase_rest1['AvgSales_LY']-waterfallbase_rest1['AvgCost_LY']))/sum(
        waterfallbase_rest1['AvgSales_LY']-waterfallbase_rest1['AvgCost_LY'])
waterfallbase_rest1['avg_YOY_tot_GC']=(
    waterfallbse_round['AvgGuestCount_CY']-waterfallbse_round['AvgGuestCount_LY'])/waterfallbse_round['AvgGuestCount_LY']
waterfallbase_rest1['RecAccept']=waterfallbase_rest1['acptflag'].sum()
waterfallbase_rest1['RecAdded']=waterfallbase_rest1['recflag'].sum()
waterfallbase_rest1['AvgUnits_LY']=waterfallbase_rest1['AvgUnits_LY'].sum()
waterfallbase_rest1['AvgUnits_Pre_CY']=waterfallbase_rest1['AvgUnits_Pre_CY'].sum()
waterfallbase_rest1['AvgUnits_CY']=waterfallbase_rest1['AvgUnits_CY'].sum()
waterfallbase_rest1['avgTotalsales_LY']=waterfallbase_rest1['AvgSales_LY'].sum()
waterfallbase_rest1['avgTotalsales_Pre_CY']=waterfallbase_rest1['AvgSales_Pre_CY'].sum()
waterfallbase_rest1['avgTotalsales_CY']=waterfallbase_rest1['AvgSales_CY'].sum()
waterfallbase_rest1['AvgTotalCost_LY']=waterfallbase_rest1['AvgCost_LY'].sum()
waterfallbase_rest1['AvgTotalCost_pre_CY']=waterfallbase_rest1['AvgCost_pre_CY'].sum()
waterfallbase_rest1['AvgTotalCost_CY']=waterfallbase_rest1['AvgCost_CY'].sum()
waterfallbase_rest1['AvgTotalMargin_LY']=(
    waterfallbase_rest1['AvgSales_LY']-waterfallbase_rest1['AvgCost_LY']).sum()
waterfallbase_rest1['AvgTotalMargin_pre_CY']=(
    waterfallbase_rest1['AvgSales_Pre_CY']-waterfallbase_rest1['AvgCost_pre_CY']).sum()
waterfallbase_rest1['AvgTotalMargin_CY']=(
    waterfallbase_rest1['AvgSales_CY']-waterfallbase_rest1['AvgCost_CY']).sum()

waterfallbase_rest1['act_Units']=waterfallbase_rest1['act_Units'].sum()
waterfallbase_rest1['act_Sales']=waterfallbase_rest1['act_Sales'].sum()
waterfallbase_rest1['act_Margin']=waterfallbase_rest1['act_Margin'].sum()
waterfallbase_rest1['pre_act_Units']=waterfallbase_rest1['pre_act_Units'].sum()
waterfallbase_rest1['pre_act_Sales']=waterfallbase_rest1['pre_act_Sales'].sum()
waterfallbase_rest1['pre_act_Margin']=waterfallbase_rest1['pre_act_Margin'].sum()

# waterfallbase_rest1[['gc_elastic', 'InfluencePrcGC',
#     'PEStore', 'new_WAP', 'old_WAP', 'Rec_WAP', 'Prcg_eng_WAP']]


waterfallbase_Rest_round=waterfallbase_rest1.copy()

waterfallbase_Rest_round.loc[(~waterfallbase_Rest_round['gc_elastic '].isna()) & (~waterfallbase_Rest_round['New_WAP'].isin([np.NaN, 0]) & (~waterfallbase_Rest_round['old_WAP'].isin(
    [np.NaN, 0]))), 'est_GC']=waterfallbase_Rest_round['gc_elastic']*(waterfallbase_Rest_round['New_WAP'].apply(np.log)-waterfallbase_Rest_round['old_WAP'].apply(np.log)).apply(np.exp)-1

waterfallbase_Rest_round.loc[(~waterfallbase_Rest_round['gc_elastic '].isna()) & (~waterfallbase_Rest_round['New_WAP'].isin([np.NaN, 0]) & (
    ~waterfallbase_Rest_round['old_WAP'].isin([np.NaN, 0]))) & (waterfallbase_Rest_round['est_GC'] < 0), 'low_est_GC']=waterfallbase_Rest_round['est_GC']*(1+file['range'])
waterfallbase_Rest_round.loc[(~waterfallbase_Rest_round['gc_elastic '].isna()) & (~waterfallbase_Rest_round['New_WAP'].isin([np.NaN, 0]) & (
    ~waterfallbase_Rest_round['old_WAP'].isin([np.NaN, 0]))) & (waterfallbase_Rest_round['est_GC'] < 0), 'up_est_GC']=waterfallbase_Rest_round['est_GC']*(1-file['range'])
waterfallbase_Rest_round.loc[~(waterfallbase_Rest_round['gc_elastic '].isna()) & (~waterfallbase_Rest_round['New_WAP'].isin([np.NaN, 0]) & (
    ~waterfallbase_Rest_round['old_WAP'].isin([np.NaN, 0]))) & (waterfallbase_Rest_round['est_GC'] < 0), 'low_est_GC']=waterfallbase_Rest_round['est_GC']*(1-file['range'])
waterfallbase_Rest_round.loc[~(~waterfallbase_Rest_round['gc_elastic '].isna()) & (~waterfallbase_Rest_round['New_WAP'].isin([np.NaN, 0]) & (
    ~waterfallbase_Rest_round['old_WAP'].isin([np.NaN, 0]))) & (waterfallbase_Rest_round['est_GC'] < 0), 'up_est_GC']=waterfallbase_Rest_round['est_GC']*(1+file['range'])

waterfallbase_Rest_round['act_GC']=(waterfallbase_Rest_round['AvgGuestCount_CY'] - \
                                    waterfallbase_Rest_round['AvgGuestCount_LY'])/waterfallbase_Rest_round['AvgGuestCount_LY']
waterfallbase_Rest_round['pre_act_GC']=(waterfallbase_Rest_round['avgGuestCount_Pre_CY'] - \
                                        waterfallbase_Rest_round['AvgGuestCount_LY'])/waterfallbase_Rest_round['AvgGuestCount_LY']

waterfallbase_Rest_round.loc[(~waterfallbase_Rest_round['gc_elastic '].isna()) & (~waterfallbase_Rest_round['Rec_WAP'].isin([np.NaN, 0]) & (~waterfallbase_Rest_round['prcg_eng_WAP '].isin(
    [np.NaN, 0]))), 'rec_GC']=waterfallbase_Rest_round['gc_elastic']*(waterfallbase_Rest_round['Rec_WAP'].apply(np.log)-waterfallbase_Rest_round['prcg_eng_WAP'].apply(np.log)).apply(np.exp)-1
waterfallbase_Rest_round.loc[(~waterfallbase_Rest_round['gc_elastic '].isna()) & (~waterfallbase_Rest_round['Rec_WAP'].isin([np.NaN, 0]) & (
    ~waterfallbase_Rest_round['prcg_eng_WAP '].isin([np.NaN, 0]))) & (waterfallbase_Rest_round['rec_GC'] < 0), 'low_rec_GC']=waterfallbase_Rest_round['rec_GC']*(1+file['range'])
waterfallbase_Rest_round.loc[(~waterfallbase_Rest_round['gc_elastic '].isna()) & (~waterfallbase_Rest_round['Rec_WAP'].isin([np.NaN, 0]) & (
    ~waterfallbase_Rest_round['prcg_eng_WAP '].isin([np.NaN, 0]))) & (waterfallbase_Rest_round['rec_GC'] < 0), 'up_rec_GC']=waterfallbase_Rest_round['rec_GC']*(1-file['range'])
waterfallbase_Rest_round.loc[~(~waterfallbase_Rest_round['gc_elastic '].isna()) & (~waterfallbase_Rest_round['Rec_WAP'].isin([np.NaN, 0]) & (
    ~waterfallbase_Rest_round['prcg_eng_WAP '].isin([np.NaN, 0]))) & (waterfallbase_Rest_round['rec_GC'] < 0), 'low_rec_GC']=waterfallbase_Rest_round['rec_GC']*(1-file['range'])
waterfallbase_Rest_round.loc[~(~waterfallbase_Rest_round['gc_elastic '].isna()) & (~waterfallbase_Rest_round['Rec_WAP'].isin([np.NaN, 0]) & (
    ~waterfallbase_Rest_round['prcg_eng_WAP '].isin([np.NaN, 0]))) & (waterfallbase_Rest_round['rec_GC'] < 0), 'up_rec_GC']=waterfallbase_Rest_round['rec_GC']*(1+file['range'])

waterfallbase_Rest_round['rec_act_GC']=(
    (waterfallbase_Rest_round['AvgGuestCount_CY'] - waterfallbase_Rest_round['AvgGuestCount_LY'])
    / (waterfallbase_Rest_round['AvgGuestCount_LY'])
)

waterfallbase_Rest_round['rec_pre_act_GC']=(
    (waterfallbase_Rest_round['avgGuestCount_Pre_CY'] - waterfallbase_Rest_round['AvgGuestCount_LY'])/
    waterfallbase_Rest_round['AvgGuestCount_LY']

)

waterfallbase_Rest_round.loc[~waterfallbase_Rest_round['menupricechgRecItems '].isin(np.NaN, 0), 'avg_measured_GCRecs']=(waterfallbase_Rest_round['AvgGuestCount_CY']-waterfallbase_Rest_round['AvgGuestCount_LY'])/(
    waterfallbase_Rest_round['AvgGuestCount_LY']*waterfallbase_Rest_round['menupricechgRecItems'])/(waterfallbase_Rest_round['Overall_PC_per_YOY']*waterfallbase_Rest_round['InfluencePrcGC'])
waterfallbase_Rest_round[~waterfallbase_Rest_round['menupricechgItems '].isin(np.NaN, 0), 'avg_measured_GCRecs']=(waterfallbase_Rest_round['AvgGuestCount_CY']-waterfallbase_Rest_round['AvgGuestCount_LY'])/(
    waterfallbase_Rest_round['AvgGuestCount_LY']*waterfallbase_Rest_round['menupricechgItems'])/(waterfallbase_Rest_round['Overall_PC_per_YOY']*waterfallbase_Rest_round['InfluencePrcGC'])

waterfallbase_Rest_round.loc[(~waterfallbase_Rest_round['low_est_margin '].isin([np.NaN])) & (waterfallbase_Rest_round['measured_marginRecs '] <= (
    waterfallbase_Rest_round['up_est_margin '] & waterfallbase_Rest_round['measured_marginRecs']) >= waterfallbase_Rest_round['low_est_margin ']), 'est_marg_ach']=1
waterfallbase_Rest_round.loc[waterfallbase_Rest_round['measured_marginRecs ']
    > waterfallbase_Rest_round['up_est_margin '], 'est_marg_ach_over']=1

waterfallbase_Rest_round.loc[(~waterfallbase_Rest_round['low_est_GC '].isna()) & (waterfallbase_Rest_round['avg_measured_GCRecs'] <= (
    waterfallbase_Rest_round['up_est_GC '] & waterfallbase_Rest_round['avg_measured_GCRecs ']) >= waterfallbase_Rest_round['low_est_GC ']), 'est_GC_ach']=1
waterfallbase_Rest_round.loc[(~waterfallbase_Rest_round['low_est_GC '].isna()) & (
    waterfallbase_Rest_round['avg_measured_GCRecs '] > waterfallbase_Rest_round['up_est_GC ']), 'est_GC_ach_over']=1

waterfallbase_Rest_round.loc[(~waterfallbase_Rest_round['low_rec_margin '].isna()) & (waterfallbase_Rest_round['measured_marginRecs'] <= (
    waterfallbase_Rest_round['up_rec_margin '] & waterfallbase_Rest_round['measured_marginRecs ']) >= waterfallbase_Rest_round['low_rec_margin']), 'rec_marg_ach']=1
waterfallbase_Rest_round.loc[(~waterfallbase_Rest_round['low_rec_margin '].isna()) & (
    waterfallbase_Rest_round['measured_marginRecs '] > waterfallbase_Rest_round['up_rec_margin ']), 'rec_marg_ach_over']=1

waterfallbase_Rest_round.loc[(~waterfallbase_Rest_round['low_rec_sales '].isna()) & (waterfallbase_Rest_round['measured_salesRecs '] <=
                              waterfallbase_Rest_round['up_rec_sales '] & waterfallbase_Rest_round['measured_salesRecs '] >= waterfallbase_Rest_round['low_rec_sales']), 'rec_sales_ach']=1
waterfallbase_Rest_round.loc[(~waterfallbase_Rest_round['low_rec_sales '].isna()) & (
    waterfallbase_Rest_round['measured_salesRecs '] > waterfallbase_Rest_round['up_rec_sales']), 'rec_sales_ach_over']=1

waterfallbase_Rest_round.loc[(~waterfallbase_Rest_round['low_rec_GC '].isna()) & (waterfallbase_Rest_round['avg_measured_GCRecs'] <= (
    waterfallbase_Rest_round['up_rec_GC '] & waterfallbase_Rest_round['avg_measured_GCRecs']) >= waterfallbase_Rest_round['low_rec_GC ']), 'rec_GC_ach']=1
waterfallbase_Rest_round.loc[(~waterfallbase_Rest_round['low_rec_GC'].isna()) & (
    waterfallbase_Rest_round['avg_measured_GCRecs '] > waterfallbase_Rest_round['up_rec_GC ']), 'rec_GC_ach_over']=1

waterfallbase_Rest_round[['rec_GC_ach_over', 'rec_GC_ach', 'rec_sales_ach', 'rec_sales_ach_over', 'rec_marg_ach_over', 'rec_marg_ach',
		'est_GC_ach_over', 'est_GC_ach', 'est_sales_ach', 'est_sales_ach_over', 'est_marg_ach_over', 'est_marg_ach',
		]].fillna(0)

waterfallbase_Rest_round['RecAcceptRate']=waterfallbase_Rest_round['RecAccept'] / \
    waterfallbase_Rest_round['RecFlag']
waterfallbase_Rest_round['PerMarginChangeRecs']=waterfallbase_Rest_round['measured_marginRecs']/(
    waterfallbase_Rest_round['avgTotalsales_Pre_CY']-waterfallbase_Rest_round['AvgTotalCost_Pre_CY'])
waterfallbase_Rest_round['PerSalesChangeRecs']=waterfallbase_Rest_round['measured_salesRecs'] / \
    waterfallbase_Rest_round['avgTotalsales_Pre_CY']

waterfallbase_Rest_round['PerMarginChange']=waterfallbase_Rest_round['measured_margin']/(
    waterfallbase_Rest_round['avgTotalsales_Pre_CY']-waterfallbase_Rest_round['AvgTotalCost_Pre_CY'])
waterfallbase_Rest_round['PerSalesChange']=waterfallbase_Rest_round['measured_sales'] / \
    waterfallbase_Rest_round['avgTotalsales_Pre_CY']

waterfallbase_Rest_round['GuestCountPerChange']=(
    waterfallbase_Rest_round['AvgGuestCount_CY']/waterfallbase_Rest_round['AvgGuestCount_LY'])-1

# /*** this is new portion ***/

waterfallbase_Rest_round['EstPerMarginChange']=waterfallbase_Rest_round['est_margin']/(
    waterfallbase_Rest_round['avgTotalsales_Pre_CY']-waterfallbase_Rest_round['AvgTotalCost_Pre_CY'])
waterfallbase_Rest_round['EstPerSalesChange']=waterfallbase_Rest_round['est_sales'] / \
    waterfallbase_Rest_round['avgTotalsales_Pre_CY']

waterfallbase_Rest_round['RecPerMarginChange']=waterfallbase_Rest_round['rec_margin']/(
    waterfallbase_Rest_round['avgTotalsales_Pre_CY']-waterfallbase_Rest_round['AvgTotalCost_Pre_CY'])
waterfallbase_Rest_round['RecPerSalesChange']=waterfallbase_Rest_round['rec_sales'] / \
    waterfallbase_Rest_round['avgTotalsales_Pre_CY']

waterfallbase_Rest_round['EST_per_new_units']=1-(
    waterfallbase_Rest_round['est_units']/waterfallbase_Rest_round['AvgUnits_Pre_CY'])
waterfallbase_Rest_round['REC_per_new_units']=1-(
    waterfallbase_Rest_round['rec_Units']/waterfallbase_Rest_round['AvgUnits_Pre_CY'])


# /*** Check that everything ties out ****/

total1=waterfallbse_round.drop(['GuestCountMrgImpact', 'MenuChangeMrgImpact', 'MixMrgImpact', 'total2', 'total3', 'total4', 'total5', 'total6',
				'MI_SlsPrice', 'MI_Slscarryover_price_inc', 'MI_Slspromo', 'MI_Slsno_price_change',
				'PP_Mrgrec_items', 'PP_Mrgrec_items_pric_dev', 'PP_Mrgcarryover_price_inc', 'PP_Mrgpromo',
				'MI_MrgPrice', 'MI_Mrgcarryover_price_inc', 'MI_Mrgpromo', 'MI_Mrgno_price_change'
				], axis=1)

total2=waterfallbse_round.drop(['GuestCountSlsImpact', 'MenuChangeSlsImpact', 'MixSlsImpact', 'total1', 'total3', 'total4', 'total5', 'total6',
				'PP_Slsrec_items', 'PP_Slsrec_items_pric_dev', 'PP_Slscarryover_price_inc', 'PP_Slspromo',
				'MI_SlsPrice', 'MI_Slscarryover_price_inc', 'MI_Slspromo', 'MI_Slsno_price_change',
				'MI_MrgPrice', 'MI_Mrgcarryover_price_inc', 'MI_Mrgpromo', 'MI_Mrgno_price_change'
				], axis=1)

total3=waterfallbse_round.drop(['GuestCountMrgImpact', 'MenuChangeMrgImpact', 'MixMrgImpact', 'total1', 'total2', 'total4', 'total5', 'total6',
				'PP_Slsrec_items', 'PP_Slsrec_items_pric_dev', 'PP_Slscarryover_price_inc', 'PP_Slspromo',
				'PP_Mrgrec_items', 'PP_Mrgrec_items_pric_dev', 'PP_Mrgcarryover_price_inc', 'PP_Mrgpromo',
				'MI_MrgPrice', 'MI_Mrgcarryover_price_inc', 'MI_Mrgpromo', 'MI_Mrgno_price_change'
				], axis=1)

total4=waterfallbse_round.drop(['GuestCountSlsImpact', 'MenuChangeSlsImpact', 'MixSlsImpact', 'total1', 'total2', 'total3', 'total5', 'total6',
				'PP_Slsrec_items', 'PP_Slsrec_items_pric_dev', 'PP_Slscarryover_price_inc', 'PP_Slspromo',
				'MI_SlsPrice', 'MI_Slscarryover_price_inc', 'MI_Slspromo', 'MI_Slsno_price_change',
				'MI_MrgPrice', 'MI_Mrgcarryover_price_inc', 'MI_Mrgpromo', 'MI_Mrgno_price_change',
				'PP_Mrgrec_items', 'PP_Mrgrec_items_pric_dev', 'PP_Mrgcarryover_price_inc', 'PP_Mrgpromo'
				], axis=1)

total5=waterfallbse_round.drop(['GuestCountMrgImpact', 'MenuChangeMrgImpact', 'MixMrgImpact', 'total1', 'total2', 'total3', 'total4', 'total6',
				'MI_SlsPrice', 'MI_Slscarryover_price_inc', 'MI_Slspromo', 'MI_Slsno_price_change', 'FPCostChangeImpact',
				'MI_MrgPrice', 'MI_Mrgcarryover_price_inc', 'MI_Mrgpromo', 'MI_Mrgno_price_change',
				'PP_Mrgrec_items', 'PP_Mrgrec_items_pric_dev', 'PP_Mrgcarryover_price_inc', 'PP_Mrgpromo', 'PP_MrgPrice',
				'PP_Slsrec_items', 'PP_Mrgrec_items', 'PP_Slsrec_items_pric_dev', 'PP_Mrgrec_items_pric_dev',
				'MI_Slsrec_items', 'MI_Mrgrec_items', 'MI_Slsrec_items_pric_dev', 'MI_Mrgrec_items_pric_dev',
				'PP_SlsPrice', 'PP_Slscarryover_price_inc', 'PP_Slsprice_inc_outside_recs', 'PP_Slsprice_dec_outside_recs', 'PP_Slspromo',
				'MI_SlsPrice', 'MI_Slscarryover_price_inc', 'MI_Slsprice_inc_outside_recs', 'MI_Slsprice_dec_outside_recs', 'MI_Slspromo', 'MI_Slsno_price_change'
				], axis=1)

total6=waterfallbse_round.drop(['GuestCountSlsImpact', 'MenuChangeSlsImpact', 'MixSlsImpact', 'total1', 'total2', 'total3', 'total4', 'total5'
				'MI_SlsPrice', 'MI_Slscarryover_price_inc', 'MI_Slspromo', 'MI_Slsno_price_change',
				'MI_MrgPrice', 'MI_Mrgcarryover_price_inc', 'MI_Mrgpromo', 'MI_Mrgno_price_change', 'PP_MrgPrice',
				'PP_Mrgrec_items', 'PP_Mrgrec_items_pric_dev', 'PP_Mrgcarryover_price_inc', 'PP_Mrgpromo',
				'PP_Slsrec_items', 'PP_Mrgrec_items', 'PP_Slsrec_items_pric_dev', 'PP_Mrgrec_items_pric_dev',
				'MI_Slsrec_items', 'MI_Mrgrec_items', 'MI_Slsrec_items_pric_dev', 'MI_Mrgrec_items_pric_dev',
				'PP_SlsPrice', 'PP_Slscarryover_price_inc', 'PP_Slsprice_inc_outside_recs', 'PP_Slsprice_dec_outside_recs', 'PP_Slspromo',
				'MI_SlsPrice', 'MI_Slscarryover_price_inc', 'MI_Slsprice_inc_outside_recs', 'MI_Slsprice_dec_outside_recs', 'MI_Slspromo', 'MI_Slsno_price_change'
				], axis=1)

waterfallbse_round['PP_SlsPrice']=waterfallbse_round[[
    'PP_Slsrec_items', 'PP_Slsrec_items_pric_dev']].sum()
waterfallbse_round['PP_MrgPrice']=waterfallbse_round[[
    'PP_Mrgrec_items', 'PP_Mrgrec_items_pric_dev']].sum()
waterfallbse_round['MI_SlsPrice']=waterfallbse_round[[
    'MI_Slsrec_items', 'MI_Slsrec_items_pric_dev']].sum()
waterfallbse_round['MI_MrgPrice']=waterfallbse_round[[
    'MI_Mrgrec_items', 'MI_Mrgrec_items_pric_dev']].sum()

total1=total1[['PP_SlsPrice', 'PP_Slsprice_inc_outside_recs',
    'PP_Slsprice_dec_outside_recs', 'PP_Slscarryover_price_inc', 'PP_Slspromo']].sum()


waterfallbse_round[['GuestCountSlsImpact', 'MenuChangeSlsImpact', 'MixSlsImpact', 'PurePriceImpact',
		'GuestCountMrgImpact', 'MenuChangeMrgImpact', 'MixMrgImpact', 'FPCostChangeImpact'
	]].fillna(0)

    # total5=sum(GuestCountSlsImpact, MenuChangeSlsImpact, MixSlsImpact, PurePriceImpact);
	# if round(total5,.01) ~= round(SalesDelta,.01) then output total5;

	# total6=sum(GuestCountMrgImpact, MenuChangeMrgImpact, MixMrgImpact, PurePriceImpact, FPCostChangeImpact);
	# if round(total6,.01) ~= round(MrgDelta,.01) then output total6;


waterfallbse_round[['mcd_gbal_lcat_id_nu', 'delivery', 'sld_menu_itm_id', 'sld_menu_itm_na', 'ScrCategory',
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
			'PP_MrgPrice', 'PP_Mrgcarryover_price_inc', 'PP_Slsprice_dec_outside_recs', 'PP_Slscarryover_price_inc', 'PP_Mrgpromo', 'accepted', 'partial',
			'MI_MrgPrice', 'MI_Mrgcarryover_price_inc', 'MI_Slsprice_dec_outside_recs', 'MI_Slscarryover_price_inc', 'MI_Mrgpromo', 'MI_Mrgno_price_change',
			'MrgDelta', 'FPCostChangeImpact', 'GuestCountSlsImpact', 'MenuChangeSlsImpact', 'MixSlsImpact',
			'GuestCountMrgImpact', 'MenuChangeMrgImpact', 'MixMrgImpact',
			'total1', 'total2', 'total3', 'total4', 'total5', 'total6']]
