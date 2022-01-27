import pandas as pd
import numpy as np
import math


from MarketDimensions import *

elasfile=pd.DataFrame()
wkly_pmix_trans_item2=pd.DataFrame()
Cross_elst1=pd.merge(elasfile, wkly_pmix_trans_item2,how='left',left_on=['mcd_gbal_lcat_id_nu','cross_elastic_item'],right_on=['mcd_gbal_lcat_id_nu','sld_menu_itm_id'])
Cross_elst1.sort_values(by=['mcd_gbal_lcat_id_nu','sld_menu_itm_id','cross_elastic_item'])
print(Cross_elst1)

Cross_elst=Cross_elst1.rename(columns={'final_elasticity':'elasticity'})
def mainvalid(Cross_elst):
    if Cross_elst['sld_menu_itm_id'].first(1):      #Doubt in this statement 😑
        def valid1(Cross_elst):
            if (Cross_elst['pestore']==1) and (Cross_elst['elasticity'].isna()):
                    if (Cross_elst['newprice'].isna()) and (Cross_elst['oldPrice'].isna()):
                        return Cross_elst['elasticity']*(math.log(Cross_elst['newprice'])-math.log(Cross_elst['oldPrice']))
                    else:
                        return NULL

        def valid2(Cross_elst):
            if (Cross_elst['pestore']==1) and (Cross_elst['elasticity'].isna()):
                    if (Cross_elst['prcg_engn_curr_prc '].isna()) and (Cross_elst['rcom_prc'].isna()):
                        return Cross_elst['elasticity']*(math.log(Cross_elst['rcom_prc']-math.log(Cross_elst['prcg_engn_curr_prc'])))
                    else:
                        return NULL
    else:
        def valid1(Cross_elst):
            if (Cross_elst['pestore']==1) and (Cross_elst['elasticity'].isna()):
                    if (Cross_elst['newprice'].isna()) and (Cross_elst['oldPrice'].isna()):
                        return ( Cross_elst['elasticity']*(math.log(Cross_elst['newprice'])-math.log(Cross_elst['oldPrice']))).sum()
        def valid2(Cross_elst):
            if (Cross_elst['pestore']==1) and (Cross_elst['elasticity'].isna()):
                    if (Cross_elst['prcg_engn_curr_prc '].isna()) and (Cross_elst['rcom_prc'].isna()):
                         return (Cross_elst['elasticity']*(math.log(Cross_elst['rcom_prc'])-math.log(Cross_elst['prcg_engn_curr_prc']))).sum()


Cross_elst=Cross_elst.assign(Val1=Cross_elst.apply(valid1,axis=1),var2=Cross_elst.apply(valid2,axis=1))
if Cross_elst.groupby('sld_menu_itm_id').last():
    if Cross_elst['val'].isna():
        Cross_elst['elast_est']=np.exp(Cross_elst['val1'])
    if Cross_elst['val2'].isna():
        Cross_elst['elast_rec']=np.exp(Cross_elst['val2'])

selected_columns=['val1 ','val2']
Cross_elst=Cross_elst[selected_columns]


waterfallbse_elasticity=pd.merge(wkly_pmix_trans_item2, Cross_elst,how='left',on=['mcd_gbal_lcat_id_nu','sld_menu_itm_id'])
waterfallbse_elasticity.sort_values(by=['mcd_gbal_lcat_id_nu'])
print(waterfallbse_elasticity)

def loopoutA(level):
    PERest_level=pd.DataFrame()
    PERest_level=wkly_pmix_trans_item2[wkly_pmix_trans_item2['PEStore']==1]
    cnt=wkly_pmix_trans_item2.groupby('level')['mcd_gbal_lcat_id_nu'].unique().size()
    PERest_level['cnt' >1]
    print(PERest_level)

def loopgeo0():
	for i in range(lvlcnt):
		# set &mylib3..levels;                      ##  Doubt while iterate the series 😑
		# 	where obsnum=&count.;
		# 	call symput('geolvl', compress(geolevel));

    loopoutA(geolvl)

waterfallbse_elasticity.sort_values(by=['mcd_gbal_lcat_id_nu','delivery'])

#  Do store item/store calculations
gstcntsDlry=pd.DataFrame()

waterfallbse_round=pd.merge(waterfallbse_elasticity,gstcntsDlry,how='left',on=['mcd_gbal_lcat_id_nu','delivery'])

if waterfallbse_round['units_cy ']==NULL:
    removeflag=1
elif (waterfallbse_round['Sales_CY']/waterfallbse_round['units_cy '])<.05 and (waterfallbse_round['Sales_LY']/waterfallbse_round['units_ly ']) <.05:
    removeflag=1
elif( waterfallbse_round['Sales_CY']/waterfallbse_round['units_cy ']< .05) and waterfallbse_round['units_cy '].isin(0 ):
    removeflag=1
elif ( waterfallbse_round['Sales_LY'] < .05) and (waterfallbse_round['units_cy'].isin(0 )):
    removeflag =1
else:
    removeflag=0


waterfallbse_round[['avgunits_ly' ,'avgunits_cy', 'avgunits_Pre_ly', 'avgunits_Pre_cy', \
		'AvgSales_LY' ,'AvgSales_CY', 'AvgSales_pre_LY', 'AvgSales_Pre_CY', \
		'AvgCost_LY', 'AvgCost_CY', 'AvgCost_pre_LY', 'AvgCost_pre_CY', \
		'units_ly', 'units_cy', 'units_Pre_ly', 'units_Pre_cy', \
		'Sales_LY', 'Sales_CY', 'Sales_pre_LY', 'Sales_Pre_CY', \
		'Cost_LY', 'Cost_CY', 'Cost_pre_LY', 'Cost_pre_CY']].fillna(0)

#  fix for seasonal or not available Pre CY but available LY ***/


if (waterfallbse_round['avgNETprice_Pre_cy '].isna() ) and (waterfallbse_round[~waterfallbse_round['avgNETprice_ly'].isin([0,NULL])]):          #  Not in  filter ..check values by rows
    CurrentRoundChange=0
    CurrentRoundCarry=1
# if it was available last year, current year, and current year pre

elif (waterfallbse_round[waterfallbse_round['avgNETprice_cy ']!=np.NaN])and (waterfallbse_round[waterfallbse_round['avgNETprice_ly '] !=np.NaN])and (waterfallbse_round[waterfallbse_round['avgNETprice_Pre_cy']!=np.NaN]):
    if ((waterfallbse_round['avgNETprice_cy']-waterfallbse_round['avgNETprice_ly']).apply(np.ceil).abs())>=.01:
        CurrentRoundChange=(waterfallbse_round['avgNETprice_cy']-waterfallbse_round['avgNETprice_Pre_cy'])/(waterfallbse_round['avgNETprice_cy']-waterfallbse_round['avgNETprice_ly']).apply(np.ceil(5))
        CurrentRoundCarry=1-CurrentRoundChange
    elif (waterfallbse_round['avgNETprice_cy']-waterfallbse_round['avgNETprice_Pre_cy']).apply(np.ceil):
        CurrentRoundChange=1
        CurrentRoundCarry=0
    else:
        CurrentRoundChange=0
        CurrentRoundCarry=0
elif ((waterfallbse_round[waterfallbse_round['newprice']!=np.NaN]) and (waterfallbse_round[waterfallbse_round['AvgPrice_Pre_CY']!=np.NaN])) and ((waterfallbse_round[waterfallbse_round['newprice']-waterfallbse_round['AvgPrice_Pre_CY']]).apply(np.ceil).abs())>=0.1:
    CurrentRoundChange=0
    CurrentRoundCarry=0
else:
    CurrentRoundChange=0
    CurrentRoundCarry=0

if waterfallbse_round[~waterfallbse_round['AvgGuestCount_LY'].isin(0,np.NaN)]:
    GuestCountPerChange=(waterfallbse_round['AvgGuestCount_CY']/waterfallbse_round['AvgGuestCount_LY'])-1
    GuestCountMrgImpact=round((waterfallbse_round['AvgSales_LY']-waterfallbse_round['AvgCost_LY'])*GuestCountPerChange,.00001)

#  with GC impact removed
if (waterfallbse_round['units_ly ']>0) and (waterfallbse_round['units_cy']<=0):
    MenuChangeMrgImpact=round(((waterfallbse_round['avgunits_ly']*-1)*(waterfallbse_round['AvgNETPrice_ly'] and waterfallbse_round['AvgCostItem_ly']))-GuestCountMrgImpact,.0001)
    MenuChangeSlsImpact=round(((waterfallbse_round['avgunits_ly']*-1)*(waterfallbse_round['AvgNETPrice_ly'] ))-GuestCountSlsImpact,.0001)
elif waterfallbse_round[waterfallbse_round['units_ly']<=0] and waterfallbse_round[waterfallbse_round['units_cy']>0]:
    MenuChangeMrgImpact=round((waterfallbse_round['avgunits_cy']*(waterfallbse_round['AvgNETPrice_cy']-waterfallbse_elasticity['AvgCostItem_cy'])-GuestCountMrgImpact),.0001)
    MenuChangeSlsImpact=round((waterfallbse_round['avgunits_cy']*(waterfallbse_round['AvgNETPrice_cy'])-GuestCountSlsImpact),.0001)

FPCostChangeImpact=round( (waterfallbse_round['AvgCostItem_ly']-waterfallbse_round['AvgCostItem_cy'] )*waterfallbse_round['avgunits_ly'],0.0001)

if round((waterfallbse_round['AvgNETPrice_cy']-waterfallbse_round['AvgNETPrice_ly']).abs(),.01)>=.01:
    PurePriceImpact=round(round(waterfallbse_round['AvgNETPrice_cy']-waterfallbse_round['AvgNETPrice_ly'],4)*waterfallbse_round['avgunits_ly'],4)

#  added as part of Price taken Calculation

if round(waterfallbse_round['avgMenuprice_CY ']-waterfallbse_round['avgMenuprice_LY'],4) >=.01:
    PurePriceImpactMB=round(round(waterfallbse_round['AvgNETPrice_cY']-waterfallbse_round['AvgNETPrice_Ly'],4)*waterfallbse_round['avgunits_ly'],4)

waterfallbse_round[['PurePriceImpact',
                    'MenuChangeSlsImpact',
                     'GuestCountSlsImpact',
		                'MenuChangeMrgImpact',
                         'GuestCountMrgImpact',
                          'FPCostChangeImpact'
		            ]].fillna(0)                                            # confirm here from Bharath Sir😶😶

arr2=[PurePriceImpact,
                    MenuChangeSlsImpact,
                     GuestCountSlsImpact,
		                MenuChangeMrgImpact,
                         GuestCountMrgImpact,
                        FPCostChangeImpact]

for i in arr2:
    if i==0:
        i=np.NaN

#  residual

SalesDelta=round(waterfallbse_round['AvgSales_CY']-waterfallbse_round['AvgSales_LY'],4)
MrgDelta=round((waterfallbse_round['AvgSales_CY']-waterfallbse_round['AvgCost_CY'])-(waterfallbse_round['AvgSales_LY']-waterfallbse_round['AvgCost_LY']),4)
MixMrgImpact=MrgDelta-(PurePriceImpact,
                        MenuChangeSlsImpact,
                        GuestCountSlsImpact,
		                MenuChangeMrgImpact,
                        GuestCountMrgImpact,
                        FPCostChangeImpact).sum(axis=1)

         # or----
mix_col=[PurePriceImpact,
        MenuChangeSlsImpact,
        GuestCountSlsImpact,
        MenuChangeMrgImpact,
        GuestCountMrgImpact,
        FPCostChangeImpact]

MixMrgImpact=MrgDelta-waterfallbse_round[mix_col].sum(axis=1)


arr3=[PurePriceImpact,
                    MenuChangeSlsImpact,
                    GuestCountSlsImpact,
                    MixMrgImpact,                       # confim once its series or variables (for preprocess)
		            MenuChangeMrgImpact,
                    GuestCountMrgImpact,
                    FPCostChangeImpact,
                    MixSlsImpact]

for i in arr3:
    if i==0:
        i=np.NaN



if round(waterfallbse_round['avgNETprice_cy']	-waterfallbse_round['avgNETprice_ly'],2)>.01:
    if waterfallbse_round[waterfallbse_round['promo'] !=1] :
        if waterfallbse_round['accepted']==1:
            PP_Mrgrec_items= PurePriceImpact*CurrentRoundChange
            PP_Slsrec_items=PurePriceImpact*CurrentRoundChange
            MI_Mrgrec_items=MixMrgImpact*CurrentRoundChange                 # # confim once its series or variables
            MI_Slsrec_items=MixSlsImpact*CurrentRoundChange

        elif waterfallbse_round[waterfallbse_round['partial']==1]:
            PP_Mrgrec_items_pric_dev=PurePriceImpact*CurrentRoundChange
            PP_Slsrec_items_pric_dev=PurePriceImpact*CurrentRoundChange
            MI_Mrgrec_items_pric_dev=MixMrgImpact*CurrentRoundChange
            MI_Slsrec_items_pric_dev=MixSlsImpact*CurrentRoundChange

        elif round(waterfallbse_round['avgNETprice_cy']-waterfallbse_round['avgNETprice_ly'],2)>=0.1:
            PP_Mrgprice_inc_outside_recs=PurePriceImpact*CurrentRoundChange
            PP_Slsprice_inc_outside_recs=PurePriceImpact*CurrentRoundChange
            MI_Mrgprice_inc_outside_recs=MixMrgImpact*CurrentRoundChange
            MI_Slsprice_inc_outside_recs=MixSlsImpact*CurrentRoundChange

        else:
            PP_Mrgprice_dec_outside_recs=PurePriceImpact*CurrentRoundChange
            PP_Slsprice_dec_outside_recs=PurePriceImpact*CurrentRoundChange
            MI_Mrgprice_dec_outside_recs=MixMrgImpact*CurrentRoundChange
            MI_Slsprice_dec_outside_recs=MixSlsImpact*CurrentRoundChange

            PP_Mrgcarryover_price_inc=PurePriceImpact*CurrentRoundCarry
            PP_Slscarryover_price_inc=PurePriceImpact*CurrentRoundCarry
            MI_Mrgcarryover_price_inc=MixMrgImpact*CurrentRoundCarry
            MI_Slscarryover_price_inc=MixSlsImpact*CurrentRoundCarry

    elif waterfallbse_round[ waterfallbse_round['promo']==1]:
        PP_Mrgpromo=PurePriceImpact
        PP_Slspromo=PurePriceImpact
        MI_Mrgpromo=MixMrgImpact
        MI_Slspromo=MixSlsImpact

else:
    MI_Mrgno_price_change=MixMrgImpact
    MI_Slsno_price_change=MixSlsImpact

measured_marginRecs=PP_Mrgrec_items+ \
                    PP_Mrgrec_items_pric_dev+ \
                    MI_Mrgrec_items+    \
                    MI_Mrgrec_items_pric_dev

measured_salesRecs= PP_Slsrec_items+ \
                    PP_Slsrec_items_pric_dev+ \
                    MI_Slsrec_items+\
                    MI_Slsrec_items_pric_dev

                    #confim once above statement for COVIDFIX_PE

measured_margin=PP_Mrgrec_items+\
                PP_Mrgrec_items_pric_dev+ \
                    MI_Mrgrec_items+ \
                    MI_Mrgrec_items_pric_dev+ \
                        PP_Mrgprice_inc_outside_recs+\
                            MI_Mrgprice_inc_outside_recs+\
                                PP_Mrgprice_dec_outside_recs+\
                                    MI_Mrgprice_dec_outside_recs


measured_sales=PP_Slsrec_items+\
                PP_Slsrec_items_pric_dev+\
                    MI_Slsrec_items+\
                        MI_Slsrec_items_pric_dev+\
                            PP_Slsprice_inc_outside_recs+\
                                MI_Slsprice_inc_outside_recs+\
                                    PP_Slsprice_dec_outside_recs+\
                                        MI_Slsprice_dec_outside_recs

if waterfallbse_round[waterfallbse_round['rcom_prc ']!=np.NaN]:
    Non_Rec_Price=waterfallbse_round['avgprice_pre_cy']
else:
    Non_Rec_Price=waterfallbse_round['avgprice_cy']

if waterfallbse_round[waterfallbse_round['pestore']==1]:
    if (waterfallbse_round[waterfallbse_round['elast_est ']!=np.NaN]) and (waterfallbse_round[waterfallbse_round['promo']!=1]) and (waterfallbse_round[waterfallbse_round['accepted']==1 |waterfallbse_round['partial']==1] ):
        #   /****** Estimated Metrics ********/
        est_units=waterfallbse_round['avgunits_pre_cy']*waterfallbse_round['elast_est']
        est_Units_full= waterfallbse_round['units_pre_cy']* waterfallbse_round['elast_est']
        est_margin=(waterfallbse_round['est_Units'] *(waterfallbse_round['avgNETprice_cy'] - waterfallbse_round['AvgCostItem_pre_cy'] ))-(waterfallbse_round['avgunits_pre_cy'] *( waterfallbse_round['avgNETprice_pre_cy'] -waterfallbse_round['AvgCostItem_pre_cy'] ))
        low_est_margin=(1-file['range'])*waterfallbse_round['est_margin']
        up_est_margin=(1+file['ragne'])*waterfallbse_round['est_margin']

        Est_sales=(waterfallbse_round['est_Units']  * waterfallbse_round['avgNETprice_cy'] )-(waterfallbse_round['avgunits_pre_cy'] *(waterfallbse_round['avgNETprice_pre_cy'] ))

        low_est_sales=(1-file['range'])* waterfallbse_round['Est_sales']
        up_est_sales=(1+file['range'])* waterfallbse_round['Est_sales']

        act_Units=waterfallbse_round['AvgUnits_CY']
        act_Sales=waterfallbse_round['AvgSales_CY']
        act_Margin=(waterfallbse_round['AvgSales_CY'] - waterfallbse_round['AvgCost_CY'] )
        pre_act_Units= waterfallbse_round['AvgUnits_Pre_CY']
        pre_act_Sales=waterfallbse_round['AvgSales_Pre_CY']
        pre_act_Margin=( waterfallbse_round['AvgSales_pre_CY'] - waterfallbse_round['AvgCost_pre_CY']  )

    if waterfallbse_round[waterfallbse_round['elast_rec ']!=np.Nan] and waterfallbse_round[waterfallbse_round['prcg_engn_curr_net_prc ']!=np.NaN] and waterfallbse_round[waterfallbse_round['rcom_net_prc ']!=np.NaN] :
        #  Recommended Metrics
        rec_Units=avgunits_pre_cy*elast_rec
        rec_Units_full=units_pre_cy*elast_rec
        rec_margin=(rec_Units*(rcom_net_prc-AvgCostItem_pre_cy))-(avgunits_pre_cy*(prcg_engn_curr_net_prc-AvgCostItem_pre_cy));
        low_rec_margin=(1-file['range'])*rec_margin
        up_rec_margin=(1+file['range'])*rec_margin

        rec_sales=(rec_Units*(rcom_net_prc))-(avgunits_pre_cy*(prcg_engn_curr_net_prc));
        low_rec_sales=(1-file['range'])*Rec_sales
        up_rec_sales=(1+file['range'])*rec_sales

    if waterfallbse_round[waterfallbse_round['rcom_prc ']!=np.NaN]:
        recflag=1
        if waterfallbse_round['accepted']==1 | waterfallbse_round['partial']==1 :
            acptflag=1
        else:
            acptflag=0
        if waterfallbse_round['accepted']==np.NaN:
            accepted=0
        if waterfallbse_round['partial']==np.NaN:
            accepted=0
        if waterfallbse_round['newprice']==np.Nan and waterfallbse_round['oldPrice'].isin(np.NaN,0) and waterfallbse_round['partial'].isin(np.NaN,0):
            outsiderec=1
    else:
        if waterfallbse_round[waterfallbse_round['newprice'] !=waterfallbse_round['oldPrice']]:
            addflag=1
        else:
            addflag=0
        recflag=0
        outsiderec=0

 #------ waterfallbse complete --
if waterfallbse_round[waterfallbse_round['rcom_prc']!=np.NaN]:
    Non_Rec_Price2=avgprice_pre_cy
elif waterfallbse_round[waterfallbse_round['addflag']==1]:
    Non_Rec_Price2=avgprice_pre_cy
if waterfallbse_round[waterfallbse_round['avgMenuprice_CY ']!=np.NaN]:
    MENU_CY_Flag=1
else:
    MENU_CY_Flag=0
if waterfallbse_round[waterfallbse_round['avgMenuprice_LY '] !=np.NaN]:
    MENU_LY_Flag=1
else:
    MENU_LY_Flag=0
if waterfallbse_round[waterfallbse_round['avgMenuprice_Pre_CY '] !=np.NaN]:
    MENU_Pre_CY_Flag=1
else:
    MENU_Pre_CY_Flag=0
if waterfallbse_round[waterfallbse_round['avgMenuprice_Pre_LY '] !=np.NaN]:
    MENU_Pre_LY_Flag=1
else:
    MENU_Pre_LY_Flag=0
if waterfallbse_round[waterfallbse_round['avgprice_LY '] != np.NaN]:
    LY_Flag=1
else:
    LY_Flag=0
if waterfallbse_round[waterfallbse_round['avgprice_Pre_LY ']!=np.NaN]:
    Pre_LY_Flag=1
else:
    Pre_LY_Flag=0
if waterfallbse_round[waterfallbse_round['avgprice_Pre_CY ']!=np.Nan]:
    Pre_CY_Flag=1
else:
    Pre_CY_Flag=0
if waterfallbse_round[waterfallbse_round['avgprice_CY ']!=np.NaN]:
    CY_Flag=1
else:
    CY_Flag=0

waterfallbse_round.sort_values(by=['LVLLIST2','mcd_gbal_lcat_id_nu ','store_nu','delivery ','sld_menu_itm_id'])

#  Added below WAP calculation for GC Estimation calculation

wap_cal_base=pd.DataFrame()

wap_cal_base=waterfallbse_round[['LVLLIST2','mcd_gbal_lcat_id_nu ','units_pre_cy ','units_ly ','oldprice','newprice','prcg_engn_curr_prc ','rcom_prc ']]


wap_cal_base['newprice']=wap_cal_base['newprice'].apply(lambda x: AvgMenuPrice_CY if x==np.NaN else x)


wap_cal_base['oldprice ']=wap_cal_base['oldprice '].apply(lambda x: AvgMenuPrice_Pre_CY if x==np.NaN else x)


wap_cal_base['rcom_prc  ']=wap_cal_base['rcom_prc  '].apply(lambda x: AvgMenuPrice_CY if x==np.NaN else x)


wap_cal_base['prcg_engn_curr_prc ']=wap_cal_base['prcg_engn_curr_prc '].apply(lambda x: AvgMenuPrice_Pre_CY if x==np.NaN else x)

wap_cal_base.groupby(['LVLLISTID','mcd_gbal_lcat_id_nu'])

wap_cal_base[wap_cal_base['newprice '].notna() & wap_cal_base['oldprice '].notna() & wap_cal_base['pestore']==1]

wap_cal_base








# added for implementation report GC from price for all price changes ***/

#/*** average measured GC for recommended price change items ***/














































