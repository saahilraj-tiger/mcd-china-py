import pandas as pd
import numpy as np

from param_file import*
from MarketDimensions import lvlcnt, lvllist, fieldlth, lvllistid, lvllistlast1, lvllistlast2
from Getdata_market import regions, PEstore, elasfile, iteminfo
from mcrGetData import price_recs_round, PriceSnap

with open(r'C:\Users\gopala.chennu\Desktop\Current\vs\Codes\master.yml', 'r') as doc:
    file = yaml.load(doc, Loader=yaml.FullLoader)
mylib1 = file['00_ParamFile_PE']['params_file']['mylib1']

trans = pd.DataFrame(f"{lisa}")

wkly_pmix_trans_item = (
    pd.merge(price_recs_round, trans, how='left', on=['mcd_gbal_lcat_id_nu', 'sld_menu_itm_id']) \
    .merge(regions, how='left', on=['mcd_gbal_lcat_id_nu'])
    .merge(PEstore, how='left', on=['mcd_gbal_lcat_id_nu']) \
    .merge(elasfile, how='left', left_on=['mcd_gbal_lcat_id_nu', 'sld_menu_itm_id', 'sld_menu_itm_id'],
                        right_on=['mcd_gbal_lcat_id_nu', 'sld_menu_itm_id', 'cross_elastic_item']) \
    .merge(iteminfo, how='left', left_on=['sld_menu_itm_id'], right_on=['sld_menu_itm_id',['ScrCategory'] != 'Exclude']) \
    .merge(PriceSnap, how='left', on=['mcd_gbal_lcat_id_nu', 'sld_menu_itm_id'])
)


print(wkly_pmix_trans_item)

wkly_pmix_trans_item =(
    wkly_pmix_trans_item[(
        (wkly_pmix_trans_item['units_ly'] > file['unitcri']) | (wkly_pmix_trans_item['units_pre_cy'] > unitcri)
    )
    &((wkly_pmix_trans_item['sales_ly'] > salecri) | (wkly_pmix_trans_item['sales_pre_cy'] < salecri))]

)


wkly_pmix_trans_item.sort_values(
    by=['LVLLIST', 'mcd_gbal_lcat_id_nu', 'store_nu', 'sld_menu_itm_id'])

wkly_pmix_trans_item =(
    wkly_pmix_trans_item[['prcg_engn_curr_prc',
    'rcom_prc',
    'prcg_engn_curr_net_prc',
    'rcom_net_prc',
    'final_elasticity',
    'accepted',
    'partial',
    'frnt_cter_itm_prc_am',
    'oldPrice',
    'increase',
    'decrease',
    'PEStore',
    'sld_menu_itm_na',
    'newcategory',
    'Product_Family',
    'Product_Offered_As',
    'Price_Tier_Strategy',
    'rank']]

)
print(wkly_pmix_trans_item)


# /*** is the item/store considered promo? ***/
wkly_pmix_trans_item2 = wkly_pmix_trans_item
wkly_pmix_trans_item['CY_MB_Delta'] = (
    wkly_pmix_trans_item[wkly_pmix_trans_item['avgMenuprice_CY'] -
    wkly_pmix_trans_item['avgMenuprice_cy']].apply(np.ceil)
)


wkly_pmix_trans_item['PreCY_MB_Delta'] = (
     wkly_pmix_trans_item[wkly_pmix_trans_item['(avgMenuprice_Pre_CY'] -
     wkly_pmix_trans_item['(avgMenuprice_Pre_cy']].apply(np.ceil)

)
wkly_pmix_trans_item['LY_MB_Delta'] =(
     wkly_pmix_trans_item[wkly_pmix_trans_item['((avgMenuprice_LY'] -
     wkly_pmix_trans_item['(avgprice_cy']].apply(np.ceil)

)

wkly_pmix_trans_item.loc[wkly_pmix_trans_item['CY_MB_Delta']> file['promoamt'], 'otherpromo'] = 1
wkly_pmix_trans_item.loc[~wkly_pmix_trans_item['CY_MB_Delta']> file['promoamt'], 'otherpromo'] = 0

wkly_pmix_trans_item.loc[wkly_pmix_trans_item['otherpromo'].sum()>= 1, 'promo'] = 1
wkly_pmix_trans_item.loc[~wkly_pmix_trans_item['otherpromo'].sum() >= 1, 'promo'] = 0

wkly_pmix_trans_item.loc[wkly_pmix_trans_item['LY_MB_Delta']> file['promoamt'], 'otherpromo_LY'] = 1
wkly_pmix_trans_item.loc[~wkly_pmix_trans_item['LY_MB_Delta']> file['promoamt'], 'otherpromo_LY'] = 0

wkly_pmix_trans_item.loc[wkly_pmix_trans_item[['otherpromo', 'otherpromo_LY']].sum() >= 1, 'promo2'] = 1
wkly_pmix_trans_item.loc[~wkly_pmix_trans_item[['otherpromo', 'otherpromo_LY']].sum() >= 1, 'promo2'] = 1


# /*************************************************************************************/
# /**** END CUSTOM PROMO LOGIC ****/
