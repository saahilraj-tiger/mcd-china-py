import os
import pandas as pd
import numpy as np
from param_file import *
with open(r'C:\Users\gopala.chennu\Desktop\Current\vs\Codes\master.yml', 'r') as doc:
    file = yaml.load(doc, Loader=yaml.FullLoader)

envir = file['envir']
cntry = file['cntry']
mylib1 = file['00_ParamFile_PE']['params_file']['mylib1']
mylib4 = file['00_ParamFile_PE']['params_file']['mylib4']
mylib3 = file['00_ParamFile_PE']['params_file']['mylib3']

advrdata = f"/opt/sasdata/{envir}/Data/{cntry}/AdvisorDashboards/data/"

ZoneXField = pd.read_sas(f"{mnthdata}ZoneXField", encoding='ISO-8859-1')

store_info_sociodemographic_data = pd.read_sas(
    f"{advrdata}store_info_sociodemographic_data", encoding='ISO-8859-1')

elasticity_for_optimization = pd.read_sas(
    f"{advrdata}elasticity_for_optimization", encoding='ISO-8859-1')

item_Sensitivity = pd.read_sas(
    f"{advrdata}item_Sensitivity", encoding='ISO-8859-1')

Store_table_all_items = pd.read_sas(
    f"{advrdata}store_table_all_items", encoding='ISO-8859-1')

store_item_table = pd.read_sas(f"{advrdata}store_item", encoding='ISO-8859-1')


def venloop2():
    if file['Vendor'].upper() == "DELOITTE" or file['Vendor'].upper() == "COMBINED":
        newfile1 = pd.read_sas(f"{dellib}newfile1", encoding='ISO-8859-1')
        newfile1.rename(columns={'mcd_glbl_loc_id': 'mcd_gbal_lcat_id_nu'})
        newfile1['cross_elastic_item'] = newfile1['sld_menu_itm_id']

        gcpric1 = pd.read_sas(f"{dellib}newfile1", encoding='ISO-8859-1')
        gcpric1.rename(columns={'mcd_glbl_loc_id': 'mcd_gbal_lcat_id_nu'})
        return newfile1, gcpric1


newfile1, gcpric1 = venloop2()


def TAFiles():

    newfile2 = pd.read_sas(f"{engine}newfile2")

    newfile2 = newfile2[((newfile2['sld_menu_itm_id']) == newfile2['Independent_item'] & (
        newfile2['sld_menu_itm_id'] == newfile2[prcg_engn_curr_prc])) != newfile2['rcom_prc']]
    newfile2 = newfile2[['sld_menu_itm_id', 'mcd_gbal_lcat_id_nu', 'prcg_engn_curr_prc', 'rcom_prc',
                         'frm_engn_rcom_flg', 'spr_de_elstc_coef', 'prcg_engn_curr_net_prc', 'rcom_net_prc']]
    newfile2['prcg_engn_curr_net_prc'] = newfile2['prcg_engn_curr_net_prc'].apply(
        np.ceil)
    newfile2['rcom_net_prc'] = newfile2['rcom_net_prc'].apply(np.ceil)

    gcelas2 = pd.read_sas(f"{engine}{file['gcpric2']}")
    merge_data = pd.merge(newfile2, gcelas2, how='left',
                          on='mcd_gbal_lcat_id_nu')
    merge_data = merge_data[merge_data['sld_menu_itm_id'] == Independent_item]
    merge_data = merge_data[['mcd_gbal_lcat_id_nu',
                             'sr_gc_elstc_coef', 'rsq_prc']]
    merge_data.rename(
        columns={'sr_gc_elstc_coef': 'gc_elastic', 'rsq_prc': 'InfluencePrcGC'})

    elasfile2 = pd.read_sas(f"{engine}{file['newfile2']}")

    elasfile2 = elasfile2[elasfile2['prcg_engn_curr_prc']
                          != elasfile2['rcom_prc']]
    elasfile2[['mcd_gbal_lcat_id_nu', 'sld_menu_itm_id',
               'spr_de_elstc_coef', 'Independent_item']]
    elasfile2.rename(columns={
                     'spr_de_elstc_coef': 'final_elasticity', 'Independent_item': 'cross_elastic_item'})
    return recfile2, gcelas2, elasfile2


recfile2, gcelas2, elasfile2 = TAFiles()


def DltFiles():
    recfile1 = pd.DataFrame(mylib4.file['newfile1'])
    prcg_engn_curr_net_prc = recfile1['prcg_engn_curr_prc']/(1+file['tax'])

    gcpric1 = pd.DataFrame(mylib4.file['gcpric1'])
    newfile1 = pd.DataFrame(mylib4.file['newfile1'])
    gcelas1 = pd.merge(newfile1, gcelas1, how='left', on='mcd_gbal_lcat_id_nu')
    gcelas1.rename(
        columns={'sr_gc_elstc_coef': 'gc_elastic', 'rsq_prc': 'InfluencePrcGC'})
    gcelas1.drop_duplicates(
        subset=['mcd_gbal_lcat_id_nu', 'gc_elastic', 'InfluencePrcGC'], keep='first')
    print(gcelas1)

    elasfile1 = pd.DataFrame(mylib4.file['newfile1'])
    elasfile1.rename(columns={'spr_de_elstc_coef': 'final_elasticity'})
    elasfile1[['mcd_gbal_lcat_id_nu', 'sld_menu_itm_id',
               'final_elasticity', 'cross_elastic_item']]
    return recfile2, gcelas2, elasfile2


recfile2, gcelas2, elasfile2 = DltFiles()


def combineloop():

    if Vendor.upper() == "COMBINED":

        recfile1 = pd.read_sas(
            f'{engine}{file["recfile1"]}', encoding='ISO-8859-1')

        recfile1.sort_values(by=[" mcd_gbal_lcat_id_nu", "sld_menu_itm_id"])

        recfile2 = pd.read_sas(f'{engine}recfile2', encoding='ISO-8859-1')

        recfile2_df.sort_values([" mcd_gbal_lcat_id_nu", "sld_menu_itm_id"])

        # if a and not b; (Merge)
        recfile3 = pd.merge(recfile1, recfile2,
                            on="mcd_gbal_lcat_id_nu", how="left")

        recfile = recfile3.append(recfile2_df)

        gcelas1 = pd.read_sas(f"{engine}{file['gcelas1']}",
                              format='sas7bdat', encoding='utf-8')

        gcelas1.sort_values(by="mcd_gbal_lcat_id_nu")

        gcelas2 = pd.read_sas(f"{engine}{file['gcelas2']}",
                              format='sas7bdat', encoding='utf-8')

        gcelas2.sort_values(by="mcd_gbal_lcat_id_nu")

        # if a and not b;
        gcelas3 = pd.merge(
            gcelas1, gcelas2, on="mcd_gbal_lcat_id_nu", how="left")

        gcelas = gcelas3.append(gcelas2)

        elasfile1_sort = pd.read_sas(
            f"{engine}{file['elasfile1']}", format="sas7bdat", encoding='utf-8')
        elasfile1_sort_df = pd.DataFrame(elasfile1_sort)
        elasfile1_sort_df.sort_values(
            by=["mcd_gbal_lcat_id_nu", "sld_menu_itm_id", "cross_elastic_item"])

        elasfile2_sort = pd.read_sas(
            f"{engine}{file['elasfile2']}", format="sas7dbat", encoding='utf-8')

        elasfile2_sort_df.sort_values(
            by=["mcd_gbal_lcat_id_nu", "sld_menu_itm_id", "cross_elastic_item"])

        elasfile3_merge = pd.merge(elasfile1_sort_df, elasfile2_sort_df,
                                   on="mcd_gbal_lcat_id_nu", how=" ")  # if a and not b; (Merge)

        elasfile = elasfile3_merge.append(elasfile2_sort_df)
        return recfile, gcelas, elasfile

    if Vendor.upper == "TIGER":
        recfile = pd.read_sas(
            f"{engine}{file['recfile2']}", format='sas7dbat', encoding='utd-8')
        gcelas = pd.read_sas(
            f"{engine}{gcelas2}", format='sas7dbat', encoding='utd-8')
        elasfile = pd.read_sas(f"{engine}{file['elasfile2']}",
                               format='sas7dbat', encoding='utd-8')
        return recfile, gcelas, elasfile

    if Vendor.upper == "DELOITTE":
        recfile = pd.read_sas(
            f"{engine}{file['recfile1']}", format='sas7dbat', encoding='utd-8')
        gcelas = pd.read_sas(
            f"{engine}{file['gcelas1']}", format='sas7dbat', encoding='utd-8')
        elasfile = pd.read_sas(f"{engine}{file['elasfile1']}",
                               format='sas7dbat', encoding='utd-8')
        return recfile, gcelas, elasfile


recfile, gcelas, elasfile = combineloop(Vendor)

price_recs_round = f'{inter1}'
recfile.to_csv(price_recs_round, f'price_recs_round{file["round"]}')

Elasticity_round = f'{inter1}'
elasfile.to_csv(Elasticity_round, f'Elasticity_round{file["round"]}')

GCElasticity_round = f'{inter1}'
gcelas.to_csv(GCElasticity_round, f'GCElasticity_round{file["round"]}')

elasfile = f"{inter1}{Elasticity_round}"+file['round']


# gbal_lcat = pd.DataFrame(rmdw_tables.gbal_lcat)
# dmsite_ops_hrcy = pd.DataFrame(MSTR_VM.dmsite_ops_hrcy)
# regions1 = pd.merge(gbal_lcat, dmsite_ops_hrcy, how='left',
#                     on=['mcd_gbal_lcat_id_nu', 'ctry_iso_nu'])
# regions1 = regions1[(regions1['ctry_iso_nu'] == terr)
#                     & (regions1.isin([1, 2]))]
# regions1.sort_values(by=['mcd_gbal_lcat_id_nu'])
# regions1.rename(columns={'Italy': 'market'})
# regions1 = regions1[['mcd_gbal_lcat_id_nu',
#                      'rest_owsh_typ_id', 'market', 'ops_lvl3_na']]
# print(regions1)

region1 = pd.read_excel(
    r'C:\Users\gopala.chennu\Desktop\Current\vs\Codes\getdata_op.xlsx')  # sent by barath


store_info_sociodemographic_data = pd.DataFrame(
    mylib4.store_info_sociodemographic_data)
ZoneXField = pd.read_sas(f"{engine}{ZoneXField}")
regions2 = pd.merge(regions1, store_info_sociodemographic_data, how='left', left_on='mcd_gbal_lcat_id_nu',
                    right_on='global_id').merge(ZoneXField, how='left', left_on='region', right_on='field')
regions2.sort_values(by='mcd_gbal_lcat_id_nu')
regions2.rename(columns={'region': 'field'})
regions2[['mcd_gbal_lcat_id_nu', 'rest_owsh_typ_id',
          'ops_lvl3_na', 'market', 'zone', 'field']]
print(regions2)

regions = regions2
regions['market'] = 'Italy'
regions.zone.replace(to_value=' ', value='Unknown')
regions.field.replace(to_value=' ', value='Unknown')
regions = regions[['market', 'zone', 'field']]
print(regions)

ItemsNames_path = f"{lisa}"
menu_items = pd.read_sas(f"{lookup}menu_items")
menu_items = menu_items[['sld_menu_itm_id', 'SLD_MENU_ITM_NA']]

#menu_items.to_csv(ItemsNames_path, 'ItemsNames.csv')
ItemsNames = pd.read_csv(f"{lisa}{ItemsNames}.csv")
ItemsNames.drop_duplicates(subset='sld_menu_itm_id')


menu_items = pd.read_sas(f"{lookup}menu_items")
Product_Hierarchy = pd.read_sas(f"{lookup}Product_Hierarchy.sasb")
Product_Characteristics = pd.read_sas(f"{lookup}Product_Characteristics")
iteminfo = pd.merge(menu_items, Product_Hierarchy, how='left', on='sld_menu_itm_id').merge(
    Product_Hierarchy, how='left', on='sld_menu_itm_id')
print(iteminfo)

itemInfo.loc[itemInfo['itm_hier_lvl3'].isin(['Not Used', 'Other No Food']), [
    'ScrCategory', 'exclude']] = ['Exclude', 1]

itemInfo.loc[itemInfo['sld_menu_itm_na '].isin(['Unknown', 'delete']), [
    'ScrCategory', 'exclude']] = ['Exclude', '1']

itemInfo.loc[~itemInfo['sld_menu_itm_na '].isin(['Unknown', 'delete', 'Not Used', 'Other No Food']), [
    'ScrCategory', 'exclude']] = ['Exclude', 0]

rank = 1

itemInfo.loc[itemInfo['ITM_HIER_LVL3'] ==
             'Drinks', 'newCategory'] = 'BEVERAGES'

itemInfo.loc[itemInfo['ITM_HIER_LVL3'] ==
             'ALC Entrees', 'newCategory'] = 'ENTREES'

itemInfo.loc[itemInfo['ITM_HIER_LVL3'] ==
             'Breakfast', 'newCategory'] = 'BKFST ENTREES'

itemInfo.loc[itemInfo['ITM_HIER_LVL3'].isin(
    ['EVM Large', 'EVM Medium', 'Easy Menu']), 'newCategory'] = 'COMBO MEALS'

itemInfo.loc[(itemInfo['ITM_HIER_LVL3'] == 'Salads') & (
    itemInfo['product_offered_as'] == 'EVM'), 'newCategory'] = 'COMBO MEALS'

itemInfo.loc[(itemInfo['ITM_HIER_LVL3'] == 'Salads') & (
    itemInfo['product_offered_as'] == 'A la Carte'), 'newCategory'] = 'ENTREES'

itemInfo.loc[(itemInfo['ITM_HIER_LVL3'] == 'Salads') & (
    itemInfo['product_offered_as'] == 'Part of EVM - Side'), 'newCategory'] = 'SIDE ITEMS'

itemInfo.loc[itemInfo['ITM_HIER_LVL3'] ==
             'Desserts', 'newCategory'] = 'DESSERTS'

itemInfo.loc[itemInfo['ITM_HIER_LVL3'] ==
             'Happy Meal', 'newCategory'] = 'HAPPY MEALS'

itemInfo.loc[itemInfo['ITM_HIER_LVL3'] == 'McCafe', 'newCategory'] = 'MCCAFE'

itemInfo.loc[itemInfo['ITM_HIER_LVL3'] ==
             'Fries', 'newCategory'] = 'SIDE ITEMS'

itemInfo.loc[~itemInfo['ITM_HIER_LVL3'] == ['Drinks', 'ALC Entrees', 'Breakfast', 'EVM Large', 'EVM Medium',
                                            'Easy Menu', 'Salads', 'Desserts', 'Happy Meal', 'McCafe', 'Fries'], 'newCategory'] = 'OTHER'

itemInfo.loc[itemInfo['ScrCategory '] == 'Exclude', 'newCategory'] = 'Exclude'

itemInfo.loc[itemInfo['newCategory'] == 'ENTREES', 'rank'] = 1

itemInfo.loc[itemInfo['newCategory'] == 'BKFST ENTREES', 'rank'] = 2

itemInfo.loc[itemInfo['newCategory'] == 'COMBO MEALS', 'rank'] = 3

itemInfo.loc[itemInfo['newCategory'] == 'BEVERAGES', 'rank'] = 4

itemInfo.loc[itemInfo['newCategory'] == 'SIDE ITEMS', 'rank'] = 5

itemInfo.loc[itemInfo['newCategory'] == 'HAPPY MEALS', 'rank'] = 6

itemInfo.loc[itemInfo['newCategory'] == 'MCCAFE', 'rank'] = 7

itemInfo.loc[itemInfo['newCategory'] == 'DESSERTS', 'rank'] = 8

itemInfo.loc[itemInfo['newCategory'] == 'OTHER', 'rank'] = 9

# /**** Are all McOpCo stores to be included?  ****/
# /*** If NO ***/


PEstore = pd.DataFrame(f"{mylib3}{price_recs_round}{file['round']}")
PEstore.groupby(by='mcd_gbal_lcat_id_nu')
PEstore.rename(columns={'1': 'PEStore '})

#price_recs_round.to_csv(PEStore_path, f"PEStores_round{file['round']}.csv")

print(PEstore)
