import os
import pandas as pd
import numpy as np
import os
import shutil

from param_file import *


with open(r'C:\Users\gopala.chennu\Desktop\Current\vs\Codes\master.yml', 'r') as doc:
    file = yaml.load(doc, Loader=yaml.FullLoader)

envir = file['envir']
cntry = file['cntry']
Vendor = file['Vendor']
mylib1 = file['00_ParamFile_PE']['params_file']['mylib1']
mylib4 = file['00_ParamFile_PE']['params_file']['mylib4']
mylib3 = file['00_ParamFile_PE']['params_file']['mylib3']


#from sqlalchemy import create_engine
#engine = create_engine('postgresql://mc96201:BRlz4p$95R?P@GDAP_GDW_DI.mcdonalds.com:5439/gdap')        (Kushagra)

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


venloop2()


def TAFiles():
    recfile2 = pd.read_sql('''
    	select sld_menu_itm_id, mcd_gbal_lcat_id_nu, prcg_engn_curr_prc, rcom_prc, frm_engn_rcom_flg, spr_de_elstc_coef
    		,round(prcg_engn_curr_net_prc,.01) as prcg_engn_curr_net_prc
    		,round(rcom_net_prc,.01) as rcom_net_prc
            from %(newfile2)s
    	where sld_menu_itm_id=Independent_item and prcg_engn_curr_prc <> rcom_prc;
        ''', engine, params={'newfile2': file['newfile2']})

    gcelas2 = pd.read_sql('''
	    select distinct a.mcd_gbal_lcat_id_nu, sr_gc_elstc_coef as gc_elastic, b.rsq_prc as InfluencePrcGC
		from %(newfile2)s a left join %(gcpric2)s b on (a.mcd_gbal_lcat_id_nu=b.mcd_gbal_lcat_id_nu)
		where sld_menu_itm_id=Independent_item; ''', engine, params={'newfile2': file['newfile2'], 'gcpric2': file['gcpric2']})

    elasfile2 = pd.read_sql('''
    select mcd_gbal_lcat_id_nu, sld_menu_itm_id, spr_de_elstc_coef as final_elasticity, Independent_item as cross_elastic_item
		from %(newfile2)s
		where prcg_engn_curr_prc ne rcom_prc;

    ''', engine, params={'newfile2': file['newfile2']})
    return recfile2, gcelas2, elasfile2


recfile2, gcelas2, elasfile2 = TAFiles()


def DltFiles():
    recfile1 = pd.read_sql('''
            select sld_menu_itm_id, mcd_gbal_lcat_id_nu, prcg_engn_curr_prc, rcom_prc, frm_engn_rcom_flg, spr_de_elstc_coef
			,round((prcg_engn_curr_prc/(1+%(tax)s)),.01) as prcg_engn_curr_net_prc
			,case when rcom_prc is not null then round((rcom_prc/(1+%(tax)s)),.01)
				else is null
				end as rcom_net_prc
		from %(newfile1)s;
        ''', engine, params={'newfile1': file['newfile1'], 'tax': file['tax']})

    gcelas1 = pd.read_sql('''
         select distinct a.mcd_gbal_lcat_id_nu, sr_gc_elstc_coef as gc_elastic, b.rsq_prc as InfluencePrcGC
		from newfile1 a left join gcpric1 b on (a.mcd_gbal_lcat_id_nu=b.mcd_gbal_lcat_id_nu);
	''', engine,)

    elasfile1 = pd.read_sql('''
    select mcd_gbal_lcat_id_nu, sld_menu_itm_id, spr_de_elstc_coef as final_elasticity, cross_elastic_item
		from newfile1
    ''', engine)

    return recfile1, gcelas1, elasfile


recfile1, gcelas1, elasfile1 = DltFiles()


def combineloop(Vendor):

    if Vendor.upper() == "COMBINED":

        recfile1 = pd.read_sas(f'{engine}recfile1', encoding='ISO-8859-1')

        recfile1.sort_values(by=[" mcd_gbal_lcat_id_nu", "sld_menu_itm_id"])

        recfile2 = pd.read_sas(f'{engine}recfile2', encoding='ISO-8859-1')

        recfile2_df.sort_values([" mcd_gbal_lcat_id_nu", "sld_menu_itm_id"])

        recfile3 = pd.merge(recfile1, recfile2,
                            on="mcd_gbal_lcat_id_nu", how="left")

        recfile = recfile3.append(recfile2_df)

        gcelas1 = pd.read_sas(f"{engine}gcelas1",
                              format='sas7bdat', encoding='utf-8')

        gcelas1.sort_values(by="mcd_gbal_lcat_id_nu")

        gcelas2 = pd.read_sas(f"{engine}gcelas2",
                              format='sas7bdat', encoding='utf-8')

        gcelas2.sort_values(by="mcd_gbal_lcat_id_nu")
        gcelas3 = pd.merge(gcelas1, gcelas2, on="mcd_gbal_lcat_id_nu", how="left")
        gcelas = gcelas3.append(gcelas2)

        elasfile1_sort = pd.read_sas(f"{engine}{file['elasfile1']}", format="sas7bdat", encoding='utf-8')

        elasfile1_sort.sort_values(by=["mcd_gbal_lcat_id_nu", "sld_menu_itm_id", "cross_elastic_item"])

        elasfile2_sort = pd.read_sas(f"{engine}{file['elasfile2']}", format="sas7dbat", encoding='utf-8')

        elasfile2.sort_values(by=["mcd_gbal_lcat_id_nu", "sld_menu_itm_id", "cross_elastic_item"])

        elasfile3 = pd.merge(elasfile1, elasfile2,on="mcd_gbal_lcat_id_nu", how="left")  # if a and not b; (Merge)

        elasfile = elasfile3.append(elasfile2)

        return recfile, gcelas, elasfile

    if Vendor.upper() == "TIGER":
        recfile = pd.read_sas(f"{engine}{file['recfile2']}", format='sas7dbat', encoding='utd-8')

        gcelas = pd.read_sas(f"{engine}{file['gcelas2']}",format='sas7dbat', encoding='utd-8')

        elasfile = pd.read_sas(f"{engine}{file['elasfile2']}",format='sas7dbat', encoding='utd-8')

        return recfile, gcelas, elasfile

    if Vendor.upper() == "DELOITTE":
        recfile = pd.read_sas(f"{engine}{file['recfile1']}", format='sas7dbat', encoding='utd-8')
        gcelas = pd.read_sas(f"{engine}{file['gcelas1']}", format='sas7dbat', encoding='utd-8')
        elasfile = pd.read_sas(f"{engine}{file['elasfile1']}",format='sas7dbat', encoding='utd-8')
        return recfile, gcelas, elasfile


recfile, gcelas, elasfile = combineloop(Vendor)

price_recs_round = f'{inter1}'
recfile.to_csv(price_recs_round, f'price_recs_round{round}')

Elasticity_round = f'{inter1}'
elasfile.to_csv(Elasticity_round, f'Elasticity_round{round}')

GCElasticity_round = f'{inter1}'
gcelas.to_csv(GCElasticity_round, f'GCElasticity_round{round}')

elasfile = f"{mylib3}/{Elasticity_round}/"+file['round']


# Modified below steps to get the store, region info

# region1=pd.read_sql('''select * from connection to redshift
# 		(select
# 			g.mcd_gbal_lcat_id_nu,
# 			g.rest_owsh_typ_id,
# 			'Italy' as market,
# 			ab.ops_lvl3_na
# 		from rmdw_tables.gbal_lcat g
# 			left join MSTR_VM.dmsite_ops_hrcy ab on (g.mcd_gbal_lcat_id_nu = ab.mcd_gbal_lcat_id_nu and g.ctry_iso_nu=ab.ctry_iso_nu)
# 		where g.ctry_iso_nu =%(terr)s and g.rest_owsh_typ_id in (1,2)
# 		order by
# 			g.mcd_gbal_lcat_id_nu
# 		);

# ''',redshift,params={'terr':file['terr']})

region1 = pd.read_excel(
    r'C:\Users\gopala.chennu\Desktop\Current\vs\Codes\getdata_op.xlsx')  # sent by barath


regions2 = pd.read_sql('''select a.mcd_gbal_lcat_id_nu,
	       a.rest_owsh_typ_id,
		   a.ops_lvl3_na,
		   a.market,
		   s.zone,
		   b.region as field
	from regions1 a
	left join store_info_sociodemographic_data b on(a.mcd_gbal_lcat_id_nu=b.global_id)
	left join ZoneXField s on (b.region=s.field)
	order by a.mcd_gbal_lcat_id_nu;

''', engine)

regions = pd.read_sas(f"{inter1}regions2")
regions['market'] = 'Italy'
regions['zone'] = regions['zone'].replace([''], 'unknown')
regions['field'] = regions['field'].replace([''], 'unknown')

regions = regions[['market', 'zone', 'field']].value_counts()
print(regions)


# ******** Modified below steps to get the item info *************************

ItemsNames_path = f"{lisa}"
menu_items = pd.read_sas(f"{lookup}menu_items")
menu_items = menu_items[['sld_menu_itm_id', 'SLD_MENU_ITM_NA']]

menu_items.to_csv(ItemsNames_path, 'ItemsNames.csv')     # mandatory to check
# shutil.copy(ItemsNames, menu_items)

ItemsNames = pd.read_csv(f"{lisa}ItemNames.csv")
ItemsNames.drop_duplicates(subset='sld_menu_itm_id')


itemInfo = pd.read_sql('''
	select a.sld_menu_itm_id, a.sld_menu_itm_na, b.itm_hier_lvl1, b.itm_hier_lvl2, b.itm_hier_lvl3,
	c.Primary_Protein, c.Product_Family, c.Product_Offered_As, c.Price_Tier_Strategy
	from menu_items a left join Product_Hierarchy b on (a.sld_menu_itm_id=b.sld_menu_itm_id)
		left join Product_Characteristics c on (b.sld_menu_itm_id=c.sld_menu_itm_id);
''', lookup)

itemInfo.to_csv(f"{inter1}", 'itemInfo.csv')

itemInfo = pd.read_csv(f"{inter1}itemInfo.csv")

itemInfo.loc[itemInfo['itm_hier_lvl3'].isin(['Not Used', 'Other No Food']), ['ScrCategory', 'exclude']] = ['Exclude', 1]

itemInfo.loc[itemInfo['sld_menu_itm_na '].isin(['Unknown', 'delete']), ['ScrCategory', 'exclude']] = ['Exclude', '1']

itemInfo.loc[~itemInfo['sld_menu_itm_na '].isin(['Unknown', 'delete', 'Not Used', 'Other No Food']), ['ScrCategory', 'exclude']] = ['Exclude', 0]

rank = 1

itemInfo.loc[itemInfo['ITM_HIER_LVL3'] =='Drinks', 'newCategory'] = 'BEVERAGES'

itemInfo.loc[itemInfo['ITM_HIER_LVL3'] =='ALC Entrees', 'newCategory'] = 'ENTREES'

itemInfo.loc[itemInfo['ITM_HIER_LVL3'] =='Breakfast', 'newCategory'] = 'BKFST ENTREES'

itemInfo.loc[itemInfo['ITM_HIER_LVL3'].isin(['EVM Large', 'EVM Medium', 'Easy Menu']), 'newCategory'] = 'COMBO MEALS'

itemInfo.loc[(itemInfo['ITM_HIER_LVL3'] == 'Salads') & (itemInfo['product_offered_as'] == 'EVM'), 'newCategory'] = 'COMBO MEALS'

itemInfo.loc[(itemInfo['ITM_HIER_LVL3'] == 'Salads') & (itemInfo['product_offered_as'] == 'A la Carte'), 'newCategory'] = 'ENTREES'

itemInfo.loc[(itemInfo['ITM_HIER_LVL3'] == 'Salads') & (itemInfo['product_offered_as'] == 'Part of EVM - Side'), 'newCategory'] = 'SIDE ITEMS'

itemInfo.loc[itemInfo['ITM_HIER_LVL3'] =='Desserts', 'newCategory'] = 'DESSERTS'

itemInfo.loc[itemInfo['ITM_HIER_LVL3'] =='Happy Meal', 'newCategory'] = 'HAPPY MEALS'

itemInfo.loc[itemInfo['ITM_HIER_LVL3'] == 'McCafe', 'newCategory'] = 'MCCAFE'

itemInfo.loc[itemInfo['ITM_HIER_LVL3'] =='Fries', 'newCategory'] = 'SIDE ITEMS'

itemInfo.loc[~itemInfo['ITM_HIER_LVL3'] == ['Drinks', 'ALC Entrees', 'Breakfast', 'EVM Large', 'EVM Medium',\
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

PEStore_path = f"{inter1}"

price_recs_round = pd.read_sql('''
select distinct mcd_gbal_lcat_id_nu, 1 as PEStore
	fromprice_recs_round%(round)s;
''', inter1, params={'round': file['round']})

price_recs_round.to_csv(PEStore_path, f"PEStores_round{file['round']}.csv")
