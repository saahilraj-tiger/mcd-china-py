import os
import pandas as pd
import numpy as np
from param_file import *



with open(r'C:\Users\gopala.chennu\Desktop\Current\vs\Codes\master.yml', 'r') as doc:
    file = yaml.load(doc, Loader=yaml.FullLoader)

envir = file['envir']
cntry = file['cntry']
mylib1=file['00_ParamFile_PE']['params_file']['mylib1']
mylib4 = file['00_ParamFile_PE']['params_file']['mylib4']
mylib3 = file['00_ParamFile_PE']['params_file']['mylib3']



advrdata = f"/opt/sasdata/{envir}/Data/{cntry}/AdvisorDashboards/data"

if(mylib4.ZoneXField):
    mylib4.ZoneXField = mnthdata.ZoneXField

if(mylib4.store_info_sociodemographic_data):
    mylib4.store_info_sociodemographic_data = advrdata.store_info_sociodemographic_data

if(mylib4.elasticity_for_optimization):
    mylib4.elasticity_for_optimization = advrdata.elasticity_for_optimization

if(mylib4.item_Sensitivity):
    mylib4.item_Sensitivity = advrdata.item_Sensitivity

if(mylib4.Store_table_all_items):
    mylib4.Store_table_all_items = advrdata.store_table_all_items

if(mylib4.store_item_table):
    mylib4.store_item_table = advrdata.store_item



def venloop2():
    if Vendor.upper == "DELOITTE" or Vendor.upper == "COMBINED":
        mylib4.newfile1(os.rename(mcd_glbl_loc_id = mcd_gbal_lcat_id_nu))
        dellib.newfile1
        # Incomplete....


def TAFiles():
    # sql_command="""create table &mylib4..&recfile2 as
    # 	select sld_menu_itm_id, mcd_gbal_lcat_id_nu, prcg_engn_curr_prc, rcom_prc, frm_engn_rcom_flg, spr_de_elstc_coef
    # 		,round(prcg_engn_curr_net_prc,.01) as prcg_engn_curr_net_prc
    # 		,round(rcom_net_prc,.01) as rcom_net_prc
    # 	from &mylib4..&newfile2.
    # 	where sld_menu_itm_id=Independent_item and prcg_engn_curr_prc ne rcom_prc;
    # """

    newfile2 = pd.DataFrame(mylib4.file['newfile2'])
    newfile2 = newfile2[((newfile2['sld_menu_itm_id']) == Independent_item & (newfile2['sld_menu_itm_id']== prcg_engn_curr_prc)) != newfile2['rcom_prc']]
    newfile2=newfile2[['sld_menu_itm_id','mcd_gbal_lcat_id_nu','prcg_engn_curr_prc','rcom_prc','frm_engn_rcom_flg','spr_de_elstc_coef','prcg_engn_curr_net_prc','rcom_net_prc']]
    newfile2['prcg_engn_curr_net_prc']=newfile2['prcg_engn_curr_net_prc'].apply(np.ceil)
    newfile2['rcom_net_prc']=newfile2['rcom_net_prc'].apply(np.ceil)



    # sql_command2 = """create table &mylib4..&gcelas2. as
	#     select distinct a.mcd_gbal_lcat_id_nu, sr_gc_elstc_coef as gc_elastic, b.rsq_prc as InfluencePrcGC
	# 	from &mylib4..&newfile2. a left join &mylib4..&gcpric2. b on (a.mcd_gbal_lcat_id_nu=b.mcd_gbal_lcat_id_nu)
	# 	where sld_menu_itm_id=Independent_item; """
    # crsr.execute(sql_command2)

    gcelas2=pd.DataFrame(mylib4.file['gcpric2'])
    merge_data=pd.merge(newfile2, gcelas2,how='left',on='mcd_gbal_lcat_id_nu')
    merge_data=merge_data[merge_data['sld_menu_itm_id']==Independent_item]
    merge_data=merge_data[['mcd_gbal_lcat_id_nu','sr_gc_elstc_coef','rsq_prc']]
    merge_data.rename(columns={'sr_gc_elstc_coef':'gc_elastic','rsq_prc':'InfluencePrcGC'})



    # sql_command3 = """create table &mylib4..&elasfile2. as
	# 	select mcd_gbal_lcat_id_nu, sld_menu_itm_id, spr_de_elstc_coef as final_elasticity, Independent_item as cross_elastic_item
	# 	from &mylib4..&newfile2.
	# 	where prcg_engn_curr_prc ne rcom_prc;"""
    # crsr.execute(sql_command3)

    elasfile2=pd.DataFrame(mylib4.file['newfile2'])
    elasfile2=elasfile2[elasfile2['prcg_engn_curr_prc'] != elasfile2['rcom_prc']]
    elasfile2[['mcd_gbal_lcat_id_nu','sld_menu_itm_id','spr_de_elstc_coef','Independent_item']]
    elasfile2.rename(columns={'spr_de_elstc_coef':'final_elasticity','Independent_item':'cross_elastic_item'})
    print(elasfile2)



def DltFiles():
    # sql_command1 = """create table &mylib4..&recfile1. as
	# 	select sld_menu_itm_id, mcd_gbal_lcat_id_nu, prcg_engn_curr_prc, rcom_prc, frm_engn_rcom_flg, spr_de_elstc_coef
	# 		,round((prcg_engn_curr_prc/(1+&tax.)),.01) as prcg_engn_curr_net_prc
	# 		,case when rcom_prc ne . then round((rcom_prc/(1+&tax.)),.01)
	# 			else .
	# 			end as rcom_net_prc
	# 	from &mylib4..&newfile1.;"""
    # crsr.execute(sql_command1)

    recfile1=pd.DataFrame(mylib4.file['newfile1'])
    prcg_engn_curr_net_prc=recfile1['prcg_engn_curr_prc']/(1+file['tax'])





    # sql_command2 = """create table &mylib4..&gcelas1. as
	# 	select distinct a.mcd_gbal_lcat_id_nu, sr_gc_elstc_coef as gc_elastic, b.rsq_prc as InfluencePrcGC
	# 	from &mylib4..&newfile1. a left join &mylib4..&gcpric1. b on (a.mcd_gbal_lcat_id_nu=b.mcd_gbal_lcat_id_nu);
	#     """
    # crsr.execute(sql_command2)

    gcpric1=pd.DataFrame(mylib4.file['gcpric1'])
    newfile1=pd.DataFrame(mylib4.file['newfile1'])
    gcelas1=pd.merge(newfile1, gcelas1,how='left',on='mcd_gbal_lcat_id_nu')
    gcelas1.rename(columns={'sr_gc_elstc_coef':'gc_elastic','rsq_prc':'InfluencePrcGC'})
    gcelas1.drop_duplicates(subset=['mcd_gbal_lcat_id_nu','gc_elastic','InfluencePrcGC'],keep='first')
    print(gcelas1)







    # sql_command3 = """create table &mylib4..&elasfile1. as
	# 	select mcd_gbal_lcat_id_nu, sld_menu_itm_id, spr_de_elstc_coef as final_elasticity, cross_elastic_item
	# 	from &mylib4..&newfile1.
    #     """
    # crsr.execute(sql_command3)

    elasfile1=pd.DataFrame(mylib4.file['newfile1'])
    elasfile1.rename(columns={'spr_de_elstc_coef':'final_elasticity'})
    elasfile1[['mcd_gbal_lcat_id_nu','sld_menu_itm_id','final_elasticity','cross_elastic_item']]




def combineloop():
    if Vendor.upper == "COMBINED":
        recfile1_sort = mylib4.file['recfile1']
        recfile1_= pd.DataFrame(recfile1_sort)
        recfile1.sort_values(by=[" mcd_gbal_lcat_id_nu", "sld_menu_itm_id"])

        recfile2 = mylib4.file['recfile2']  # Doubt  (sort )
        recfile2_df = pd.DataFrame(recfile2)
        recfile2_df.sort_values(
            [" mcd_gbal_lcat_id_nu", "sld_menu_itm_id"], inplace=True)

        # if a and not b; (Merge)
        recfile3 = pd.merge(recfile1_df, recfile2_df,
                            on="mcd_gbal_lcat_id_nu", how=" ")

        recfile = recfile3.append(recfile2_df)  # set function in sas

        gcelas1_sort = pd.read_sas(
            "mylib4/gcelas1", format='sas7bdat', encoding='utf-8')
        gcelas1_sort_df = pd.DataFrame(gcelas1_sort)
        gcelas1_sort_df.sort_values(by="mcd_gbal_lcat_id_nu", inplace=True)

        gcelas2_sort = pd.read_sas(
            "mylib4/gcelas2", format='sas7bdat', encoding='utf-8')
        gcelas2_sort_df = pd.DataFrame(gcelas2_sort)
        gcelas2_sort_df.sort_values(by="mcd_gbal_lcat_id_nu", inplace=True)

        gcelas3 = pd.merge(gcelas1_sort_df, gcelas2_sort_df,
                           on="mcd_gbal_lcat_id_nu", how=" ")  # if a and not b;

        gcelas = gcelas3.append(gcelas2_sort_df)

        elasfile1_sort = pd.read_sas(
            "mylib4/elasfile1", format="sas7bdat", encoding='utf-8')
        elasfile1_sort_df = pd.DataFrame(elasfile1_sort)
        elasfile1_sort_df.sort_values(
            by=["mcd_gbal_lcat_id_nu", "sld_menu_itm_id", "cross_elastic_item"])

        elasfile2_sort = pd.read_sas(
            "mylib4/elasfile2", format="sas7dbat", encoding='utf-8')
        elasfile2_sort_df = pd.DataFrame(elasfile2_sort)
        elasfile2_sort_df.sort_values(
            by=["mcd_gbal_lcat_id_nu", "sld_menu_itm_id", "cross_elastic_item"])

        elasfile3_merge = pd.merge(elasfile1_sort_df, elasfile2_sort_df,
                                   on="mcd_gbal_lcat_id_nu", how=" ")  # if a and not b; (Merge)

        elasfile = elasfile3_merge.append(elasfile2_sort_df)

        if Vendor.upper == "TIGER":
            recfile = pd.read_sas(
                "mylib4/recfile2", format='sas7dbat', encoding='utd-8')
            gcelas = pd.read_sas(
                "mylib4/gcelas2", format='sas7dbat', encoding='utd-8')
            elasfile = pd.read_sas("&mylib4/elasfile2",
                                   format='sas7dbat', encoding='utd-8')

        if Vendor.upper == "DELOITTE":
            recfile = pd.read_sas(
                "mylib4/recfile1", format='sas7dbat', encoding='utd-8')
            gcelas = pd.read_sas(
                "mylib4/gcelas1", format='sas7dbat', encoding='utd-8')
            elasfile = pd.read_sas("&mylib4/elasfile1",
                                   format='sas7dbat', encoding='utd-8')





mylib3.price_recs_round.file['round']=recfile
mylib3.Elasticity_round.file['round']=elasfile
mylib3.GCElasticity_round.file['round']=gcelas

elasfile=f"{mylib3}/{Elasticity_round}/"+file['round']


print(mylib3.price_recs_round.file['round'])
print(mylib3.Elasticity_round.file['round'])
print(mylib3.GCElasticity_round.file['round'])





# Modified below steps to get the store, region info


redshift = mysql.connector.connect(
    host="localhost",
    user=reduser,
    password=redpass,
    database=gdap
)
# region1 = """create table regions1 as
# 	select * from connection to redshift
# 		(select
# 			g.mcd_gbal_lcat_id_nu,
# 			g.rest_owsh_typ_id,
# 			'Italy' as market,
# 			ab.ops_lvl3_na
# 		from
# _tables.gbal_lcat g
# 			left join MSTR_VM.dmsite_ops_hrcy ab on (g.mcd_gbal_lcat_id_nu = ab.mcd_gbal_lcat_id_nu and g.ctry_iso_nu=ab.ctry_iso_nu)
# 		where g.ctry_iso_nu =&terr. and g.rest_owsh_typ_id in (1,2)
# 		order by
# 			g.mcd_gbal_lcat_id_nu
# 		);"""

gbal_lcat=pd.DataFrame(rmdw_tables.gbal_lcat)
dmsite_ops_hrcy=pd.DataFrame(MSTR_VM.dmsite_ops_hrcy)
regions1=pd.merge(gbal_lcat, dmsite_ops_hrcy,how='left',on=['mcd_gbal_lcat_id_nu','ctry_iso_nu'])
regions1=regions1[(regions1['ctry_iso_nu']==terr) & (regions1.isin([1,2]))]
regions1.sort_values(by=['mcd_gbal_lcat_id_nu'])
regions1.rename(columns={'Italy':'market'})
regions1=regions1[['mcd_gbal_lcat_id_nu','rest_owsh_typ_id','market','ops_lvl3_na']]
print(regions1)


store_info_sociodemographic_data=pd.DataFrame(mylib4.store_info_sociodemographic_data)
ZoneXField=pd.DataFrame(mylib4.ZoneXField)
regions2=pd.merge(regions1, store_info_sociodemographic_data,how='left',left_on='mcd_gbal_lcat_id_nu',right_on='global_id').merge(ZoneXField,how='left',left_on='region',right_on='field')
regions2.sort_values(by='mcd_gbal_lcat_id_nu')
regions2.rename(columns={'region':'field'})
regions2[['mcd_gbal_lcat_id_nu','rest_owsh_typ_id','ops_lvl3_na','market','zone','field']]
print(regions2)

regions=regions2
regions['market']='Italy'
regions.zone.replace(to_value=' ',value='Unknown')
regions.field.replace(to_value=' ',value='Unknown')
regions=regions[['market','zone','field']]
print(regions)



# ******** Modified below steps to get the item info *************************

# data &mylib1..ItemsNames;
# 	set lookup.menu_items (keep=sld_menu_itm_id SLD_MENU_ITM_NA);
# run;
# proc sort data=&mylib1..ItemsNames nodupkey;
# 	by sld_menu_itm_id;

ItemsNames=pd.DataFrame(lookup.menu_items)  #mylib1 - doubt
ItemsNames=ItemsNames[['sld_menu_itm_id','SLD_MENU_ITM_NA']]
ItemsNames.drop_duplicates(subset='sld_menu_itm_id')


menu_items=pd.DataFrame(lookup.menu_items)
Product_Hierarchy=pd.DataFrame(lookup.Product_Hierarchy)
Product_Characteristics=pd.DataFrame(lookup.Product_Characteristics)
iteminfo=pd.merge(menu_items,Product_Hierarchy,how='left',on='sld_menu_itm_id').merge(Product_Hierarchy,how='left',on='sld_menu_itm_id')
print(iteminfo)

# doubt below steps (Related are not !!)  -barath

if itm_hier_lvl3 in ('Not Used' 'Other No Food'):
    exclude = 1
    ScrCategory = 'Exclude'

elif sld_menu_itm_na in ('Unknown' 'delete'):
    exclude = 1
    ScrCategory = 'Exclude'

else:
    exclude = 0
    ScrCategory = ITM_HIER_LVL3

rank = 1


if ITM_HIER_LVL3 == 'Drinks':
    newCategory = 'BEVERAGES'
elif ITM_HIER_LVL3 == 'ALC Entrees':
    newCategory = 'ENTREES'
elif ITM_HIER_LVL3 == 'Breakfast':
    newCategory = 'BKFST ENTREES'
elif ITM_HIER_LVL3 in ('EVM Large', 'EVM Medium', 'Easy Menu'):
    newCategory = 'COMBO MEALS'
elif ITM_HIER_LVL3 == 'Salads':
    if product_offered_as == 'EVM':
        newCategory = 'COMBO MEALS'
    elif product_offered_as == 'A la Carte':
        newCategory = 'ENTREES'
    elif product_offered_as == 'Part of EVM - Side':
        newCategory = 'SIDE ITEMS'

elif ITM_HIER_LVL3 == 'Desserts':
    newCategory = 'DESSERTS'
elif ITM_HIER_LVL3 == 'Happy Meal':
    newCategory = 'HAPPY MEALS'
elif ITM_HIER_LVL3 == 'McCafe':
    newCategory = 'MCCAFE'
elif ITM_HIER_LVL3 == 'Fries':
    newCategory = 'SIDE ITEMS'
elif ITM_HIER_LVL3 in ('MySelection', 'Others', 'Mymenu'):
    newCategory = 'OTHER'
else:
    newCategory = 'OTHER'

if ScrCategory == 'Exclude':
    newCategory = "Exclude"

if newCategory == 'ENTREES':
    rank = 1
elif newCategory == 'BKFST ENTREES':
    rank = 2
elif newCategory == 'COMBO MEALS':
    rank = 3
elif newCategory == 'BEVERAGES':
    rank = 4
elif newCategory == 'SIDE ITEMS':
    rank = 5
elif newCategory == 'HAPPY MEALS':
    rank = 6
elif newCategory == 'MCCAFE':
    rank = 7
elif newCategory == 'DESSERTS':
    rank = 8
elif newCategory == 'OTHER':
    rank = 9


#   **** Are all McOpCo stores to be included?  ****/
#   *** If NO ***/

# PEstores = """create table &mylib3..PEStores_round.round. as
# 	select distinct mcd_gbal_lcat_id_nu, 1 as PEStore
# 	from &mylib3..price_recs_round&round.;"""

PEstore=pd.DataFrame(f"{mylib3}.{price_recs_round}"+file['round'])
PEstore.groupby(by='mcd_gbal_lcat_id_nu')
PEstore.rename(columns={'1':'PEStore '})
print(PEstore)




