import pandas as pd
import numpy as np

from MarketDimensions import*


def grossnet(grssnet):
    wkly_pmix=pd.read_sas()
    if grssnet.upper.[:1]=="G":
        wkly_pmix.groupby(['mcd_gbal_lcat_id_nu','store_nu','delivery','sld_menu_itm_id','period']).agg(
            units=NamedAgg(column="ALACARTE_UNITS",aggfunc="sum")
            sales=NamedAgg(column="Net_sales_lcl",aggfunc="sum")
            avgunits=NamedAgg(column="ALACARTE_UNITS",aggfunc="sum")
            avgsales=NamedAgg(column="Net_sales_lcl",aggfunc="sum")
            cost=NamedAgg(column="Total_cost_lcl",aggfunc="sum")
            AvgCost=NamedAgg(column="Total_cost_lcl",aggfunc="sum")
        ).reset_index()
        wkly_pmix['avg_NET_price']=sum(wkly_pmix['Net_sales_lcl'])/sum(wkly_pmix['ALACARTE_UNITS'])
        wkly_pmix['avg_price']=sum(wkly_pmix['Net_sales_lcl'])/sum(wkly_pmix['ALACARTE_UNITS'])
        wkly_pmix['avg_MenuPrice']=sum(wkly_pmix['Menu_sales'])/sum(wkly_pmix['ALACARTE_UNITS'])
        wkly_pmix.sort_values(by=['mcd_gbal_lcat_id_nu','store_nu','delivery','sld_menu_itm_id','period'])
        return wkly_pmix
    else :
        wkly_pmix.groupby(['mcd_gbal_lcat_id_nu','store_nu','delivery','sld_menu_itm_id','period']).agg(
            units=NamedAgg(column="ALACARTE_UNITS",aggfunc="sum")
            sales=NamedAgg(column="Net_sales_lcl",aggfunc="sum")
            avgunits=NamedAgg(column="ALACARTE_UNITS",aggfunc="sum")
            avgsales=NamedAgg(column="Net_sales_lcl",aggfunc="sum")
            cost=NamedAgg(column="Total_cost_lcl",aggfunc="sum")
            AvgCost=NamedAgg(column="Total_cost_lcl",aggfunc="sum")
        )
        wkly_pmix['avg_NET_price']=sum(wkly_pmix['Net_sales_lcl'])/sum(wkly_pmix['ALACARTE_UNITS'])
        wkly_pmix['avg_price']=sum(wkly_pmix['Net_sales_lcl'])/sum(wkly_pmix['ALACARTE_UNITS'])
        wkly_pmix['avg_MenuPrice']=sum(wkly_pmix['Menu_sales'])/sum(wkly_pmix['ALACARTE_UNITS'])
        wkly_pmix.sort_values(by=['mcd_gbal_lcat_id_nu','store_nu','delivery','sld_menu_itm_id','period'])


wkly_pmix2=grossnet(file['grssnet'])

sales=wkly_pmix2.copy()
sales.pivot_table(
    index=['mcd_gbal_lcat_id_nu ','store_nu ','delivery','sld_menu_itm_id'],
    columns='period',
    values='sales',
).add_prefix('Sales_').reset_index().rename_axis(None,axis=1)

Units=wkly_pmix2.copy()
Units.pivot_table(
    index=['mcd_gbal_lcat_id_nu ','store_nu ','delivery','sld_menu_itm_id'],
    columns='period'
    values='Units'
).add_prefix('Units_').reset_index().rename_axis(None,axis=1)

avgsales=wkly_pmix2.copy()
avgsales.pivot_table(
    index=['mcd_gbal_lcat_id_nu ','store_nu ','delivery','sld_menu_itm_id'],
    columns='period'
    values='avgsales'
).add_prefix('AvgSales_').reset_index().rename_axis(None,axis=1)

avgunits=wkly_pmix2.copy()
avgunits.pivot_table(
    index=['mcd_gbal_lcat_id_nu ','store_nu ','delivery','sld_menu_itm_id'],
    columns='period'
    values='avgunits'
).add_prefix('AvgUnits_').reset_index().rename_axis(None,axis=1)

price=wkly_pmix2.copy()
price.pivot_table(
    index=['mcd_gbal_lcat_id_nu ','store_nu ','delivery','sld_menu_itm_id'],
    columns='period'
    values='avg_price'
).add_prefix('AvgPrice_').reset_index().rename_axis(None,axis=1)

NEtprice=wkly_pmix2.copy()
NEtprice.pivot_table(
    index=['mcd_gbal_lcat_id_nu ','store_nu ','delivery','sld_menu_itm_id'],
    columns='period'
    values='avg_NET_price'
).add_prefix('AvgNETPrice_').reset_index().rename_axis(None,axis=1)

MenuPrice=wkly_pmix2.copy()
MenuPrice.pivot_table(
    index=['mcd_gbal_lcat_id_nu ','store_nu ','delivery','sld_menu_itm_id'],
    columns='period'
    values='avg_Menuprice'
).add_prefix('AvgMenuPrice_').reset_index().rename_axis(None,axis=1)

FP_COST=wkly_pmix2.copy()
FP_COST.pivot_table(
    index=['mcd_gbal_lcat_id_nu ','store_nu ','delivery','sld_menu_itm_id'],
    columns='period'
    values='cost'
).add_prefix('Cost_').reset_index().rename_axis(None,axis=1)

AvgFP_COST=wkly_pmix2.copy()
AvgFP_COST.pivot_table(
    index=['mcd_gbal_lcat_id_nu ','store_nu ','delivery','sld_menu_itm_id'],
    columns='period'
    values='AvgCost'
).add_prefix('AvgCost_').reset_index().rename_axis(None,axis=1)

AvgFP_COSTItem=wkly_pmix2.copy()
AvgFP_COSTItem.pivot_table(
    index=['mcd_gbal_lcat_id_nu ','store_nu ','delivery','sld_menu_itm_id'],
    columns='period'
    values='AvgCostItem'
).add_prefix('AvgCostItem_').reset_index().rename_axis(None,axis=1)

# Summarize Guest counts by store.delivery transpose
gstcnt3=pd.read_sas(f"{lisa}gstcnt3.sas7bdat")
gstcnt3.groupby(['mcd_gbal_lcat_id_nu','delivery','period']).agg(
    GuestCount=NamedAgg(column='totalguestcount',aggfunc='sum')
    AvgGuestCount=NamedAgg(column='totalguestcount',aggfunc=np.mean())
).sort_values(by['mcd_gbal_lcat_id_nu','delivery','period'])

GuestCount=gstcnt3.copy()
GuestCount.pivot_table(
    index=['mcd_gbal_lcat_id_nu','delivery'],
    columns='period'
    values='GuestCount'
).add_prefix('GuestCount_').reset_index().rename_axis(None,axis=1)

AvgGuestCount=gstcnt3.copy()
AvgGuestCount.pivot_table(
    index=['mcd_gbal_lcat_id_nu','delivery'],
    columns='period'
    values='AvgGuestCount'
).add_prefix('AvgGuestCount_').reset_index().rename_axis(None,axis=1)

# Summarize Guest counts by store transpose
gstcnts=pd.read_sas(f"{lisa}gstcnts2.sas7bdat")
gstcnts.groupby(['mcd_gbal_lcat_id_nu','period']).agg(
    GuestCount=NamedAgg(column='totalguestcount',aggfunc='sum')
    AvgGuestCount=NamedAgg(column='totalguestcount',aggfunc=np.mean())
).sort_values(by=['mcd_gbal_lcat_id_nu','period'])

GuestCount=gstcnt3.copy()
GuestCount.pivot_table(
    index=['mcd_gbal_lcat_id_nu'],
    columns='period'
    values='GuestCount'
).add_prefix('GuestCount_').reset_index().rename_axis(None,axis=1)

AvgGuestCount=gstcnt3.copy()
AvgGuestCount.pivot_table(
    index=['mcd_gbal_lcat_id_nu'],
    columns='period'
    values='AvgGuestCount'
).add_prefix('AvgGuestCount_').reset_index().rename_axis(None,axis=1)

gstcnts_trans=pd.merge(GuestCount,AvgGuestCount,on='mcd_gbal_lcat_id_nu')

# Summarize by store and transpose

wkly_pmix3=wkly_pmix.copy()
wkly_pmix3.groupby(['mcd_gbal_lcat_id_nu','store_nu','WK_END_THU_ID_NU','period']).agg(
    units=NamedAgg(column='ALACARTE_UNITS',aggfunc='sum')
    sales=NamedAgg(column='Net_sales_lcl',aggfunc='sum')
    cost=NamedAgg(column='Total_cost_lcl',aggfunc='sum')
).sort_values(by=['mcd_gbal_lcat_id_nu','store_nu','WK_END_THU_ID_NU','period'])

#create table at mylib1_wkly_pmix3
wkly_pmix3.groupby(['mcd_gbal_lcat_id_nu','store_nu','period']).agg(
    units=NamedAgg(column='units',aggfunc='sum')
    sales=NamedAgg(column='sales',aggfunc='sum')
    avgTotalunits=NamedAgg(column='units',aggfunc='sum')
    avgTotalsales=NamedAgg(column='sales',aggfunc='sum')
    cost=NamedAgg(column='cost',aggfunc='sum')
    AvgTotalCost=NamedAgg(column='cost',aggfunc='sum')

).sort_values(by=['mcd_gbal_lcat_id_nu','store_nu','period'])

sales=wkly_pmix3.copy()
sales.pivot_table(
    index=['mcd_gbal_lcat_id_nu ','store_nu '],
    columns='period',
    values='sales',
).add_prefix('Sales_').reset_index().rename_axis(None,axis=1)

Units=wkly_pmix3.copy()
Units.pivot_table(
    index=['mcd_gbal_lcat_id_nu ','store_nu '],
    columns='period',
    values='Units',
).add_prefix('Units_').reset_index().rename_axis(None,axis=1)

avgsales=wkly_pmix3.copy()
avgsales.pivot_table(
    index=['mcd_gbal_lcat_id_nu ','store_nu '],
    columns='period',
    values='avgTotalsales',
).add_prefix('avgTotalsales_').reset_index().rename_axis(None,axis=1)

avgunits=wkly_pmix3.copy()
avgunits.pivot_table(
    index=['mcd_gbal_lcat_id_nu ','store_nu '],
    columns='period',
    values='avgTotalunits',
).add_prefix('avgTotalunits_').reset_index().rename_axis(None,axis=1)

FP_COST=wkly_pmix3.copy()
FP_COST.pivot_table(
    index=['mcd_gbal_lcat_id_nu ','store_nu '],
    columns='period',
    values='cost',
).add_prefix('Cost_').reset_index().rename_axis(None,axis=1)

AvgFP_COST=wkly_pmix3.copy()
AvgFP_COST.pivot_table(
    index=['mcd_gbal_lcat_id_nu ','store_nu '],
    columns='period',
    values='AvgTotalCost',
).add_prefix('AvgTotalCost_').reset_index().rename_axis(None,axis=1)

trans=pd.merge(

)

wkly_pmix_trans_REST=(
    trans.merge(regions,on='mcd_gbal_lcat_id_nu',how='left')
    .merge(gstcnts_trans,on='mcd_gbal_lcat_id_nu')
    .merge(f"GCElasticity_round{file['round']}",on='mcd_gbal_lcat_id_nu')
).sort_values(by=['mcd_gbal_lcat_id_nu','store_nu'])
