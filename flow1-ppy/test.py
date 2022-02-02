regions1 = pd.read_sql('''select * from
(select g.mcd_gbal_lcat_id_nu, g.rest_owsh_typ_id, 'Italy' as market, ab.ops_lvl3_na
from rmdw_tables.gbal_lcat g
    left join MSTR_VM.dmsite_ops_hrcy ab on (g.mcd_gbal_lcat_id_nu = ab.mcd_gbal_lcat_id_nu and g.ctry_iso_nu=ab.ctry_iso_nu)
    where g.ctry_iso_nu =%(terr)s and g.rest_owsh_typ_id in (1,2)
    order by
        g.mcd_gbal_lcat_id_nu
);''', engine, params = {'terr':config['param']['terr']})

store_info_data = pd.read_sas(f"{config['path']['advrdata']}store_info_sociodemographic_data.sas7bdat",encoding='ISO-8859-1')
zonexfield = pd.read_sas(f"{config['path']['repdata']}zonexfield.sas7bdat", encoding = 'ISO-8859-1')

regions = regions1.merge(store_info_data[['Global_ID','REGION']],how='left', left_on = 'mcd_gbal_lcat_id_nu',right_on='Global_ID')
regions = regions.merge(zonexfield, how = 'left', left_on='REGION', right_on='field')
regions = regions[['mcd_gbal_lcat_id_nu','rest_owsh_typ_id','market','ops_lvl3_na','zone','field']]

regions['market'] = 'Italy'
regions['zone'] = regions['zone'].replace([''],'unknown')
regions['field'] = regions['field'].replace([''],'unknown')