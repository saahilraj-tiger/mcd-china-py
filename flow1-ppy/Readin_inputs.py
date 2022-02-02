import pandas as pd
import yaml
import os
from param_file import *

# current working directory
# print(os.getcwd())


with open(r'C:\Users\gopala.chennu\Desktop\Current\vs\Codes\master.yml', 'r') as doc:
    file = yaml.load(doc, Loader=yaml.FullLoader)

mylib4 = file['mylib4']
newfile2 = file['newfile2']
gcpric2=file['gcpric2']

newfile2txt = pd.read_table(f'{infile}/newfile2.txt', delim_whitespace=True)

# newfile2_data = f"{mylib4}{newfile2}"
newfile2_data = pd.DataFrame(newfile2txt)

newfile_data.columns = ["sld_menu_itm_id",
                        "Independent_item",
                        "MCD_GBAL_LCAT_ID_NU",
                        "prcg_engn_curr_prc",
                        "rcom_prc",
                        "spr_de_elstc_coef",
                        "sr_gc_elstc_coef",
                        "gc_intercept",
                        "rcom_eff_date",
                        "frm_engn_rcom_flg",
                        "prcg_engn_curr_net_prc",
                        "rcom_net_prc"]
print(newfile2_data)

gcpric2_data=pd.read_table(f'{infile}/gcpric2.txt')

gcpric2_data.columns=['MCD_GBAL_LCAT_ID_NU ','rsq_prc']

print(gcpric2)


# ------------ Sample data------------------------(arkit sir)

# with open(file['mylib4'].gcpric2, "w") as file:
#     gcpric2 = file

# with open(infile/file['gcpric2']) as file:
#     gcpric2 = pd.DataFrame(gcpric2, columns=["MCD_GBAL_LCAT_ID_NU", "rsq_prc"])
