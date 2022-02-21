import pandas as pd
import numpy as np

# /********** CONTENT DATA *******************************/

POST_DropDownDates_VA=pd.DataFrame(mylib2.cntr1._POST_DropDownDates_VA)
POST_DropDownDates_VA.to_excel(f'{outpath1}'/Post_Round_Reports.xlsx,sheet_name='Content Data')

# /*********** PRICE TAKEN REPORT ********************************/

FIELDREPORT=pd.DataFrame(mylib2.FIELDREPORT)
FIELDREPORT.to_excel(f'{outpath1}'/Post_Round_Reports.xlsx,sheet_name='Price Taken Data')

# /********** TOP 20 PRICE TAKEN & REVENUE CHANGE REPORT *********/

TOP20REPORT=pd.DataFrame()



