import pandas as pd
import numpy as np

# /********** CONTENT DATA *******************************/
# proc export data=&mylib2..&cntr1._POST_DropDownDates_VA outfile="&outpath1./Post_Round_Reports.xlsx" REPLACE label
# 	DBMS=xlsx;
# 	sheet="Content Data";
POST_DropDownDates_VA=pd.DataFrame(mylib2.cntr1._POST_DropDownDates_VA)
POST_DropDownDates_VA.to_excel(f'{outpath1}'/Post_Round_Reports.xlsx,sheet_name='Content Data')

# /*********** PRICE TAKEN REPORT ********************************/
# proc export data=&mylib2..FIELDREPORT outfile="&outpath1./Post_Round_Reports.xlsx" REPLACE label
# 	DBMS=xlsx;
# 	sheet="Price Taken Data";

FIELDREPORT=pd.DataFrame(mylib2.FIELDREPORT)
FIELDREPORT.to_excel(f'{outpath1}'/Post_Round_Reports.xlsx,sheet_name='Price Taken Data')

# /********** TOP 20 PRICE TAKEN & REVENUE CHANGE REPORT *********/
# proc export data=&mylib2..TOP20REPORT outfile="&outpath1./Post_Round_Reports.xlsx" REPLACE label
# 	DBMS=xlsx;
# 	sheet="Top20 Items Data";
TOP20REPORT=pd.DataFrame()



