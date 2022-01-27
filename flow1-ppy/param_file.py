import yaml

reduser = "sysuserid"
pwdsas = f"/home/{reduser}/pwd.sas"

with open(r"C:\Users\gopala.chennu\Desktop\Current\vs\Codes\master.yml", "r") as doc:
    file = yaml.load(doc, Loader=yaml.FullLoader)

cntry = file["cntry"]
envir = file["envir"]

wkpmix = f"/opt/sasdata/prod/Data/{cntry}/pmix/weekly"
dlpmix = "/opt/sasdata/prod/Datacntry/pmix/daily"
gcpmix = f"/opt/sasdata/prod/Data/{cntry}/pmix/gstcnts"
lookup = f"/opt/sasdata/prod/Data/{cntry}"
lisa = f"/opt/saswork/temp/lisa/PEReports/{cntry}."
engine = f"/opt/sasdata/{envir}/Data/{cntry}/PEReports/engine"
inter1 = f"/opt/sasdata/{envir}/Data/{cntry}/PEReports/working"
peRep = f"/opt/sasdata/{envir}/Data/{cntry}/PEReports/reports"
mnthdata = f"/opt/sasdata/{envir}/Data/{cntry}/MonthlyReports/data"
dellib = f"/opt/sasdata/dev/Data/{cntry}/deloitte/For_gbl_prcg"
dlvDpmix = f"/opt/sasdata/prod/Data/{cntry}/pmixXchannel/daily"
dlvWpmix = f"/opt/sasdata/prod/Data/{cntry}/pmixXchannel/weekly"
dlvgcpmx = f"/opt/sasdata/prod/Data/{cntry}/pmixXchannel/gstcnts"

progpath = f"/opt/sasdata/{envir}/SASCode/ReportingPackages/PEReports/{cntry}"
outpath1 = f"/opt/sasdata/{envir}/Data/{cntry}/PEReports/templates"
infile = f"/opt/sasdata/{envir}/SASCode/ReportingPackages/PEReports/{cntry}/infiles"


def country(cntry):

    if cntry == "USA":
        x = {"terr": 840, "currcde": 840, "cntr1": "US", "grssnet": "Net"}
        return x
    elif cntry == "Austria":
        x = {"terr": 40, "currcde": 978, "cntr1": "AT", "grssnet": "Gross"}
        return x
    elif cntry == "Italy":
        x = {"terr": 380, "currcde": 978, "cntr1": "IT", "grssnet": "Gross"}
        return x
    elif cntry == "France":
        x = {"terr": 250, "currcde": 978, "cntr1": "FR", "grssnet": "Gross"}
        return x
    elif cntry == "Russia":
        x = {"terr": 643, "currcde": 643, "cntr1": "RU", "grssnet": "Gross"}
        return x
    elif cntry == "Switzerland":
        x = {"terr": 756, "currcde": 756, "cntr1": "CH", "grssnet": "Gross"}
        return x
    elif cntry == "Spain":
        x = {"terr": 724, "currcde": 978, "cntr1": "ES", "grssnet": "Gross"}
        return x
    elif cntry == "Netherlands":
        x = {"terr": 528, "currcde": 978, "cntr1": "NL", "grssnet": "Gross"}
        return x


y = country(cntry)  # enter the country details and return it
print(y.items())
terr=y["terr"]
currcde=y["currcde"]
cntr1=y["cntr1"]
grssnet=y["grssnet"]

