# Looking at anomalies around leap years
# Creates one column per month

import pandas as p

BASE_PATH = "/mnt/c/Users/nearw/data/temperature/"
# BASE_PATH = "/Volumes/data/temperature/"

SRC_PATH = BASE_PATH + "best-monthly/Land_and_Ocean_complete.csv"
DEST_PATH = BASE_PATH + "best-monthly/Land_and_Ocean_complete_by_month.csv"

# months
MONTHS=["nothing", "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]

def by_month ():
    df = p.read_csv(SRC_PATH)
    outdf = p.DataFrame()
    df = df.set_index('Year')
    for i in range(1, 13):
        mo = MONTHS[i]
        outdf[mo] = df[df.Month == i]['Anomaly']
    outdf.to_csv(DEST_PATH)
    return outdf

