# Looking at anomalies around leap years
# Uses BEST monthly land+ocean data

from prefect import task, Flow
import pandas as p

SRC_PATH = "/Volumes/data/temperature/best-monthly/Land_and_Ocean_complete.csv"
DEST_PATH = "/Volumes/data/temperature/best-monthly/Land_and_Ocean_monthly.csv"

# get the data loaded, drop unused stuff
@task
def load_drop ():
	unused_cols = ["Anomaly1","Unc1","Anomaly5", "Unc5", "Anomaly10", "Unc10", "Anomaly20", "Unc20"]
	df = p.read_csv(SRC_PATH)
	df_out = df.drop(columns=unused_cols)
	return df_out
	
@task 
def compute_by_month (df):
	dfgm = df.groupby(["Month", "Cycle4"]).mean()
	df_out = p.DataFrame()
	df_out['Anomaly'] = dfgm['Anomaly']
	return df_out
	
@task
def write_file (df):
	df.to_csv(DEST_PATH)
	return True
	
with Flow("best-monthly-anomaly") as flow:
	df = load_drop()
	df_result = compute_by_month(df)
	write_file(df_result)
	
# flow.run()
	