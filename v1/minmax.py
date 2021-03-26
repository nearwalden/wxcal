# Calculate min/max for each lat/lon

import files
import pandas as p

out_dir = files.ll1_dir + 'minmax/'

def max_temp_day():
	df_out_max = p.DataFrame()
	df_out_max5 = p.DataFrame()			
	for file in files.lat_lon_files():
		df = p.read_csv (file)
		if len(df) > 0:
			cols = files.lat_lon_cols(df)
			lat_lon_5 = cols[0]
			df = df.set_index('day_of_year')
			dfg = df.groupby('year')
			for col in cols:
				df_out_max[col] = dfg[col].idxmax()
			df_out_max5[lat_lon_5] = dfg['mean'].idxmax()
			print ('processed ' + lat_lon_5)
	df_out_max.to_csv(out_dir + 'max_lat_lon.csv')
	decades(df_out_max).to_csv(out_dir + 'max_lat_lon_dec.csv')	
	df_out_max5.to_csv(out_dir + 'max_lat_lon5.csv')
	decades(df_out_max5).to_csv(out_dir + 'max_lat_lon5_dec.csv')	
	
def min_temp_day():
	df_out_min = p.DataFrame()
	df_out_min5 = p.DataFrame()
	for file in files.lat_lon_files():
		df = p.read_csv (file)
		if len(df) > 0:
			cols = files.lat_lon_cols(df)
			lat_lon_5 = cols[0]
			# make year associated with winter
			df['year2'] = df['year'].shift(190)
			df['doy2'] = df['day_of_year']
			# make doy be part of previous year in the winter
			df.loc[(df.doy2 < 190), 'doy2'] = df.doy2 + 365
			# lop off the beginning which isn't useful
			df2 = df.iloc[190:].copy()
			df = df.set_index('doy2')
			dfg = df.groupby('year2')
			for col in cols:
				df_out_min[col] = dfg[col].idxmin()
			df_out_min5[lat_lon_5] = dfg['mean'].idxmin()
			print ('processed ' + lat_lon_5)
	df_out_min.to_csv(out_dir + 'min_lat_lon.csv')
	decades(df_out_min).to_csv(out_dir + 'min_lat_lon_dec.csv')
	df_out_min5.to_csv(out_dir + 'min_lat_lon5.csv')
	decades(df_out_min5).to_csv(out_dir + 'min_lat_lon5_dec.csv')	

def decades(df):
	df['decade'] = df.index.map(lambda x: str(x)[0:3])
	dfg = df.groupby('decade')
	df_out = dfg.mean()
	return df_out


