# Calculate min/max for each lat/lon

import files
import pandas as p

out_dir = files.ll1_dir + 'minmax/'

def min_max():
	df_out_min = p.DataFrame()
	df_out_max = p.DataFrame()
	df_out_min5 = p.DataFrame()
	df_out_max5 = p.DataFrame()			
	for file in files.lat_lon_files():
		df = p.read_csv (file)
		if len(df) > 0:
			cols = files.lat_lon_cols(df)
			lat_lon_5 = cols[0]
			df = df.set_index('day_of_year')
			dfg = df.groupby('year')
			for col in cols:
				df_out_min[col] = dfg[col].idxmin()
				df_out_max[col] = dfg[col].idxmax()
			df_out_min5[lat_lon_5] = dfg['mean'].idxmin()
			df_out_max5[lat_lon_5] = dfg['mean'].idxmax()
			print ('processed ' + lat_lon_5)
	df_out_min.to_csv(out_dir + 'min_lat_lon.csv')
	decades(df_out_min).to_csv(out_dir + 'min_lat_lon_dec.csv')
	df_out_max.to_csv(out_dir + 'max_lat_lon.csv')
	decades(df_out_max).to_csv(out_dir + 'max_lat_lon_dec.csv')	
	df_out_min5.to_csv(out_dir + 'min_lat_lon5.csv')
	decades(df_out_min5).to_csv(out_dir + 'min_lat_lon5_dec.csv')	
	df_out_max5.to_csv(out_dir + 'max_lat_lon5.csv')
	decades(df_out_max5).to_csv(out_dir + 'max_lat_lon5_dec.csv')	
	
def decades(df):
	df['decade'] = df.index.map(lambda x: str(x)[0:3])
	dfg = df.groupby('decade')
	df_out = dfg.mean()
	return df_out


