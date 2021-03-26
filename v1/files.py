# names of files and columns
file_loc = "/Volumes/data/temperature/"
ll1_dir = file_loc + "daily-1deg-sep2020/"
ll1_filename = "Complete_TAVG_Daily_LatLong1_"
ll1_filename_short = "TAVG_LL1_"

def lat_str (lat):
	if lat > 0:
		return str(lat) + "N"
	else:
		return str(-lat) + "S"
		
def lon_str (lon):
	if lon > 0:
		return str(lon) + "E"
	else:
		return str(-lon) + "W"

# return the lat_lon files	
def lat_lon_files ():
	files = []
	for lat in range(25, 49, 5):
		for lon in range(-119, -60, 5):
			files.append(ll1_dir + ll1_filename_short + "all_" + lat_str(lat) + "_" + lon_str(lon) + "_temp.csv")
	return files
	
cols_to_remove = ['Unnamed: 0', 'time', 'year', 'month', 'day','day_of_year', 'mean']
	
# returns the useful columns, except average
def lat_lon_cols (df):
	cols = df.columns.to_list()
	for col in cols_to_remove:
		cols.remove(col)
	return cols
	
				
	 