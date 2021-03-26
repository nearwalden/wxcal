# berkeley earth data cleanup
# Creating clean CSVs

import xarray as xr
import pandas as p
from prefect import task, Flow

land_limit = 0.8
    
# LatLong1 version of above
ll1_dir = "daily-1deg-sep2020/"
ll1_filename = "Complete_TAVG_Daily_LatLong1_"
ll1_filename_short = "TAVG_LL1_"

# create a time file for each decade
@task    
def process_time (year):
    path = ll1_dir + ll1_filename + str(year) + ".nc"
    ds = xr.open_dataset(path)
    df = ds['year'].to_dataframe()
    df = df.join(ds['month'].to_dataframe())
    df = df.join(ds['day'].to_dataframe())  
    df = df.join(ds['day_of_year'].to_dataframe())              
    print ("Processed year " + str (year))
    return (df)

# creates a DF over US with landmass > target     
@task
def process_clim_ll1 (year):
    path = ll1_dir + ll1_filename + str(year) + ".nc"
    ds = xr.open_dataset(path)
    dft = ds['temperature'].to_dataframe()
    dfc = ds['climatology'].to_dataframe()
    dfd = ds['day_of_year'].to_dataframe()
    dfl = ds['land_mask'].to_dataframe()
    print ("  created dataframes")
    df = dft.reset_index()
    df = df [(df.latitude > 22) & (df.latitude < 51) ]
    df = df [(df.longitude > -121) & (df.longitude < -60) ]   
    df = df.set_index(['time', 'latitude', 'longitude'])
    # adds dfl
    df = df.join(dfl)
    df = df[df.land_mask > land_limit]
    df = df.drop('land_mask', 1)
    df = df.dropna()
    print ("  limited to US, land > 80%")
    # adds day_of_year
    df = df.join(dfd, 'time')
    print('  added day_of_year')
    # changes to match day_number
    df['day_number'] = df['day_of_year'] - 1
    # df = df.drop('day_of_year', 1)
    print('  added day_number')
    df = df.reset_index()
    df = df.set_index(['day_number', 'latitude', 'longitude'])
    # df = df.drop('index', 1)
    print ('  reset index for climate data')
    # add in climatology data
    df = df.join(dfc)
    print ('  added climatology')
    df['temperature'] = df['temperature'] + df['climatology']
    df = df.drop('climatology', 1)
    print ('  turned temp into real temp')
    return (df)
    

# join in time
@task
def cleanup_ll1 (df, df_time):
    df = df.drop(['day_number', 'day_of_year'], 1)
    df = df.join(df_time.set_index('time'), on='time')
    print ("Processed year " + str (year))
    return (df)
    
@task
def write_ll1 (df, year):




# extract data based on lat, lon
def extract_data (df, lat_min, lat_max, lon_min, lon_max):
    l = len(df)
    df2 = df[(df.latitude < lat_max) & (df.latitude > lat_min)]
    df2 = df2[(df.longitude < lon_max) & (df.longitude > lon_min)]
    l2 = len(df2)
    print ("Original size = " + str(l) + "rows, new size = " + str(l2) + "rows" )
    return df2
    
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
    
def extract_loc (lat, lon):
    # cycle through years
    time_offset = 0
    for offset in range(0, 140, 10):
        year = 1880 + offset
        temp_path = ll1_dir + ll1_filename + str(year) + "_temp.csv"
        df_time = p.read_csv (temp_path)
        df2 = df_time[(df_time.latitude == lat + 0.5) & (df_time.longitude == lon + 0.5)].copy()
        if offset == 0:
            df_all = df2
        else: 
            # add time_offset
            df2.time += time_offset
            df_all = df_all.append(df2)
        # figure out max time
        days = df2.time.max()
        time_offset += days
    df_all = df_all.drop(['Unnamed: 0', 'latitude', 'longitude'], 1)
    outpath = ll1_dir + ll1_filename_short + "all_" + lat_str(lat) + "_" + lon_str(lon) + "_temp.csv"
    df_all.to_csv (outpath)
    print ("Created file for " + lat_str(lat) + ", " + lon_str(lon) + ". " + str(len(df_all)) + " records.")


def extract_loc_25 (lat, lon):
    # cycle through years
    time_offset = 0
    cols = 0
    for offset in range(0, 140, 10): 
        year = 1880 + offset
        temp_path = ll1_dir + ll1_filename + str(year) + "_temp.csv"
        df_temp = p.read_csv (temp_path)
        longest = 0
        first = True
        # determine longest
        for i in range (0, 5):
            for j in range (0, 5):
                df2 = df_temp[(df_temp.latitude == lat + 0.5 + i) & (df_temp.longitude == lon + 0.5 + j)].copy()
                if len(df2) > longest:
                    longest = len(df2)
        for i in range (0, 5):
            for j in range (0, 5):
                df2 = df_temp[(df_temp.latitude == lat + 0.5 + i) & (df_temp.longitude == lon + 0.5 + j)].copy()
                if len(df2) ==  longest:
                    if first == True:
                        cols = 1
                        df3 = df2.copy()
                        df3['total'] = df3['temperature']
                        df3 = df3.rename(columns={'temperature': lat_str(lat) + '_' + lon_str(lon)})  
                        first = False
                    else:
                        temp = df2.temperature.to_list()
                        df3[lat_str(lat + i) + '_' + lon_str(lon + j)] = temp
                        df3['total'] = df3['total'] + df3[lat_str(lat + i) + '_' + lon_str(lon + j)]
                        cols += 1
        df3['mean'] = df3['total']/cols
        if offset == 0:
            df_all = df3
        else: 
            # add time_offset
            df3.time += time_offset
            df_all = df_all.append(df3)
        # figure out max time
        days = df2.time.max()
        time_offset += days
        # print ("Complete for year " + str(year))
    df_all = df_all.drop(['Unnamed: 0', 'latitude', 'longitude', 'total'], 1)
    outpath = ll1_dir + ll1_filename_short + "all_" + lat_str(lat) + "_" + lon_str(lon) + "_temp.csv"
    df_all.to_csv (outpath)
    print ("Created file for " + lat_str(lat) + ", " + lon_str(lon) + ". " + str(len(df_all)) + " records.")
    
def extract_loc_25_all ():
    for lat in range(25, 49, 5):
        for lon in range(-119, -60, 5):
            extract_loc_25 (lat, lon)
    print ("All done")
    
