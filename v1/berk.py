# berkeley earth data cleanup
# Creating clean CSVs

import xarray as xr
import pandas as p

land_limit = 0.8



# creates a DF with climatology data and day_number added to each record
def add_clim (ds, dft):
    dfc = ds['climatology'].to_dataframe()
    # don't need lat, lon
    dfc = dfc.drop(['latitude', 'longitude'], 1)
    dfd = ds['day_of_year'].to_dataframe()
    # adds day_of_year
    df = dft.drop(['latitude', 'longitude'], 1)
    df = df.join(dfd, 'time')
    print('added day_of_year')
    # changes to match day_number
    df['day_number'] = df['day_of_year'] - 1
    df = df.drop('day_of_year', 1)
    print('added day_number')
    df = df.reset_index()
    df = df.set_index(['day_number', 'map_points'])
    # df = df.drop('index', 1)
    print ('reset index for climate data')
    # add in climatology data
    df = df.join(dfc)
    print ('added climatology')
    df['temperature'] = df['temperature'] + df['climatology']
    print ('turned temp into real temp')
    return (df)

# returns df with lat, lon, map_point
def add_lat_lon (ds, dfm):
    return df

# returns df with year, month, day, doy and time
def add_dmy (ds):
    return df
    
# LatLong1 version of above

ll1_dir = "daily-1deg-sep2020/"
ll1_filename = "Complete_TAVG_Daily_LatLong1_"
ll1_filename_short = "TAVG_LL1_"

# creates a DF with climatology data and day_number added to each record
def add_clim_ll1 (ds):
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

def process_clim_ll1 ():
    # cycle through years
    time_offset = 0
    for offset in range(0, 140, 10):
        year = 1880 + offset
        path = ll1_dir + ll1_filename + str(year) + ".nc"
        ds = xr.open_dataset(path)
        df = add_clim_ll1 (ds)
        outpath = ll1_dir + ll1_filename + str(year) + "_temp.csv"
        df.to_csv (outpath)
        print ("Processed year " + str (year))
        if offset == 0:
            df_all = df
        else: 
            # add time_offset
            df.time += time_offset
            df_all = df_all.append(df)
        # figure out max time
        days = df.time.max()
        time_offset += days
    outpath = ll1_dir + ll1_filename + "_all_temp.csv"
    df_all.to_csv (outpath)
    
def cleanup_ll1 ():
    # cycle through years
    time_offset = 0
    for offset in range(0, 140, 10):
        year = 1880 + offset
        temp_path = ll1_dir + ll1_filename + str(year) + "_temp.csv"
        df = p.read_csv (temp_path)
        time_path = ll1_dir + ll1_filename + str(year) + "_time.csv"        
        df_time = p.read_csv (time_path)
        df = df.drop(['day_number', 'day_of_year'], 1)
        df = df.join(df_time.set_index('time'), on='time')
        outpath = ll1_dir + ll1_filename + str(year) + "_temp.csv"
        df.to_csv (outpath)
        print ("Processed year " + str (year))


# create a time file for each decade    
def process_time ():
    # cycle through years
    time_offset = 0
    for offset in range(0, 140, 10):
        year = 1880 + offset
        path = ll1_dir + ll1_filename + str(year) + ".nc"
        ds = xr.open_dataset(path)
        df = ds['year'].to_dataframe()
        df = df.join(ds['month'].to_dataframe())
        df = df.join(ds['day'].to_dataframe())  
        df = df.join(ds['day_of_year'].to_dataframe())              
        outpath = ll1_dir + ll1_filename + str(year) + "_time.csv"
        df.to_csv (outpath)
        print ("Processed year " + str (year))


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
    
