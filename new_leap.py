# Calculates a different kind of leap year, compares to normal leap year

import files
import pandas as p
# import numpy as np
import matplotlib.pyplot as plt
import math

BASE_DIR = "/mnt/c/Users/nearw/data/temperature/"
DIR = BASE_DIR + "daily-1deg-sep2020/"
SHORT_NAME = "TAVG_LL1_"
OUT_DIR = BASE_DIR + "new-leap/"


def process():
    # df_out_norm_mo = p.DataFrame()
    # df_out_new_leap_mo = p.DataFrame()
    df_out_norm_yr = p.DataFrame()
    df_out_new_leap_yr = p.DataFrame()
    df_out_tropical_yr = p.DataFrame()
#  = p.DataFrame()
    for file in files.lat_lon_files(DIR, SHORT_NAME):
        df = p.read_csv(file)
        years = find_full_years(df)
        if len(years) > 0:
            cols = files.lat_lon_cols(df)
            # only use the mean of the col
            # use the first lat_lon as the name for the new column
            lat_lon = cols[0]
            # df_out_norm_mo[lat_lon] = calc_normal_leap_mo(df, years)['temp']
            # df_out_new_leap_mo[lat_lon] = calc_new_leap_mo(df, years)['temp']
            df_out_norm_yr[lat_lon] = calc_normal_leap_yr(df, years)['temp']
            df_out_new_leap_yr[lat_lon] = calc_new_leap_yr(df, years)['temp']
            df_out_tropical_yr[lat_lon] = calc_tropical_yr(df, years)['temp']
            # df_out_no_leap_mo[lat_lon] = no_leap(df, years)['temp']
            print('processed ' + lat_lon)
    # df_out_norm_mo.reset_index(inplace=True)
    # df_out_new_leap_mo.reset_index(inplace=True)
    df_out_norm_yr.reset_index(inplace=True)
    df_out_new_leap_yr.reset_index(inplace=True)
    df_out_tropical_yr.reset_index(inplace=True)
    # df_out_no_leap_mo.reset_index(inplace=True)
    # df_out_norm_mo.to_csv(OUT_DIR + 'normal_mo.csv')
    # df_out_new_leap_mo.to_csv(OUT_DIR + 'new_leap_mo.csv')
    # df_out_no_leap_mo.to_csv(OUT_DIR + 'no_leap_mo.csv')
    df_out_norm_yr.to_csv(OUT_DIR + 'normal_yr.csv')
    df_out_new_leap_yr.to_csv(OUT_DIR + 'new_leap_yr.csv')
    df_out_tropical_yr.to_csv(OUT_DIR + 'tropical_yr.csv')
    # df_out_no_leap_yr = df_out_no_leap_mo.groupby('year').mean()
    # df_out_no_leap_yr.to_csv(OUT_DIR + 'no_leap_yr.csv')
    return True


# figure out which years are full, return list
# use number of days
def find_full_years(df):
    years = []
    for year in df['year'].unique():
        if len(df[df.year == year]) > 364:
            years.append(year)
    return years


# check if leap year
def is_leap_year(year):
    if (year % 4 == 0) & (year != 1900):
        return True
    else:
        return False

#####################
# Month tools
######################


# normal year
NORMAL_YEAR = [0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]

# leap years
LEAP_YEAR = [0, 31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]


# process a year of data, return year
def process_months_in_year(df, year, days):
    df_year = df[df.year == year]
    out = []
    start = 0
    for i in range(1, 13):
        end = start + days[i]
        df_month = df_year[(df_year.day_of_year >= start) &
                           (df_year.day_of_year < end)]
        out.append({'year': year, 'month': i, 'temp': df_month['mean'].mean()})
        start += days[i]
    return out


# calculate normal months
def calc_normal_leap_mo(df, years):
    out = []
    # run through the years
    for year in years:
        if is_leap_year(year):
            out += process_months_in_year(df, year, LEAP_YEAR)
        else:
            out += process_months_in_year(df, year, NORMAL_YEAR)
    outdf = p.DataFrame(out)
    return outdf.set_index(['year', 'month'])


# calculate new-leap months
def calc_new_leap_mo(df, years):
    out = []
    # run through the years
    for year in years:
        out += process_months_in_year(df, year, NORMAL_YEAR)
    outdf = p.DataFrame(out)
    return outdf.set_index(['year', 'month'])


#####################
# Calculate years tools
######################


# process a year of data, return year
# BEST does duplicate day number in early July
# should drop day at end of year

def process_full_year(df, year, drop_last):
    df_year = df[df.year == year]
    if drop_last:
        df_year = df_year[df_year.day_of_year != 365]
    out = [{'year': year,
            'temp': df_year['mean'].mean()}]
    return out


# calculate normal years
def calc_normal_leap_yr(df, years):
    out = []
    # run through the years
    for year in years:
        out += process_full_year(df, year, False)
    outdf = p.DataFrame(out)
    return outdf.set_index('year')


# calculate new-leap years
def calc_new_leap_yr(df, years):
    out = []
    # run through the years
    for year in years:
        if is_leap_year(year):
            out += process_full_year(df, year, True)
        else:
            out += process_full_year(df, year, False)
    outdf = p.DataFrame(out)
    return outdf.set_index('year')


# tropical year in days
TROPICAL_YR = 365.2421


# calculate tropical years
def calc_tropical_yr(df, years):
    out = []
    df.sort_values('time', inplace=True)
    dft = df.set_index('time')
    start_day = dft[dft.year == years[0]].index.min()
    # run through the years
    for year in years:
        end_day = start_day + TROPICAL_YR
        if end_day > len(df):
            break
        first_full = math.ceil(start_day)
        last_full = math.floor(end_day)
        temp = df[(df.index >= first_full) & (df.index < last_full)]['mean'].sum()
        # print(temp)
        first_partial = first_full - start_day
        if first_partial != 0:
            temp += df.loc[first_full - 1]['mean'] * first_partial
        last_partial = end_day - last_full
        if last_partial != 0:
            temp += df.loc[last_full]['mean'] * last_partial
        out.append({'year': year, 'temp': temp/TROPICAL_YR})
        start_day = end_day
    outdf = p.DataFrame(out)
    return outdf.set_index('year')


# calculates how much the last day changes the average
def leap_year_delta():
    df_out_delta = p.DataFrame()
    for file in files.lat_lon_files(DIR, SHORT_NAME):
        df = p.read_csv(file)
        years = find_full_years(df)
        if len(years) > 0:
            cols = files.lat_lon_cols(df)
            lat_lon = cols[0]
            df_out_delta[lat_lon] = calc_leap_delta(df, years)['temp']
            # print('processed ' + lat_lon)
    df_out_delta.reset_index(inplace=True)
    df_out_delta.to_csv(OUT_DIR + 'leap_deltas.csv')
    out = []
    for loc in df_out_delta.columns.to_list().remove('year'):
        delta = df_out_delta[loc]
        delta = delta[delta != 0]
        out.append({'loc': loc, 'mean': delta.mean(), 'max': delta.max()})
    return p.DataFrame(out)


# calculate last of leap years
def calc_leap_delta(df, years):
    dfs = df.sort_values(['year', 'day_of_year'])
    # dfi = df.set_index(['year', 'day_of_year'])
    out = []
    # run through the years
    for year in years:
        if is_leap_year(year):
            last = dfs[dfs.year == year].iloc[-1]['mean']
            mean = df[df.year == year]['mean'].mean()
            delta = (last - mean)/365
            out.append({'year': year, 'temp': delta})
    # print(out)
    outdf = p.DataFrame(out)
    return outdf.set_index('year')


######################
# Comparing tools
######################


COLS_TO_REMOVE = ['Unnamed: 0', 'year']


def year_locs(df):
    cols = df.columns.to_list()
    for col in COLS_TO_REMOVE:
        cols.remove(col)
    return cols


# compare two methods in each location
def compare_new():
    normaldf = p.read_csv(OUT_DIR + 'normal_yr.csv')
    new_leapdf = p.read_csv(OUT_DIR + 'new_leap_yr.csv')
    out = []
    for loc in year_locs(normaldf):
        delta = new_leapdf[loc] - normaldf[loc]
        delta = delta[delta != 0]
        out.append({'loc': loc, 'mean': delta.mean(), 'max': delta.max()})
    return p.DataFrame(out)


# compare two methods in each location
def compare_tropical():
    normaldf = p.read_csv(OUT_DIR + 'normal_yr.csv')
    tropicaldf = p.read_csv(OUT_DIR + 'tropical_yr.csv')
    out = []
    for loc in year_locs(normaldf):
        delta = tropicaldf[loc] - normaldf[loc]
        delta = delta[delta != 0]
        out.append({'loc': loc, 'mean': delta.mean(), 'max': delta.max()})
    return p.DataFrame(out)


# do measurement of 4 years
def four_year_stats(df):
    diff = p.DataFrame()
    locs = year_locs(df)
    # only do 1901 and later
    df = df[df.year > 1900]
    df = df.set_index('year')
    for loc in locs:
        diff[loc] = df[loc].diff()
    diff = diff.reset_index()
    diff['year4'] = diff.year % 4
    diffg = diff.groupby('year4')
    out = p.DataFrame()
    out['sum'] = 0
    for loc in locs:
        out[loc] = diffg[loc].mean()
        out['sum'] = out['sum'] + out[loc]
    out['sum'] = out['sum']/len(locs)
    return out


######################
# Plotting stuff
######################


# plot wrapper when doing one
def plot_loc(lat, lon, start=1880, end=2020, path=None):
    normaldf = p.read_csv(OUT_DIR + 'normal_yr.csv').set_index('year')
    new_leapdf = p.read_csv(OUT_DIR + 'new_leap_yr.csv').set_index('year')
    plot_loc_from_df(normaldf, new_leapdf, lat, lon, start, end, path)


# plot inner core
def plot_loc_from_df(normaldf, new_leapdf, lat, lon, start, end, path):
    col = files.lat_str(lat) + '_' + files.lon_str(lon)
    plotdf = p.DataFrame()
    plotdf['normal'] = normaldf[col]
    plotdf['new_leap'] = new_leapdf[col]
    plt.figure()
    plotdf[(plotdf.index >= start) & (plotdf.index < end)].plot()
    plt.legend()
    if path is not None:
        plt.savefit(path)


# plot wrapper when doing one
def plot_delta(lat, lon, start=1880, end=2020, path=None):
    normaldf = p.read_csv(OUT_DIR + 'normal_yr.csv').set_index('year')
    new_leapdf = p.read_csv(OUT_DIR + 'new_leap_yr.csv').set_index('year')
    plot_delta_from_df(normaldf, new_leapdf, lat, lon, start, end, path)


# plot inner core
def plot_delta_from_df(normaldf, new_leapdf, lat, lon, start, end, path):
    col = files.lat_str(lat) + '_' + files.lon_str(lon)
    plotdf = p.DataFrame()
    plotdf['normal'] = normaldf[col]
    plotdf['new_leap'] = new_leapdf[col]
    plotdf['delta'] = plotdf['new_leap'] - plotdf['normal']
    plt.figure()
    plotdf[(plotdf.index >= start) & (plotdf.index < end)]['delta'].plot()
    plt.legend()
    if path is not None:
        plt.savefit(path)