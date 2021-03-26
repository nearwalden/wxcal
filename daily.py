# tools to work with simple daily files

import pandas as p
import files


# look at jumps from year to year based on anomalies
# uses the Berkeley global daily file
def four_year_stats_day():
    df = p.read_csv(files.ll1_dir + 'Complete_TAVG_daily.csv')
    # only do 1901 and later
    df = df[df.year > 1900]
    dfg = df.groupby('year')
    years = p.DataFrame()
    years['temp'] = dfg['temp'].mean()
    years['diff'] = years['temp'].diff()
    years = years.reset_index()
    years['year4'] = years.year % 4
    yearsg = years.groupby('year4')
    out = p.DataFrame()
    out['diff'] = yearsg['diff'].mean()
    return out


# look at jumps from year to year based on anomalies
# uses the Berkeley global annual file
def four_year_stats_year(col='air'):
    df = p.read_csv(files.ll1_dir + 'Land and Ocean summary-clean.csv')
    # only do 1901 and later
    dfy = df[df.year > 1900].copy()
    dfy['diff'] = dfy[col].diff()
    dfy['year4'] = dfy.year % 4
    dfyg = dfy.groupby('year4')
    out = p.DataFrame()
    out['diff'] = dfyg['diff'].mean()
    return out
