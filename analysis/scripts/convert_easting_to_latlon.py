"""Convert easting/northing in merged CSV to Lat/Lon using pyproj.
Uses false_east/false_north from .shn if present (adds to easting/northing).
Defaults to EPSG:32631 (UTM zone 31N) near longitude ~9E.
Writes output to analysis/output_new/merged_data_with_latlon.csv
"""
import os
import pandas as pd
from xml.etree import ElementTree as ET
from pyproj import Transformer

BASE='owendo-05-04-26-4-Outcome data_uzf'
SHN1=os.path.join(BASE, BASE+'_backup.shn')
SHN2=os.path.join(BASE, BASE+'.shn')
CSV_IN='analysis/output_new/merged_data_with_owendo_cols.csv'
CSV_OUT='analysis/output_new/merged_data_with_latlon.csv'

def read_shn(shnpath):
    if not os.path.exists(shnpath):
        return {}
    try:
        tree=ET.parse(shnpath)
        root=tree.getroot()
        vals={}
        for child in root:
            vals[child.tag]=child.text
        return vals
    except Exception as e:
        print('Error parsing',shnpath,e)
        return {}

sh=read_shn(SHN1)
if not sh:
    sh=read_shn(SHN2)

false_east=float(sh.get('false_east') or 0)
false_north=float(sh.get('false_north') or 0)
zone_shn=sh.get('zone')
print('Read false_east=',false_east,'false_north=',false_north,'zone=',zone_shn)

if not os.path.exists(CSV_IN):
    print('Input CSV missing:',CSV_IN)
    raise SystemExit(1)

df=pd.read_csv(CSV_IN)
if 'easting' not in df.columns or 'northing' not in df.columns:
    print('Missing easting/northing columns in',CSV_IN)
    raise SystemExit(1)

# compute projected coordinates by adding false easting/northing when reasonable
proj_x = df['easting'].astype(float) + false_east
proj_y = df['northing'].astype(float) + false_north

# default EPSG guess: UTM zone 31N (EPSG:32631)
from_crs='EPSG:32631'
try_crs_list=['EPSG:32631','EPSG:4326']
# try transforming; if fails, report
for from_crs in try_crs_list:
    try:
        t=Transformer.from_crs(from_crs,'EPSG:4326',always_xy=True)
        lon,lat = t.transform(proj_x.values, proj_y.values)
        if (abs(lon.mean())>1 and abs(lat.mean())>0.0001) or from_crs=='EPSG:4326':
            df['Lon']=lon
            df['Lat']=lat
            df.to_csv(CSV_OUT,index=False)
            print('Wrote',CSV_OUT,'using',from_crs)
            break
    except Exception as e:
        print('transform failed for',from_crs,e)

print('\nSample Lat/Lon (first 5):')
print(df[['easting','northing','Lon','Lat']].head())
