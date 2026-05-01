"""Assign GNSS positions to ping rows by nearest spatial match.

Searches the OWENDO folder for CSV/NMEA-like files with lat/lon columns.
If found, projects GNSS lat/lon to EPSG:32632 and assigns nearest GNSS sample
to each ping (based on easting+false_east, northing). Updates
`analysis/output_new/merged_data_with_owendo_cols.csv` filling `Lat`/`Lon`.
"""
import os
from pathlib import Path
import pandas as pd
import numpy as np
from pyproj import Transformer
from scipy.spatial import cKDTree
import pandas as _pd
import datetime as _dt

BASE = Path('owendo-05-04-26-4-Outcome data_uzf')
OUT = Path('analysis/output_new/merged_data_with_owendo_cols.csv')

def find_gnss_candidates(base: Path):
    cands = []
    for p in base.rglob('*'):
        if p.is_file() and p.suffix.lower() in ('.csv', '.txt', '.log'):
            low = p.name.lower()
            if 'gps' in low or 'nmea' in low or 'gnss' in low or 'track' in low:
                cands.append(p)
            else:
                # peek into file header
                try:
                    with open(p, 'r', errors='ignore') as fh:
                        h = fh.readline()
                    if any(k in h.lower() for k in ('lat', 'lon', 'latitude', 'longitude')):
                        cands.append(p)
                except Exception:
                    pass
    return cands


def detect_latlon_cols(df: pd.DataFrame):
    cols = [c.lower() for c in df.columns]
    lat = None
    lon = None
    for opt in ['lat','latitude','gps_latitude']:
        if opt in cols:
            lat = df.columns[cols.index(opt)]
            break
    for opt in ['lon','longitude','lng','gps_longitude']:
        if opt in cols:
            lon = df.columns[cols.index(opt)]
            break
    return lat, lon


def main():
    if not OUT.exists():
        print('No merged OWENDO CSV to update:', OUT)
        return
    df = pd.read_csv(OUT)
    # read false_east from shn if available
    false_east = 500000.0
    shn = BASE / (BASE.name + '.shn')
    if shn.exists():
        try:
            import xml.etree.ElementTree as ET
            root = ET.parse(shn).getroot()
            fe = root.find('false_east')
            if fe is not None and fe.text:
                false_east = float(fe.text)
        except Exception:
            pass

    candidates = find_gnss_candidates(BASE)
    if not candidates:
        print('No GNSS candidate files found under', BASE)
        return

    # try each candidate until one works
    for p in candidates:
        try:
            gdf = pd.read_csv(p)
        except Exception:
            try:
                # try whitespace or nmea-like
                gdf = pd.read_csv(p, header=None)
            except Exception:
                continue
        latc, lonc = detect_latlon_cols(gdf)
        # detect time columns if any
        gtime = None
        for cand in ('datetime','time','timestamp'):
            if cand in [c.lower() for c in gdf.columns]:
                gtime = gdf.columns[[c.lower() for c in gdf.columns].index(cand)]
                break

        # if GNSS has lat/lon and ping table has datetime, try time-based matching first
        if latc and lonc and 'datetime_parsed' in df.columns and not df['datetime_parsed'].isna().all():
            try:
                gdf = gdf.dropna(subset=[latc, lonc])
                # parse GNSS times if present
                if gtime is not None:
                    try:
                        gdf['__t'] = _pd.to_datetime(gdf[gtime], errors='coerce')
                    except Exception:
                        gdf['__t'] = _pd.NaT
                else:
                    gdf['__t'] = _pd.NaT

                if gdf['__t'].notna().any():
                    # build time-indexed array and for each ping find nearest time
                    gtimes = gdf['__t'].astype('datetime64[ns]').values.astype('int64')
                    ptimes = df['datetime_parsed'].astype('datetime64[ns]').values.astype('int64')
                    # for each ping, find argmin absolute time difference
                    idx_time = np.abs(ptimes[:, None] - gtimes[None, :]).argmin(axis=1)
                    df['Lon'] = gdf[lonc].astype(float).values[idx_time]
                    df['Lat'] = gdf[latc].astype(float).values[idx_time]
                    df.to_csv(OUT, index=False)
                    print('Assigned GNSS by nearest time from', p, 'to', OUT)
                    return
            except Exception:
                pass

        # fallback: spatial nearest if lat/lon present
        if latc and lonc:
            try:
                gdf = gdf.dropna(subset=[latc, lonc])
                if gdf.empty:
                    continue
                # project GNSS to EPSG:32632
                transformer = Transformer.from_crs('EPSG:4326','EPSG:32632',always_xy=True)
                lons = gdf[lonc].astype(float).values
                lats = gdf[latc].astype(float).values
                gx, gy = transformer.transform(lons, lats)
                # build KDTree on GNSS projected coords
                tree = cKDTree(np.column_stack([gx, gy]))

                # prepare pings projected coords (CoordinateX likely easting without false_east)
                # try both with and without false_east
                pe = df['CoordinateX'].astype(float)
                pn = df['CoordinateY'].astype(float)
                pts = np.column_stack([pe.values, pn.values])
                dists, idx = tree.query(pts, k=1)
                # if distances are large, retry adding false_east
                if np.nanmedian(dists) > 1000:
                    pe2 = pe + false_east
                    pts2 = np.column_stack([pe2.values, pn.values])
                    dists2, idx2 = tree.query(pts2, k=1)
                    if np.nanmedian(dists2) < np.nanmedian(dists):
                        idx = idx2

                df['Lon'] = lons[idx]
                df['Lat'] = lats[idx]
                df.to_csv(OUT, index=False)
                print('Assigned GNSS positions from', p, 'to', OUT)
                return
            except Exception:
                pass

    print('No usable GNSS file among candidates:', candidates)


if __name__ == '__main__':
    main()
