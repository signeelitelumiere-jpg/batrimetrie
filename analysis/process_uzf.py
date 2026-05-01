import zipfile
import tempfile
import shutil
import sqlite3
import struct
from pathlib import Path
import pandas as pd
import numpy as np
try:
    from pyproj import Transformer
except Exception:
    Transformer = None


def _ensure_outdir(outdir: Path):
    outdir.mkdir(parents=True, exist_ok=True)


def _decode_blob_value(b: bytes):
    # Try to decode a bytes blob as float32 (or float16) and return scalar if possible
    if not isinstance(b, (bytes, bytearray)):
        return b
    lb = len(b)
    if lb == 0:
        return None
    try:
        if lb % 4 == 0:
            arr = np.frombuffer(b, dtype=np.float32)
            if arr.size == 1:
                return float(arr[0])
            return arr.tolist()
        elif lb % 2 == 0:
            arr = np.frombuffer(b, dtype=np.float16)
            if arr.size == 1:
                return float(arr[0])
            return arr.tolist()
    except Exception:
        pass
    # fallback: attempt to interpret as utf-8 text
    try:
        return b.decode('utf-8', errors='ignore')
    except Exception:
        return repr(b)


def _decode_dataframe_blobs(df: pd.DataFrame) -> pd.DataFrame:
    # For object dtype columns, attempt to decode bytes values
    for col in df.columns:
        if df[col].dtype == object:
            sample = df[col].dropna().head(30)
            if any(isinstance(x, (bytes, bytearray)) for x in sample):
                df[col] = df[col].map(_decode_blob_value)
    return df


def _find_column(df: pd.DataFrame, candidates):
    for c in df.columns:
        low = c.lower()
        for cand in candidates:
            if cand in low:
                return c
    return None


def process_uzf_file(uzf_path, outdir='analysis/output_new'):
    p = Path(uzf_path)
    outdir = Path(outdir)
    _ensure_outdir(outdir)

    tmpdir = Path(tempfile.mkdtemp(prefix='uzf_'))
    extracted = tmpdir / 'extracted'
    extracted.mkdir()
    try:
        # If file is .uzf treat as zip
        if p.suffix.lower() in ['.uzf', '.zip']:
            ztmp = tmpdir / 'in.zip'
            shutil.copy(p, ztmp)
            try:
                with zipfile.ZipFile(ztmp, 'r') as zf:
                    zf.extractall(extracted)
            except zipfile.BadZipFile:
                # maybe it's already an unzippable structure
                pass
        else:
            # treat path as folder
            extracted = p

        # find sqlite .data files
        db_files = list(extracted.rglob('*.data'))
        if not db_files:
            # try common Backup path
            db_files = list((extracted).glob('Backup/*.data'))
        if not db_files:
            raise FileNotFoundError('No .data (SQLite) file found inside the UZF archive.')

        # Prefer a .data file that contains gps_data rows (Survey), fallback to first
        db = None
        for cand in db_files:
            try:
                conn_test = sqlite3.connect(str(cand))
                tabs = [r[0] for r in conn_test.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()]
                if 'gps_data' in tabs:
                    try:
                        cnt = conn_test.execute('SELECT COUNT(*) FROM gps_data').fetchone()[0]
                        conn_test.close()
                        if cnt and int(cnt) > 0:
                            db = cand
                            break
                    except Exception:
                        conn_test.close()
                        continue
                conn_test.close()
            except Exception:
                continue
        if db is None:
            db = db_files[0]
        conn = sqlite3.connect(str(db))
        # read gps_data if present
        tables = [r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()]
        result = {}
        if 'gps_data' in tables:
            df_gps = pd.read_sql_query('SELECT * FROM gps_data', conn)
            df_gps = _decode_dataframe_blobs(df_gps)
            gps_csv = outdir / (p.stem + '_gps.csv')
            df_gps.to_csv(gps_csv, index=False)
            result['gps_csv'] = str(gps_csv)
        else:
            df_gps = None

        if 'boat_multi_data' in tables:
            df_bmd = pd.read_sql_query('SELECT * FROM boat_multi_data', conn)
            df_bmd = _decode_dataframe_blobs(df_bmd)
            bmd_csv = outdir / (p.stem + '_pings.csv')
            df_bmd.to_csv(bmd_csv, index=False)
            result['pings_csv'] = str(bmd_csv)
        else:
            df_bmd = None

        # For safety: export raw tables and their column lists; do NOT guess mappings.
        tables_info = {}
        try:
            for tbl in tables:
                df_tbl = pd.read_sql_query(f"SELECT * FROM {tbl}", conn)
                df_tbl = _decode_dataframe_blobs(df_tbl)
                out_csv = outdir / f"{p.stem}_{tbl}.csv"
                df_tbl.to_csv(out_csv, index=False)
                tables_info[tbl] = {
                    'csv': str(out_csv),
                    'columns': list(df_tbl.columns)
                }
        except Exception:
            pass

        result['tables'] = tables_info

        # Attempt an authoritative automatic merge when gps_data exists
        try:
            if 'gps_data' in tables:
                dfg = pd.read_sql_query('SELECT * FROM gps_data', conn)
                dfg = _decode_dataframe_blobs(dfg)
                # build merged dataframe using explicit column names
                m = pd.DataFrame()
                # easting / northing (nez_x / nez_y) are present in this schema
                if 'nez_x' in dfg.columns:
                    m['easting'] = pd.to_numeric(dfg['nez_x'], errors='coerce')
                if 'nez_y' in dfg.columns:
                    m['northing'] = pd.to_numeric(dfg['nez_y'], errors='coerce')
                # depth: prefer high_depth then low_depth then try point_name
                if 'high_depth' in dfg.columns:
                    m['depth'] = pd.to_numeric(dfg['high_depth'], errors='coerce')
                elif 'low_depth' in dfg.columns:
                    m['depth'] = pd.to_numeric(dfg['low_depth'], errors='coerce')
                elif 'point_name' in dfg.columns:
                    # some datasets encode depth in point_name like '1.120'
                    m['depth'] = pd.to_numeric(dfg['point_name'], errors='coerce')

                # lat/lon
                if 'latitude' in dfg.columns:
                    m['Lat'] = pd.to_numeric(dfg['latitude'], errors='coerce')
                if 'longitude' in dfg.columns:
                    m['Lon'] = pd.to_numeric(dfg['longitude'], errors='coerce')

                # datetime from utcTime (try ms then s)
                if 'utcTime' in dfg.columns:
                    try:
                        m['datetime'] = pd.to_datetime(dfg['utcTime'], unit='ms', errors='coerce')
                        if m['datetime'].isna().all():
                            m['datetime'] = pd.to_datetime(dfg['utcTime'], unit='s', errors='coerce')
                    except Exception:
                        m['datetime'] = pd.NaT

                # GroundH(H): prefer 'altitude' or 'annerHigh'
                gh = None
                if 'altitude' in dfg.columns:
                    gh = pd.to_numeric(dfg['altitude'], errors='coerce')
                elif 'annerHigh' in dfg.columns:
                    gh = pd.to_numeric(dfg['annerHigh'], errors='coerce')
                if gh is not None:
                    m['GroundH(H)'] = gh

                # compute z_water and z_bed if possible
                if 'GroundH(H)' in m.columns:
                    m['z_water'] = m['GroundH(H)']
                elif 'Lat' in m.columns and 'Lon' in m.columns:
                    # fallback: no ground elevation available, set 0
                    m['z_water'] = 0.0
                if 'depth' in m.columns:
                    m['z_bed'] = m['z_water'] - m['depth']

                # save merged CSV
                merged_auto = outdir / (p.stem + '_merged_auto.csv')
                m.to_csv(merged_auto, index=False)
                result['merged_csv'] = str(merged_auto)

                # Attempt to produce a canonical GPS-like CSV with Lat/Lon if possible.
                # Priority: use exported gps_data if present; otherwise derive from easting/northing.
                try:
                    canonical_gps = outdir / (p.stem + '_gps_data.csv')
                    created_gps = None
                    # prefer existing gps export
                    if df_gps is not None:
                        # if gps already has Lat/Lon columns, copy to canonical
                        gps_df = df_gps.copy()
                        # normalize possible column names
                        if any(c.lower() in ('latitude','lat') for c in gps_df.columns) and any(c.lower() in ('longitude','lon') for c in gps_df.columns):
                            gps_df.to_csv(canonical_gps, index=False)
                            created_gps = canonical_gps
                    # else, try to project easting/northing to lat/lon
                    if created_gps is None and 'easting' in m.columns and 'northing' in m.columns and Transformer is not None:
                        xs = pd.to_numeric(m['easting'], errors='coerce').fillna(0).values.astype(float)
                        ys = pd.to_numeric(m['northing'], errors='coerce').fillna(0).values.astype(float)
                        # heuristic false offsets
                        false_east = 0.0
                        false_north = 0.0
                        if xs.max() < 1e6:
                            false_east = 500000.0
                            xs = xs + false_east
                        if ys.max() < 1e6:
                            false_north = 4000000.0
                            ys = ys + false_north
                        try:
                            transformer = Transformer.from_crs('EPSG:32632', 'EPSG:4326', always_xy=True)
                            lon, lat = transformer.transform(xs, ys)
                            m['Lon'] = lon
                            m['Lat'] = lat
                            m.to_csv(canonical_gps, index=False)
                            created_gps = canonical_gps
                        except Exception:
                            created_gps = None
                    if created_gps is not None:
                        result['gps_data_csv'] = str(created_gps)
                except Exception:
                    pass

                # write OWENDO TXT with exact header string
                ow_txt = outdir / (p.stem + '_OWENDO-BATHY-SURVEY-generated.txt')
                header = 'CoordinateY CoordinateX h GroundH(H) Lat Lon Locked Sats Status'
                lines = [header]
                for _, rr in m.iterrows():
                    cy = rr.get('northing', '')
                    cx = rr.get('easting', '')
                    h = rr.get('depth', '')
                    ghv = rr.get('GroundH(H)', '')
                    lat = rr.get('Lat', '')
                    lon = rr.get('Lon', '')
                    lines.append(f"{cy} {cx} {h} {ghv} {lat} {lon} 0 0 0")
                ow_txt.write_text('\n'.join(lines), encoding='utf-8')
                result['owendo_txt'] = str(ow_txt)

                # try a simple 3D scatter (html + png)
                try:
                    import plotly.graph_objects as go
                    dfz = m.dropna(subset=['easting', 'northing', 'z_bed']) if 'z_bed' in m.columns else m.dropna(subset=['easting', 'northing', 'depth'])
                    if not dfz.empty:
                        fig = go.Figure(data=[go.Scatter3d(x=dfz['easting'], y=dfz['northing'], z=(dfz.get('z_bed') if 'z_bed' in dfz.columns else dfz['depth']), mode='markers', marker=dict(size=2))])
                        fig.update_layout(title=f'UZF: {p.name}', scene=dict(xaxis_title='Easting', yaxis_title='Northing', zaxis_title='Z'))
                        html_out = outdir / (p.stem + '_3d.html')
                        png_out = outdir / (p.stem + '_3d.png')
                        fig.write_html(str(html_out), include_plotlyjs='cdn')
                        result['3d_html'] = str(html_out)
                        try:
                            fig.write_image(str(png_out), width=1400, height=900)
                            result['3d_png'] = str(png_out)
                        except Exception:
                            result['3d_png'] = None
                except Exception:
                    pass
        except Exception:
            pass

        conn.close()
        return result

    finally:
        try:
            shutil.rmtree(tmpdir)
        except Exception:
            pass


if __name__ == '__main__':
    import sys
    if len(sys.argv) < 2:
        print('Usage: process_uzf.py <path.to.uzf>')
        raise SystemExit(1)
    print(process_uzf_file(sys.argv[1]))
import zipfile
import tempfile
import shutil
from pathlib import Path
import sqlite3
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go


def _safe_extract_zip(zip_path: Path, dest: Path):
    with zipfile.ZipFile(zip_path, 'r') as z:
        z.extractall(dest)


def _find_sqlite_file(extracted_dir: Path):
    # search for files that look like sqlite DB (header 'SQLite format 3')
    for p in extracted_dir.rglob('*'):
        if p.is_file():
            try:
                with open(p, 'rb') as fh:
                    header = fh.read(16)
                if header.startswith(b'SQLite format 3'):
                    return p
            except Exception:
                continue
    return None


def process_uzf(uzf_path: Path, out_dir: Path = Path('analysis/output_new'), generate_plots: bool = True):
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    tmpdir = Path(tempfile.mkdtemp(prefix='uzf_extract_'))
    try:
        # if .uzf, copy/rename to zip and extract
        zip_tmp = tmpdir / (uzf_path.stem + '.zip')
        shutil.copy2(str(uzf_path), str(zip_tmp))
        try:
            _safe_extract_zip(zip_tmp, tmpdir)
        except zipfile.BadZipFile:
            # maybe already a zip file with different extension
            raise

        # find sqlite DB inside
        sq = _find_sqlite_file(tmpdir)
        if sq is None:
            raise FileNotFoundError('No SQLite DB found inside UZF archive')

        conn = sqlite3.connect(str(sq))
        # read candidate tables
        tables = [r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()]
        result = {'tables': tables, 'extracted_db': str(sq)}

        # try to read gps_data and boat_multi_data if present
        if 'gps_data' in tables:
            df_gps = pd.read_sql_query('SELECT * FROM gps_data', conn)
            gps_csv = out_dir / (uzf_path.stem + '_gps_data.csv')
            df_gps.to_csv(gps_csv, index=False)
            result['gps_csv'] = str(gps_csv)
        else:
            df_gps = None

        if 'boat_multi_data' in tables:
            df_boat = pd.read_sql_query('SELECT * FROM boat_multi_data', conn)
            boat_csv = out_dir / (uzf_path.stem + '_boat_multi_data.csv')
            df_boat.to_csv(boat_csv, index=False)
            result['boat_csv'] = str(boat_csv)
        else:
            df_boat = None

        # Attempt to build a merged structured CSV
        merged = None
        if df_boat is not None:
            # heuristics: find columns
            cols = [c.lower() for c in df_boat.columns]
            # detect depth column
            depth_col = None
            for cand in ['depth','z','depth_m','elevation']:
                for c in df_boat.columns:
                    if cand in c.lower():
                        depth_col = c
                        break
                if depth_col:
                    break

            # detect easting/northing
            east_col = None; north_col = None
            for c in df_boat.columns:
                lc = c.lower()
                if 'easting' in lc or 'east'==lc or lc=='x':
                    east_col = c
                if 'northing' in lc or 'north'==lc or lc=='y':
                    north_col = c

            # ping index
            ping_col = None
            for c in df_boat.columns:
                if 'ping' in c.lower() or 'index' in c.lower():
                    ping_col = c
                    break

            merged = df_boat.copy()
            # normalize columns
            if depth_col and 'depth' not in merged.columns:
                merged = merged.rename(columns={depth_col: 'depth'})
            if east_col and 'easting' not in merged.columns:
                merged = merged.rename(columns={east_col: 'easting'})
            if north_col and 'northing' not in merged.columns:
                merged = merged.rename(columns={north_col: 'northing'})
            if ping_col and 'ping' not in merged.columns:
                merged = merged.rename(columns={ping_col: 'ping'})

        # If we also have gps, try to merge by nearest timestamp or ping
        if df_gps is not None and merged is not None:
            # attempt to find timestamp columns
            def find_time_col(df):
                for c in df.columns:
                    if 'time' in c.lower() or 'date' in c.lower() or 'timestamp' in c.lower():
                        return c
                return None

            t_gps = find_time_col(df_gps)
            t_boat = find_time_col(merged)
            if t_gps and t_boat:
                # convert to datetime
                try:
                    df_gps['__t'] = pd.to_datetime(df_gps[t_gps], errors='coerce')
                    merged['__t'] = pd.to_datetime(merged[t_boat], errors='coerce')
                    # nearest merge by timestamp: left join using merge_asof
                    df_gps_sorted = df_gps.sort_values('__t')
                    merged_sorted = merged.sort_values('__t')
                    merged2 = pd.merge_asof(merged_sorted, df_gps_sorted, on='__t', direction='nearest')
                    merged = merged2
                except Exception:
                    pass
            else:
                # try merge by ping if available
                if 'ping' in merged.columns and 'ping' in df_gps.columns:
                    merged = pd.merge(merged, df_gps, on='ping', how='left', suffixes=('', '_gps'))

        if merged is not None:
            merged_csv = out_dir / (uzf_path.stem + '_merged_structured.csv')
            merged.to_csv(merged_csv, index=False)
            result['merged_csv'] = str(merged_csv)

            # Create OWENDO TXT with required header
            owendo_txt = out_dir / (uzf_path.stem + '_OWENDO-BATHY-SURVEY-generated.txt')
            header = "CoordinateY CoordinateX h GroundH(H) Lat Lon Locked Sats Status"
            lines = [header]
            for _, r in merged.iterrows():
                cy = r.get('northing') if 'northing' in r else r.get('CoordinateY', '')
                cx = r.get('easting') if 'easting' in r else r.get('CoordinateX', '')
                h = r.get('depth', '')
                gh = r.get('GroundH(H)', h)
                lat = r.get('Lat', '')
                lon = r.get('Lon', '')
                locked = r.get('Locked', 0)
                sats = r.get('Sats', '')
                status = r.get('Status', '')
                line = f"{cy} {cx} {h} {gh} {lat} {lon} {locked} {sats} {status}"
                lines.append(line)
            owendo_txt.write_text('\n'.join(lines), encoding='utf-8')
            result['owendo_txt'] = str(owendo_txt)

            # simple 3D plot if easting/northing/depth available
            if generate_plots and {'easting','northing','depth'}.issubset(set(merged.columns)):
                dfp = merged.dropna(subset=['easting','northing','depth'])
                fig = go.Figure()
                fig.add_trace(go.Scatter3d(x=dfp['easting'], y=dfp['northing'], z=dfp['depth'], mode='markers', marker=dict(size=2, color=dfp['depth'], colorscale='Viridis', colorbar=dict(title='depth'))))
                fig.update_layout(scene=dict(xaxis_title='Easting', yaxis_title='Northing', zaxis_title='Depth'), height=700)
                # save html and png
                htmlp = out_dir / (uzf_path.stem + '_3d.html')
                pngp = out_dir / (uzf_path.stem + '_3d.png')
                try:
                    fig.write_html(str(htmlp), include_plotlyjs='cdn')
                    result['3d_html'] = str(htmlp)
                except Exception:
                    pass
                try:
                    fig.write_image(str(pngp), width=1400, height=900)
                    result['3d_png'] = str(pngp)
                except Exception:
                    pass
                result['fig'] = fig

        conn.close()
        return result
    finally:
        try:
            shutil.rmtree(tmpdir)
        except Exception:
            pass
