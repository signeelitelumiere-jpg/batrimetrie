from pathlib import Path
import datetime
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import os
from mpl_toolkits.mplot3d import Axes3D  # noqa: F401
from scipy import ndimage

ROOT = Path(__file__).resolve().parents[3]
DEFAULT_DATA_DIR = ROOT / 'owendo-05-04-26-4-Outcome data_uzf' / 'data'
OUT_BASE = ROOT / 'analysis' / 'output_new'

print('enhancer: ROOT=', ROOT)
print('enhancer: DEFAULT_DATA_DIR=', DEFAULT_DATA_DIR)


def collect_and_merge(data_dir: Path):
    # import functions from existing script (non-intrusive)
    from analysis import parse_raw_and_merge as prm

    files = prm.find_data_files(data_dir)
    all_rows = []
    for f in files:
        rows = prm.parse_data_file(f)
        all_rows.extend(rows)
    df = prm.normalize_rows(all_rows)
    return df


def write_merged(df: pd.DataFrame, outdir: Path):
    outdir.mkdir(parents=True, exist_ok=True)
    outp = outdir / 'merged_data_enhanced.csv'
    df.to_csv(outp, index=False)
    return outp


def per_source_plots(df: pd.DataFrame, outdir: Path):
    outdir.mkdir(parents=True, exist_ok=True)
    results = []
    if 'src_file' not in df.columns:
        return results
    groups = df.groupby('src_file')
    for name, g in groups:
        # prefer explicit easting/northing/depth columns
        if all(c in g.columns for c in ('easting', 'northing', 'depth')):
            g2 = g.dropna(subset=['easting', 'northing', 'depth'])
        else:
            g2 = g
        # if still no numeric coords, attempt to recover from last three numeric f*_num columns
        if g2[['easting','northing','depth']].dropna().empty if {'easting','northing','depth'}.issubset(g2.columns) else False:
            # find numeric f*_num columns
            num_cols = [c for c in g.columns if c.endswith('_num')]
            if len(num_cols) >= 3:
                last3 = num_cols[-3:]
                g = g.copy()
                g['easting'] = g[last3[0]]
                g['northing'] = g[last3[1]]
                g['depth'] = g[last3[2]]
                g2 = g.dropna(subset=['easting','northing','depth'])
        if g2.empty:
            continue
        # Along-track distance approx by sorting easting
        g2 = g2.sort_values(by='easting')
        coords = g2[['easting', 'northing']].values
        if coords.shape[0] <= 1:
            # single point: plot a marker and expand vertical axis for visibility
            dist = np.array([0.0])
            zs = g2['depth'].values[:1]
        else:
            dists = np.concatenate([[0.0], np.cumsum(np.sqrt(np.sum(np.diff(coords, axis=0)**2, axis=1)) )])
            dist = dists
            zs = g2['depth'].values
        fig, ax = plt.subplots(figsize=(8,3))
        ax.plot(dist, zs, '-o', color='k', markersize=3)
        ax.invert_yaxis()
        ax.set_title(f'Cross-section {name}')
        ax.set_xlabel('Along-track (m)')
        ax.set_ylabel('Depth (m)')
        # if single point, set sensible y-limits
        try:
            if len(zs) == 1:
                z = float(zs[0])
                ax.set_ylim(z + 1.0, z - 1.0)
        except Exception:
            pass
        png = outdir / f'cross_section_{name}.png'
        fig.tight_layout()
        fig.savefig(png)
        plt.close(fig)
        # depth histogram
        fig, ax = plt.subplots(figsize=(4,3))
        ax.hist(g2['depth'].values, bins=40)
        ax.set_title(f'Depth hist {name}')
        png2 = outdir / f'depth_hist_{name}.png'
        fig.tight_layout()
        fig.savefig(png2)
        plt.close(fig)
        results.append((png, png2))
    return results


def multi_cross_sections(df: pd.DataFrame, outdir: Path, n_slices: int = 5):
    outdir.mkdir(parents=True, exist_ok=True)
    # create n_slices cross-sections by binning along easting
    df2 = df.dropna(subset=['easting','depth'])
    if df2.empty:
        return []
    xs = df2['easting'].values
    bins = np.linspace(xs.min(), xs.max(), n_slices+1)
    outputs = []
    for i in range(n_slices):
        sel = df2[(xs >= bins[i]) & (xs < bins[i+1])]
        if sel.empty:
            continue
        sel = sel.sort_values('easting')
        coords = sel['easting'].values
        dist = coords - coords.min()
        fig, ax = plt.subplots(figsize=(8,3))
        ax.plot(dist, sel['depth'].values, '-b')
        ax.invert_yaxis()
        ax.set_title(f'Slice {i+1} easting {bins[i]:.1f}-{bins[i+1]:.1f}')
        png = outdir / f'multi_slice_{i+1}.png'
        fig.tight_layout()
        fig.savefig(png)
        plt.close(fig)
        outputs.append(png)
    return outputs


def detect_anomalies(df: pd.DataFrame, window: int = 21, thresh: float = 0.5):
    """Simple anomaly detector on depth using moving median and threshold.

    Returns a DataFrame with an `anomaly` boolean column.
    """
    df2 = df.dropna(subset=['easting','depth']).copy()
    if df2.empty:
        df['anomaly'] = False
        return df

    # sort along easting to approximate along-track
    df2 = df2.sort_values('easting')
    depths = df2['depth'].values
    # moving median
    med = ndimage.median_filter(depths, size=window, mode='nearest')
    resid = depths - med
    df2['anomaly'] = (np.abs(resid) > thresh)
    # map back to original df by index
    df = df.copy()
    df['anomaly'] = False
    df.loc[df2.index, 'anomaly'] = df2['anomaly']
    return df


def point_cloud_plot(df: pd.DataFrame, outdir: Path):
    outdir.mkdir(parents=True, exist_ok=True)
    df2 = df.dropna(subset=['easting','northing','depth'])
    if df2.empty:
        return None

    x = df2['easting'].values
    y = df2['northing'].values
    z = df2['depth'].values

    fig = plt.figure(figsize=(8,6))
    ax = fig.add_subplot(111, projection='3d')
    p = ax.scatter(x, y, z, c=z, cmap='viridis', s=2)
    ax.set_xlabel('Easting')
    ax.set_ylabel('Northing')
    ax.set_zlabel('Depth')
    ax.invert_zaxis()
    fig.colorbar(p, ax=ax, shrink=0.5, label='Depth')
    png = outdir / 'point_cloud.png'
    fig.tight_layout()
    fig.savefig(png, dpi=200)
    plt.close(fig)
    return png


def hydraulic_cross_section(df: pd.DataFrame, outdir: Path):
    outdir.mkdir(parents=True, exist_ok=True)
    df2 = df.dropna(subset=['easting','depth']).copy()
    if df2.empty:
        return None

    # sort by easting
    df2 = df2.sort_values('easting')
    coords = df2['easting'].values
    depth = df2['depth'].values

    # detect anomalies
    df2 = detect_anomalies(df2)
    anomalies = df2[df2['anomaly']]

    fig, ax = plt.subplots(figsize=(10,4))
    ax.plot(coords - coords.min(), depth, '-b', linewidth=1)
    ax.invert_yaxis()
    ax.set_xlabel('Along-track (m)')
    ax.set_ylabel('Depth (m)')
    ax.set_title('Hydraulic cross-section with detected anomalies')
    # plot anomalies
    if not anomalies.empty:
        ax.scatter(anomalies['easting'].values - coords.min(), anomalies['depth'].values, c='r', s=20, label='anomaly')
        ax.legend()

    png = outdir / 'hydraulic_cross_section.png'
    fig.tight_layout()
    fig.savefig(png, dpi=200)
    plt.close(fig)
    return png


def summary_report(df: pd.DataFrame, outdir: Path):
    outdir.mkdir(parents=True, exist_ok=True)
    rpt = outdir / 'enhanced_report.txt'
    with open(rpt, 'w', encoding='utf-8') as f:
        f.write('Enhanced pipeline report\n')
        f.write(f'Generated: {datetime.datetime.utcnow().isoformat()}Z\n')
        f.write(f'Rows: {len(df)}\n')
        if 'depth' in df.columns:
            f.write(f'Depth min/max/mean: {df.depth.min()}/{df.depth.max()}/{df.depth.mean()}\n')
        if 'src_file' in df.columns:
            f.write('\nPer-source counts:\n')
            counts = df['src_file'].value_counts()
            for k,v in counts.items():
                f.write(f' - {k}: {v}\n')
    return rpt


def generate_bathyscatter_from_df(df: pd.DataFrame, dest_base: Path):
    """Generate bathyscatter and cross-section images into `dest_base` (analysis/output).

    Depths are rounded to integers (`depth_int`) to avoid false precision.
    Returns dict of written paths.
    """
    dest = Path(dest_base)
    dest.mkdir(parents=True, exist_ok=True)
    # ensure required cols
    if not all(c in df.columns for c in ('easting','northing','depth')):
        raise ValueError('DataFrame missing required columns')
    df2 = df.dropna(subset=['easting','northing','depth']).copy()
    if df2.empty:
        raise RuntimeError('No valid points to plot')
    df2['depth_int'] = np.rint(df2['depth'].astype(float)).astype(int)

    scatter_path = dest / 'bathyscatter.png'
    cross_path = dest / 'bathy_cross_section.png'
    csv_path = dest / 'cross_section_points.csv'

    # scatter
    fig = plt.figure(figsize=(10,8))
    ax = fig.add_subplot(111)
    sc = ax.scatter(df2['easting'], df2['northing'], c=df2['depth_int'], cmap='viridis', s=6)
    fig.colorbar(sc, ax=ax, label='Depth (m, integer)')
    ax.set_aspect('equal', adjustable='box')
    ax.set_xlabel('Easting')
    ax.set_ylabel('Northing')
    ax.set_title('Bathymetry scatter (depth integers)')
    ax.grid(alpha=0.3)
    fig.tight_layout()
    fig.savefig(scatter_path, dpi=200)
    plt.close(fig)

    # cross-section: median northing slice
    median_n = df2['northing'].median()
    tol = (df2['northing'].max() - df2['northing'].min()) * 0.02
    slice_df = df2[np.abs(df2['northing'] - median_n) <= tol].copy()
    if slice_df.empty:
        tol = (df2['northing'].max() - df2['northing'].min()) * 0.05
        slice_df = df2[np.abs(df2['northing'] - median_n) <= tol].copy()
    if slice_df.empty:
        slice_df = df2.sort_values(by=lambda s: np.abs(s - median_n)).head(1000)
    slice_df = slice_df.sort_values('easting')

    fig = plt.figure(figsize=(12,6))
    ax = fig.add_subplot(111)
    ax.scatter(slice_df['easting'], slice_df['depth_int'], c=slice_df['depth_int'], cmap='plasma', s=8)
    try:
        y_smooth = slice_df['depth_int'].rolling(window=max(3, len(slice_df)//50), center=True).median()
        ax.plot(slice_df['easting'], y_smooth, color='k', linewidth=1)
    except Exception:
        pass
    ax.invert_yaxis()
    ax.set_xlabel('Easting')
    ax.set_ylabel('Depth (m, integer)')
    ax.set_title(f'Cross-section at northing ≈ {median_n:.2f}')
    ax.grid(alpha=0.3)
    fig.tight_layout()
    fig.savefig(cross_path, dpi=200)
    plt.close(fig)

    slice_df.to_csv(csv_path, index=False)

    return {'bathyscatter': str(scatter_path), 'bathy_cross_section': str(cross_path), 'cross_csv': str(csv_path)}


def generate_bathyscatter_from_merged(merged_csv: Path, dest_base: Path = None):
    merged_csv = Path(merged_csv)
    if not merged_csv.exists():
        raise FileNotFoundError('Merged CSV not found: ' + str(merged_csv))
    df = pd.read_csv(merged_csv)
    if dest_base is None:
        dest_base = Path('.').resolve() / 'analysis' / 'output'
    return generate_bathyscatter_from_df(df, Path(dest_base))


def run_enhanced(data_dir: Path = None, out_base: Path = None, n_slices: int = 6):
    if data_dir is None:
        data_dir = DEFAULT_DATA_DIR
    # fallback: if computed path doesn't exist or yields no files, try workspace-relative path
    if not data_dir.exists():
        alt = Path('.').resolve() / 'owendo-05-04-26-4-Outcome data_uzf' / 'data'
        if alt.exists():
            print('run_enhanced: switching to alternate data dir', alt)
            data_dir = alt
    if out_base is None:
        # prefer repository-relative output folder
        out_base = Path('.').resolve() / 'analysis' / 'output_new'
    stamp = datetime.datetime.utcnow().strftime('enhanced_%Y%m%dT%H%M%SZ')
    outdir = out_base / stamp
    outdir.mkdir(parents=True, exist_ok=True)
    print('run_enhanced: using data_dir=', data_dir)
    df = collect_and_merge(data_dir)
    print('run_enhanced: merged rows=', len(df))
    if df.empty:
        raise RuntimeError('No records parsed from data directory')
    merged = write_merged(df, outdir)
    per_results = per_source_plots(df, outdir)
    multi = multi_cross_sections(df, outdir, n_slices=n_slices)
    # point cloud and hydraulic cross-section
    pc_png = point_cloud_plot(df, outdir)
    hydro_png = hydraulic_cross_section(df, outdir)
    rpt = summary_report(df, outdir)
    # also write a small index json
    try:
        import json
        idx = {'merged': str(merged), 'report': str(rpt), 'per_source_plots': [str(a) for a,b in per_results], 'multi_slices': [str(x) for x in multi]}
        if pc_png is not None:
            idx['point_cloud'] = str(pc_png)
        if hydro_png is not None:
            idx['hydraulic_cross_section'] = str(hydro_png)
        # generate bathyscatter images into analysis/output for app consumption
        try:
            bathy_idx = generate_bathyscatter_from_df(df, Path('.').resolve() / 'analysis' / 'output')
            idx.update(bathy_idx)
        except Exception:
            pass
        with open(outdir / 'index.json', 'w', encoding='utf-8') as f:
            json.dump(idx, f, indent=2)
    except Exception:
        pass
    return outdir
