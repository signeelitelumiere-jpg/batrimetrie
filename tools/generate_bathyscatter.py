#!/usr/bin/env python3
from pathlib import Path
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import sys

def find_latest_output(base_dir: Path) -> Path:
    out_base = base_dir / 'analysis' / 'output_new'
    if not out_base.exists():
        raise FileNotFoundError(f'Output base not found: {out_base}')
    dirs = sorted([d for d in out_base.iterdir() if d.is_dir()], key=lambda p: p.stat().st_mtime, reverse=True)
    if not dirs:
        raise FileNotFoundError('No output directories found under ' + str(out_base))
    return dirs[0]

def ensure_output_folder(base_dir: Path) -> Path:
    out = base_dir / 'analysis' / 'output'
    out.mkdir(parents=True, exist_ok=True)
    return out

def main():
    root = Path('.').resolve()
    try:
        latest = find_latest_output(root)
    except FileNotFoundError as e:
        print(e)
        sys.exit(1)

    merged = latest / 'merged_data_enhanced.csv'
    if not merged.exists():
        print('Merged CSV not found in', latest)
        sys.exit(1)

    df = pd.read_csv(merged)
    # require numeric coords
    for c in ('easting','northing','depth'):
        if c not in df.columns:
            print('Missing column:', c)
            sys.exit(1)

    df = df.dropna(subset=['easting','northing','depth']).copy()
    if df.empty:
        print('No valid points')
        sys.exit(1)

    # add integer depth column (rounded to nearest int)
    df['depth_int'] = np.rint(df['depth'].astype(float)).astype(int)

    outdir = ensure_output_folder(root)
    scatter_path = outdir / 'bathyscatter.png'
    cross_path = outdir / 'bathy_cross_section.png'

    # scatter map: easting vs northing colored by integer depth
    plt.figure(figsize=(10,8))
    sc = plt.scatter(df['easting'], df['northing'], c=df['depth_int'], cmap='viridis', s=6)
    plt.colorbar(sc, label='Depth (m, integer)')
    plt.gca().set_aspect('equal', adjustable='box')
    plt.xlabel('Easting')
    plt.ylabel('Northing')
    plt.title('Bathymetry scatter (depth integers)')
    plt.grid(alpha=0.3)
    plt.tight_layout()
    plt.savefig(scatter_path, dpi=200)
    plt.close()
    print('Wrote:', scatter_path)

    # cross-section: choose median northing and take a slice
    median_n = df['northing'].median()
    tol = (df['northing'].max() - df['northing'].min()) * 0.02  # 2% span tolerance
    slice_df = df[np.abs(df['northing'] - median_n) <= tol].copy()
    if slice_df.empty:
        # widen tolerance
        tol = (df['northing'].max() - df['northing'].min()) * 0.05
        slice_df = df[np.abs(df['northing'] - median_n) <= tol].copy()

    if slice_df.empty:
        # fallback: take nearest 1000 points by northing distance
        slice_df = df.sort_values(key=lambda s: np.abs(s - median_n)).head(1000)

    slice_df = slice_df.sort_values('easting')
    plt.figure(figsize=(12,6))
    plt.scatter(slice_df['easting'], slice_df['depth_int'], c=slice_df['depth_int'], cmap='plasma', s=8)
    # connect with a smoothed line using rolling median
    try:
        y_smooth = slice_df['depth_int'].rolling(window=max(3, len(slice_df)//50), center=True).median()
        plt.plot(slice_df['easting'], y_smooth, color='k', linewidth=1)
    except Exception:
        pass
    plt.gca().invert_yaxis()
    plt.xlabel('Easting')
    plt.ylabel('Depth (m, integer)')
    plt.title(f'Cross-section at northing ≈ {median_n:.2f}')
    plt.grid(alpha=0.3)
    plt.tight_layout()
    plt.savefig(cross_path, dpi=200)
    plt.close()
    print('Wrote:', cross_path)

    # also write a small CSV with integer depths near cross-section for traceability
    slice_csv = outdir / 'cross_section_points.csv'
    slice_df.to_csv(slice_csv, index=False)
    print('Wrote:', slice_csv)

if __name__ == '__main__':
    main()
#!/usr/bin/env python3
from pathlib import Path
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import sys

def find_latest_output(base_dir: Path) -> Path:
    out_base = base_dir / 'analysis' / 'output_new'
    if not out_base.exists():
        raise FileNotFoundError(f'Output base not found: {out_base}')
    dirs = sorted([d for d in out_base.iterdir() if d.is_dir()], key=lambda p: p.stat().st_mtime, reverse=True)
    if not dirs:
        raise FileNotFoundError('No output directories found under ' + str(out_base))
    return dirs[0]

def ensure_output_folder(base_dir: Path) -> Path:
    out = base_dir / 'analysis' / 'output'
    out.mkdir(parents=True, exist_ok=True)
    return out

def main():
    root = Path('.').resolve()
    try:
        latest = find_latest_output(root)
    except FileNotFoundError as e:
        print(e)
        sys.exit(1)

    merged = latest / 'merged_data_enhanced.csv'
    if not merged.exists():
        print('Merged CSV not found in', latest)
        sys.exit(1)

    df = pd.read_csv(merged)
    # require numeric coords
    for c in ('easting','northing','depth'):
        if c not in df.columns:
            print('Missing column:', c)
            sys.exit(1)

    df = df.dropna(subset=['easting','northing','depth']).copy()
    if df.empty:
        print('No valid points')
        sys.exit(1)

    # add integer depth column (rounded to nearest int)
    df['depth_int'] = np.rint(df['depth'].astype(float)).astype(int)

    outdir = ensure_output_folder(root)
    scatter_path = outdir / 'bathyscatter.png'
    cross_path = outdir / 'bathy_cross_section.png'

    # scatter map: easting vs northing colored by integer depth
    plt.figure(figsize=(10,8))
    sc = plt.scatter(df['easting'], df['northing'], c=df['depth_int'], cmap='viridis', s=6)
    plt.colorbar(sc, label='Depth (m, integer)')
    plt.gca().set_aspect('equal', adjustable='box')
    plt.xlabel('Easting')
    plt.ylabel('Northing')
    plt.title('Bathymetry scatter (depth integers)')
    plt.grid(alpha=0.3)
    plt.tight_layout()
    plt.savefig(scatter_path, dpi=200)
    plt.close()
    print('Wrote:', scatter_path)

    # cross-section: choose median northing and take a slice
    median_n = df['northing'].median()
    tol = (df['northing'].max() - df['northing'].min()) * 0.02  # 2% span tolerance
    slice_df = df[np.abs(df['northing'] - median_n) <= tol].copy()
    if slice_df.empty:
        # widen tolerance
        tol = (df['northing'].max() - df['northing'].min()) * 0.05
        slice_df = df[np.abs(df['northing'] - median_n) <= tol].copy()

    if slice_df.empty:
        # fallback: take nearest 1000 points by northing distance
        slice_df = df.sort_values(key=lambda s: np.abs(s - median_n)).head(1000)

    slice_df = slice_df.sort_values('easting')
    plt.figure(figsize=(12,6))
    plt.scatter(slice_df['easting'], slice_df['depth_int'], c=slice_df['depth_int'], cmap='plasma', s=8)
    # connect with a smoothed line using rolling median
    try:
        y_smooth = slice_df['depth_int'].rolling(window=max(3, len(slice_df)//50), center=True).median()
        plt.plot(slice_df['easting'], y_smooth, color='k', linewidth=1)
    except Exception:
        pass
    plt.gca().invert_yaxis()
    plt.xlabel('Easting')
    plt.ylabel('Depth (m, integer)')
    plt.title(f'Cross-section at northing ≈ {median_n:.2f}')
    plt.grid(alpha=0.3)
    plt.tight_layout()
    plt.savefig(cross_path, dpi=200)
    plt.close()
    print('Wrote:', cross_path)

    # also write a small CSV with integer depths near cross-section for traceability
    slice_csv = outdir / 'cross_section_points.csv'
    slice_df.to_csv(slice_csv, index=False)
    print('Wrote:', slice_csv)

if __name__ == '__main__':
    main()
