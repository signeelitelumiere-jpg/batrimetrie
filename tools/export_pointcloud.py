#!/usr/bin/env python3
from pathlib import Path
import pandas as pd
import plotly.express as px
import sys

def main():
    root = Path('.').resolve()
    out_base = root / 'analysis' / 'output_new'
    if not out_base.exists():
        print('No output base found:', out_base)
        sys.exit(1)

    dirs = sorted([d for d in out_base.iterdir() if d.is_dir()], key=lambda p: p.stat().st_mtime, reverse=True)
    if not dirs:
        print('No output directories')
        sys.exit(1)

    outdir = dirs[0]
    merged = outdir / 'merged_data_enhanced.csv'
    if not merged.exists():
        print('Merged CSV not found in', outdir)
        sys.exit(1)

    df = pd.read_csv(merged)
    if not all(c in df.columns for c in ('easting','northing','depth')):
        print('Required columns not found in merged CSV')
        sys.exit(1)

    df2 = df.dropna(subset=['easting','northing','depth'])
    if df2.empty:
        print('No numeric records to plot')
        sys.exit(1)

    fig3d = px.scatter_3d(df2, x='easting', y='northing', z='depth', color='depth',
                         color_continuous_scale='Viridis', height=800)
    fig3d.update_layout(scene=dict(zaxis=dict(autorange='reversed')),
                        margin=dict(l=0, r=0, t=40, b=0))

    png_path = outdir / 'point_cloud_plotly.png'
    try:
        # try to use kaleido
        fig3d.write_image(str(png_path), width=1600, height=900, scale=2)
        print('Wrote PNG:', png_path)
    except Exception as e:
        print('Failed to write PNG via kaleido:', e)
        sys.exit(2)

if __name__ == '__main__':
    main()
