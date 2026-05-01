#!/usr/bin/env python3
"""
Plot a 3D interactive view of a merged OWENDO CSV using Plotly.
Writes HTML to same folder as input and attempts to write PNG (requires kaleido).

Usage:
    plot_3d_merged.py --csv path/to/merged_data_with_owendo_cols_z.csv
"""
import argparse
from pathlib import Path
import pandas as pd
import numpy as np
import sys

def build_fig(df, zcol='z_bed'):
    import plotly.graph_objects as go
    from scipy.interpolate import griddata

    # Prepare points
    x = df['easting'].values
    y = df['northing'].values
    z = df[zcol].values

    # Create an interpolated surface grid (coarse for speed)
    try:
        N = 120
        xi = np.linspace(np.nanmin(x), np.nanmax(x), N)
        yi = np.linspace(np.nanmin(y), np.nanmax(y), N)
        xg, yg = np.meshgrid(xi, yi)
        zg = griddata((x, y), z, (xg, yg), method='linear')
        # fill holes with nanmean
        zg = np.where(np.isnan(zg), np.nanmean(z), zg)
    except Exception:
        xg = yg = zg = None

    fig = go.Figure()

    if xg is not None:
        fig.add_trace(go.Surface(x=xg, y=yg, z=zg, colorscale='Viridis', opacity=0.7, showscale=False, name='Interpolated surface'))

    fig.add_trace(go.Scatter3d(x=x, y=y, z=z,
                               mode='markers', marker=dict(size=2, color=z, colorscale='Viridis', colorbar=dict(title='m')),
                               name='Points'))

    fig.update_layout(scene=dict(xaxis_title='Easting (m)', yaxis_title='Northing (m)', zaxis_title=f'{zcol} (m)',
                                 aspectmode='data'), title=f'3D view ({zcol})')
    return fig


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--csv', required=True, help='Path to merged CSV with z columns')
    p.add_argument('--zcol', default=None, help='Preferred z column (z_bed,z_water,depth)')
    args = p.parse_args()

    csvp = Path(args.csv)
    if not csvp.exists():
        print('CSV not found:', csvp, file=sys.stderr)
        sys.exit(2)

    df = pd.read_csv(csvp)

    # find z column
    candidates = []
    if args.zcol:
        candidates.append(args.zcol)
    candidates += ['z_bed', 'z_water', 'depth']
    zcol = None
    for c in candidates:
        if c in df.columns:
            zcol = c
            break
    if zcol is None:
        print('No z column found among candidates:', candidates, file=sys.stderr)
        sys.exit(3)

    # Ensure easting/northing exist
    if 'easting' not in df.columns or 'northing' not in df.columns:
        # try CoordinateX/Y
        if 'CoordinateX' in df.columns and 'CoordinateY' in df.columns:
            df['easting'] = df['CoordinateX']
            df['northing'] = df['CoordinateY']
        else:
            print('No easting/northing or CoordinateX/Y found', file=sys.stderr)
            sys.exit(4)

    # drop NaNs for plotting
    dfp = df[[ 'easting', 'northing', zcol ]].dropna()
    if dfp.empty:
        print('No valid points to plot', file=sys.stderr)
        sys.exit(5)

    fig = build_fig(dfp, zcol=zcol)

    out_html = csvp.with_name(csvp.stem + '_3d.html')
    fig.write_html(str(out_html), include_plotlyjs='cdn')
    print('Wrote', out_html)

    # try PNG
    out_png = csvp.with_name(csvp.stem + '_3d.png')
    try:
        fig.write_image(str(out_png), width=1600, height=1000)
        print('Wrote', out_png)
    except Exception as e:
        print('PNG export failed (kaleido missing?):', e)


if __name__ == '__main__':
    main()
