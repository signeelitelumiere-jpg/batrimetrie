#!/usr/bin/env python3
"""Enhance merged CSV with z columns and generate 2D/3D section plots.

Adds columns:
 - z_water : water surface elevation (default 0.0 or from config groundh_offset)
 - z_bed   : seabed elevation (z_water + h)

Produces:
 - analysis/output/cross_section_1.png
 - analysis/output/cross_section_2.png
 - analysis/output/section_3d_1.png
 - analysis/output/section_3d_2.png
 - analysis/output/merged_data_enhanced.csv
"""
from __future__ import annotations
from pathlib import Path
import json
import argparse
import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from mpl_toolkits.mplot3d import Axes3D  # noqa: F401
import plotly.graph_objects as go


def add_z_columns(df: pd.DataFrame, groundh_offset: float | None) -> pd.DataFrame:
    # assume 'h' column exists (depth, negative down).
    df = df.copy()
    # prefer a per-row ground height if present (common OWENDO header: 'GroundH(H)')
    ground_col = None
    for c in df.columns:
        if c.lower().startswith('groundh') or 'groundh' in c.lower():
            ground_col = c
            break

    if ground_col is not None:
        # try numeric conversion, otherwise fallback to config/global
        gw = pd.to_numeric(df[ground_col], errors='coerce')
        if gw.notna().any():
            df['z_water'] = gw.fillna(float(groundh_offset) if groundh_offset is not None else 0.0)
        else:
            df['z_water'] = 0.0 if groundh_offset is None else float(groundh_offset)
    else:
        df['z_water'] = 0.0 if groundh_offset is None else float(groundh_offset)

    # z_bed: seabed elevation = z_water + h (h typically negative)
    hcol = 'h' if 'h' in df.columns else ('depth' if 'depth' in df.columns else None)
    if hcol is not None:
        df['z_bed'] = pd.to_numeric(df[hcol], errors='coerce') + df['z_water']
    else:
        df['z_bed'] = pd.Series([np.nan] * len(df))
    return df


def pick_transect_x(df: pd.DataFrame, frac: float) -> float:
    xs = pd.to_numeric(df["CoordinateX"], errors="coerce").dropna()
    return float(np.quantile(xs, frac))


def make_2d_section(df: pd.DataFrame, x_center: float, width: float, out_png: Path, title: str):
    # select points within +/- width around x_center
    dx = abs(df["CoordinateX"] - x_center)
    sel = df[dx <= width].copy()
    if sel.empty:
        raise RuntimeError("No points near transect center")
    sel = sel.sort_values(by="CoordinateY")
    plt.figure(figsize=(10,4))
    plt.plot(sel["CoordinateY"], sel["z_bed"], '-k', label='Seabed')
    plt.fill_between(sel["CoordinateY"], sel["z_bed"], sel["z_water"], where=sel["z_bed"]<sel["z_water"], color='deepskyblue', alpha=0.4)
    plt.gca().invert_yaxis()
    plt.xlabel('CoordinateY')
    plt.ylabel('Elevation (m)')
    plt.title(title)
    plt.legend()
    out_png.parent.mkdir(parents=True, exist_ok=True)
    plt.tight_layout()
    plt.savefig(out_png, dpi=150)
    plt.close()


def make_3d_view_matplotlib(df: pd.DataFrame, out_png: Path, title: str):
    x = pd.to_numeric(df["CoordinateX"], errors="coerce")
    y = pd.to_numeric(df["CoordinateY"], errors="coerce")
    z = pd.to_numeric(df["z_bed"], errors="coerce")
    valid = (~x.isna()) & (~y.isna()) & (~z.isna())
    x = x[valid]
    y = y[valid]
    z = z[valid]
    fig = plt.figure(figsize=(8,6))
    ax = fig.add_subplot(111, projection='3d')
    p = ax.scatter(x, y, z, c=z, cmap='viridis', s=2)
    ax.set_xlabel('X')
    ax.set_ylabel('Y')
    ax.set_zlabel('Elevation (m)')
    ax.set_title(title)
    fig.colorbar(p, ax=ax, label='z_bed')
    out_png.parent.mkdir(parents=True, exist_ok=True)
    plt.tight_layout()
    plt.savefig(out_png, dpi=150)
    plt.close()


def make_3d_view_plotly(df: pd.DataFrame, out_html: Path, title: str):
    x = pd.to_numeric(df["CoordinateX"], errors="coerce")
    y = pd.to_numeric(df["CoordinateY"], errors="coerce")
    z = pd.to_numeric(df["z_bed"], errors="coerce")
    valid = (~x.isna()) & (~y.isna()) & (~z.isna())
    x = x[valid]
    y = y[valid]
    z = z[valid]
    fig = go.Figure(data=[go.Scatter3d(x=x, y=y, z=z, mode='markers', marker=dict(size=2, color=z, colorscale='Viridis'))])
    fig.update_layout(title=title, scene=dict(xaxis_title='X', yaxis_title='Y', zaxis_title='Elevation (m)'))
    out_html.parent.mkdir(parents=True, exist_ok=True)
    fig.write_html(str(out_html), include_plotlyjs='cdn')


def main(merged_csv: Path, cfg_path: Path | None = None, x_centers: list[float] | None = None, width_frac: float = 0.01, bbox: tuple | None = None, sample_limit: int = 5000, write_plotly_html: bool = True):
    df = pd.read_csv(merged_csv)
    groundh_offset = None
    if cfg_path and Path(cfg_path).exists():
        try:
            cfg = json.loads(Path(cfg_path).read_text(encoding='utf-8'))
            groundh_offset = cfg.get('groundh_offset')
        except Exception:
            groundh_offset = None

    df_enh = add_z_columns(df, groundh_offset)

    out_dir = Path('analysis/output')
    out_dir.mkdir(parents=True, exist_ok=True)
    enhanced_csv = out_dir / 'merged_data_enhanced.csv'
    df_enh.to_csv(enhanced_csv, index=False)

    # determine transect centers
    if x_centers and len(x_centers) >= 2:
        x1, x2 = x_centers[0], x_centers[1]
    else:
        x1 = pick_transect_x(df_enh, 0.25)
        x2 = pick_transect_x(df_enh, 0.75)

    width = (pd.to_numeric(df_enh["CoordinateX"], errors='coerce').max() - pd.to_numeric(df_enh["CoordinateX"], errors='coerce').min()) * float(width_frac)

    cs1 = out_dir / 'cross_section_1.png'
    cs2 = out_dir / 'cross_section_2.png'
    make_2d_section(df_enh, x1, width, cs1, f'Cross-section 1 (x={x1:.2f})')
    make_2d_section(df_enh, x2, width, cs2, f'Cross-section 2 (x={x2:.2f})')

    # 3D outputs (matplotlib PNG and optional Plotly HTML)
    mm1 = out_dir / 'section_3d_1.png'
    mm2 = out_dir / 'section_3d_2.png'
    make_3d_view_matplotlib(df_enh.sample(frac=min(1.0, sample_limit/max(1, len(df_enh)))), mm1, '3D view (sample)')
    make_3d_view_matplotlib(df_enh, mm2, '3D view (full)')

    html1 = out_dir / 'section_3d_1.html'
    html2 = out_dir / 'section_3d_2.html'
    if write_plotly_html:
        # create interactive versions
        try:
            make_3d_view_plotly(df_enh.sample(frac=min(1.0, sample_limit/max(1, len(df_enh)))), html1, '3D view (sample)')
            make_3d_view_plotly(df_enh, html2, '3D view (full)')
        except Exception as e:
            # if plotly fails, skip HTML but keep PNGs
            html1 = None
            html2 = None

    return {
        'enhanced_csv': str(enhanced_csv),
        'cs1': str(cs1),
        'cs2': str(cs2),
        '3d_png_1': str(mm1),
        '3d_png_2': str(mm2),
        '3d_html_1': str(html1) if html1 else None,
        '3d_html_2': str(html2) if html2 else None,
    }


if __name__ == '__main__':
    p = argparse.ArgumentParser()
    p.add_argument('--merged', default='analysis/output_new/merged_data.csv')
    p.add_argument('--cfg', default='owendo-05-04-26-4-Outcome data_uzf/owendo_config.json')
    args = p.parse_args()
    res = main(Path(args.merged), Path(args.cfg))
    print('Wrote:', res)
