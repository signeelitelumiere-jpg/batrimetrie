import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import os
import sys

INPUT_CSV = os.path.join(os.path.dirname(__file__), "output", "cross_section_points.csv")
OUT_PNG = os.path.join(os.path.dirname(__file__), "output", "cross_section_render.png")


def load_points(csv_path=INPUT_CSV):
    df = pd.read_csv(csv_path)
    # Expect columns: easting, northing, depth
    if not {'easting','northing','depth'}.issubset(df.columns):
        raise ValueError('CSV must contain easting,northing,depth columns')
    df = df.dropna(subset=['easting','northing','depth'])
    df['easting'] = df['easting'].astype(float)
    df['northing'] = df['northing'].astype(float)
    df['depth'] = df['depth'].astype(float)
    return df


def compute_profile(df):
    # compute cumulative distance along track
    dx = df['easting'].diff().fillna(0)
    dy = df['northing'].diff().fillna(0)
    dist = np.sqrt(dx*dx + dy*dy)
    s = dist.cumsum()
    return s.values, df['depth'].values


def detect_features(s, depth):
    # talweg = min depth
    talweg_idx = int(np.argmin(depth))

    # smooth depth to reduce noise
    try:
        import pandas as pd
        depth_sm = pd.Series(depth).rolling(window=7, min_periods=1, center=True).median().values
    except Exception:
        # fallback: simple uniform filter
        kernel = np.ones(5) / 5.0
        depth_sm = np.convolve(depth, kernel, mode='same')

    # gradient along distance
    g = np.gradient(depth_sm, s)
    # threshold based on gradient percentiles
    if len(g) > 0:
        thresh = np.nanpercentile(np.abs(g), 80)
    else:
        thresh = 0

    # find left bank: search from talweg leftwards for a strong gradient (descending slope)
    left_candidates = [i for i in range(0, talweg_idx) if g[i] < -thresh]
    if left_candidates:
        left_bank_idx = left_candidates[-1]
    else:
        # fallback: first shallow point from left (closest to max depth value)
        left_bank_idx = int(np.argmin(np.abs(depth - np.percentile(depth, 20)))) if len(depth) > 1 else 0

    # find right bank: search from talweg rightwards for a strong gradient (ascending slope)
    right_candidates = [i for i in range(talweg_idx+1, len(depth)) if g[i] > thresh]
    if right_candidates:
        right_bank_idx = right_candidates[0]
    else:
        right_bank_idx = int(np.argmin(np.abs(depth - np.percentile(depth, 80)))) if len(depth) > 1 else len(depth)-1

    # ensure indices sensible
    left_bank_idx = max(0, min(left_bank_idx, talweg_idx))
    right_bank_idx = min(len(depth)-1, max(right_bank_idx, talweg_idx))

    return left_bank_idx, right_bank_idx, talweg_idx


def plot_profile(s, depth, left_idx, right_idx, talweg_idx, out_png=OUT_PNG):
    plt.figure(figsize=(10,4))
    # plot bed profile (depth positive downwards), invert y-axis so water at top
    plt.plot(s, -depth, color='#1f4ea3', linewidth=2, label='Topographie')
    # hatch-filled water area to mimic PDF style
    plt.fill_between(s, -depth, 0, where=(-depth<=0) | (-depth>=0), facecolor='#b6ecff', alpha=0.6, hatch='\\', edgecolor='#77cde0')
    # mark feature points: black dots for 'autres points caractéristiques'
    plt.scatter(s, -depth, color='black', s=12, zorder=5, label='Autres points caractéristiques')
    # highlight banks and talweg
    plt.scatter([s[left_idx], s[right_idx]], [-depth[left_idx], -depth[right_idx]], color='black', s=40, zorder=6)
    plt.scatter([s[talweg_idx]], [-depth[talweg_idx]], color='red', s=40, zorder=7, label='Talweg')
    plt.annotate('Haut de berge', xy=(s[left_idx], -depth[left_idx]), xytext=(s[left_idx]+(s[-1]-s[0])*0.05, -depth[left_idx]+0.5), arrowprops=dict(arrowstyle='->'))
    plt.annotate('Bas de berge', xy=(s[right_idx], -depth[right_idx]), xytext=(s[right_idx]-(s[-1]-s[0])*0.25, -depth[right_idx]+0.5), arrowprops=dict(arrowstyle='->'))
    plt.annotate('Talweg', xy=(s[talweg_idx], -depth[talweg_idx]), xytext=(s[talweg_idx], -depth[talweg_idx]-1.5), ha='center', color='red')
    plt.xlabel("Distance le long du transect (m)")
    plt.ylabel("Altitude du fond (m) - (négatif = profondeur)")
    plt.title("Coupe transversale - Topographie du lit mineur")
    plt.gca().invert_yaxis()
    plt.grid(alpha=0.3)

    # reproduce a PDF-like legend: line sample + black dot
    from matplotlib.lines import Line2D
    legend_elements = [Line2D([0], [0], color='#1f4ea3', lw=2, label='Topographie'),
                       Line2D([0], [0], marker='o', color='w', label='Autres points caractéristiques', markerfacecolor='black', markersize=6)]
    plt.legend(handles=legend_elements, loc='lower right')
    plt.tight_layout()
    plt.savefig(out_png, dpi=150)
    plt.close()
    return out_png


def main():
    df = load_points()
    s, depth = compute_profile(df)
    left_idx, right_idx, talweg_idx = detect_features(s, depth)
    out = plot_profile(s, depth, left_idx, right_idx, talweg_idx)
    print(out)

if __name__ == '__main__':
    main()
