"""
╔══════════════════════════════════════════════════════════════════════╗
║   DEEP SCAN — Bathymétrie Futuriste v3.1  (FIXED + UZF PIPELINE)   ║
╚══════════════════════════════════════════════════════════════════════╝
CORRECTIONS v3.1 :
  - Bouton UZF dédié propre (upload → traitement → tableau)
  - Clés Streamlit uniques (plus de doublons de widgets)
  - Portée des variables corrigée (detections, df, merged, outdir)
  - Imports circulaires protégés
  - Gestion d'erreurs robuste dans chaque section
  - Suppression des blocs upload_uzf dupliqués
  - Variables locales vs globales séparées
"""

import sys
from pathlib import Path
import importlib.util
import tempfile
import zipfile
import shutil
import json
import time
import io
import base64

import streamlit as st
import pandas as pd
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
from PIL import Image
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from scipy import ndimage
from scipy.signal import find_peaks
from sklearn.preprocessing import StandardScaler

ROOT = Path(".").resolve()
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# ── Imports optionnels (ne bloquent pas si absents) ──────────────────────
def _safe_import(module_path, func_name):
    try:
        spec = importlib.util.spec_from_file_location("_mod", module_path)
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        return getattr(mod, func_name, None)
    except Exception:
        return None

try:
    from analysis.export_survey_format import export_survey
except Exception:
    export_survey = None

try:
    from analysis.analyze_and_generate_config import analyze_and_write_config
except Exception:
    analyze_and_write_config = None

try:
    from analysis.standardize_merged_csv import standardize
except Exception:
    standardize = None

try:
    from analysis.augment_config_fcols import main as augment_config_fcols
except Exception:
    augment_config_fcols = None

try:
    from analysis.generate_sections import main as generate_sections
except Exception:
    generate_sections = None

import subprocess
import streamlit.components.v1 as components

# ── UTILITY ─────────────────────────────────────────────────────────────
def hex_to_rgba(hex_color: str, alpha: float = 1.0) -> str:
    h = hex_color.lstrip("#")
    r, g, b = int(h[0:2], 16), int(h[2:4], 16), int(h[4:6], 16)
    return f"rgba({r},{g},{b},{alpha})"

# ── CONFIG ──────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="DEEP SCAN — Bathymétrie",
    page_icon="🌊",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── STYLE FUTURISTE ─────────────────────────────────────────────────────
FUTURISTIC_CSS = """
<style>
@import url('https://fonts.googleapis.com/css2?family=Orbitron:wght@400;700;900&family=Share+Tech+Mono&family=Rajdhani:wght@300;400;600&display=swap');
:root {
    --bg-void:#00040f; --bg-deep:#000d1a; --bg-panel:#010e1f; --bg-card:#021428;
    --cyan:#00e5ff; --cyan-dim:#007a99; --teal:#00ffd4; --teal-dim:#007a64;
    --amber:#ffb300; --red:#ff1a4b; --purple:#7c3aed;
    --border:rgba(0,229,255,0.18); --border-glow:rgba(0,229,255,0.45);
    --text-primary:#e0f7fa; --text-dim:#5e92a8; --scan-line:rgba(0,229,255,0.04);
}
html,body,[data-testid="stAppViewContainer"]{background:var(--bg-void)!important;color:var(--text-primary)!important;font-family:'Rajdhani',sans-serif!important;}
[data-testid="stAppViewContainer"]::before{content:'';position:fixed;top:0;left:0;right:0;bottom:0;background:repeating-linear-gradient(0deg,transparent,transparent 2px,var(--scan-line) 2px,var(--scan-line) 4px);pointer-events:none;z-index:9999;}
[data-testid="stSidebar"]{background:linear-gradient(180deg,#000d1a 0%,#00040f 100%)!important;border-right:1px solid var(--border)!important;}
[data-testid="block-container"]{padding:1rem 2rem!important;}
h1,h2,h3{font-family:'Orbitron',monospace!important;letter-spacing:0.12em;}
.scan-card{background:var(--bg-card);border:1px solid var(--border);border-radius:4px;padding:1.4rem 1.8rem;margin-bottom:1.2rem;position:relative;overflow:hidden;}
.scan-card::before{content:'';position:absolute;top:0;left:0;width:100%;height:2px;background:linear-gradient(90deg,var(--cyan),var(--teal));box-shadow:0 0 14px var(--cyan);}
.hero-title{font-family:'Orbitron',monospace;font-weight:900;font-size:2.4rem;background:linear-gradient(90deg,var(--cyan),var(--teal) 60%,var(--amber));-webkit-background-clip:text;-webkit-text-fill-color:transparent;background-clip:text;letter-spacing:0.18em;line-height:1.1;}
.hero-sub{font-family:'Share Tech Mono',monospace;color:var(--cyan-dim);font-size:0.85rem;letter-spacing:0.25em;margin-top:0.3rem;}
.metric-box{background:linear-gradient(135deg,#021428 0%,#010e1f 100%);border:1px solid var(--border);border-radius:3px;padding:1rem 1.2rem;text-align:center;position:relative;}
.metric-box .val{font-family:'Orbitron',monospace;font-size:1.6rem;font-weight:700;color:var(--cyan);text-shadow:0 0 12px var(--cyan);}
.metric-box .lbl{font-family:'Share Tech Mono',monospace;font-size:0.7rem;color:var(--text-dim);letter-spacing:0.15em;margin-top:0.2rem;}
.metric-box .corner{position:absolute;width:8px;height:8px;border-color:var(--cyan);border-style:solid;}
.corner-tl{top:3px;left:3px;border-width:2px 0 0 2px;}.corner-br{bottom:3px;right:3px;border-width:0 2px 2px 0;}
.hline{height:1px;background:linear-gradient(90deg,transparent,var(--cyan),transparent);margin:1.5rem 0;box-shadow:0 0 6px var(--cyan);}
.badge{display:inline-block;padding:0.2rem 0.7rem;border-radius:2px;font-family:'Share Tech Mono',monospace;font-size:0.78rem;letter-spacing:0.1em;font-weight:600;}
.badge-anomaly{background:rgba(255,26,75,0.15);border:1px solid var(--red);color:var(--red);}
.badge-mineral{background:rgba(255,179,0,0.12);border:1px solid var(--amber);color:var(--amber);}
.badge-object{background:rgba(0,229,255,0.1);border:1px solid var(--cyan);color:var(--cyan);}
.badge-normal{background:rgba(0,255,212,0.08);border:1px solid var(--teal);color:var(--teal);}
@keyframes pulse{0%,100%{opacity:1;box-shadow:0 0 6px var(--cyan);}50%{opacity:0.3;box-shadow:none;}}
.pulse-dot{display:inline-block;width:8px;height:8px;background:var(--cyan);border-radius:50%;animation:pulse 1.4s ease-in-out infinite;margin-right:6px;vertical-align:middle;}
div[data-testid="stButton"] button{background:transparent!important;border:1px solid var(--cyan)!important;color:var(--cyan)!important;font-family:'Orbitron',monospace!important;font-size:0.75rem!important;letter-spacing:0.12em!important;padding:0.5rem 1.4rem!important;border-radius:2px!important;transition:all 0.25s ease!important;}
div[data-testid="stButton"] button:hover{background:rgba(0,229,255,0.12)!important;box-shadow:0 0 18px var(--cyan)!important;color:#fff!important;}
.uzf-banner{background:linear-gradient(135deg,rgba(0,229,255,0.08),rgba(0,255,212,0.05));border:1px solid rgba(0,229,255,0.4);border-radius:6px;padding:1.5rem 2rem;margin:1rem 0;position:relative;}
.uzf-banner::before{content:'';position:absolute;top:0;left:0;width:100%;height:3px;background:linear-gradient(90deg,var(--cyan),var(--teal),var(--amber));border-radius:6px 6px 0 0;}
.det-table{width:100%;border-collapse:collapse;}
.det-table th{font-family:'Share Tech Mono',monospace;font-size:0.72rem;letter-spacing:0.12em;color:var(--cyan-dim);border-bottom:1px solid var(--border);padding:0.4rem 0.6rem;text-align:left;}
.det-table td{font-family:'Rajdhani',sans-serif;font-size:0.88rem;padding:0.35rem 0.6rem;border-bottom:1px solid rgba(0,229,255,0.07);color:var(--text-primary);}
</style>
"""
st.markdown(FUTURISTIC_CSS, unsafe_allow_html=True)

# ── PLOTLY DARK TEMPLATE ─────────────────────────────────────────────────
_BASE_LAYOUT = dict(
    paper_bgcolor="rgba(0,4,15,0)",
    plot_bgcolor="rgba(1,14,31,0.6)",
    font=dict(family="Share Tech Mono, monospace", color="#e0f7fa"),
    colorway=["#00e5ff","#00ffd4","#ffb300","#ff1a4b","#7c3aed"],
    title_font=dict(family="Orbitron, monospace", size=14, color="#00e5ff"),
    margin=dict(l=10, r=10, t=40, b=10),
)
_AXIS_STYLE = dict(gridcolor="rgba(0,229,255,0.1)", linecolor="rgba(0,229,255,0.3)", zerolinecolor="rgba(0,229,255,0.2)")
_LEGEND_STYLE = dict(bgcolor="rgba(0,4,15,0.7)", bordercolor="rgba(0,229,255,0.3)", borderwidth=1, font=dict(family="Share Tech Mono, monospace", size=10, color="#e0f7fa"))

def _apply_base(fig, title="", height=500, xaxis=None, yaxis=None, legend=None, **extra):
    fig.update_layout(**_BASE_LAYOUT, title=title, height=height, **extra)
    xa = dict(_AXIS_STYLE); xa.update(xaxis or {})
    ya = dict(_AXIS_STYLE); ya.update(yaxis or {})
    fig.update_xaxes(**xa)
    fig.update_yaxes(**ya)
    leg = dict(_LEGEND_STYLE); leg.update(legend or {})
    fig.update_layout(legend=leg)
    return fig

# ── HELPERS ─────────────────────────────────────────────────────────────
def card(html_content):
    st.markdown(f'<div class="scan-card">{html_content}</div>', unsafe_allow_html=True)

def hline():
    st.markdown('<div class="hline"></div>', unsafe_allow_html=True)

def metric_box(val, lbl, color="var(--cyan)"):
    return f"""<div class="metric-box" style="border-color:{color};">
        <div class="corner corner-tl" style="border-color:{color};"></div>
        <div class="val" style="color:{color};text-shadow:0 0 12px {color};">{val}</div>
        <div class="lbl">{lbl}</div>
        <div class="corner corner-br" style="border-color:{color};"></div>
    </div>"""

def badge(text, kind="normal"):
    return f'<span class="badge badge-{kind}">{text}</span>'

# ── LOGO ─────────────────────────────────────────────────────────────────
_LOGO_CANDIDATES = [
    Path("LOGO VECTORISE PNG.png"),
    ROOT / "LOGO VECTORISE PNG.png",
    Path(r"C:/Users/Admin/Pictures/DAT.ERT/batrimetrie/LOGO VECTORISE PNG.png"),
    Path("assets/logo_flanc_eau.svg"),
]

def _load_logo_b64_and_mime():
    for p in _LOGO_CANDIDATES:
        try:
            if p.exists():
                data = p.read_bytes()
                b64 = base64.b64encode(data).decode()
                suffix = p.suffix.lower()
                if suffix == ".svg":
                    mime = "image/svg+xml"
                elif suffix in (".jpg", ".jpeg"):
                    mime = "image/jpeg"
                elif suffix == ".webp":
                    mime = "image/webp"
                else:
                    mime = "image/png"
                return mime, b64
        except Exception:
            pass
    return None, None

_logo_mime, _logo_b64 = _load_logo_b64_and_mime()
_logo_html = (
    (f'<img src="data:{_logo_mime};base64,{_logo_b64}" style="height:68px;object-fit:contain;'
     f'filter:drop-shadow(0 0 8px #00e5ff);margin-right:1.2rem;flex-shrink:0;" alt="Logo"/>')
    if _logo_b64 else
    '<div style="width:68px;height:68px;border:1px solid rgba(0,229,255,0.3);'
    'border-radius:4px;display:flex;align-items:center;justify-content:center;margin-right:1.2rem;">'
    '<span style="font-size:1.8rem;">🌊</span></div>'
)

# ══════════════════════════════════════════════════════════════════════════
#   ★★★  PIPELINE UZF — NOUVEAU BOUTON DÉDIÉ  ★★★
# ══════════════════════════════════════════════════════════════════════════

def process_uzf_file_safe(uzf_path: Path, outdir: Path) -> dict:
    """
    Traitement robuste d'un fichier .uzf :
      1. Essaie d'importer analysis.process_uzf
      2. Sinon, extrait le .uzf comme un ZIP et cherche les données directement
    Retourne un dict avec les clés : gps_df, pings_df, merged_df, gps_csv, merged_csv, error
    """
    result = {"gps_df": None, "pings_df": None, "merged_df": None,
              "gps_csv": None, "merged_csv": None, "error": None}
    outdir.mkdir(parents=True, exist_ok=True)

    # ── Tentative 1 : module process_uzf ─────────────────────────────────
    try:
        from analysis.process_uzf import process_uzf_file as _puzf
        res = _puzf(uzf_path, outdir=str(outdir))
        if isinstance(res, dict):
            for k in ("gps_csv", "merged_csv", "boat_csv"):
                if res.get(k) and Path(res[k]).exists():
                    result[k.replace("boat_", "pings_")] = res[k]
            # Charger les DataFrames
            if result.get("gps_csv"):
                try:
                    result["gps_df"] = pd.read_csv(result["gps_csv"])
                except Exception:
                    pass
            if result.get("merged_csv"):
                try:
                    result["merged_df"] = pd.read_csv(result["merged_csv"])
                except Exception:
                    pass
            return result
    except ImportError:
        pass
    except Exception as e:
        result["error"] = f"process_uzf échoué : {e}"

    # ── Tentative 2 : extraction ZIP + SQLite ─────────────────────────────
    try:
        import sqlite3, zipfile as _zf
        stem = uzf_path.stem
        extract_dir = outdir / f"_uzf_{stem}"
        extract_dir.mkdir(exist_ok=True)

        try:
            with _zf.ZipFile(uzf_path, "r") as z:
                z.extractall(extract_dir)
        except Exception as e:
            result["error"] = f"Impossible d'extraire le ZIP : {e}"
            return result

        # Cherche une base SQLite
        db_files = list(extract_dir.rglob("*.db")) + list(extract_dir.rglob("*.sqlite")) + list(extract_dir.rglob("*.s3db"))
        if db_files:
            db = db_files[0]
            conn = sqlite3.connect(db)
            tables = pd.read_sql("SELECT name FROM sqlite_master WHERE type='table'", conn)["name"].tolist()

            gps_table = next((t for t in tables if "gps" in t.lower()), None)
            ping_table = next((t for t in tables if any(k in t.lower() for k in ["ping","boat","echo","depth","data"])), None)

            if gps_table:
                gdf = pd.read_sql(f"SELECT * FROM [{gps_table}]", conn)
                gps_csv = outdir / f"{stem}_gps_data.csv"
                gdf.to_csv(gps_csv, index=False)
                result["gps_df"] = gdf
                result["gps_csv"] = str(gps_csv)

            if ping_table:
                pdf = pd.read_sql(f"SELECT * FROM [{ping_table}]", conn)
                pings_csv = outdir / f"{stem}_pings.csv"
                pdf.to_csv(pings_csv, index=False)
                result["pings_df"] = pdf
                result["pings_csv"] = str(pings_csv)

            conn.close()

            # Tentative fusion temporelle si les deux existent
            if result["gps_df"] is not None and result["pings_df"] is not None:
                try:
                    gdf = result["gps_df"].copy()
                    pdf = result["pings_df"].copy()
                    # Normaliser les colonnes de temps
                    for df_, col_candidates in [(gdf, ["time","timestamp","datetime","Time"]),
                                                 (pdf, ["time","timestamp","datetime","Time"])]:
                        for c in col_candidates:
                            if c in df_.columns:
                                df_["__t"] = pd.to_datetime(df_[c], errors="coerce")
                                break
                    if "__t" in gdf.columns and "__t" in pdf.columns:
                        gdf = gdf.sort_values("__t")
                        pdf = pdf.sort_values("__t")
                        merged = pd.merge_asof(pdf, gdf, on="__t", direction="nearest", suffixes=("","_gps"))
                    else:
                        merged = pdf.copy()

                    merged_csv = outdir / f"{stem}_merged.csv"
                    merged.to_csv(merged_csv, index=False)
                    result["merged_df"] = merged
                    result["merged_csv"] = str(merged_csv)
                except Exception as e:
                    result["error"] = (result.get("error") or "") + f" | Fusion échouée : {e}"

            return result

        # Sinon chercher des fichiers CSV/TXT dans l'archive
        csvs = list(extract_dir.rglob("*.csv")) + list(extract_dir.rglob("*.txt"))
        gps_candidates = [f for f in csvs if "gps" in f.name.lower()]
        if gps_candidates:
            gdf = pd.read_csv(gps_candidates[0])
            gps_csv = outdir / f"{stem}_gps_data.csv"
            gdf.to_csv(gps_csv, index=False)
            result["gps_df"] = gdf
            result["gps_csv"] = str(gps_csv)

        if not result["gps_csv"]:
            result["error"] = "Aucune donnée GPS trouvée dans le fichier .uzf"

        return result

    except Exception as e:
        result["error"] = f"Extraction échouée : {e}"
        return result


def render_uzf_section():
    """
    ★ SECTION DÉDIÉE — Pipeline UZF propre et autonome
    Upload .uzf → traitement → affichage tableau → téléchargement
    """
    st.markdown("""
    <div class="uzf-banner">
        <div style="font-family:'Orbitron',monospace;font-size:1rem;color:#00e5ff;
                    letter-spacing:0.18em;margin-bottom:0.4rem;">
            ◈ PIPELINE UZF — TRAITEMENT DIRECT
        </div>
        <div style="font-family:'Share Tech Mono',monospace;font-size:0.78rem;
                    color:#5e92a8;line-height:1.8em;">
            Uploadez un fichier <span style="color:#00ffd4;">.uzf</span>
            (archive de projet bathymétrique) pour extraire
            automatiquement les données GPS et de profondeur.
        </div>
    </div>
    """, unsafe_allow_html=True)

    col_up, col_info = st.columns([2, 1])

    with col_up:
        uzf_file = st.file_uploader(
            "📁 Glissez votre fichier .uzf ici",
            type=["uzf", "zip"],
            key="uzf_main_uploader",
            help="Fichier .uzf exporté depuis le logiciel de bathymétrie (ex: testbaty-Outcome data.uzf)"
        )

    with col_info:
        st.markdown("""
        <div style="background:rgba(0,229,255,0.04);border:1px solid rgba(0,229,255,0.15);
                    border-radius:4px;padding:0.8rem;margin-top:1.5rem;">
            <div style="font-family:'Share Tech Mono',monospace;font-size:0.72rem;
                        color:#5e92a8;line-height:1.9em;">
                ◆ Format : archive .uzf / .zip<br>
                ◆ Contenu détecté auto : SQLite, CSV, Ln*.data<br>
                ◆ Sortie : GPS CSV + tableau interactif<br>
                ◆ Export : CSV, GNSS, TXT OWENDO
            </div>
        </div>
        """, unsafe_allow_html=True)

    if uzf_file is None:
        # Afficher un exemple du résultat attendu
        st.markdown("""
        <div style="text-align:center;padding:2rem;color:#2a4a5a;
                    font-family:'Share Tech Mono',monospace;font-size:0.8rem;">
            ↑ Uploadez un fichier .uzf pour démarrer le pipeline
        </div>
        """, unsafe_allow_html=True)
        return

    # Sauvegarder le fichier uploadé
    tmp_dir = Path(tempfile.mkdtemp(prefix="uzf_pipeline_"))
    uzf_path = tmp_dir / uzf_file.name
    with open(uzf_path, "wb") as f:
        f.write(uzf_file.getbuffer())

    outdir = ROOT / "analysis" / "output_new"

    # Bouton de traitement
    col_btn, col_status = st.columns([1, 3])
    with col_btn:
        run_uzf = st.button(
            "⚡ TRAITER CE FICHIER UZF",
            key="uzf_run_btn",
            help=f"Traitement de : {uzf_file.name}"
        )

    if not run_uzf:
        st.markdown(f"""
        <div style="font-family:'Share Tech Mono',monospace;font-size:0.78rem;
                    color:#007a99;padding:0.5rem 0;">
            Fichier prêt : <span style="color:#00ffd4;">{uzf_file.name}</span>
            ({uzf_file.size / 1024:.1f} Ko) — Appuyez sur ⚡ TRAITER pour démarrer
        </div>
        """, unsafe_allow_html=True)
        return

    # ── Traitement ────────────────────────────────────────────────────────
    with st.spinner(f"⟳ Traitement de {uzf_file.name} en cours…"):
        result = process_uzf_file_safe(uzf_path, outdir)

    # ── Affichage des erreurs (non bloquant) ─────────────────────────────
    if result.get("error"):
        st.warning(f"⚠ Avertissement traitement : {result['error']}")

    # ── Affichage des résultats ──────────────────────────────────────────
    gps_df     = result.get("gps_df")
    pings_df   = result.get("pings_df")
    merged_df  = result.get("merged_df")

    if gps_df is None and pings_df is None and merged_df is None:
        st.error("❌ Aucune donnée extraite. Vérifiez que le fichier .uzf est valide.")
        return

    st.markdown("""
    <div style="font-family:'Orbitron',monospace;font-size:0.85rem;color:#00ffd4;
                letter-spacing:0.15em;margin:1rem 0 0.5rem 0;">
        ✓ EXTRACTION RÉUSSIE — DONNÉES DISPONIBLES
    </div>
    """, unsafe_allow_html=True)

    # ── Métriques rapides ─────────────────────────────────────────────────
    metrics_data = []
    if gps_df is not None:
        metrics_data.append((f"{len(gps_df):,}", "POINTS GPS", "var(--teal)"))
        # Coordonnées
        lat_col = next((c for c in gps_df.columns if "lat" in c.lower()), None)
        lon_col = next((c for c in gps_df.columns if "lon" in c.lower()), None)
        if lat_col:
            metrics_data.append((f"{gps_df[lat_col].mean():.5f}°", "LAT MOY", "var(--cyan)"))
        if lon_col:
            metrics_data.append((f"{gps_df[lon_col].mean():.5f}°", "LON MOY", "var(--cyan)"))
    if pings_df is not None:
        metrics_data.append((f"{len(pings_df):,}", "PINGS SONAR", "var(--amber)"))
        depth_col = next((c for c in pings_df.columns if "depth" in c.lower() or "prof" in c.lower()), None)
        if depth_col:
            try:
                d = pd.to_numeric(pings_df[depth_col], errors="coerce").dropna()
                metrics_data.append((f"{d.mean():.2f} m", "PROFONDEUR MOY", "var(--red)"))
            except Exception:
                pass
    if merged_df is not None:
        metrics_data.append((f"{len(merged_df):,}", "POINTS FUSIONNÉS", "var(--purple)"))

    if metrics_data:
        m_cols = st.columns(min(len(metrics_data), 6))
        for col, (v, l, c) in zip(m_cols, metrics_data):
            with col:
                st.markdown(metric_box(v, l, c), unsafe_allow_html=True)

    hline()

    # ── Tableaux ─────────────────────────────────────────────────────────
    data_tabs_labels = []
    data_tabs_dfs = []
    data_tabs_csvs = []

    if gps_df is not None:
        data_tabs_labels.append(f"📡 GPS DATA ({len(gps_df)} pts)")
        data_tabs_dfs.append(gps_df)
        data_tabs_csvs.append(result.get("gps_csv"))

    if pings_df is not None:
        data_tabs_labels.append(f"🔊 PINGS SONAR ({len(pings_df)} pts)")
        data_tabs_dfs.append(pings_df)
        data_tabs_csvs.append(result.get("pings_csv"))

    if merged_df is not None:
        data_tabs_labels.append(f"⊕ DONNÉES FUSIONNÉES ({len(merged_df)} pts)")
        data_tabs_dfs.append(merged_df)
        data_tabs_csvs.append(result.get("merged_csv"))

    if data_tabs_labels:
        dtabs = st.tabs(data_tabs_labels)
        for dtab, df_show, csv_path, label in zip(dtabs, data_tabs_dfs, data_tabs_csvs, data_tabs_labels):
            with dtab:
                # Statistiques descriptives
                with st.expander("📊 Statistiques descriptives", expanded=False):
                    try:
                        numeric_cols = df_show.select_dtypes(include=[np.number]).columns.tolist()
                        if numeric_cols:
                            st.dataframe(df_show[numeric_cols].describe().round(4), use_container_width=True)
                    except Exception:
                        pass

                # Aperçu du tableau
                st.markdown(f"""
                <div style="font-family:'Share Tech Mono',monospace;font-size:0.75rem;
                            color:#5e92a8;margin-bottom:0.3rem;">
                    Colonnes : {', '.join(f'<span style="color:#00e5ff;">{c}</span>' for c in df_show.columns[:12])}
                    {'...' if len(df_show.columns) > 12 else ''}
                </div>
                """, unsafe_allow_html=True)

                st.dataframe(
                    df_show.head(500),
                    use_container_width=True,
                    height=400,
                )

                # Téléchargement
                col_dl1, col_dl2 = st.columns([1, 2])
                with col_dl1:
                    if csv_path and Path(csv_path).exists():
                        with open(csv_path, "rb") as f:
                            st.download_button(
                                f"⬇ Télécharger CSV ({label.split('(')[0].strip()})",
                                data=f.read(),
                                file_name=Path(csv_path).name,
                                mime="text/csv",
                                key=f"dl_uzf_{label[:10]}",
                            )
                    else:
                        csv_bytes = df_show.to_csv(index=False).encode("utf-8")
                        st.download_button(
                            f"⬇ Télécharger CSV",
                            data=csv_bytes,
                            file_name=f"{uzf_file.stem}_{label[:6].strip()}.csv",
                            mime="text/csv",
                            key=f"dl_uzf_mem_{label[:10]}",
                        )

    # ── Visualisation rapide si coordonnées disponibles ──────────────────
    df_viz = merged_df if merged_df is not None else gps_df
    if df_viz is not None:
        lat_col = next((c for c in df_viz.columns if "lat" in c.lower()), None)
        lon_col = next((c for c in df_viz.columns if "lon" in c.lower()), None)
        depth_col = next((c for c in df_viz.columns if "depth" in c.lower() or "prof" in c.lower() or c.lower() in ["h","z","alt"]), None)
        x_col = next((c for c in df_viz.columns if "east" in c.lower() or "x" == c.lower() or "coordinatex" in c.lower()), None)
        y_col = next((c for c in df_viz.columns if "north" in c.lower() or "y" == c.lower() or "coordinatey" in c.lower()), None)

        hline()
        st.markdown("""
        <div style="font-family:'Orbitron',monospace;font-size:0.8rem;color:#00e5ff;
                    letter-spacing:0.14em;margin-bottom:0.5rem;">
            ◈ VISUALISATION RAPIDE
        </div>
        """, unsafe_allow_html=True)

        viz_tabs = st.tabs(["🗺 Carte trajectoire", "📈 Profil de profondeur", "🌐 3D Points"])

        with viz_tabs[0]:
            # Carte lat/lon ou easting/northing
            if lat_col and lon_col:
                try:
                    df_map = df_viz[[lat_col, lon_col]].dropna()
                    df_map = df_map.rename(columns={lat_col: "lat", lon_col: "lon"})
                    if depth_col:
                        df_map["depth"] = pd.to_numeric(df_viz[depth_col], errors="coerce").values
                    st.map(df_map, latitude="lat", longitude="lon", size=3)
                except Exception as e:
                    st.warning(f"Carte non disponible : {e}")
            elif x_col and y_col:
                try:
                    fig_map = go.Figure(go.Scattergl(
                        x=df_viz[x_col], y=df_viz[y_col],
                        mode="markers",
                        marker=dict(size=3, color="#00e5ff", opacity=0.7),
                        hovertemplate=f"{x_col}: %{{x:.1f}}<br>{y_col}: %{{y:.1f}}<extra></extra>",
                    ))
                    _apply_base(fig_map, title="TRAJECTOIRE DU NAVIRE", height=400,
                                xaxis=dict(title=x_col), yaxis=dict(title=y_col))
                    ph_map = st.empty()
                    ph_map.plotly_chart(fig_map, width='stretch', key='ph_map_plot')
                except Exception as e:
                    st.warning(f"Tracé non disponible : {e}")
            else:
                st.info("Colonnes de coordonnées non détectées pour la carte.")

        with viz_tabs[1]:
            if depth_col:
                try:
                    d_vals = pd.to_numeric(df_viz[depth_col], errors="coerce").dropna()
                    fig_depth = go.Figure()
                    fig_depth.add_trace(go.Scatter(
                        y=d_vals.values,
                        mode="lines",
                        line=dict(color="#00e5ff", width=1.5),
                        fill="tozeroy",
                        fillcolor="rgba(0,77,96,0.2)",
                        name="Profondeur",
                    ))
                    _apply_base(fig_depth, title=f"PROFIL DE PROFONDEUR ({depth_col})", height=350,
                                xaxis=dict(title="Index ping"),
                                yaxis=dict(title="Profondeur (m)", autorange="reversed"))
                    ph_depth = st.empty()
                    ph_depth.plotly_chart(fig_depth, width='stretch', key='ph_depth_plot')
                except Exception as e:
                    st.warning(f"Profil non disponible : {e}")
            else:
                st.info("Colonne de profondeur non détectée.")

        with viz_tabs[2]:
            x3 = x_col or lon_col
            y3 = y_col or lat_col
            z3 = depth_col
            if x3 and y3 and z3:
                try:
                    # keep potential feature/object columns for overlay
                    cols = [x3, y3, z3]
                    for extra_col in ("feature_type", "feature_score", "feature_color"):
                        if extra_col in df_viz.columns:
                            cols.append(extra_col)
                    df3 = df_viz[cols].dropna(subset=[x3, y3, z3])
                    df3[x3] = pd.to_numeric(df3[x3], errors="coerce")
                    df3[y3] = pd.to_numeric(df3[y3], errors="coerce")
                    df3[z3] = pd.to_numeric(df3[z3], errors="coerce")
                    df3 = df3.dropna(subset=[x3, y3, z3])
                    # normalize column names for plotting helpers
                    rename_map = {x3: "easting", y3: "northing", z3: "depth"}
                    df3 = df3.rename(columns=rename_map)

                    mode = st.radio("Mode 3D", ["Nuage de points", "Surface interpolée"], horizontal=True, key="3d_mode")
                    show_objs = st.checkbox("Afficher objets détectés", value=True, key="3d_show_objects")

                    if mode == "Surface interpolée":
                        fig = plot_3d_surface(df3)
                    else:
                        # plot pointcloud; the plotting helper will read feature_ columns if present
                        fig = plot_3d_pointcloud(df3)

                    ph_3d = st.empty()
                    ph_3d.plotly_chart(fig, width='stretch', key='ph_3d_plot')
                except Exception as e:
                    st.warning(f"3D non disponible : {e}")
            else:
                st.info("Colonnes X/Y/Z insuffisantes pour le rendu 3D.")

    # ── Export OWENDO ─────────────────────────────────────────────────────
    hline()
    st.markdown("""
    <div style="font-family:'Orbitron',monospace;font-size:0.78rem;color:#ffb300;
                letter-spacing:0.14em;margin-bottom:0.5rem;">
        ◈ EXPORT FORMAT OWENDO
    </div>
    """, unsafe_allow_html=True)

    col_ow1, col_ow2 = st.columns([1, 2])
    with col_ow1:
        if st.button("📄 Générer TXT OWENDO", key="uzf_gen_owendo"):
            df_src = merged_df if merged_df is not None else (pings_df if pings_df is not None else gps_df)
            if df_src is not None:
                try:
                    owendo_path = outdir / f"{uzf_file.stem}_OWENDO.txt"
                    cols = df_src.columns.tolist()
                    north = next((c for c in cols if "north" in c.lower() or "coordinatey" in c.lower() or ("y" == c.lower())), None)
                    east  = next((c for c in cols if "east"  in c.lower() or "coordinatex" in c.lower() or ("x" == c.lower())), None)
                    h     = next((c for c in cols if "depth" in c.lower() or c.lower() in ["h","z","alt","groundh"]), None)
                    lat_c = next((c for c in cols if "lat" in c.lower()), None)
                    lon_c = next((c for c in cols if "lon" in c.lower()), None)

                    with open(owendo_path, "w", encoding="utf-8") as f:
                        f.write("CoordinateY CoordinateX h GroundH(H) Lat Lon Locked Sats Status\n")
                        for _, row in df_src.iterrows():
                            cy = row.get(north, "") if north else ""
                            cx = row.get(east, "")  if east  else ""
                            hv = row.get(h, "")     if h     else ""
                            la = row.get(lat_c, "") if lat_c else ""
                            lo = row.get(lon_c, "") if lon_c else ""
                            f.write(f"{cy} {cx} {hv}  {la} {lo} 0 0 0\n")

                    st.success(f"✓ Fichier OWENDO généré : {owendo_path.name}")
                    with open(owendo_path, "rb") as fh:
                        st.download_button(
                            "⬇ Télécharger TXT OWENDO",
                            data=fh.read(),
                            file_name=owendo_path.name,
                            mime="text/plain",
                            key="dl_uzf_owendo",
                        )
                except Exception as e:
                    st.error(f"Export OWENDO échoué : {e}")
            else:
                st.error("Aucune donnée disponible pour l'export.")

    # Nettoyage
    try:
        shutil.rmtree(tmp_dir, ignore_errors=True)
    except Exception:
        pass

    # ------- Button: add z_water / z_bed to canonical GPS CSVs
    try:
        st.markdown('<div style="height:0.6rem"></div>', unsafe_allow_html=True)
        gps_dir = Path('analysis') / 'output_new'
        gps_files = sorted([p for p in gps_dir.glob('*_gps_data.csv')]) if gps_dir.exists() else []
        gps_choices = ['Tous les fichiers'] + [p.name for p in gps_files]
        # default to validated canonical GPS file if present
        default_name = 'testbaty-Outcome data_gps_data.csv'
        try:
            default_index = gps_choices.index(default_name) if default_name in gps_choices else 0
        except Exception:
            default_index = 0
        sel = st.selectbox('Fichier GPS cible', gps_choices, index=default_index, key='gps_target_select')
        if st.button('➕ Ajouter z_water / z_bed aux GPS', key='btn_add_z_gps'):
            try:
                if sel != 'Tous les fichiers':
                    target = str(gps_dir / sel)
                    cmd = [sys.executable, 'analysis/scripts/add_z_to_gps.py', target]
                else:
                    cmd = [sys.executable, 'analysis/scripts/add_z_to_gps.py']
                proc = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
                if proc.returncode == 0:
                    st.success('z_water / z_bed ajoutés avec succès.')
                    if proc.stdout:
                        st.code(proc.stdout[:2000])
                else:
                    st.error('Erreur lors de l ajout des colonnes z.')
                    st.code((proc.stdout + '\n' + proc.stderr)[:4000])
            except Exception as e:
                st.error(f'Exception: {e}')
        # Populate Status heuristic
        st.markdown('<div style="height:0.6rem"></div>', unsafe_allow_html=True)
        if st.button('🛈 Remplir Status via heuristique (auto)', key='btn_assign_status'):
            try:
                cmd = [sys.executable, 'analysis/scripts/assign_status_from_gps.py']
                proc = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
                if proc.returncode == 0:
                    st.success('Status et Sats assignés automatiquement.')
                    if proc.stdout:
                        st.code(proc.stdout[:4000])
                else:
                    st.error('Erreur lors de l assignation des Status.')
                    st.code((proc.stdout + '\n' + proc.stderr)[:4000])
            except Exception as e:
                st.error(f'Exception: {e}')
        # Run canonical UZF pipeline (testbaty)
        st.markdown('<div style="height:0.4rem"></div>', unsafe_allow_html=True)
        if st.button('▶️ Exécuter pipeline UZF canonique (testbaty)', key='btn_run_testbaty'):
            try:
                cmd = [sys.executable, 'analysis/scripts/run_process_testbaty_uzf.py']
                proc = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
                if proc.returncode == 0:
                    st.success('Pipeline UZF exécuté — fichiers écrits dans analysis/output_new.')
                    if proc.stdout:
                        st.code(proc.stdout[:4000])
                else:
                    st.error('Erreur lors de l exécution du pipeline UZF.')
                    st.code((proc.stdout + '\n' + proc.stderr)[:8000])
            except Exception as e:
                st.error(f'Exception: {e}')
    except Exception:
        pass


# ══════════════════════════════════════════════════════════════════════════
#   MOTEUR DE CORRECTION DE PROFONDEUR
# ══════════════════════════════════════════════════════════════════════════

class DepthCorrectionEngine:
    WATER_SOUND_SPEED = {"douce": 1480.0, "salée": 1520.0, "moyenne": 1500.0}

    def __init__(self, water_type="salée", manual_factor=None, offset_mode="surface"):
        self.v = self.WATER_SOUND_SPEED.get(water_type, 1500.0)
        self.manual_factor = manual_factor
        self.offset_mode = offset_mode
        self.scale_factor_ = None
        self.offset_ = 0.0
        self.unit_detected_ = "?"
        self.report_ = []

    def _auto_detect(self, depths):
        d = np.abs(depths[np.isfinite(depths)])
        if len(d) == 0:
            return 1.0, "inconnu"
        max_d = float(np.nanmax(d))
        self.report_.append(f"Max brut : {max_d:.4f}")
        if 1.0 <= max_d <= 12_000:
            self.report_.append("→ Données déjà métriques")
            return 1.0, "m (natif)"
        if max_d < 1.0:
            factor = 1000.0 if max_d < 0.05 else (100.0 if max_d < 0.5 else 10.0)
            return factor, f"×{factor}"
        if max_d > 12_000:
            return (self.v * 1e-6) / 2, "µs→m"
        if max_d > 500:
            return (self.v * 1e-3) / 2, "ms→m"
        return 1.0, "m (natif)"

    def fit_transform(self, depths):
        d = np.array(depths, dtype=float)
        if self.manual_factor is not None:
            self.scale_factor_ = float(self.manual_factor)
            self.unit_detected_ = f"×{self.manual_factor}"
        else:
            self.scale_factor_, self.unit_detected_ = self._auto_detect(d)
        d_scaled = d * self.scale_factor_
        if self.offset_mode == "surface":
            self.offset_ = float(np.nanmin(d_scaled))
            d_corrected = d_scaled - self.offset_
        elif self.offset_mode == "center":
            self.offset_ = float(np.nanmean(d_scaled))
            d_corrected = d_scaled - self.offset_
        else:
            d_corrected = d_scaled
        self.report_.append(f"Plage finale : {np.nanmin(d_corrected):.2f} → {np.nanmax(d_corrected):.2f} m")
        return d_corrected

    def summary(self):
        return "\n".join(self.report_)


def apply_depth_correction(df, water_type="salée", manual_factor=None, offset_mode="surface"):
    if "depth" not in df.columns:
        return df, None, "Colonne 'depth' introuvable."
    engine = DepthCorrectionEngine(water_type=water_type, manual_factor=manual_factor, offset_mode=offset_mode)
    df_out = df.copy()
    df_out["depth_raw"] = df_out["depth"].values
    df_out["depth"] = engine.fit_transform(df_out["depth_raw"].values)
    df_out["depth_real"] = df_out["depth"].values
    return df_out, engine, engine.summary()


# ══════════════════════════════════════════════════════════════════════════
#   DÉTECTION SOUS-MARINE
# ══════════════════════════════════════════════════════════════════════════

MINERAL_SIGNATURES = {
    "Nodule Polymétallique": {"depth_range":(4000,6000),"roughness_thresh":0.8,"color":"#ffb300","confidence_base":0.72},
    "Encroûtement de Cobalt":{"depth_range":(800,2500), "roughness_thresh":0.6,"color":"#00ffd4","confidence_base":0.65},
    "Sulfure Hydrothermale": {"depth_range":(1500,3500),"roughness_thresh":1.4,"color":"#ff1a4b","confidence_base":0.58},
    "Sable Aurifère":        {"depth_range":(0,200),    "roughness_thresh":0.3,"color":"#ffe082","confidence_base":0.42},
    "Phosphorite":           {"depth_range":(50,400),   "roughness_thresh":0.4,"color":"#b39ddb","confidence_base":0.48},
}

OBJECT_SIGNATURES = {
    "Épave / Structure":    {"anomaly_size":(5,200),   "depth_contrast":2.0,"color":"#00e5ff"},
    "Formation Rocheuse":   {"anomaly_size":(50,5000), "depth_contrast":1.0,"color":"#80cbc4"},
    "Objet Artificiel":     {"anomaly_size":(1,30),    "depth_contrast":3.0,"color":"#ff1a4b"},
    "Dépression / Cratère": {"anomaly_size":(20,500),  "depth_contrast":1.5,"color":"#ffb300"},
    "Bio-monticule":        {"anomaly_size":(10,100),  "depth_contrast":0.8,"color":"#a5d6a7"},
}

def detect_underwater(df):
    results = {"minerals":[],"objects":[],"anomalies":[],"stats":{}}
    if df.empty or "depth" not in df.columns:
        return results
    depth = df["depth"].dropna()
    z = -np.abs(depth.values)
    mean_d = float(np.mean(np.abs(z)))
    std_d  = float(np.std(np.abs(z)))
    max_d  = float(np.max(np.abs(z)))
    roughness = float(std_d / (mean_d + 1e-9))
    results["stats"] = {"mean":mean_d,"std":std_d,"max":max_d,"roughness":roughness}
    for mineral, sig in MINERAL_SIGNATURES.items():
        dmin, dmax = sig["depth_range"]
        in_range = dmin <= mean_d <= dmax
        rough_ok = roughness >= sig["roughness_thresh"] * 0.4
        if in_range:
            conf = min(0.97, sig["confidence_base"] + (0.18 if rough_ok else 0))
            results["minerals"].append({"name":mineral,"confidence":conf,"color":sig["color"],"roughness_match":rough_ok})
    results["minerals"].sort(key=lambda x: x["confidence"], reverse=True)
    if len(z) > 20:
        grad = np.abs(np.gradient(np.abs(z)))
        threshold = np.mean(grad) + 2.5 * np.std(grad)
        anomaly_idx, _ = find_peaks(grad, height=threshold, distance=3)
        for idx in anomaly_idx[:30]:
            local_depth = float(np.abs(z[idx]))
            contrast = float(grad[idx])
            classified, obj_color = "Anomalie Inconnue", "#e0f7fa"
            for obj, sig in OBJECT_SIGNATURES.items():
                amin, amax = sig["anomaly_size"]
                spread = int(np.sum(grad > threshold * 0.5))
                if amin <= spread <= amax and contrast >= sig["depth_contrast"]:
                    classified, obj_color = obj, sig["color"]
                    break
            results["objects"].append({"idx":int(idx),"depth":local_depth,"contrast":round(contrast,2),"type":classified,"color":obj_color})
    if len(z) > 5:
        scaler = StandardScaler()
        zscores = np.abs(scaler.fit_transform(np.abs(z).reshape(-1,1)).ravel())
        results["anomalies"] = [{"idx":int(i),"depth":float(np.abs(z[i])),"zscore":float(zscores[i])} for i in np.where(zscores>2.8)[0][:50]]
    return results


# ══════════════════════════════════════════════════════════════════════════
#   DÉTECTION GÉOMORPHOLOGIQUE
# ══════════════════════════════════════════════════════════════════════════

SEABED_FEATURES = {
    "Crevasse / Faille":       {"description":"Gradient > 3σ","color":"#ff1a4b","symbol":"x","priority":1},
    "Pic / Montagne ss-marine":{"description":"Élévation locale > 1σ","color":"#ffb300","symbol":"diamond","priority":2},
    "Dépression / Cratère":    {"description":"Creux isolé > 1σ","color":"#7c3aed","symbol":"circle-open","priority":3},
    "Épave / Objet dur":       {"description":"Anomalie HF + contraste","color":"#00e5ff","symbol":"square","priority":4},
    "Affleurement rocheux":    {"description":"Rugosité persistante élevée","color":"#00ffd4","symbol":"pentagon","priority":5},
    "Zone plate / Sédiment":   {"description":"Faible gradient","color":"#5e92a8","symbol":"circle","priority":6},
}

def detect_seabed_features(df, window=15):
    df_out = df.copy().reset_index(drop=True)
    n = len(df_out)
    depth = df_out["depth"].values.astype(float)
    grad1 = np.gradient(depth)
    grad2 = np.gradient(grad1)
    grad_abs = np.abs(grad1)
    half = window // 2
    std_local = np.zeros(n)
    mean_local = np.zeros(n)
    for i in range(n):
        lo, hi = max(0, i-half), min(n, i+half)
        std_local[i]  = np.std(depth[lo:hi])
        mean_local[i] = np.mean(depth[lo:hi])
    g_mean, g_std = np.mean(grad_abs), np.std(grad_abs)
    d_mean, d_std = np.mean(depth), np.std(depth)
    s_mean, s_std = np.mean(std_local), np.std(std_local)
    feature_type  = np.full(n, "Zone plate / Sédiment", dtype=object)
    feature_score = np.zeros(n)
    feature_color = np.full(n, SEABED_FEATURES["Zone plate / Sédiment"]["color"], dtype=object)
    for i in range(n):
        g, c, s, d, dm = grad_abs[i], grad2[i], std_local[i], depth[i], mean_local[i]
        ftype, score = "Zone plate / Sédiment", 0.0
        if g > g_mean + 3*g_std and d > dm + 0.5*d_std:
            ftype, score = "Crevasse / Faille", min(1.0,(g-g_mean)/(4*g_std+1e-9))
        elif d < dm - 1.2*d_std and g < g_mean+g_std:
            ftype, score = "Pic / Montagne ss-marine", min(1.0,(dm-d)/(2*d_std+1e-9))
        elif d > dm + 1.2*d_std and g < g_mean+g_std:
            ftype, score = "Dépression / Cratère", min(1.0,(d-dm)/(2*d_std+1e-9))
        elif g > g_mean + 2*g_std and abs(c) > np.std(grad2)*2:
            ftype, score = "Épave / Objet dur", min(1.0,(g-g_mean)/(3*g_std+1e-9)*0.7+abs(c)/(np.std(grad2)*3+1e-9)*0.3)
        elif s > s_mean + 1.5*s_std:
            ftype, score = "Affleurement rocheux", min(1.0,(s-s_mean)/(2*s_std+1e-9))
        else:
            score = max(0.0, 1.0-(g/(g_mean+1e-9))*0.5)
        feature_type[i] = ftype
        feature_score[i] = round(float(score), 3)
        feature_color[i] = SEABED_FEATURES[ftype]["color"]
    df_out["grad_local"]    = grad_abs
    df_out["curvature"]     = grad2
    df_out["std_local"]     = std_local
    df_out["mean_local"]    = mean_local
    df_out["feature_type"]  = feature_type
    df_out["feature_score"] = feature_score
    df_out["feature_color"] = feature_color
    return df_out


# ══════════════════════════════════════════════════════════════════════════
#   VISUALISATIONS 3D
# ══════════════════════════════════════════════════════════════════════════

def plot_3d_pointcloud(df):
    # Base point cloud
    fig = go.Figure()
    main_trace = go.Scatter3d(
        x=df["easting"], y=df["northing"], z=df["depth"], mode="markers",
        marker=dict(size=2.5, color=df["depth"],
                    colorscale=[[0,"#00e5ff"],[0.3,"#0097a7"],[0.6,"#006064"],[1,"#000d1a"]],
                    colorbar=dict(title=dict(text="Profondeur (m)", font=dict(family="Share Tech Mono",color="#00e5ff")),
                                  tickfont=dict(family="Share Tech Mono",color="#00e5ff"),thickness=12,len=0.7),
                    opacity=0.75),
        hovertemplate="<b>E:</b> %{x:.1f}m<br><b>N:</b> %{y:.1f}m<br><b>Prof:</b> %{z:.2f}m<extra></extra>",
        name="Points",
    )
    fig.add_trace(main_trace)

    # Overlay detected objects / features if present in the dataframe
    if "feature_type" in df.columns and df["feature_type"].notna().any():
        try:
            feat_df = df[df["feature_type"].notna()]
            types = feat_df["feature_type"].unique()
            for t in types:
                sub = feat_df[feat_df["feature_type"] == t]
                # prefer provided color if available
                color = None
                if "feature_color" in sub.columns and sub["feature_color"].notna().any():
                    color = sub["feature_color"].iloc[0]
                marker_kwargs = dict(symbol="diamond", size=6, line=dict(width=1,color="#ffffff"))
                if color:
                    marker_kwargs["color"] = color
                else:
                    marker_kwargs["color"] = "#ffb300"
                fig.add_trace(go.Scatter3d(
                    x=sub["easting"], y=sub["northing"], z=sub["depth"], mode="markers+text",
                    marker=marker_kwargs,
                    text=sub.get("feature_type", [t]*len(sub)),
                    textposition="top center",
                    name=f"Objet: {t} ({len(sub)})",
                    hovertemplate="<b>Type:</b> %{text}<br><b>Score:</b> %{customdata:.2f}<br>E:%{x:.1f} N:%{y:.1f} Prof:%{z:.2f}<extra></extra>",
                    customdata=sub.get("feature_score", [np.nan]*len(sub)).values,
                ))
        except Exception:
            pass

    _apply_base(fig, title="NUAGE DE POINTS 3D — FOND MARIN", height=600,
        scene=dict(
            xaxis=dict(title="Easting (m)",  backgroundcolor="#000408",gridcolor="rgba(0,229,255,0.1)"),
            yaxis=dict(title="Northing (m)", backgroundcolor="#000408",gridcolor="rgba(0,229,255,0.1)"),
            zaxis=dict(title="Profondeur (m)",backgroundcolor="#000408",gridcolor="rgba(0,229,255,0.1)",autorange="reversed"),
            bgcolor="#000408", camera=dict(eye=dict(x=1.4,y=1.4,z=0.7)),
        ))
    return fig


def plot_3d_surface(df):
    try:
        from scipy.interpolate import griddata
        N = 100
        xi = np.linspace(df["easting"].min(), df["easting"].max(), N)
        yi = np.linspace(df["northing"].min(),df["northing"].max(),N)
        xi, yi = np.meshgrid(xi, yi)
        zi = griddata((df["easting"].values,df["northing"].values),df["depth"].values,(xi,yi),method="linear")
        fig = go.Figure(go.Surface(
            x=xi,y=yi,z=zi,
            colorscale=[[0,"#00e5ff"],[0.15,"#0097a7"],[0.4,"#004d60"],[0.7,"#001f26"],[1,"#000408"]],
            contours=dict(z=dict(show=True,usecolormap=True,highlightcolor="#00e5ff",project=dict(z=True),width=1)),
            opacity=0.88,
            hovertemplate="E:%{x:.1f}<br>N:%{y:.1f}<br>Prof:%{z:.2f}m<extra></extra>",
        ))
        _apply_base(fig,title="SURFACE BATHYMÉTRIQUE 3D INTERPOLÉE",height=600,
            scene=dict(
                xaxis=dict(title="Easting →",backgroundcolor="#000408",gridcolor="rgba(0,229,255,0.08)"),
                yaxis=dict(title="Northing →",backgroundcolor="#000408",gridcolor="rgba(0,229,255,0.08)"),
                zaxis=dict(title="↓ Profondeur (m)",backgroundcolor="#000408",gridcolor="rgba(0,229,255,0.08)",autorange="reversed"),
                bgcolor="#000408",camera=dict(eye=dict(x=1.3,y=-1.3,z=0.8)),
            ))
        return fig
    except Exception:
        return plot_3d_pointcloud(df)


def plot_cross_sections(df, n=6):
    if not all(c in df.columns for c in ("easting","northing")):
        return go.Figure()
    fig = make_subplots(rows=max(1,int(np.ceil(n/2))),cols=2,
        subplot_titles=[f"COUPE {i+1}" for i in range(n)],
        vertical_spacing=0.08,horizontal_spacing=0.06)
    northing_vals = np.linspace(df["northing"].min(),df["northing"].max(),n+2)[1:-1]
    tol = (df["northing"].max()-df["northing"].min())/(n*1.5)
    colors = ["#00e5ff","#00ffd4","#ffb300","#ff1a4b","#7c3aed","#80deea","#ffe082","#ff8a80","#b39ddb","#80cbc4"]
    for i, nv in enumerate(northing_vals):
        r, c = i//2+1, i%2+1
        mask = (df["northing"]-nv).abs()<tol
        sub = df[mask].sort_values("easting")
        if sub.empty:
            continue
        col = colors[i%len(colors)]
        fig.add_trace(go.Scatter(
            x=sub["easting"],y=sub["depth"],mode="lines",
            fill="tozeroy",fillcolor=hex_to_rgba(col,0.08),
            line=dict(color=col,width=2),name=f"N={nv:.0f}m",
            hovertemplate="E:%{x:.1f}m<br>Prof:%{y:.2f}m<extra></extra>",
        ),row=r,col=c)
        if len(sub)>5:
            gr = np.abs(np.gradient(sub["depth"].values))
            th = np.mean(gr)+2*np.std(gr)
            peaks, _ = find_peaks(gr,height=th)
            if len(peaks):
                sr = sub.reset_index(drop=True)
                fig.add_trace(go.Scatter(
                    x=sr.iloc[peaks]["easting"],y=sr.iloc[peaks]["depth"],
                    mode="markers",
                    marker=dict(symbol="diamond",size=9,color="#ff1a4b",line=dict(width=1,color="#fff")),
                    name="Anomalie",showlegend=(i==0),
                    hovertemplate="⚠ Anomalie<br>E:%{x:.1f}m<br>Prof:%{y:.2f}m<extra></extra>",
                ),row=r,col=c)
    _apply_base(fig,title="COUPES TRANSVERSALES BATHYMÉTRIQUES",
        height=250*max(1,int(np.ceil(n/2))),showlegend=False)
    fig.update_yaxes(autorange="reversed",title_text="Profondeur (m)")
    fig.update_xaxes(title_text="Easting (m)")
    return fig


def plot_depth_histogram(df):
    depth_vals = df["depth"].dropna()
    fig = go.Figure(go.Histogram(x=depth_vals,nbinsx=60,marker_color="#00e5ff",
        marker_line=dict(width=0.5,color="#000d1a"),opacity=0.8,
        hovertemplate="Prof:%{x:.1f}m<br>N:%{y}<extra></extra>"))
    try:
        from scipy.stats import gaussian_kde
        kde = gaussian_kde(depth_vals.values, bw_method=0.15)
        xr = np.linspace(depth_vals.min(),depth_vals.max(),300)
        yr = kde(xr)*len(depth_vals)*(depth_vals.max()-depth_vals.min())/60
        fig.add_trace(go.Scatter(x=xr,y=yr,mode="lines",line=dict(color="#00ffd4",width=2),name="Densité",hoverinfo="skip"))
    except Exception:
        pass
    _apply_base(fig,title="DISTRIBUTION DES PROFONDEURS",height=320,
        xaxis=dict(title="Profondeur (m)"),yaxis=dict(title="Fréquence"))
    return fig


def plot_gradient_map(df):
    try:
        from scipy.interpolate import griddata
        N = 100
        xi = np.linspace(df["easting"].min(),df["easting"].max(),N)
        yi = np.linspace(df["northing"].min(),df["northing"].max(),N)
        xi, yi = np.meshgrid(xi,yi)
        zi = griddata((df["easting"].values,df["northing"].values),df["depth"].values,(xi,yi),method="linear")
        gy, gx = np.gradient(zi)
        grad_mag = np.sqrt(gx**2+gy**2)
        fig = go.Figure(go.Heatmap(
            x=xi[0],y=yi[:,0],z=grad_mag,
            colorscale=[[0,"#000d1a"],[0.4,"#006064"],[0.7,"#00e5ff"],[1,"#ff1a4b"]],
            colorbar=dict(title=dict(text="Gradient",font=dict(family="Share Tech Mono",color="#00e5ff"))),
            hovertemplate="E:%{x:.1f}|N:%{y:.1f}<br>Gradient:%{z:.3f}<extra></extra>",
        ))
        _apply_base(fig,title="CARTE DES PENTES / GRADIENT BATHYMÉTRIQUE",height=420,
            xaxis=dict(title="Easting (m)"),yaxis=dict(title="Northing (m)"))
        return fig
    except Exception:
        return go.Figure()


def plot_detection_overlay(df, detections):
    fig = go.Figure()
    fig.add_trace(go.Scattergl(
        x=df["easting"],y=df["northing"],mode="markers",
        marker=dict(size=3,color=df["depth"],colorscale=[[0,"#001f26"],[0.5,"#007a99"],[1,"#00e5ff"]],opacity=0.7),
        name="Points",hovertemplate="E:%{x:.1f}|N:%{y:.1f}<extra></extra>",
    ))
    for det in detections.get("objects",[]):
        if det["idx"] < len(df) and all(c in df.columns for c in ("easting","northing")):
            row = df.iloc[det["idx"]]
            fig.add_trace(go.Scatter(
                x=[row["easting"]],y=[row["northing"]],mode="markers+text",
                marker=dict(size=14,color=det["color"],symbol="diamond",line=dict(width=2,color="#fff"),opacity=0.9),
                text=[det["type"][:12]],textposition="top center",
                textfont=dict(size=9,color=det["color"],family="Share Tech Mono"),
                name=det["type"],showlegend=False,
                hovertemplate=f"<b>{det['type']}</b><br>Prof:{det['depth']:.1f}m<extra></extra>",
            ))
    _apply_base(fig,title="CARTE DE DÉTECTION TOP-VIEW",height=500,
        xaxis=dict(title="Easting (m)"),yaxis=dict(title="Northing (m)"))
    return fig


def export_pdf_report(outdir, imgs, df, detections):
    buf = io.BytesIO()
    with PdfPages(buf) as pdf:
        fig, ax = plt.subplots(figsize=(8.27,11.69),facecolor="#00040f")
        ax.set_facecolor("#00040f"); ax.axis("off")
        ax.text(0.5,0.96,"DEEP SCAN — RAPPORT BATHYMÉTRIQUE",ha="center",va="top",fontsize=16,color="#00e5ff",fontweight="bold",transform=ax.transAxes)
        ax.text(0.5,0.91,f"Généré le {time.strftime('%Y-%m-%d %H:%M UTC')}",ha="center",va="top",fontsize=9,color="#5e92a8",transform=ax.transAxes)
        stat_text="".join(f"  {k.upper():15s}: {v:.3f}\n" for k,v in detections.get("stats",{}).items())
        ax.text(0.1,0.82,"STATISTIQUES:\n"+stat_text,va="top",fontsize=10,color="#00ffd4",fontfamily="monospace",transform=ax.transAxes)
        minerals_text="\n".join(f"  {m['name']}: {m['confidence']*100:.0f}%" for m in detections.get("minerals",[])[:5])
        ax.text(0.1,0.62,"MINÉRAUX:\n"+minerals_text,va="top",fontsize=10,color="#ffb300",fontfamily="monospace",transform=ax.transAxes)
        pdf.savefig(fig,bbox_inches="tight",facecolor="#00040f"); plt.close(fig)
        for img_path in imgs:
            try:
                im=Image.open(img_path)
                fig,ax=plt.subplots(figsize=(8.27,11.69),facecolor="#00040f")
                ax.set_facecolor("#00040f"); ax.axis("off"); ax.imshow(im)
                ax.set_title(img_path.name,color="#00e5ff",fontsize=10,pad=8)
                pdf.savefig(fig,bbox_inches="tight",facecolor="#00040f"); plt.close(fig)
            except Exception:
                continue
    buf.seek(0)
    return buf.read()


# ── Pipeline principal ───────────────────────────────────────────────────
def load_enhancer_module():
    p = ROOT/"analysis"/"pipeline_first_extra"/"enhancer.py"
    if not p.exists():
        return None
    spec = importlib.util.spec_from_file_location("analysis.pipeline_first_extra.enhancer",str(p))
    mod = importlib.util.module_from_spec(spec)
    mod.__package__ = "analysis.pipeline_first_extra"
    spec.loader.exec_module(mod)
    return mod

def run_pipeline(data_dir, out_base=None, n_slices=6):
    mod = load_enhancer_module()
    if mod is None:
        raise RuntimeError("Module enhancer introuvable")
    outdir = mod.run_enhanced(data_dir=Path(data_dir), out_base=out_base, n_slices=n_slices)
    return Path(outdir)


# ══════════════════════════════════════════════════════════════════════════
#   SIDEBAR — SCAN AUTO + PARAMÈTRES
# ══════════════════════════════════════════════════════════════════════════
with st.sidebar:
    st.markdown('<div style="margin-bottom:8px"></div>', unsafe_allow_html=True)

    # Auto-scan
    auto_run = st.checkbox("Exécuter automatiquement au lancement", value=True, key="auto_run_cb")
    if auto_run and not st.session_state.get("scan_auto_run_done"):
        st.session_state["scan_auto_run_done"] = True
        merged_check = Path("analysis/output_new/merged_data.csv")
        has_local = merged_check.exists() or any(Path(".").glob("*.dat")) or \
                    any((Path("owendo-05-04-26-4-Outcome data_uzf")/"data").glob("Ln*.data")) \
                    if (Path("owendo-05-04-26-4-Outcome data_uzf")/"data").exists() else False
        if not has_local:
            st.info("Aucun jeu de données local — uploadez vos fichiers.")
        else:
            with st.spinner("Scan auto en cours…"):
                try:
                    cmd = [sys.executable,"analysis/scripts/run_full_scan.py"]
                    proc = subprocess.run(cmd,capture_output=True,text=True,timeout=120)
                    if proc.returncode == 0:
                        st.success("Scan auto terminé.")
                    else:
                        st.error("Erreur scan auto.")
                except Exception as e:
                    st.warning(f"Scan auto : {e}")

    if st.button("🚀 LANCER LE SCAN", key="sidebar_scan_btn"):
        with st.spinner("Scan en cours…"):
            try:
                cmd = [sys.executable,"analysis/scripts/run_full_scan.py"]
                proc = subprocess.run(cmd,capture_output=True,text=True,timeout=180)
                if proc.returncode == 0:
                    st.success("Scan terminé.")
                    st.code(proc.stdout[:1000])
                else:
                    st.error("Erreur scan.")
                    st.code((proc.stdout+"\n"+proc.stderr)[:2000])
            except Exception as e:
                st.error(f"Exception : {e}")

    st.markdown('<div style="height:1px;background:linear-gradient(90deg,transparent,#00e5ff,transparent);margin:0.8rem 0;"></div>', unsafe_allow_html=True)

    # z_water global
    try:
        global_zw = st.number_input("Forcer z_water global (m)", value=0.0, step=0.01, format="%.3f", key="global_zw")
        if st.button("Appliquer z_water global", key="apply_zw_btn"):
            merged_path = Path("analysis/output_new/merged_data_with_owendo_cols.csv")
            if not merged_path.exists():
                st.error("Fichier merged introuvable.")
            else:
                try:
                    cmd = [sys.executable,"analysis/scripts/apply_global_zwater.py","--merged",str(merged_path),"--zw",str(global_zw)]
                    proc = subprocess.run(cmd,capture_output=True,text=True,timeout=60)
                    if proc.returncode == 0:
                        st.success("z_water appliqué.")
                    else:
                        st.error(f"Erreur : {proc.stderr[:500]}")
                except Exception as e:
                    st.error(f"Exception : {e}")
    except Exception:
        pass

    st.markdown('<div style="height:1px;background:linear-gradient(90deg,transparent,#00e5ff,transparent);margin:0.8rem 0;"></div>', unsafe_allow_html=True)

    st.markdown('<div style="font-family:Orbitron,monospace;font-size:0.8rem;color:#00e5ff;letter-spacing:0.18em;margin-bottom:0.6rem;">⚙ PARAMÈTRES DE SCAN</div>', unsafe_allow_html=True)

    mode = st.radio("MODE D'ENTRÉE", ["Dossier local","Charger ZIP"], format_func=lambda x: f"▸ {x}", key="mode_radio")
    if mode == "Dossier local":
        data_path = st.text_input("CHEMIN DU DOSSIER data/",
            value=str(ROOT/"owendo-05-04-26-4-Outcome data_uzf"/"data"), key="data_path_input")
        upload_zip_sidebar = None
    else:
        upload_zip_sidebar = st.file_uploader("ZIP (contenant data/ avec Ln*.data)", type=["zip"], key="zip_sidebar")
        data_path = None

    n_slices = st.number_input("NOMBRE DE COUPES", min_value=2, max_value=20, value=6, step=1, key="n_slices_input")

    st.markdown('<div style="font-family:Orbitron,monospace;font-size:0.8rem;color:#ffb300;letter-spacing:0.18em;margin:0.8rem 0 0.5rem 0;">⚗ CORRECTION PROFONDEUR</div>', unsafe_allow_html=True)

    SPEED_MAP = {"salée":1520,"douce":1480,"moyenne":1500}
    water_type = st.selectbox("Type d'eau",["salée","douce","moyenne"],
        format_func=lambda x: f"▸ {x.capitalize()} ({SPEED_MAP[x]} m/s)", key="water_type_sel")
    offset_mode = st.radio("Offset",["surface","center","none"],
        format_func=lambda x: {"surface":"▸ Surface=0","center":"▸ Centré","none":"▸ Aucun"}[x], key="offset_mode_radio")
    use_manual = st.checkbox("Facteur manuel", value=False, key="use_manual_cb")
    manual_factor = None
    if use_manual:
        manual_factor = st.number_input("Facteur ×", min_value=0.001, max_value=10000.0,
                                         value=10.0, step=0.1, format="%.3f", key="manual_factor_input")

    st.markdown('<div style="font-family:Orbitron,monospace;font-size:0.8rem;color:#00e5ff;letter-spacing:0.18em;margin:0.8rem 0 0.5rem 0;">MODULES ACTIFS</div>', unsafe_allow_html=True)

    show_3d_cloud   = st.checkbox("🌐 Nuage 3D",           value=True, key="mod_cloud")
    show_3d_surface = st.checkbox("🏔 Surface 3D",          value=True, key="mod_surf")
    show_cross      = st.checkbox("✂ Coupes Transversales", value=True, key="mod_cross")
    show_gradient   = st.checkbox("📐 Carte des Pentes",    value=True, key="mod_grad")
    show_detection  = st.checkbox("🔍 Détection",           value=True, key="mod_detect")

    run_btn = st.button("🚀 LANCER LE SCAN COMPLET", key="run_btn_sidebar_main")


# ══════════════════════════════════════════════════════════════════════════
#   EN-TÊTE
# ══════════════════════════════════════════════════════════════════════════
st.markdown(f"""
<div style="padding:1.2rem 0 0.5rem 0;display:flex;align-items:center;">
    {_logo_html}
    <div>
        <div class="hero-title">◈ DEEP SCAN</div>
        <div class="hero-sub">
            <span class="pulse-dot"></span>
            SYSTÈME D'ANALYSE BATHYMÉTRIQUE AVANCÉ — v3.1 · ENHANCED PIPELINE + UZF DIRECT
        </div>
    </div>
</div>
""", unsafe_allow_html=True)
hline()

# ══════════════════════════════════════════════════════════════════════════
#   NAVIGATION PRINCIPALE PAR ONGLETS
# ══════════════════════════════════════════════════════════════════════════
main_tabs = st.tabs([
    "🗂 PIPELINE UZF",
    "🌊 SCAN BATHYMÉTRIQUE",
    "🔍 DÉTECTION",
    "📊 DONNÉES",
    "🖼 IMAGES",
])

# ──────────────────────────────────────────────────────────────────────────
# ONGLET 1 : PIPELINE UZF (NOUVEAU — PROPRE ET DÉDIÉ)
# ──────────────────────────────────────────────────────────────────────────
with main_tabs[0]:
    render_uzf_section()

# ──────────────────────────────────────────────────────────────────────────
# ONGLET 2 : SCAN BATHYMÉTRIQUE COMPLET
# ──────────────────────────────────────────────────────────────────────────
with main_tabs[1]:
    if not run_btn:
        st.markdown("""
        <div class="scan-card" style="text-align:center;padding:3rem 2rem;">
            <div style="font-family:'Orbitron',monospace;font-size:1.1rem;color:#00e5ff;letter-spacing:0.2em;margin-bottom:1rem;">
                SYSTÈME EN ATTENTE
            </div>
            <div style="font-family:'Share Tech Mono',monospace;color:#5e92a8;font-size:0.8rem;line-height:2em;">
                Configurez les paramètres dans le panneau latéral<br>
                puis appuyez sur <strong style="color:#00e5ff;">🚀 LANCER LE SCAN COMPLET</strong><br><br>
                ◈ Nuage de points 3D interactif<br>
                ◈ Surface bathymétrique interpolée<br>
                ◈ Coupes transversales avec détection<br>
                ◈ Détection objets &amp; minéraux<br>
                ◈ Export PDF complet
            </div>
        </div>
        """, unsafe_allow_html=True)
    else:
        tmpdir = None
        # Variables partagées entre les blocs (initialisées à None)
        df = None
        outdir = ROOT / "analysis" / "output_new"
        depth_engine = None
        depth_report = ""
        detections = {"minerals":[],"objects":[],"anomalies":[],"stats":{}}
        merged_path = None

        try:
            # ── Préparer données ──────────────────────────────────────────
            if mode == "Dossier local":
                data_dir = Path(data_path)
                if not data_dir.exists():
                    st.error(f"❌ Chemin introuvable : {data_path}")
                    st.stop()
            else:
                if upload_zip_sidebar is None:
                    st.error("❌ Veuillez charger un fichier ZIP")
                    st.stop()
                tmpdir = Path(tempfile.mkdtemp(prefix="bathy_"))
                zpath = tmpdir / "upload.zip"
                with open(zpath,"wb") as f:
                    f.write(upload_zip_sidebar.getbuffer())
                with zipfile.ZipFile(zpath,"r") as z:
                    z.extractall(tmpdir)
                candidates = list(tmpdir.glob("**/data"))
                if not candidates:
                    st.error("❌ Aucun dossier data/ dans le ZIP")
                    shutil.rmtree(tmpdir)
                    st.stop()
                data_dir = candidates[0]

            out_base = ROOT / "analysis" / "output_new"

            with st.spinner("⟳ PIPELINE EN COURS…"):
                outdir = run_pipeline(data_dir, out_base=out_base, n_slices=int(n_slices))

            st.markdown(f"""
            <div class="scan-card" style="background:rgba(0,229,255,0.04);">
                <span style="color:#00ffd4;font-family:'Orbitron',monospace;font-size:0.85rem;letter-spacing:0.15em;">✓ SCAN TERMINÉ</span>
                <span style="color:#5e92a8;font-family:'Share Tech Mono',monospace;font-size:0.75rem;margin-left:1rem;">{outdir}</span>
            </div>""", unsafe_allow_html=True)

            # ── Charger CSV ───────────────────────────────────────────────
            merged_path = outdir / "merged_data_enhanced.csv"
            if merged_path.exists():
                df_raw = pd.read_csv(merged_path)
                df, depth_engine, depth_report = apply_depth_correction(
                    df_raw, water_type=water_type,
                    manual_factor=manual_factor, offset_mode=offset_mode)

            # ── Métriques ─────────────────────────────────────────────────
            if df is not None and not df.empty:
                d = df["depth"].dropna() if "depth" in df.columns else pd.Series([0])
                scale_f = depth_engine.scale_factor_ if depth_engine else 1.0
                m_cols = st.columns(6)
                metrics = [
                    (f"{len(df):,}",            "POINTS",          "var(--cyan)"),
                    (f"{d.mean():.2f} m",       "PROF MOY",        "var(--teal)"),
                    (f"{d.max():.2f} m",        "PROF MAX",        "var(--amber)"),
                    (f"×{scale_f:.1f}",         "FACTEUR ÉCHELLE", "var(--purple)"),
                    (f"{d.max()-d.min():.2f} m","AMPLITUDE",       "var(--red)"),
                    (f"{d.std():.3f}",          "ÉCART-TYPE",      "var(--cyan-dim)"),
                ]
                for col, (v, l, c) in zip(m_cols, metrics):
                    with col:
                        st.markdown(metric_box(v,l,c), unsafe_allow_html=True)

            hline()

            # ── Sous-onglets ──────────────────────────────────────────────
            sub_tabs = st.tabs(["☁ NUAGE 3D","🏔 SURFACE","✂ COUPES","📐 PENTES","🗺 FOND RÉEL"])

            if df is not None and all(c in df.columns for c in ("easting","northing","depth")):
                df_valid = df.dropna(subset=["easting","northing","depth"])

                with sub_tabs[0]:
                    if show_3d_cloud:
                        ph_cloud = st.empty()
                        ph_cloud.plotly_chart(plot_3d_pointcloud(df_valid), width='stretch', key='ph_cloud_plot')
                        ph_hist = st.empty()
                        ph_hist.plotly_chart(plot_depth_histogram(df_valid), width='stretch', key='ph_hist_plot')
                    else:
                        st.info("Module désactivé.")

                with sub_tabs[1]:
                    if show_3d_surface:
                        ph_surf = st.empty()
                        ph_surf.plotly_chart(plot_3d_surface(df_valid), width='stretch', key='ph_surf_plot')
                    else:
                        st.info("Module désactivé.")

                with sub_tabs[2]:
                    if show_cross:
                        n_s = st.slider("Coupes à afficher", 2, 20, int(n_slices), key="cross_n_main")
                        ph_cross = st.empty()
                        ph_cross.plotly_chart(plot_cross_sections(df_valid, n=n_s), width='stretch', key='ph_cross_plot')
                    else:
                        st.info("Module désactivé.")

                with sub_tabs[3]:
                    if show_gradient:
                        ph_grad = st.empty()
                        ph_grad.plotly_chart(plot_gradient_map(df_valid), width='stretch', key='ph_grad_plot')
                    else:
                        st.info("Module désactivé.")

                with sub_tabs[4]:
                    if depth_report:
                        with st.expander("⚗ RAPPORT CORRECTION PROFONDEUR"):
                            st.code(depth_report, language=None)
                    df_feat = detect_seabed_features(df_valid)
                    # Tableau récap
                    rows = []
                    for fname, finfo in SEABED_FEATURES.items():
                        mask = df_feat["feature_type"] == fname
                        sub = df_feat[mask]
                        if not sub.empty:
                            rows.append({"Structure":fname,"Nb":int(mask.sum()),
                                "Prof min":round(float(sub["depth"].min()),2),
                                "Prof max":round(float(sub["depth"].max()),2),
                                "Prof moy":round(float(sub["depth"].mean()),2),
                                "Score moy":round(float(sub["feature_score"].mean()),3)})
                    if rows:
                        st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)
            else:
                for tab in sub_tabs:
                    with tab:
                        st.warning("Colonnes easting / northing / depth introuvables.")

        except Exception as e:
            st.error(f"❌ Erreur pipeline : {e}")
            import traceback
            with st.expander("Détails de l'erreur"):
                st.code(traceback.format_exc())
        finally:
            if tmpdir and tmpdir.exists():
                shutil.rmtree(tmpdir, ignore_errors=True)


# ──────────────────────────────────────────────────────────────────────────
# ONGLET 3 : DÉTECTION SOUS-MARINE
# ──────────────────────────────────────────────────────────────────────────
with main_tabs[2]:
    if not show_detection:
        st.info("Module détection désactivé dans les paramètres.")
    else:
        # Charger le dernier merged disponible
        _merged_candidates = [
            ROOT/"analysis"/"output_new"/"merged_data_enhanced.csv",
            ROOT/"analysis"/"output_new"/"merged_data_with_owendo_cols.csv",
            ROOT/"analysis"/"output_new"/"merged_data.csv",
        ]
        _df_detect = None
        for _p in _merged_candidates:
            if _p.exists():
                try:
                    _df_raw = pd.read_csv(_p)
                    _df_detect, _, _ = apply_depth_correction(_df_raw, water_type="salée", offset_mode="surface")
                    break
                except Exception:
                    continue

        if _df_detect is None or _df_detect.empty:
            st.info("Aucune donnée disponible. Lancez d'abord un scan ou uploadez un .uzf.")
        elif "depth" not in _df_detect.columns:
            st.warning("Colonne 'depth' introuvable dans les données chargées.")
        else:
            _df_valid = _df_detect.dropna(subset=["depth"])
            _detections = detect_underwater(_df_valid)

            if all(c in _df_valid.columns for c in ("easting","northing")):
                ph_det = st.empty()
                ph_det.plotly_chart(plot_detection_overlay(_df_valid, _detections), width='stretch', key='ph_det_plot')

            col_a, col_b = st.columns(2)
            with col_a:
                st.markdown('<div style="font-family:Orbitron,monospace;font-size:0.82rem;color:#ffb300;letter-spacing:0.15em;margin-bottom:0.8rem;">⬡ MINÉRAUX POTENTIELS</div>', unsafe_allow_html=True)
                if _detections["minerals"]:
                    for m in _detections["minerals"]:
                        bw = int(m["confidence"]*100)
                        st.markdown(f"""
                        <div class="metric-box" style="margin-bottom:0.6rem;text-align:left;border-color:{m['color']}20;">
                            <div style="font-family:'Orbitron',monospace;font-size:0.75rem;color:{m['color']};margin-bottom:0.3rem;">{m['name']}</div>
                            <div style="background:rgba(255,255,255,0.06);border-radius:2px;height:4px;margin-bottom:0.3rem;">
                                <div style="width:{bw}%;height:100%;background:{m['color']};box-shadow:0 0 8px {m['color']};border-radius:2px;"></div>
                            </div>
                            <div style="font-family:'Share Tech Mono',monospace;font-size:0.72rem;color:#5e92a8;">Confiance : {m['confidence']*100:.0f}%</div>
                        </div>""", unsafe_allow_html=True)
                else:
                    st.markdown(badge("Aucun minéral identifié","normal"), unsafe_allow_html=True)

            with col_b:
                st.markdown('<div style="font-family:Orbitron,monospace;font-size:0.82rem;color:#00e5ff;letter-spacing:0.15em;margin-bottom:0.8rem;">⬢ OBJETS & ANOMALIES</div>', unsafe_allow_html=True)
                if _detections["objects"]:
                    rows_html = "".join(
                        f'<tr><td style="color:{o["color"]}">{o["type"]}</td><td>{o["depth"]:.1f} m</td><td>{o["contrast"]:.2f}</td></tr>'
                        for o in _detections["objects"][:15])
                    st.markdown(f'<table class="det-table"><thead><tr><th>TYPE</th><th>PROFONDEUR</th><th>CONTRASTE</th></tr></thead><tbody>{rows_html}</tbody></table>', unsafe_allow_html=True)
                else:
                    st.markdown(badge("Aucun objet détecté","normal"), unsafe_allow_html=True)

            hline()
            s = _detections.get("stats",{})
            s_cols = st.columns(4)
            for col, (v,l,c) in zip(s_cols,[
                (f"{s.get('mean',0):.2f} m","PROFONDEUR MOY","var(--cyan)"),
                (f"{s.get('std',0):.3f}","ÉCART-TYPE","var(--teal)"),
                (f"{s.get('max',0):.2f} m","MAX PROFONDEUR","var(--amber)"),
                (f"{s.get('roughness',0):.4f}","RUGOSITÉ","var(--red)"),
            ]):
                with col:
                    st.markdown(metric_box(v,l,c), unsafe_allow_html=True)


# ──────────────────────────────────────────────────────────────────────────
# ONGLET 4 : DONNÉES BRUTES
# ──────────────────────────────────────────────────────────────────────────
with main_tabs[3]:
    st.markdown('<div style="font-family:Orbitron,monospace;font-size:0.85rem;color:#00e5ff;letter-spacing:0.16em;margin-bottom:1rem;">◈ TABLEAUX GÉNÉRÉS</div>', unsafe_allow_html=True)

    out_dirs_list = [
        ROOT/"analysis"/"output_new",
        ROOT/"analysis"/"output",
        ROOT/"owendo-05-04-26-4-Outcome data_uzf"/"Output",
    ]

    shown = 0
    for _d in out_dirs_list:
        if not _d.exists():
            continue
        for _p in sorted(_d.glob("*.csv"))[:8]:
            try:
                _df_p = pd.read_csv(_p, nrows=200)
                with st.expander(f"📄 {_p.name} ({len(_df_p)} lignes aperçu)"):
                    st.dataframe(_df_p, use_container_width=True, height=280)
                    try:
                        st.download_button(
                            f"⬇ {_p.name}",
                            data=_p.read_bytes(),
                            file_name=_p.name,
                            mime="text/csv",
                            key=f"dl_data_{_p.stem}_{_p.parent.name}",
                        )
                    except Exception:
                        pass
                shown += 1
                if shown >= 15:
                    break
            except Exception:
                continue
        if shown >= 15:
            break

    if shown == 0:
        st.info("Aucun tableau généré trouvé. Lancez un scan ou uploadez un .uzf.")

    hline()

    # Section explicite : Aperçu canonique merged_data.csv (force load)
    try:
        mpath = ROOT / 'analysis' / 'output_new' / 'merged_data.csv'
        if mpath.exists():
            st.markdown("<div style='font-family:Orbitron,monospace;font-size:0.78rem;color:#00ffd4;margin-top:0.6rem;'>◈ APERÇU : merged_data.csv (CANONIQUE)</div>", unsafe_allow_html=True)
            if st.button('🔄 Rafraîchir merged_data.csv', key='refresh_merged_preview'):
                st.experimental_rerun()
            try:
                df_m = pd.read_csv(mpath)
                # Prioriser affichage des colonnes clés
                show_cols = [c for c in ['datetime_parsed','Lat','Lon','z_bed','z_water','h','CoordinateX','CoordinateY'] if c in df_m.columns]
                if show_cols:
                    st.dataframe(df_m[show_cols].head(500), use_container_width=True, height=360)
                else:
                    st.dataframe(df_m.head(200), use_container_width=True, height=360)
                st.download_button('⬇ Télécharger merged_data.csv', data=mpath.read_bytes(), file_name=mpath.name, mime='text/csv', key='dl_merged_data_file')
            except Exception as e:
                st.warning(f"Impossible de lire {mpath.name} : {e}")
    except Exception:
        pass

    # Run canonical parse pipeline (Ln*.data -> merged)
    st.markdown('<div style="font-family:Orbitron,monospace;font-size:0.78rem;color:#00ffd4;letter-spacing:0.14em;margin-bottom:0.5rem;margin-top:0.6rem;">◈ LANCER LES PIPELINES CANONIQUES</div>', unsafe_allow_html=True)
    col_run1, col_run2 = st.columns(2)
    with col_run1:
        if st.button('▶️ Exécuter parse_raw_and_merge → merged_data.csv', key='btn_run_parse_raw'):
            try:
                cmd = [sys.executable, 'analysis/scripts/run_process_owendo_data.py']
                proc = subprocess.run(cmd, capture_output=True, text=True, timeout=600)
                if proc.returncode == 0:
                    st.success('Pipeline parse_raw_and_merge exécuté — merged_data.csv écrit.')
                    if proc.stdout:
                        st.code(proc.stdout[:4000])
                else:
                    st.error('Erreur lors de l exécution du pipeline parse_raw_and_merge.')
                    st.code((proc.stdout + '\n' + proc.stderr)[:8000])
            except Exception as e:
                st.error(f'Exception: {e}')
    with col_run2:
        if st.button('▶️ Rafraîchir la liste des CSVs générés', key='btn_refresh_csvs'):
            st.experimental_rerun()

    # Upload merged manuellement
    st.markdown('<div style="font-family:Orbitron,monospace;font-size:0.78rem;color:#ffb300;letter-spacing:0.14em;margin-bottom:0.5rem;">◈ UPLOADER UN CSV FUSIONNÉ</div>', unsafe_allow_html=True)
    _upload_merged = st.file_uploader("CSV fusionné (merged_data.csv)", type=["csv"], key="data_tab_upload_merged")
    if _upload_merged:
        try:
            _df_up = pd.read_csv(_upload_merged)
            st.success(f"CSV chargé : {len(_df_up)} lignes, {len(_df_up.columns)} colonnes")
            st.dataframe(_df_up.head(200), use_container_width=True, height=350)
            st.download_button(
                "⬇ Re-télécharger",
                data=_df_up.to_csv(index=False).encode("utf-8"),
                file_name=_upload_merged.name,
                mime="text/csv",
                key="redownload_merged",
            )
        except Exception as e:
            st.error(f"Erreur lecture CSV : {e}")

    # Export OWENDO
    hline()
    st.markdown('<div style="font-family:Orbitron,monospace;font-size:0.78rem;color:#00e5ff;letter-spacing:0.14em;margin-bottom:0.5rem;">◈ EXPORT FORMAT OWENDO</div>', unsafe_allow_html=True)
    col_ow1, col_ow2 = st.columns([1,2])
    with col_ow1:
        merged_for_export = ROOT/"analysis"/"output_new"/"merged_data_with_owendo_cols.csv"
        if not merged_for_export.exists():
            merged_for_export = ROOT/"analysis"/"output_new"/"merged_data.csv"
        if merged_for_export.exists() and export_survey is not None:
            if st.button("📄 Générer OWENDO-BATHY-SURVEY.txt", key="gen_owendo_data_tab"):
                try:
                    out_path = ROOT/"owendo-05-04-26-4-Outcome data_uzf"/"Output"/"OWENDO-BATHY-SURVEY.txt"
                    out_path.parent.mkdir(parents=True, exist_ok=True)
                    res = export_survey(str(merged_for_export), out_path)
                    st.success(f"✓ Fichier généré : {res}")
                    with open(res,"rb") as fh:
                        st.download_button("⬇ Télécharger", fh.read(), file_name=Path(res).name, mime="text/plain", key="dl_owendo_data_tab")
                except Exception as e:
                    st.error(f"Export échoué : {e}")
        else:
            st.info("Aucun CSV disponible pour l'export OWENDO.")


# ──────────────────────────────────────────────────────────────────────────
# ONGLET 5 : IMAGES PIPELINE
# ──────────────────────────────────────────────────────────────────────────
with main_tabs[4]:
    _out_img_dir = ROOT / "analysis" / "output_new"
    _imgs = sorted(_out_img_dir.glob("*.png")) if _out_img_dir.exists() else []

    if not _imgs:
        st.info("Aucune image générée — lancez d'abord un scan complet.")
    else:
        cols_per_row = st.slider("Images par ligne", 1, 3, 2, key="img_cols_slider")
        for i in range(0, len(_imgs), cols_per_row):
            row_imgs = _imgs[i:i+cols_per_row]
            rcols = st.columns(len(row_imgs))
            for col_, img in zip(rcols, row_imgs):
                with col_:
                    try:
                        im = Image.open(img)
                        fig_im = px.imshow(im)
                        fig_im.update_layout(
                            margin=dict(l=0,r=0,t=26,b=0),
                            paper_bgcolor="rgba(0,4,15,0)",
                            plot_bgcolor="rgba(0,4,15,0)",
                            title_text=img.name,
                            title_font=dict(family="Share Tech Mono",size=11,color="#00e5ff"),
                        )
                        fig_im.update_xaxes(showticklabels=False)
                        fig_im.update_yaxes(showticklabels=False)
                        ph_img = st.empty()
                        ph_img.plotly_chart(fig_im, width='stretch', key=f'ph_img_{img.stem}')
                    except Exception:
                        st.image(str(img), caption=img.name)

        hline()
        # Export PDF
        if st.button("📄 EXPORTER RAPPORT PDF COMPLET", key="export_pdf_btn"):
            _df_for_pdf = None
            for _p in [ROOT/"analysis"/"output_new"/"merged_data_enhanced.csv",
                        ROOT/"analysis"/"output_new"/"merged_data.csv"]:
                if _p.exists():
                    try:
                        _df_for_pdf = pd.read_csv(_p)
                        break
                    except Exception:
                        pass
            _det_for_pdf = {"minerals":[],"objects":[],"anomalies":[],"stats":{}}
            if _df_for_pdf is not None and "depth" in _df_for_pdf.columns:
                _det_for_pdf = detect_underwater(_df_for_pdf.dropna(subset=["depth"]))
            with st.spinner("Génération du PDF…"):
                try:
                    pdf_bytes = export_pdf_report(_out_img_dir, _imgs, _df_for_pdf or pd.DataFrame(), _det_for_pdf)
                    st.download_button("⬇ Télécharger le PDF", data=pdf_bytes, file_name="deep_scan_report.pdf", mime="application/pdf", key="dl_pdf_final")
                except Exception as e:
                    st.error(f"Erreur PDF : {e}")

# ── FOOTER ───────────────────────────────────────────────────────────────
hline()
st.markdown("""
<div style="font-family:'Share Tech Mono',monospace;font-size:0.7rem;color:#2a4a5a;text-align:center;padding:0.5rem 0 1rem 0;">
    DEEP SCAN v3.1 · Pipeline Bathymétrique ·
    <span style="color:#00e5ff;">pip install streamlit plotly scipy scikit-learn pillow</span>
</div>""", unsafe_allow_html=True)