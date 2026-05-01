from pathlib import Path
import argparse
from .enhancer import run_enhanced

def main():
    p = argparse.ArgumentParser()
    p.add_argument('--data-dir', default=None)
    p.add_argument('--out-base', default=None)
    p.add_argument('--slices', type=int, default=6)
    args = p.parse_args()

    data_dir = Path(args.data_dir) if args.data_dir else None
    out_base = Path(args.out_base) if args.out_base else None
    outdir = run_enhanced(data_dir=data_dir, out_base=out_base, n_slices=args.slices)
    print('Enhanced outputs written to', outdir)

if __name__ == '__main__':
    main()
