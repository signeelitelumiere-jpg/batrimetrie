from pathlib import Path
from analysis.process_uzf import process_uzf_file


def process_usf_file(usf_path, outdir='analysis/output_new'):
    # USF shares the same internal layout as UZF in our datasets; reuse logic
    return process_uzf_file(usf_path, outdir=outdir)


if __name__ == '__main__':
    import sys
    if len(sys.argv) < 2:
        print('Usage: process_usf.py <path.to.usf>')
        raise SystemExit(1)
    print(process_usf_file(sys.argv[1]))
