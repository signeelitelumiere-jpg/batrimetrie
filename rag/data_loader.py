import os
import string
import numpy as np

def is_printable(byte_seq, threshold=0.85):
    printable = set(bytes(string.printable, 'ascii'))
    count = sum(1 for b in byte_seq if b in printable)
    return (count / len(byte_seq)) >= threshold

def detect_float_type(path, sample_size=8192):
    """Inspect a binary file and heuristically detect whether embedded numeric
    blocks are better interpreted as float16 or float32.

    Returns a dict with printable_ratio and simple scores for float16/float32.
    """
    with open(path, 'rb') as f:
        data = f.read(sample_size)

    if not data:
        return {'error': 'empty file'}

    printable_ratio = sum(1 for b in data if 32 <= b <= 126) / len(data)

    # Heuristic: try interpreting whole buffer as float32 array and float16 array
    # and count how many values fall in a plausible bathymetry range (-1000, 100000)
    results = {'path': path, 'sample_size': len(data), 'printable_ratio': printable_ratio}

    try:
        arr32 = np.frombuffer(data, dtype=np.float32)
        valid32 = np.isfinite(arr32) & (arr32 > -1000.0) & (arr32 < 1e6)
        score32 = int(valid32.sum())
        results.update({'float32_count': len(arr32), 'float32_valid': int(score32)})
    except Exception:
        results.update({'float32_count': 0, 'float32_valid': 0})

    try:
        arr16 = np.frombuffer(data, dtype=np.float16)
        valid16 = np.isfinite(arr16) & (arr16 > -1000.0) & (arr16 < 1e6)
        score16 = int(valid16.sum())
        results.update({'float16_count': len(arr16), 'float16_valid': int(score16)})
    except Exception:
        results.update({'float16_count': 0, 'float16_valid': 0})

    # Also detect long ASCII regions
    results['is_mostly_text'] = printable_ratio > 0.8

    return results


def inspect_folder(folder, pattern='.data'):
    report = []
    for name in sorted(os.listdir(folder)):
        if not name.endswith(pattern):
            continue
        path = os.path.join(folder, name)
        r = detect_float_type(path)
        report.append(r)
    return report


if __name__ == '__main__':
    import argparse

    p = argparse.ArgumentParser(description='Inspect .data files for float16/float32 patterns')
    p.add_argument('path', nargs='?', default='.', help='File or folder to inspect')
    p.add_argument('--sample', type=int, default=8192, help='Bytes to sample')
    args = p.parse_args()

    if os.path.isdir(args.path):
        rep = inspect_folder(args.path, pattern='.data')
        for r in rep:
            print(r)
    else:
        print(detect_float_type(args.path, sample_size=args.sample))
