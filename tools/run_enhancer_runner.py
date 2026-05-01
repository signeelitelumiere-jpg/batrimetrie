#!/usr/bin/env python3
import sys
from pathlib import Path
import importlib.util

root = Path('.').resolve()
if str(root) not in sys.path:
    sys.path.insert(0, str(root))

# ensure parse_raw_and_merge is importable
try:
    import analysis.parse_raw_and_merge as prm
except Exception as e:
    print('Error importing analysis.parse_raw_and_merge:', e)
    raise

# load enhancer module from file and run
p = root / 'analysis' / 'pipeline_first_extra' / 'enhancer.py'
spec = importlib.util.spec_from_file_location('analysis.pipeline_first_extra.enhancer', str(p))
mod = importlib.util.module_from_spec(spec)
mod.__package__ = 'analysis.pipeline_first_extra'
spec.loader.exec_module(mod)

if __name__ == '__main__':
    out = mod.run_enhanced(n_slices=6)
    print('Enhanced outputs written to', out)
