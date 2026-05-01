"""Search owendo folder for GNSS/NMEA/RINEX/CSV tracks and report findings.
If NMEA files contain GGA/RMC lines, extract lat/lon samples.
"""
import os, re
BASE='owendo-05-04-26-4-Outcome data_uzf'
folder=BASE
candidates=[]
for root,dirs,files in os.walk(folder):
    for f in files:
        if f.lower().endswith(('.nmea','.nmea.txt','.log','.txt','.csv','.rnx','.nav')) or 'gps' in f.lower():
            candidates.append(os.path.join(root,f))
print('Found',len(candidates),'candidate GNSS files')
pattern=re.compile(r'(GGA|RMC)')
for p in candidates:
    try:
        with open(p,'r',errors='ignore') as fh:
            head=''.join([fh.readline() for _ in range(50)])
        if pattern.search(head):
            print('NMEA-like file:',p)
            # extract some lat/lon-looking substrings
            with open(p,'r',errors='ignore') as fh:
                lines=fh.readlines()
            samples=[]
            for ln in lines[:200]:
                if 'GGA' in ln or 'RMC' in ln or ',' in ln:
                    samples.append(ln.strip())
                    if len(samples)>=5:
                        break
            print(' Sample lines:')
            for s in samples:
                print('  ',s)
        else:
            # try CSV with lat/lon headers
            low=head.lower()
            if 'lat' in low and 'lon' in low:
                print('CSV-like with lat/lon headers:',p)
    except Exception as e:
        print('error reading',p,e)
