from pyproj import Transformer
import pandas as pd
f = 'analysis/output_new/merged_data_with_owendo_cols.csv'
df = pd.read_csv(f)

# Heuristic fixes: some datasets store easting/northing without the UTM false easting/northing.
# If values are small (<1e6) we add the usual UTM false offsets for zone 32N.
xs = df['easting'].values.astype(float)
ys = df['northing'].values.astype(float)

# Detect and apply false easting if needed
if xs.max() < 1e6:
	false_east = 500000.0
	xs = xs + false_east
else:
	false_east = 0.0

# Detect and apply false northing (common omission of 4,000,000) if needed
if ys.max() < 1e6:
	false_north = 4000000.0
	ys = ys + false_north
else:
	false_north = 0.0

from_crs = 'EPSG:32632'
print('Using', from_crs, f'(applied false_east={false_east}, false_north={false_north})')
t = Transformer.from_crs(from_crs, 'EPSG:4326', always_xy=True)
lon, lat = t.transform(xs, ys)
df['Lon'] = lon
df['Lat'] = lat
out = 'analysis/output_new/merged_data_latlon_epsg32632.csv'
df.to_csv(out, index=False)
print('Wrote', out)
print(df[['easting', 'northing', 'Lon', 'Lat']].head())
