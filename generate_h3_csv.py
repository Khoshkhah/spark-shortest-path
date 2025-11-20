import h3
import pandas as pd

cells = sorted(list(h3.get_res0_cells()))
df = pd.DataFrame(cells, columns=['h3_index'])
df.to_csv('res0_cells.csv', index=False)
print(f"Generated res0_cells.csv with {len(df)} cells.")
