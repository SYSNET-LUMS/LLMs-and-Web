import pandas as pd
import os

# === Step 1: Load and shuffle the data ===
df = pd.read_csv('ORCAS-I-gold.tsv', sep='\t')
df = df.sample(frac=1).reset_index(drop=True)

# === Step 2: Count unique labels ===
label_counts = df['label_manual'].value_counts().rename_axis('label_manual').reset_index(name='count')
print(label_counts)

# === Step 3: Separate DataFrames per label ===
dfs_by_label = {label: df[df['label_manual'] == label].copy() for label in label_counts['label_manual']}

# === Step 4: Save one CSV per label ===
output_dir = 'by_label_csvs'
os.makedirs(output_dir, exist_ok=True)

for label, subdf in dfs_by_label.items():
    safe_label = label.replace(" ", "_").replace("/", "_")
    filename = f'ORCAS-I-gold_label_{safe_label}.csv'
    subdf.to_csv(os.path.join(output_dir, filename), index=False)

print(f"\nSaved {len(dfs_by_label)} label CSVs in '{output_dir}' directory.")