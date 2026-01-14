import pandas as pd
import sqlite3
import os
from pathlib import Path

print("🚀 Pharma ETL Pipeline - Pharma Style")
print("=" * 50)

# 🎯 RUTAS ABSOLUTAS    
base_path = Path("C:/Users/david/OneDrive/Documentos/GitHub/pharma-etl-dashboard")
raw_path = base_path / "data" / "raw"
processed_path = base_path / "data" / "processed"
db_path = base_path / "data" / "pharma_clean.db"

# Crear TODAS las carpetas  si no existen
raw_path.mkdir(parents=True, exist_ok=True) 
processed_path.mkdir(parents=True, exist_ok=True)

print(f"📁 Raw: {raw_path}")    
print(f"📁 Processed: {processed_path}")    

# 1. CARGAR 4 CSVs  
csv_files = ['salesdaily.csv', 'saleshourly.csv', 'salesmonthly.csv', 'salesweekly.csv']
dfs = {}

for file in csv_files:
    file_path = raw_path / file
    if file_path.exists():
        df = pd.read_csv(file_path)
        granularity = file.replace('sales', '').replace('.csv', '')
        df['granularity'] = granularity
        dfs[granularity] = df
        print(f"✅ {file}: {df.shape[0]} filas")
    else:
        print(f"❌ {file} NO encontrado")

# 2. ETL LIMPIEZA INDIVIDUAL    
print(f"\n🧹 Limpiando {len(dfs)} datasets...")

cleaned_dfs = {}
total_original = 0
total_cleaned = 0

for granu, df in dfs.items():
    original_rows = len(df)
    total_original += original_rows
    
    # LIMPIEZA Pharma Style
    df_clean = df.dropna(subset=df.columns[:3])  # Primeras 3 columnas NO nulas
    
    numeric_cols = df.select_dtypes(include=['number']).columns
    if len(numeric_cols) > 0:
        df_clean = df_clean[(df_clean[numeric_cols] > 0).all(axis=1)]
    
    cleaned_dfs[granu] = df_clean
    total_cleaned += len(df_clean)
    
    print(f"  {granu}: {original_rows} → {len(df_clean)} ({((original_rows-len(df_clean))/original_rows*100):.1f}% eliminados)")

print(f"\n🎯 TOTAL: {total_original} → {total_cleaned} ({((total_original-total_cleaned)/total_original*100):.1f}% limpieza)")

# 3. GUARDAR 4 CSVs CLEANED     
for granu, df_clean in cleaned_dfs.items():
    output_file = processed_path / f"sales{granu}_cleaned.csv"
    df_clean.to_csv(output_file, index=False)
    print(f"💾 data/processed/sales{granu}_cleaned.csv ({len(df_clean)} filas)")

# 4. MASTER CSV + SQLite    
master_clean = pd.concat(cleaned_dfs.values(), ignore_index=True)
master_path = processed_path / "ventas_ALL_cleaned.csv"
master_clean.to_csv(master_path, index=False)
print(f"💾 data/processed/ventas_ALL_cleaned.csv ({len(master_clean)} filas)")

# SQLite para Power BI  
conn = sqlite3.connect(db_path)
master_clean.to_sql('ventas_limpias', conn, if_exists='replace', index=False)
conn.close()
print(f"💾 data/pharma_clean.db ({len(master_clean)} filas)")

print("\n✅ ESTRUCTURA FINAL:")
print("data/")
print("├── raw/              ← 53k filas SUCIAS")
print("├── processed/        ← CSVs LIMPIOS ⭐")
print("│   ├── salesdaily_cleaned.csv")
print("│   ├── saleshourly_cleaned.csv")
print("│   ├── salesmonthly_cleaned.csv")
print("│   ├── salesweekly_cleaned.csv")
print("│   └── ventas_ALL_cleaned.csv")
print("└── pharma_clean.db   ← Power BI")