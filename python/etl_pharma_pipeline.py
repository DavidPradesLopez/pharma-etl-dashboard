import pandas as pd
import sqlite3
from pathlib import Path
import os

print("🚀 Hefadi Pharma ETL Pipeline")
print("=" * 50)

# 1. VERIFICAR 4 CSVs
csv_files = ['salesdaily.csv', 'saleshourly.csv', 'salesmonthly.csv', 'salesweekly.csv']
data_path = Path('../data/raw')
dfs = {}

for file in csv_files:
    file_path = data_path / file
    if file_path.exists():
        df = pd.read_csv(file_path)
        granularity = file.replace('sales', '').replace('.csv', '')
        df['time_granularity'] = granularity
        dfs[granularity] = df
        print(f"✅ {file}: {df.shape}")
    else:
        print(f"❌ {file_path} NO encontrado")

print(f"\n📊 Datasets cargados: {len(dfs)}")

# 2. MOSTRAR PROBLEMAS CSV (para entender error SSMS)
for name, df in dfs.items():
    print(f"\n🔍 {name.upper()} - Primeras columnas:")
    print(df.columns.tolist()[:5])
    print(df.head(2))
    print(f"NULLs por columna: {df.isnull().sum().sum()}")

# 3. SQLITE (funciona 100% sin SQL Server)
conn = sqlite3.connect('../data/pharma_clean.db')
print("\n🗄️ Creando SQLite database: pharma_clean.db")

# 4. ETL SIMPLE (limpieza básica)
df_clean = pd.concat(dfs.values(), ignore_index=True)
print(f"\n📈 Antes limpieza: {df_clean.shape}")

# LIMPIEZA (simulando tu rol Hefadi)
df_clean = df_clean.dropna(subset=df_clean.columns[:3])  # Primeras 3 columnas NO null
df_clean = df_clean[df_clean.select_dtypes(include=['number']).gt(0).all(axis=1)]  # Valores > 0

print(f"✅ Después limpieza: {df_clean.shape}")
print(f"Errores eliminados: {((1 - len(df_clean)/len(pd.concat(dfs.values()))) * 100):.1f}%")

# 5. GUARDAR
df_clean.to_sql('ventas_consolidadas', conn, if_exists='replace', index=False)
conn.close()

print("🎉 ETL COMPLETADO!")
print("📁 pharma_clean.db creado en data/")
print(df_clean.head())
