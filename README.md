# Pharma ETL Dashboard 💊📊

Este proyecto implementa un pipeline ETL (Extracción, Transformación y Carga) diseñado para procesar datos de ventas farmacéuticas. El objetivo es limpiar, normalizar y estructurar los datos para su posterior análisis en un dashboard (por ejemplo, en Power BI).

## 📋 Descripción del Proyecto

El script de Python automatiza el flujo de trabajo de datos:
1.  **Ingesta**: Carga archivos CSV con datos de ventas diarias, horarias, mensuales y semanales desde la carpeta `data/raw`.
2.  **Limpieza**: Aplica reglas de negocio específicas "Pharma Style":
    *   Eliminación de registros con valores nulos en columnas críticas.
    *   Filtrado de valores numéricos para asegurar que sean positivos.
3.  **Transformación**: Unifica los datos y genera archivos limpios.
4.  **Carga**: 
    *   Guarda los datasets limpios individualmente en CSV.
    *   Genera un archivo maestro `ventas_ALL_cleaned.csv`.
    *   Exporta los datos a una base de datos SQLite `pharma_clean.db` para integración directa con herramientas de BI.

## 📂 Estructura del Proyecto

```
pharma-etl-dashboard/
├── data/
│   ├── raw/              # Archivos CSV de entrada (datos crudos)
│   ├── processed/        # Archivos CSV procesados y limpios
│   └── pharma_clean.db   # Base de datos SQLite generada
├── python/
│   ├── etl_pharma_pipeline.py  # Script principal del pipeline ETL
│   └── requirements.txt        # Dependencias del proyecto
├── powerbi/              # Archivos de Power BI (si aplica)
├── sql/                  # Scripts SQL (si aplica)
└── README.md             # Documentación del proyecto
```

## 🛠️ Requisitos

*   Python 3.x
*   Las librerías listadas en `requirements.txt`:
    *   pandas
    *   sqlite3 (incluido en la librería estándar de Python)

## 🚀 Instalación y Uso

1.  **Clonar el repositorio:**
    ```bash
    git clone https://github.com/DavidPradesLopez/pharma-etl-dashboard.git
    cd pharma-etl-dashboard
    ```

2.  **Instalar dependencias:**
    ```bash
    pip install -r python/requirements.txt
    ```

3.  **Ejecutar el Pipeline:**
    Desde la raíz del proyecto, ejecuta el script:
    ```bash
    python python/etl_pharma_pipeline.py
    ```

## ⚙️ Detalles del Proceso ETL

El script `etl_pharma_pipeline.py` realiza las siguientes acciones:
*   Define rutas absolutas para asegurar la correcta localización de archivos.
*   Itera sobre los archivos `salesdaily.csv`, `saleshourly.csv`, `salesmonthly.csv` y `salesweekly.csv`.
*   Añade una columna de `granularity` para identificar el origen de los datos.
*   Calcula y muestra estadísticas de limpieza (porcentaje de filas eliminadas).
*   Consolida todo en un archivo maestro y una base de datos.
