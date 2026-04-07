"""
ETL Pipeline Automatizado
Autor: Julio Carvallo
Descripcion: Pipeline de extraccion, transformacion y carga de datos.
             Genera datos de ejemplo automaticamente - no requiere CSV externo.
"""

import pandas as pd
import numpy as np
import logging
import os
from datetime import datetime, timedelta

# -- Configuracion de logging (compatible con Windows) ------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("etl_log.log", encoding="utf-8"),
        logging.StreamHandler()
    ]
)
log = logging.getLogger(__name__)


# -- GENERACION DE DATOS DE EJEMPLO ------------------------------------------

def generar_datos_ejemplo(n: int = 500) -> pd.DataFrame:
    """Genera un dataset de ejemplo con transacciones ficticias."""
    np.random.seed(42)
    hoy = datetime.now()
    categorias = ["Consulta", "Terapia", "Evaluacion", "Seguimiento", "Taller"]

    data = {
        "id":          range(1, n + 1),
        "cliente_id":  [f"CLI_{str(np.random.randint(1, 100)).zfill(3)}" for _ in range(n)],
        "fecha":       [(hoy - timedelta(days=int(np.random.exponential(60)))).strftime("%Y-%m-%d") for _ in range(n)],
        "monto":       np.round(np.random.lognormal(mean=10.5, sigma=0.4, size=n), 2),
        "categoria":   np.random.choice(categorias, n, p=[0.3, 0.35, 0.15, 0.1, 0.1]),
        "estado":      np.random.choice(["Activo", "Completado", "Pendiente"], n, p=[0.5, 0.35, 0.15]),
    }

    # Se introducen algunos valores nulos y duplicados para que el pipeline tenga trabajo
    df = pd.DataFrame(data)
    df.loc[np.random.choice(df.index, 10, replace=False), "cliente_id"] = None
    df = pd.concat([df, df.sample(5)], ignore_index=True)  # duplicados

    os.makedirs("data/raw", exist_ok=True)
    ruta = "data/raw/datos_entrada.csv"
    df.to_csv(ruta, index=False, encoding="utf-8")
    log.info(f"Datos de ejemplo generados: {ruta} ({len(df)} filas)")
    return ruta


# -- EXTRACT -----------------------------------------------------------------

def extract_csv(filepath: str) -> pd.DataFrame:
    """Extrae datos desde un archivo CSV."""
    log.info(f"Extrayendo datos desde: {filepath}")
    df = pd.read_csv(filepath, encoding="utf-8")
    log.info(f"  -> {len(df)} filas extraidas.")
    return df


# -- TRANSFORM ---------------------------------------------------------------

def limpiar_nulos(df: pd.DataFrame, columnas_criticas: list) -> pd.DataFrame:
    """Elimina filas con valores nulos en columnas criticas."""
    antes = len(df)
    df = df.dropna(subset=columnas_criticas)
    log.info(f"  -> Filas eliminadas por nulos: {antes - len(df)}")
    return df


def normalizar_fechas(df: pd.DataFrame, columna: str) -> pd.DataFrame:
    """Convierte columna de fechas al tipo datetime."""
    df[columna] = pd.to_datetime(df[columna], errors="coerce")
    invalidas = df[columna].isna().sum()
    if invalidas > 0:
        log.warning(f"  -> {invalidas} fechas invalidas convertidas a NaT en '{columna}'")
    return df


def eliminar_duplicados(df: pd.DataFrame, subset: list) -> pd.DataFrame:
    """Elimina filas duplicadas segun columnas clave."""
    antes = len(df)
    df = df.drop_duplicates(subset=subset, keep="last")
    log.info(f"  -> Duplicados eliminados: {antes - len(df)}")
    return df


def agregar_metadatos(df: pd.DataFrame) -> pd.DataFrame:
    """Agrega columnas de auditoria."""
    df["fecha_carga"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    df["fuente_etl"]  = "etl_pipeline_v1"
    return df


def transform(df: pd.DataFrame, config: dict) -> pd.DataFrame:
    """Orquesta todas las transformaciones."""
    log.info("Iniciando transformaciones...")
    df = limpiar_nulos(df, config.get("columnas_criticas", []))
    if config.get("columna_fecha"):
        df = normalizar_fechas(df, config["columna_fecha"])
    if config.get("columnas_dedup"):
        df = eliminar_duplicados(df, config["columnas_dedup"])
    df = agregar_metadatos(df)
    log.info(f"  -> Transformacion completa. Filas resultantes: {len(df)}")
    return df


# -- LOAD --------------------------------------------------------------------

def load_to_csv(df: pd.DataFrame, output_path: str) -> None:
    """Guarda el DataFrame transformado en CSV."""
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df.to_csv(output_path, index=False, encoding="utf-8")
    log.info(f"  -> Datos guardados en: {output_path}")


# -- PIPELINE PRINCIPAL ------------------------------------------------------

def run_pipeline(source_path: str, output_path: str, config: dict) -> None:
    """Ejecuta el pipeline ETL completo: Extract -> Transform -> Load."""
    log.info("=" * 55)
    log.info("INICIANDO PIPELINE ETL")
    log.info("=" * 55)
    inicio = datetime.now()

    try:
        df_raw   = extract_csv(source_path)
        df_clean = transform(df_raw, config)
        load_to_csv(df_clean, output_path)

        duracion = (datetime.now() - inicio).seconds
        log.info(f"Pipeline completado en {duracion}s - {len(df_clean)} registros procesados.")
        log.info(f"Archivo de salida: {output_path}")

    except Exception as e:
        log.error(f"Error en el pipeline: {e}")
        raise


# -- EJECUCION ---------------------------------------------------------------

if __name__ == "__main__":
    CONFIG = {
        "columnas_criticas": ["id", "fecha", "monto"],
        "columna_fecha":     "fecha",
        "columnas_dedup":    ["id"],
    }

    # Paso 0: Generar datos de ejemplo automaticamente
    log.info("Generando datos de ejemplo...")
    source = generar_datos_ejemplo(n=500)

    # Paso 1-2-3: ETL
    run_pipeline(
        source_path=source,
        output_path="data/processed/datos_limpios.csv",
        config=CONFIG
    )

    # Mostrar preview del resultado
    resultado = pd.read_csv("data/processed/datos_limpios.csv")
    print("\nPREVIEW DEL RESULTADO:")
    print(resultado.head(10).to_string(index=False))
    print(f"\nTotal registros procesados: {len(resultado)}")
