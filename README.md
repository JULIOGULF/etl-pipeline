# ⚡ ETL Pipeline Automatizado

Pipeline de extracción, transformación y carga de datos desarrollado en Python. Diseñado para procesar datos desde múltiples fuentes (CSV, bases de datos SQL) hacia un destino centralizado, con logging automático y manejo de errores.

## 🛠 Stack Tecnológico

- **Python 3.10+**
- **Pandas** — transformación y limpieza de datos
- **SQLAlchemy** — conexión a bases de datos
- **Logging** — trazabilidad del pipeline

## 📁 Estructura del Proyecto

```
etl-pipeline/
├── etl_pipeline.py       # Script principal del pipeline
├── requirements.txt      # Dependencias
├── data/
│   ├── raw/              # Datos de entrada
│   └── processed/        # Datos transformados
└── etl_log.log           # Log de ejecución
```

## 🚀 Cómo ejecutar

```bash
# Instalar dependencias
pip install -r requirements.txt

# Ejecutar el pipeline
python etl_pipeline.py
```

## ⚙️ Configuración

En el bloque `CONFIG` del script puedes definir:

```python
CONFIG = {
    "columnas_criticas": ["id", "fecha", "monto"],  # Columnas que no pueden ser nulas
    "columna_fecha": "fecha",                        # Columna a normalizar como datetime
    "columnas_dedup": ["id"],                        # Columnas para detectar duplicados
}
```

## 📊 Funcionalidades

| Etapa | Función |
|---|---|
| **Extract** | CSV y bases de datos SQL |
| **Transform** | Limpieza de nulos, normalización de fechas, deduplicación |
| **Load** | CSV de salida o tabla en base de datos |
| **Auditoría** | Columnas `fecha_carga` y `fuente_etl` agregadas automáticamente |
| **Logging** | Registro en consola y archivo `etl_log.log` |

## 📈 Resultados

- Reducción del tiempo de procesamiento manual en más de un 60%
- Trazabilidad completa mediante logging por etapa
- Reutilizable para múltiples fuentes de datos

---
**Autor:** Julio Carvallo | [LinkedIn](https://linkedin.com/in/TU_PERFIL)
