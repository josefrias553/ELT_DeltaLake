# 🔄 GitHub ELT Pipeline con Delta Lake

Este proyecto implementa un pipeline **ELT** en Python, diseñado para extraer datos desde la API de GitHub, cargarlos en formato Delta Lake y luego transformarlos eficientemente mediante pandas.
Actualmente, el código cumple su objetivo funcional, aunque soy consciente de que requiere mejoras, como documentar detalladamente lo que hace cada función en sus respectivos módulos, con el fin de facilitar su mantenimiento, escalabilidad y comprensión a futuro.

---

## 📂 Estructura del Proyecto

```
.
├── main.py
├── extraction_full.py
├── extraction_incremental.py
├── build_tables.py
├── deltalake_module_bronze.py
├── deltalake_module_silver.py
├── process_data_bronze.py
├── preprocessing.py
├── config.py
├── pipeline.conf
├── last_update.txt
├── requirements.txt
└── README.md
```

---

## 🚀 Funcionalidades Principales

- Extracción **full** e **incremental** desde GitHub.
- Carga de datos sin procesar (raw) a tablas Delta (Bronze Layer).
- Transformaciones posteriores a la carga, generando tablas limpias y estructuradas (Silver Layer).
- Gestión de fechas de ejecución para carga incremental de commits.

---

## ⚙️ Configuración

Editá el archivo `pipeline.conf` con tu token personal:

```ini
[api_credentials]
token = TU_TOKEN_AQUI
```

También podés modificar headers, endpoints y parámetros generales en `config.py`.

---

## ▶️ Ejecución del Pipeline

Ejecutá el archivo principal con:

```bash
python main.py
```

---

## 🧩 Módulos Clave

### `main.py`
Orquesta el flujo general, solicitando tipo de extracción y delegando a los módulos apropiados.

---

## 🧠 Requisitos Técnicos

Instalación recomendada:

```bash
pip install -r requirements.txt
```

### Principales dependencias

| Paquete               | Versión       | Descripción breve                                  |
|----------------------|---------------|----------------------------------------------------|
| `deltalake`          | `0.25.5`      | Soporte para tablas ACID sobre Parquet             |
| `pandas`             | `2.2.3`       | Manipulación de datos estructurados                |
| `pyarrow`            | `20.0.0`      | Compatibilidad con Apache Arrow / Parquet          |
| `requests`           | `2.32.3`      | Cliente HTTP robusto para APIs                     |
| `python-dateutil`    | `2.9.0.post0` | Manejo avanzado de fechas                          |
| `pytz` / `tzdata`    | `2025.2`      | Zonas horarias actualizadas                        |

---

## 📝 Notas Finales

- El pipeline puede ejecutarse múltiples veces sin generar duplicados.
- Las transformaciones (Lógica "T" del ELT) se aplican después de la carga inicial en la capa Bronze.

---

Proyecto desarrollado por **José David Frías** – Data Engineer en formación.
