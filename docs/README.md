# TP Final: Ecosistema de Datos de Marketing (EcoBottle)

Proyecto final para la materia "Introducción al Marketing Online y los Negocios Digitales". El objetivo es diseñar e implementar un mini-ecosistema de datos comercial (online + offline) para la empresa ficticia EcoBottle.

El pipeline completo ingesta datos crudos (CSV), los transforma usando Python (Pandas) para crear un Data Warehouse dimensional usando star-schema, y finalmente presenta los KPIs clave en un dashboard de Looker Studio.

**Dashboard Final (Looker Studio):** [Inserta aquí el enlace público a tu dashboard]

---

## 🛠️ Herramientas Utilizadas

* **Python 3.10+**
* **Pandas:** Para las transformaciones (ETL).
* **Git / GitHub:** Para control de versiones y gestión del proyecto.
* **Looker Studio (Google Data Studio):** Para la visualización y el dashboard.

---

## 🚀 Arquitectura del Proyecto

El proyecto sigue una estructura ETL clásica:

1.  **`data/raw/`**: Contiene los 13 archivos .CSV fuente que simulan la base de datos transaccional (OLTP) de la empresa.
2.  **`etl/`**: Contiene toda la lógica de transformación, separada en:
    * **`etl/extract/`**: Scripts para leer los datos desde `data/raw/`.
    * **`etl/transform/`**: Scripts para limpiar, denormalizar y construir cada tabla de Dimensión y Hechos.
    * **`etl/load/`**: Scripts para guardar los dataframes transformados en el directorio `warehouse/`.
3.  **`warehouse/`**: Es el Data Warehouse de salida. Los archivos aquí están listos para ser consumidos por Looker Studio:
    * **`warehouse/dim/`**: Contiene las tablas de dimensiones (ej. `dim_products.csv`, `dim_customers.csv`).
    * **`warehouse/fact/`**: Contiene las tablas de hechos (ej. `fact_sales_order.csv`, `fact_nps_response.csv`).
4.  **`main.py`**: El script orquestador que llama a las funciones de `extract`, `transform` y `load` en el orden correcto.

---

## 📋 Instrucciones de Ejecución Local

Sigue estos pasos para ejecutar el pipeline de transformación localmente:

1.  **Clonar el repositorio:**
    ```bash
    git clone [https://github.com/Maximo-Verdondoni/mkt_tp_final.git](https://github.com/Maximo-Verdondoni/mkt_tp_final.git)
    cd mkt_tp_final
    ```

2.  **Crear y activar un entorno virtual**:
    ```bash
    # En macOS/Linux
    python3 -m venv .venv
    source .venv/bin/activate

    # En Windows (cmd)
    python -m venv .venv
    .\.venv\Scripts\activate
    ```

3.  **Instalar dependencias:**
    ```bash
    pip install -r requirements.txt
    ```

4.  **Ejecutar el pipeline de transformación:**
    El script `main.py` en la raíz del proyecto orquesta todo el proceso.
    ```bash
    python main.py
    ```

5.  **Verificar la salida:**
    Tras la ejecución, la carpeta `warehouse/` deberá contener las carpetas `dim/` y `fact/` con los archivos .CSV transformados.
