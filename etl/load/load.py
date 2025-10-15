# etl/load.py
from pathlib import Path
from etl.extract.extract import extract_all
#from etl.transform.build_dim_product import build as build_dim_product

OUTPUT_PATH = Path("warehouse")  #A donde apunta el pipeline

def run_pipeline():
    data = extract_all()

    print("Construyendo dimensiones y hechos...")

    #df_dim_products = build_dim_product(data, OUTPUT_PATH)
    #df_dim_calendar = build_dim_calendar(OUTPUT_PATH, "2025-01-01", "2025-12-31")
    print("✅ Pipeline completado. Archivos guardados en warehouse/")

if __name__ == "__main__":
    run_pipeline()
