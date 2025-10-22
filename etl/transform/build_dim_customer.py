# etl/transform/build_dim_customer.py
import pandas as pd

def build_dim_customer(data: dict, output_path):
    """
    Construye la dimensión de clientes 
    Guarda el resultado en warehouse/dim/dim_customer.csv
    """
    dim_customer = data["customer"].copy()


    # salida en warehouse/dim
    file_path = output_path / "dim" / "dim_customers.csv"
    file_path.parent.mkdir(parents=True, exist_ok=True)  # aseguro que exista la carpeta
    dim_customer.to_csv(file_path, index=False)
    
    print(f"✅ dim_customers guardado en {file_path}")
    return dim_customer