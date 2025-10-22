# etl/extract.py
import pandas as pd
from pathlib import Path

RAW_PATH = Path("data/raw")

def load_csv(filename: str) -> pd.DataFrame:
    """
    Carga un CSV desde data/raw/ y lo devuelve como DataFrame.
    """
    file_path = RAW_PATH / filename
    return pd.read_csv(file_path)

def extract_all():
    """
    Carga todas las tablas raw y devuelve un diccionario {nombre: DataFrame}.
    """
    data = {
        "address": load_csv("address.csv"),
        "channel": load_csv("channel.csv"),
        "customer": load_csv("customer.csv"),
        "nps_response": load_csv("nps_response.csv"),
        "payment": load_csv("payment.csv"),
        "product_category": load_csv("product_category.csv"),
        "product": load_csv("product.csv"),
        "province": load_csv("province.csv"),
        "sales_order_item": load_csv("sales_order_item.csv"),
        "sales_order": load_csv("sales_order.csv"),
        "shipment": load_csv("shipment.csv"),
        "store": load_csv("store.csv"),
        "web_session": load_csv("web_session.csv"),
    }
    return data

if __name__ == "__main__":
    dfs = extract_all()
    for name, df in dfs.items():
        print(f"{name}: {df.shape[0]} filas, {df.shape[1]} columnas")
