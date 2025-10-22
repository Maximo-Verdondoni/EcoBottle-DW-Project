# etl/transform/build_dim_store.py
import pandas as pd

def build_dim_store(data: dict, output_path):
    """
    Construye la dimensión de tienda desnormalizando con direccion y luego con provincia
    Guarda el resultado en warehouse/dim/dim_store.csv
    """
    stores = data["store"].copy()
    addresses = data["address"].copy()
    provinces = data["province"].copy()

    # Paso 1: Unir stores con addresses
    stores_with_address = pd.merge(
        stores,
        addresses,
        left_on="address_id",
        right_on="address_id",
        how="left",
        suffixes=('', '_address')
    )

    # Paso 2: Unir con provinces
    stores_complete = pd.merge(
        stores_with_address,
        provinces[['province_id', 'name', 'code']].rename(
            columns={'name': 'province_name', 'code': 'province_code'}
        ),
        left_on="province_id",
        right_on="province_id",
        how="left"
    )

    stores_complete = stores_complete.rename(columns={"store_id": "id"})

    # Seleccionar y renombrar columnas finales
    dim_store = stores_complete[[
        'id',
        'name',
        'line1',
        'line2', 
        'city',
        'province_name',
        'province_code',
        'postal_code',
        'country_code',
        'created_at'
    ]]

    # salida en warehouse/dim
    file_path = output_path / "dim" / "dim_store.csv"
    file_path.parent.mkdir(parents=True, exist_ok=True)  # aseguro que exista la carpeta
    dim_store.to_csv(file_path, index=False)
    
    print(f"✅ dim_store guardado en {file_path}")
    return dim_store