# etl/transform/build_dim_address.py
import pandas as pd

def build_dim_address(data: dict, output_path):
    """
    Construye la dimensión de dirección desnormalizando con provincia
    Guarda el resultado en warehouse/dim/dim_address.csv
    """
    addresses = data["address"].copy()
    provinces = data["province"].copy()

    # Unir direcciones con provincias
    dim_address = pd.merge(
        addresses,
        provinces[['province_id', 'name', 'code']].rename(
            columns={'name': 'province_name', 'code': 'province_code'}
        ),
        on='province_id',
        how='left'
    )

    # Agregar tipo de dirección basado en el ID (opcional)
    dim_address['address_type'] = dim_address['address_id'].apply(
        lambda x: 'store' if x <= 4 else 'customer'
    )

    # CREAR SURROGATE KEY 
    dim_address['id'] = range(1, len(dim_address) + 1)

    # Reordenar columnas - ID surrogada primero, mantener address_id como business key
    dim_address = dim_address[[
        'id',                           # Surrogate key
        'line1', 'line2', 'city', 
        'province_name', 'province_code',
        'postal_code', 'country_code', 'address_type', 'created_at'
    ]]

    # salida en warehouse/dim
    file_path = output_path / "dim" / "dim_address.csv"
    file_path.parent.mkdir(parents=True, exist_ok=True)  # aseguro que exista la carpeta
    dim_address.to_csv(file_path, index=False)
    
    print(f"✅ dim_address guardado en {file_path}")
    return dim_address