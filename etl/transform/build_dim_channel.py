# etl/transform/build_dim_channel.py
import pandas as pd

def build_dim_channel(data: dict, output_path):
    """
    Construye la dimensión de canales
    Guarda el resultado en warehouse/dim/dim_channel.csv
    """
    dim_channel = data["channel"].copy()

    dim_channel = dim_channel.rename(columns={"channel_id": "id"})

    # salida en warehouse/dim
    file_path = output_path / "dim" / "dim_channel.csv"
    file_path.parent.mkdir(parents=True, exist_ok=True)  # aseguro que exista la carpeta
    dim_channel.to_csv(file_path, index=False)
    
    print(f"✅ dim_channel guardado en {file_path}")
    return dim_channel