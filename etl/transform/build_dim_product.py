# etl/transform/build_dim_products.py
import pandas as pd

def build_dim_product(data: dict, output_path):
    """
    Construye la dimensión de productos desnormalizando categorías y productos.
    Guarda el resultado en warehouse/dim/dim_products.csv
    """
    categories = data["product_category"].copy()
    products = data["product"].copy()

    # Limpiar y convertir parent_id
    categories['parent_id'] = categories['parent_id'].replace('', None)
    categories['parent_id'] = pd.to_numeric(categories['parent_id'], errors='coerce')

    # Preparar categorías padre
    parent_categories = categories[["category_id", "name"]].copy()
    parent_categories = parent_categories.rename(
        columns={"category_id": "parent_id", "name": "parent_category_name"}
    )

    # Unir categorías con sus padres
    categories_enriched = pd.merge(
        categories,
        parent_categories,
        on="parent_id",
        how="left",
        suffixes=('', '_parent')
    )

    # Renombrar columnas
    categories_enriched = categories_enriched.rename(
        columns={"name": "category_name"}
    )

    # Combinar productos con categorías enriquecidas
    dim_products = pd.merge(
        products,
        categories_enriched[["category_id", "category_name", "parent_category_name"]],
        on="category_id",
        how="left"
    )

    dim_products = dim_products.rename(columns={"product_id": "id"})
    # Eliminar category_id y reordenar columnas si es necesario
    dim_products = dim_products.drop("category_id", axis=1)

    # salida en warehouse/dim
    file_path = output_path / "dim" / "dim_products.csv"
    file_path.parent.mkdir(parents=True, exist_ok=True)  # aseguro que exista la carpeta
    dim_products.to_csv(file_path, index=False)
    
    print(f"✅ dim_products guardado en {file_path}")
    return dim_products