# etl/transform/build_fact_sales_order_item.py
import pandas as pd
def build_fact_sales_order_item(data,dim_calendar,dim_customer,dim_channel,dim_store,dim_product, output_path):
    """
    Genera una tabla de hechos sales_order con campos:
    id, customer_id, channel_id, store_id, product_id,
    quantity, unit_price, discount_amount, line_total
    """
    fact_sales_order_item = data["sales_order_item"].copy()
    sales_order = data["sales_order"].copy()
    dim_calendar = dim_calendar.copy()
    dim_customer = dim_customer.copy()
    dim_channel = dim_channel.copy()
    dim_store = dim_store.copy()
    dim_product = dim_product.copy()

    fact_sales_order_item = pd.merge(
        fact_sales_order_item,
        sales_order[['order_id', 'customer_id', 'channel_id', 'store_id', 'order_date']],
        on="order_id",
        how="left"
    )

    #DIM CUSTOMER
    fact_sales_order_item = pd.merge(
        fact_sales_order_item,
        dim_customer,
        left_on="customer_id",
        right_on="customer_key",
        how="left",
        suffixes=('_order', '_customer')
    ).drop(columns=["customer_id", "customer_key"])
    fact_sales_order_item = fact_sales_order_item.rename(columns={"id": "customer_id"})

    #DIM CHANNEL
    fact_sales_order_item = pd.merge(
        fact_sales_order_item,
        dim_channel,
        left_on="channel_id",
        right_on="channel_key",
        how="left",
    ).drop(columns=["channel_id", "channel_key"])
    fact_sales_order_item = fact_sales_order_item.rename(columns={"id": "channel_id"})

    #DIM STORE
    fact_sales_order_item = pd.merge(
        fact_sales_order_item,
        dim_store,
        left_on="store_id",
        right_on="store_key",
        how="left",
    ).drop(columns=["store_id", "store_key"])
    fact_sales_order_item = fact_sales_order_item.rename(columns={"id": "store_id"})
    fact_sales_order_item["store_id"] = fact_sales_order_item["store_id"].astype("Int64")

    #DIM PRODUCT
    fact_sales_order_item = pd.merge(
        fact_sales_order_item,
        dim_product,
        left_on="product_id",
        right_on="product_key",
        how="left"
    ).drop(columns=["product_id", "product_key"])
    fact_sales_order_item = fact_sales_order_item.rename(columns={"id": "product_id"})

    #DIM CALENDAR
    #separamos order_date en fecha y hora
    fact_sales_order_item['order_date'] = pd.to_datetime(fact_sales_order_item['order_date'])
    fact_sales_order_item['new_order_date'] = fact_sales_order_item['order_date'].dt.normalize()
    fact_sales_order_item['order_time'] = fact_sales_order_item['order_date'].dt.time
    fact_sales_order_item = fact_sales_order_item.drop(columns=['order_date'])

     #usamos la sk de dim_calendar para shipment_at_date
    fact_sales_order_item = pd.merge(
        fact_sales_order_item,
        dim_calendar, 
        left_on='new_order_date', # La NK de la tabla de hechos
        right_on='date',             # La NK de la dimensión
        how='left'
    )
    fact_sales_order_item = fact_sales_order_item.drop(columns=["new_order_date", "date"])
    fact_sales_order_item = fact_sales_order_item.rename(columns={'id': 'order_date_id'})
    fact_sales_order_item["order_date_id"] = fact_sales_order_item["order_date_id"].astype("Int64") #para que sea int por si hay algun NaN



    #surrogate key
    fact_sales_order_item['id'] = range(1, len(fact_sales_order_item) + 1)

    # Seleccionar y renombrar columnas finales
    fact_sales_order_item = fact_sales_order_item[[
        'id', 'customer_id', 'channel_id', 'store_id', 'product_id', 'order_date_id',
        'quantity', 'unit_price', 'discount_amount', 'line_total'
    ]]

    # Guardamos en warehouse/fact
    file_path = output_path / "fact" / "fact_sales_order_item.csv"
    file_path.parent.mkdir(parents=True, exist_ok=True)
    fact_sales_order_item.to_csv(file_path, index=False)
    
    print(f"✅ fact_sales_order_item guardado en {file_path}")
    return fact_sales_order_item