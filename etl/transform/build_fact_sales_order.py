# etl/transform/build_fact_sales_order.py
import pandas as pd
def build_fact_sales_order(data,dim_calendar,dim_customer,dim_channel,dim_store,dim_address, output_path):
    """
    Genera una tabla de hechos sales_order con campos:
    id, customer_id,channel_id, store_id, order_date_id, order_time, billing_address_id, shipping_address_id, 
    status, currency_code, subtotal, tax_amount, shipping_fee, total_amount
    """
    fact_sales_order = data["sales_order"]
    fact_sales_order["billing_address_id"] = fact_sales_order["billing_address_id"].astype("Int64")
    fact_sales_order["store_id"] = fact_sales_order["store_id"].astype("Int64")
    dim_calendar = dim_calendar.copy()
    dim_customer = dim_customer.copy()
    dim_channel = dim_channel.copy()
    dim_store = dim_store.copy()
    dim_address = dim_address.copy()

    #DIM CUSTOMER
    fact_sales_order = pd.merge(
        fact_sales_order,
        dim_customer,
        left_on="customer_id",
        right_on="customer_key",
        how="left",
        suffixes=('_order', '_customer')
    ).drop(columns=["customer_id", "customer_key"])
    fact_sales_order = fact_sales_order.rename(columns={"id": "customer_id"})


    #DIM CHANNEL
    fact_sales_order = pd.merge(
        fact_sales_order,
        dim_channel,
        left_on="channel_id",
        right_on="channel_key",
        how="left",
    ).drop(columns=["channel_id", "channel_key"])
    fact_sales_order = fact_sales_order.rename(columns={"id": "channel_id"})

    #DIM STORE
    fact_sales_order = pd.merge(
        fact_sales_order,
        dim_store,
        left_on="store_id",
        right_on="store_key",
        how="left",
    ).drop(columns=["store_id", "store_key"])
    fact_sales_order = fact_sales_order.rename(columns={"id": "store_id"})
    fact_sales_order["store_id"] = fact_sales_order["store_id"].astype("Int64")

    #DIM ADDRESS    
    #billing
    fact_sales_order = pd.merge(
        fact_sales_order,
        dim_address,
        left_on="billing_address_id",
        right_on="address_key",
        how="left",
        suffixes=('_order', '_billing')
    ).drop(columns=["billing_address_id", "address_key"])
    fact_sales_order = fact_sales_order.rename(columns={"id": "billing_address_id"})
    fact_sales_order["billing_address_id"] = fact_sales_order["billing_address_id"].astype("Int64")
    #shipping
    fact_sales_order = pd.merge(
        fact_sales_order,
        dim_address,
        left_on="shipping_address_id",
        right_on="address_key",
        how="left",
        suffixes=('_order', '_shipping')
    ).drop(columns=["shipping_address_id", "address_key"])
    fact_sales_order = fact_sales_order.rename(columns={"id": "shipping_address_id"})

    #separamos order_date en fecha y hora
    fact_sales_order['order_date'] = pd.to_datetime(fact_sales_order['order_date'])
    fact_sales_order['order_date_date'] = fact_sales_order['order_date'].dt.normalize()
    fact_sales_order['order_time'] = fact_sales_order['order_date'].dt.time
    fact_sales_order = fact_sales_order.drop(columns=['order_date'])

    #usamos la sk de dim_calendar para shipment_at_date
    fact_sales_order = pd.merge(
        fact_sales_order,
        dim_calendar, 
        left_on='order_date_date', # La NK de la tabla de hechos
        right_on='date',             # La NK de la dimensión
        how='left'
    )
    fact_sales_order = fact_sales_order.drop(columns=["order_date_date", "date"])
    fact_sales_order = fact_sales_order.rename(columns={'id': 'order_date_id'})
    fact_sales_order["order_date_id"] = fact_sales_order["order_date_id"].astype("Int64") #algunas veces la FK es null

    #surrogate key
    fact_sales_order['id'] = range(1, len(fact_sales_order) + 1)

    # Seleccionar y renombrar columnas finales
    fact_sales_order = fact_sales_order[[
        'id', 'customer_id','channel_id', 'store_id', 'order_date_id', 
        'order_time', 'billing_address_id', 'shipping_address_id', 
        'status_order', 'currency_code', 'subtotal', 'tax_amount', 'shipping_fee', 'total_amount'
    ]]


    # Guardamos en warehouse/fact
    file_path = output_path / "fact" / "fact_sales_order.csv"
    file_path.parent.mkdir(parents=True, exist_ok=True)
    fact_sales_order.to_csv(file_path, index=False)
    
    print(f"✅ fact_sales_order guardado en {file_path}")
    return fact_sales_order