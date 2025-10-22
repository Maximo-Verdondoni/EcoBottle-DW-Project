# etl/transform/build_fact_shipment.py
import pandas as pd
def build_fact_shipment(data,dim_calendar,dim_customer,dim_channel,dim_address, output_path):
    """
    Genera una tabla de hechos shipment con campos:
    id, customer_id, address_id, carrier, shipped_at_date_id, shipped_at_time, delivered_at_date_id, delivered_at_time, tracking_number
    """
    fact_shipment = data["shipment"].copy()
    sales_order = data["sales_order"].copy()

    dim_calendar = dim_calendar.copy()
    dim_customer = dim_customer.copy()
    dim_channel = dim_channel.copy()
    dim_address = dim_address.copy()

    ## DIM_CUSTOMER
    #agarramos las ids de customer y shipping_address y channel a traves de sales_order, y le buscamos sus surrogadas en sus dim y nos la quedamos en fact_shipment
    # PASO 1: Obtener customer_id/shipping_address_id/channel_id desde sales_order
    fact_shipment = pd.merge(
        fact_shipment,
        sales_order[['order_id', 'customer_id', 'shipping_address_id', 'channel_id']],
        on='order_id',
        how='left'
    )

    # DIM CUSTOMER
    fact_shipment = pd.merge(
        fact_shipment,
        dim_customer,
        left_on='customer_id',
        right_on='customer_key',
        how='left',
        suffixes=('_payment', '_customer')
    ).drop(columns=['customer_id','customer_key'])
    fact_shipment = fact_shipment.rename(columns={'id': 'customer_id'})


    ## DIM_ADDRESS (shipping_address)
    fact_shipment = pd.merge(
        fact_shipment,
        dim_address,
        left_on='shipping_address_id',
        right_on='address_key',
        how='left'
    ).drop(columns=['shipping_address_id','address_key'])
    fact_shipment = fact_shipment.rename(columns={'id': 'shipping_address_id'})
    fact_shipment['shipping_address_id'] = fact_shipment['shipping_address_id'].astype('Int64') #algunos pagos no tienen billing

    # DIM_CHANNEL
    fact_shipment = pd.merge(
        fact_shipment,
        dim_channel,
        left_on='channel_id',
        right_on='channel_key',
        how='left'
    ).drop(columns=['channel_id', 'channel_key'])
    fact_shipment = fact_shipment.rename(columns={'id': 'channel_id'})

    #separamos shipped_at y delivered_at en fecha y hora
    fact_shipment['shipped_at'] = pd.to_datetime(fact_shipment['shipped_at'])
    fact_shipment['shipped_at_date'] = fact_shipment['shipped_at'].dt.normalize()
    fact_shipment['shipped_at_time'] = fact_shipment['shipped_at'].dt.time
    fact_shipment = fact_shipment.drop(columns=['shipped_at'])

    #usamos la sk de dim_calendar para shipment_at_date
    fact_shipment = pd.merge(
        fact_shipment,
        dim_calendar, 
        left_on='shipped_at_date', # La NK de la tabla de hechos
        right_on='date',             # La NK de la dimensión
        how='left'
    )
    fact_shipment = fact_shipment.drop(columns=["shipped_at_date", "date"])
    fact_shipment = fact_shipment.rename(columns={'id': 'shipped_at_date_id'})
    fact_shipment["shipped_at_date_id"] = fact_shipment["shipped_at_date_id"].astype("Int64") #algunas veces la FK es null


    fact_shipment['delivered_at'] = pd.to_datetime(fact_shipment['delivered_at'])
    fact_shipment['delivered_at_date'] = fact_shipment['delivered_at'].dt.normalize()
    fact_shipment['delivered_at_time'] = fact_shipment['delivered_at'].dt.time
    fact_shipment = fact_shipment.drop(columns=['delivered_at'])

    #usamos la sk de dim_calendar para delivered_at_date
    fact_shipment = pd.merge(
        fact_shipment,
        dim_calendar, 
        left_on='delivered_at_date', # La NK de la tabla de hechos
        right_on='date',             # La NK de la dimensión
        how='left'
    )
    fact_shipment = fact_shipment.drop(columns=["delivered_at_date", "date"])
    fact_shipment = fact_shipment.rename(columns={'id': 'delivered_at_date_id'})
    fact_shipment["delivered_at_date_id"] = fact_shipment["delivered_at_date_id"].astype("Int64") #algunas veces la FK es null

    #surrogate key
    fact_shipment['id'] = range(1, len(fact_shipment) + 1)

    # Seleccionar y renombrar columnas finales
    fact_shipment = fact_shipment[[
        'id', "customer_id", "shipping_address_id", "channel_id",
        'carrier','shipped_at_date_id','shipped_at_time','delivered_at_date_id',
        'delivered_at_time','tracking_number'
    ]]

    # Guardamos en warehouse/fact
    file_path = output_path / "fact" / "fact_shipment.csv"
    file_path.parent.mkdir(parents=True, exist_ok=True)
    fact_shipment.to_csv(file_path, index=False)
    
    print(f"✅ fact_shipment guardado en {file_path}")
    return fact_shipment