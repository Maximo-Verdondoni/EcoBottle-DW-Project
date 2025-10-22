# etl/transform/build_fact_shipment.py
import pandas as pd
def build_fact_shipment(data,dim_calendar,dim_customer,dim_channel,dim_address, output_path):
    """
    Genera una tabla de hechos shipment con campos:
    id, customer_id, address_id, carrier, shipped_at_date_id, shipped_at_time, delivered_at_date_id, delivered_at_time, tracking_number
    """
    fact_shipment = data["shipment"].copy()
    sales_order = data["sales_order"].copy()
    customers_raw = data["customer"].copy()
    addresses_raw = data["address"].copy()
    channels_raw = data["channel"].copy()
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

    # PASO 2: Crear mapeo de customer_id business key a surrogate key
    # Necesitamos los datos crudos de customer para tener el customer_id original
    customer_map = customers_raw[['customer_id']].reset_index(drop=True)
    customer_map = customer_map.merge(
        dim_customer.reset_index()[['id']], 
        left_index=True, 
        right_index=True
    ).rename(columns={'id': 'customer_sk'})

    # PASO 3: Obtener la surrogate key de dim_customer
    fact_shipment = pd.merge(
        fact_shipment,
        customer_map,
        on='customer_id',
        how='left'
    )
    # borramos la id original y nos quedamos con la surrogada
    fact_shipment = fact_shipment.drop(columns=["customer_id"])
    fact_shipment = fact_shipment.rename(columns={'customer_sk': 'customer_id'})


    ## DIM_ADDRESS (shipping_address)
    # PASO 1: Crear mapeo de shipping_address_id business key a surrogate key
    # Necesitamos los datos crudos de customer para tener el customer_id original
    address_map = addresses_raw[['address_id']].reset_index(drop=True)
    address_map = address_map.merge(
        dim_address.reset_index()[['id']], 
        left_index=True, 
        right_index=True
    ).rename(columns={'id': 'shipping_address_sk'})
    # PASO 2: Obtener la surrogate key de dim_address
    fact_shipment = pd.merge(
        fact_shipment,
        address_map,
        left_on='shipping_address_id',
        right_on= 'address_id',
        how='left'
    )
    # borramos la id original y nos quedamos con la surrogada
    fact_shipment = fact_shipment.drop(columns=["address_id", "shipping_address_id"]) #Borramos las ids originales
    fact_shipment = fact_shipment.rename(columns={'shipping_address_sk': 'shipping_address_id'})

    # DIM_CHANNEL
    #buscamos channel_id en order_sales, buscamos su surrogada en dim_channel
    # y nos traemos esa columna en la fact
    # PASO 1: Crear mapeo de channel_id business key a surrogate key
    # Necesitamos los datos crudos de channel para tener el channel_id original
    channel_map = channels_raw[['channel_id']].reset_index(drop=True)
    channel_map = channel_map.merge(
        dim_channel.reset_index()[['id']], 
        left_index=True, 
        right_index=True
    ).rename(columns={'id': 'channel_sk'})
    # PASO 2: Obtener la surrogate key de dim_channel
    fact_shipment = pd.merge(
        fact_shipment,
        channel_map,
        on="channel_id",
        how='left'
    )
    # borramos la id original y nos quedamos con la surrogada
    fact_shipment = fact_shipment.drop(columns=["channel_id"])
    fact_shipment = fact_shipment.rename(columns={'channel_sk': 'channel_id'})

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