# etl/transform/build_fact_payment.py
import pandas as pd
def build_fact_payment(data,dim_calendar,dim_customer,dim_channel,dim_address,dim_store, output_path):
    """
    Genera una tabla de hechos payment con campos:
    id,customer_id,channel_id,store_id,method,status,amount,paid_at_date_id,paid_at_time,transaction_ref
    """
    fact_payment = data['payment'].copy()
    sales_order = data["sales_order"].copy()
    customers_raw = data["customer"].copy()
    addresses_raw = data["address"].copy()
    channels_raw = data["channel"].copy()
    stores_raw = data["store"].copy()

    dim_calendar = dim_calendar.copy()
    dim_customer = dim_customer.copy()
    dim_channel = dim_channel.copy()
    dim_address = dim_address.copy()
    dim_store = dim_store.copy()

    #Unimos payment con sales_order para obtener las dimensiones
    # PASO 1: Obtener customer_id/billing_address_id/channel_id/store_id desde sales_order
    fact_payment = pd.merge(
        fact_payment,
        sales_order[['order_id', 'customer_id', 'billing_address_id', 'channel_id', 'store_id']],
        on='order_id',
        how='left'
    )

    #DIM CUSTOMER
    # PASO 1: Crear mapeo de customer_id business key a surrogate key
    # Necesitamos los datos crudos de customer para tener el customer_id original
    customer_map = customers_raw[['customer_id']].reset_index(drop=True)
    customer_map = customer_map.merge(
        dim_customer.reset_index()[['id']], 
        left_index=True, 
        right_index=True
    ).rename(columns={'id': 'customer_sk'})

    # PASO 2: Obtener la surrogate key de dim_customer
    fact_payment = pd.merge(
        fact_payment,
        customer_map,
        on='customer_id',
        how='left'
    )
    # borramos la id original y nos quedamos con la surrogada
    fact_payment = fact_payment.drop(columns=["customer_id"])
    fact_payment = fact_payment.rename(columns={'customer_sk': 'customer_id'})

    ## DIM_ADDRESS (billing_address)
    # PASO 1: Crear mapeo de billing_address_id business key a surrogate key
    # Necesitamos los datos crudos de address para tener el address_id original
    address_map = addresses_raw[['address_id']].reset_index(drop=True)
    address_map = address_map.merge(
        dim_address.reset_index()[['id']], 
        left_index=True, 
        right_index=True
    ).rename(columns={'id': 'billing_address_sk'})
    # PASO 2: Obtener la surrogate key de dim_address
    fact_payment = pd.merge(
        fact_payment,
        address_map,
        left_on='billing_address_id',
        right_on= 'address_id',
        how='left'
    )
    # borramos la id original y nos quedamos con la surrogada
    fact_payment = fact_payment.drop(columns=["address_id", "billing_address_id"]) #Borramos las ids originales
    fact_payment = fact_payment.rename(columns={'billing_address_sk': 'billing_address_id'})
    fact_payment['billing_address_id'] = fact_payment['billing_address_id'].astype('Int64') #algunos pagos no tienen billing


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
    fact_payment = pd.merge(
        fact_payment,
        channel_map,
        on="channel_id",
        how='left'
    )
    # borramos la id original y nos quedamos con la surrogada
    fact_payment = fact_payment.drop(columns=["channel_id"])
    fact_payment = fact_payment.rename(columns={'channel_sk': 'channel_id'})

    #DIM STORE
    #Buscamos store_id en sales_order, buscamos su surrogada en dim_store
    #y la traemos a la fact.
    # PASO 1: Crear mapeo de store_id business key a surrogate key
    # Necesitamos los datos crudos de store para tener el store_id original
    store_map = stores_raw[['store_id']].reset_index(drop=True)
    store_map = store_map.merge(
        dim_store.reset_index()[['id']], 
        left_index=True, 
        right_index=True
    ).rename(columns={'id': 'store_sk'})
    # PASO 2: Obtener la surrogate key de dim_store
    fact_payment = pd.merge(
        fact_payment,
        store_map,
        on='store_id',
        how='left'
    )
    # borramos la id original y nos quedamos con la surrogada
    fact_payment = fact_payment.drop(columns=["store_id"])
    fact_payment = fact_payment.rename(columns={'store_sk': 'store_id'})
    fact_payment['store_id'] = fact_payment['store_id'].astype('Int64')


    #separamos paid_at en fecha y hora
    fact_payment['paid_at'] = pd.to_datetime(fact_payment['paid_at'])
    fact_payment['paid_at_date'] = fact_payment['paid_at'].dt.normalize()
    fact_payment['paid_at_time'] = fact_payment['paid_at'].dt.time
    fact_payment = fact_payment.drop(columns=['paid_at'])

    #usamos la sk de dim_calendar para shipment_at_date
    fact_payment = pd.merge(
        fact_payment,
        dim_calendar, 
        left_on='paid_at_date', # La NK de la tabla de hechos
        right_on='date',             # La NK de la dimensión
        how='left'
    )
    fact_payment = fact_payment.drop(columns=["paid_at_date", "date"])
    fact_payment = fact_payment.rename(columns={'id': 'paid_at_date_id'})
    fact_payment["paid_at_date_id"] = fact_payment["paid_at_date_id"].astype("Int64") #algun

    #surrogate key
    fact_payment['id'] = range(1, len(fact_payment) + 1)

    #elegimos columnas y reordenamos
    fact_payment = fact_payment[[
        'id', "customer_id", "billing_address_id", "channel_id", "store_id",
        'method','status','amount','paid_at_date_id', 'paid_at_time',
        'transaction_ref'
    ]]

    # Guardamos en warehouse/fact
    file_path = output_path / "fact" / "fact_payment.csv"
    file_path.parent.mkdir(parents=True, exist_ok=True)
    fact_payment.to_csv(file_path, index=False)
    
    print(f"✅ fact_payment guardado en {file_path}")
    return fact_payment