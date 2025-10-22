# etl/transform/build_fact_payment.py
import pandas as pd
def build_fact_payment(data,dim_calendar,dim_customer,dim_channel,dim_address,dim_store, output_path):
    """
    Genera una tabla de hechos payment con campos:
    id,customer_id,channel_id,store_id,method,status,amount,paid_at_date_id,paid_at_time,transaction_ref
    """
    fact_payment = data['payment'].copy()
    sales_order = data["sales_order"].copy()

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
    fact_payment = pd.merge(
        fact_payment,
        dim_customer,
        left_on='customer_id',
        right_on='customer_key',
        how='left',
        suffixes=('_payment', '_customer')
    ).drop(columns=['customer_id','customer_key'])
    fact_payment = fact_payment.rename(columns={'id': 'customer_id'})

    ## DIM_ADDRESS (billing_address)
    fact_payment = pd.merge(
        fact_payment,
        dim_address,
        left_on='billing_address_id',
        right_on='address_key',
        how='left'
    ).drop(columns=['billing_address_id','address_key'])
    fact_payment = fact_payment.rename(columns={'id': 'billing_address_id'})
    fact_payment['billing_address_id'] = fact_payment['billing_address_id'].astype('Int64') #algunos pagos no tienen billing


    # DIM_CHANNEL
    fact_payment = pd.merge(
        fact_payment,
        dim_channel,
        left_on='channel_id',
        right_on='channel_key',
        how='left'
    ).drop(columns=['channel_id', 'channel_key'])
    fact_payment = fact_payment.rename(columns={'id': 'channel_id'})

    #DIM STORE
    fact_payment = pd.merge(
        fact_payment,
        dim_store,
        left_on='store_id',
        right_on='store_key',
        how='left'
    ).drop(columns=['store_id', 'store_key'])
    fact_payment = fact_payment.rename(columns={'id': 'store_id'})
    fact_payment['store_id'] = fact_payment['store_id'].astype('Int64') #algunos pagos no tienen store

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
        'method','status_payment','amount','paid_at_date_id', 'paid_at_time',
        'transaction_ref'
    ]]

    # Guardamos en warehouse/fact
    file_path = output_path / "fact" / "fact_payment.csv"
    file_path.parent.mkdir(parents=True, exist_ok=True)
    fact_payment.to_csv(file_path, index=False)
    
    print(f"✅ fact_payment guardado en {file_path}")
    return fact_payment