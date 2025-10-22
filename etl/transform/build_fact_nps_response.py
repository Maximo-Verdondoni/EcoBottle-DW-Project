# etl/transform/build_fact_nps_response.py
import pandas as pd
def build_fact_nps_response(data,dim_customer,dim_channel,dim_calendar, output_path):
    """
    Genera una tabla de hechos nps_response con campos:
    id,customer_id,channel_id,score,comment,responded_date, responded_at
    """
    nps_responses = data["nps_response"].copy()
    dim_customer = dim_customer.copy()
    dim_channel = dim_channel.copy()

    fact_nps_response = pd.merge(
        nps_responses,
        dim_customer, 
        left_on='customer_id', 
        right_on='id',
        how='left',
        suffixes=('_nk', '_sk') # _nk para nps_responses, _sk para dim_customer
    )
    fact_nps_response = fact_nps_response.drop(columns=["customer_id"])
    fact_nps_response = fact_nps_response.rename(columns={'id': 'customer_id'})

    fact_nps_response = pd.merge(
        fact_nps_response,
        dim_channel, 
        left_on='channel_id', 
        right_on='id',
        how='left',
        suffixes=('_nk', '_sk') # _nk para nps_responses, _sk para dim_channel
    )
    fact_nps_response = fact_nps_response.drop(columns=["channel_id"])
    fact_nps_response = fact_nps_response.rename(columns={'id': 'channel_id'})    

    #Separamos responded_at en fecha y hora
    fact_nps_response['responded_at'] = pd.to_datetime(fact_nps_response['responded_at'])
    fact_nps_response['responded_at_date'] = fact_nps_response['responded_at'].dt.normalize()
    fact_nps_response['responded_at_time'] = fact_nps_response['responded_at'].dt.time
    fact_nps_response = fact_nps_response.drop(columns=['responded_at'])

    #usamos la sk de dim_calendar para responded_at_date
    fact_nps_response = pd.merge(
        fact_nps_response,
        dim_calendar, # No es necesario seleccionar, pero 'id' y 'date' son clave
        left_on='responded_at_date', # La NK de la tabla de hechos
        right_on='date',             # La NK de la dimensión
        how='left'
    )
    fact_nps_response = fact_nps_response.drop(columns=["responded_at_date", "date"])
    fact_nps_response = fact_nps_response.rename(columns={'id': 'responded_at_date_id'})

    #surrogate key
    fact_nps_response['id'] = range(1, len(fact_nps_response) + 1)

    # Seleccionar y renombrar columnas finales
    fact_nps_response = fact_nps_response[[
        'id', 'customer_id', 'channel_id', 'score', 'comment', 
        'responded_at_date_id', 'responded_at_time'
    ]]


    # Guardamos en warehouse/fact
    file_path = output_path / "fact" / "fact_nps_response.csv"
    file_path.parent.mkdir(parents=True, exist_ok=True)
    fact_nps_response.to_csv(file_path, index=False)
    
    print(f"✅ fact_nps_response guardado en {file_path}")
    return fact_nps_response