# etl/transform/build_fact_web_session.py
import pandas as pd
def build_fact_web_session(data,dim_calendar,dim_customer, output_path):
    """
    Genera una tabla de hechos web_session con campos:
    id, customer_id, started_at_date_id, started_at_time, ended_at_date_id, ended_at_time, source, device
    """
    fact_web_session = data["web_session"].copy()
    dim_calendar = dim_calendar.copy()
    dim_customer = dim_customer.copy()

    # DIM CUSTOMER
    fact_web_session = pd.merge(
        fact_web_session,
        dim_customer,
        left_on='customer_id',
        right_on='customer_key',
        how='left'
    ).drop(columns=['customer_id','customer_key'])
    fact_web_session = fact_web_session.rename(columns={'id': 'customer_id'})
    fact_web_session["customer_id"] = fact_web_session["customer_id"].astype("Int64") #algunas veces la FK es null

    #separamos started_at y ended_at en fecha y hora
    fact_web_session['started_at'] = pd.to_datetime(fact_web_session['started_at'])
    fact_web_session['started_at_date'] = fact_web_session['started_at'].dt.normalize()
    fact_web_session['started_at_time'] = fact_web_session['started_at'].dt.time
    fact_web_session = fact_web_session.drop(columns=['started_at'])

    #usamos la sk de dim_calendar para shipment_at_date
    fact_web_session = pd.merge(
        fact_web_session,
        dim_calendar, 
        left_on='started_at_date', # La NK de la tabla de hechos
        right_on='date',             # La NK de la dimensión
        how='left'
    )
    fact_web_session = fact_web_session.drop(columns=["started_at_date", "date"])
    fact_web_session = fact_web_session.rename(columns={'id': 'started_at_date_id'})
    fact_web_session["started_at_date_id"] = fact_web_session["started_at_date_id"].astype("Int64") #algunas veces la FK es null

    fact_web_session['ended_at'] = pd.to_datetime(fact_web_session['ended_at'])
    fact_web_session['ended_at_date'] = fact_web_session['ended_at'].dt.normalize()
    fact_web_session['ended_at_time'] = fact_web_session['ended_at'].dt.time
    fact_web_session = fact_web_session.drop(columns=['ended_at'])

    #usamos la sk de dim_calendar para shipment_at_date
    fact_web_session = pd.merge(
        fact_web_session,
        dim_calendar, 
        left_on='ended_at_date', # La NK de la tabla de hechos
        right_on='date',             # La NK de la dimensión
        how='left'
    )
    fact_web_session = fact_web_session.drop(columns=["ended_at_date", "date"])
    fact_web_session = fact_web_session.rename(columns={'id': 'ended_at_date_id'})
    fact_web_session["ended_at_date_id"] = fact_web_session["ended_at_date_id"].astype("Int64") #algunas veces la FK es null

    #surrogate key
    fact_web_session['id'] = range(1, len(fact_web_session) + 1)

    # Seleccionar y renombrar columnas finales
    fact_web_session = fact_web_session[[
        'id', 'customer_id', 'started_at_date_id', 'started_at_time', 
        'ended_at_date_id', 'ended_at_time', 'source', 'device'
    ]]


    # Guardamos en warehouse/fact
    file_path = output_path / "fact" / "fact_web_session.csv"
    file_path.parent.mkdir(parents=True, exist_ok=True)
    fact_web_session.to_csv(file_path, index=False)
    
    print(f"✅ fact_web_session guardado en {file_path}")
    return fact_web_session