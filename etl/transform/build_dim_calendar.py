# etl/transform/build_dim_calendar.py
import pandas as pd

def build_dim_calendar(output_path, start_date="2025-01-01", end_date="2025-12-31"):
    """
    Genera una tabla de dimensión calendario con campos:
    id (surrogada), date, day, month, year, day_name, month_name, quarter, week_number, year_month, is_weekend
    """
    dates = pd.date_range(start=start_date, end=end_date, freq="D")
    df = pd.DataFrame({"date": dates})

    # Agregamos columnas derivadas
    df['id'] = range(1, len(df) + 1)
    df['day'] = df['date'].dt.day
    df['month'] = df['date'].dt.month
    df['year'] = df['date'].dt.year
    df['day_name'] = df['date'].dt.day_name()
    df['month_name'] = df['date'].dt.month_name()
    df['quarter'] = df['date'].dt.quarter
    df['week_number'] = df['date'].dt.isocalendar().week
    df['year_month'] = df['date'].dt.strftime('%Y-%m')
    df['is_weekend'] = df['date'].dt.dayofweek.isin([5, 6])  # 5=Sábado, 6=Domingo
    
    # Damos el orden a la columna
    dim_calendar = df[['id', 'date', 'day', 'month', 'year', 'day_name', 
                      'month_name', 'quarter', 'week_number', 'year_month', 'is_weekend']]

    # Guardamos en warehouse/dim
    file_path = output_path / "dim" / "dim_calendar.csv"
    file_path.parent.mkdir(parents=True, exist_ok=True)
    dim_calendar.to_csv(file_path, index=False)
    
    print(f"✅ dim_calendar guardado en {file_path}")
    return dim_calendar