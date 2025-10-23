# etl/load.py
from pathlib import Path
from etl.extract.extract import extract_all
from etl.transform.build_dim_calendar import build_dim_calendar
from etl.transform.build_dim_customer import build_dim_customer
from etl.transform.build_dim_product import build_dim_product
from etl.transform.build_dim_address import build_dim_address
from etl.transform.build_dim_channel import build_dim_channel
from etl.transform.build_dim_store import build_dim_store
#from etl.transform.build_dim_product import build as build_dim_product

from etl.transform.build_fact_nps_response import build_fact_nps_response
from etl.transform.build_fact_shipment import build_fact_shipment
from etl.transform.build_fact_payment import build_fact_payment
from etl.transform.build_fact_web_session import build_fact_web_session
from etl.transform.build_fact_sales_order import build_fact_sales_order
from etl.transform.build_fact_sales_order_item import build_fact_sales_order_item

OUTPUT_PATH = Path("warehouse")  #A donde apunta el pipeline

def run_pipeline():
    data = extract_all()

    print("Construyendo dimensiones y hechos...")

    df_dim_calendar = build_dim_calendar(OUTPUT_PATH, "2023-01-01", "2026-12-31")
    df_dim_customer = build_dim_customer(data, OUTPUT_PATH)
    df_dim_product = build_dim_product(data, OUTPUT_PATH)
    df_dim_address = build_dim_address(data, OUTPUT_PATH)
    df_dim_channel = build_dim_channel(data, OUTPUT_PATH)
    df_dim_store = build_dim_store(data, OUTPUT_PATH)

    df_fact_nps_response = build_fact_nps_response(data, df_dim_customer, df_dim_channel,df_dim_calendar, OUTPUT_PATH)
    df_fact_shipment = build_fact_shipment(data, df_dim_calendar, df_dim_customer, df_dim_channel, df_dim_address, OUTPUT_PATH)
    df_fact_payment = build_fact_payment(data, df_dim_calendar,df_dim_customer,df_dim_channel,df_dim_address,df_dim_store, OUTPUT_PATH)
    df_fact_web_session = build_fact_web_session(data, df_dim_calendar, df_dim_customer, OUTPUT_PATH)
    df_fact_sales_order = build_fact_sales_order(data, df_dim_calendar, df_dim_customer, df_dim_channel, df_dim_store, df_dim_address, OUTPUT_PATH)
    df_fact_sales_order_item = build_fact_sales_order_item(data, df_dim_customer, df_dim_channel, df_dim_store, df_dim_product, OUTPUT_PATH)

    print("✅ Pipeline completado. Archivos guardados en warehouse/")

if __name__ == "__main__":
    run_pipeline()
