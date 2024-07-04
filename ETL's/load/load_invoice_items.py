import traceback
import pandas as pd
from sqlalchemy import create_engine

def load_invoice_items():
    try:
        engine_stg = create_engine('mysql+pymysql://dwh:elcaro_4U@192.168.10.193:3306/staging')
        connection_stg = engine_stg.connect()
        print("Connected successfully to staging database.")
        
        engine_sor = create_engine('mysql+pymysql://dwh:elcaro_4U@192.168.10.193:3306/sor')
        connection_sor = engine_sor.connect()
        print("Connected successfully to SOR database.")

        # Obtener IDs válidos de la tabla Fact_Invoice_Billing y Dim_Time
        valid_invoice_ids = pd.read_sql('SELECT fact_invoice_id FROM Fact_Invoice_Billing', engine_sor)['fact_invoice_id'].tolist()
        valid_date_ids = pd.read_sql('SELECT date_id FROM Dim_Time', engine_sor)['date_id'].tolist()

        sql_query = f'''
        SELECT invoice_id, item_name, quantity, cost, total, date_id
        FROM tra_invoice_items
        WHERE invoice_id IN ({','.join(['%s']*len(valid_invoice_ids))})
          AND date_id IN ({','.join(['%s']*len(valid_date_ids))})
        '''
        params = tuple(valid_invoice_ids + valid_date_ids)

        invoice_items_data = pd.read_sql(sql_query, connection_stg, params=params)
        print(f"Retrieved {invoice_items_data.shape[0]} rows from tra_invoice_items.")

        df_fact_invoice_items = pd.DataFrame({
            "invoice_id": invoice_items_data['invoice_id'],
            "item_name": invoice_items_data['item_name'],
            "quantity": invoice_items_data['quantity'],
            "cost": invoice_items_data['cost'],
            "total": invoice_items_data['total'],
            "date_id": invoice_items_data['date_id']
        })

        df_fact_invoice_items.to_sql('Fact_Invoice_Items', connection_sor, if_exists='append', index=False)
        print(f"Successfully loaded {df_fact_invoice_items.shape[0]} rows into Fact_Invoice_Items.")

    except Exception as e:
        print("Error during data loading:")
        traceback.print_exc()
    finally:
        if 'connection_stg' in locals():
            connection_stg.close()
        if 'connection_sor' in locals():
            connection_sor.close()

load_invoice_items()
