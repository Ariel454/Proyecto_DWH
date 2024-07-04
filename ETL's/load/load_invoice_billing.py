import traceback
import pandas as pd
from sqlalchemy import create_engine

def load_invoice_billing():
    try:
        engine_stg = create_engine('mysql+pymysql://dwh:elcaro_4U@192.168.10.193:3306/staging')
        connection_stg = engine_stg.connect()
        print("Connected successfully to staging database.")
        
        engine_sor = create_engine('mysql+pymysql://dwh:elcaro_4U@192.168.10.193:3306/sor')
        connection_sor = engine_sor.connect()
        print("Connected successfully to SOR database.")

        # Obtener IDs válidos de las tablas dimensionales
        valid_supplier_ids = pd.read_sql('SELECT supplier_id FROM Dim_Suppliers', engine_sor)['supplier_id'].tolist()
        valid_program_ids = pd.read_sql('SELECT program_id FROM Dim_Programs', engine_sor)['program_id'].tolist()
        valid_date_ids = pd.read_sql('SELECT date_id FROM Dim_Time', engine_sor)['date_id'].tolist()

        sql_query = f'''
        SELECT supplier_id, program_id, date_id, total_billed, discounts, taxes
        FROM tra_fact_invoice_billing
        WHERE supplier_id IN ({','.join(['%s']*len(valid_supplier_ids))})
          AND program_id IN ({','.join(['%s']*len(valid_program_ids))})
          AND date_id IN ({','.join(['%s']*len(valid_date_ids))})
        '''
        params = tuple(valid_supplier_ids + valid_program_ids + valid_date_ids)

        invoice_billing_data = pd.read_sql(sql_query, connection_stg, params=params)
        print(f"Retrieved {invoice_billing_data.shape[0]} rows from tra_fact_invoice_billing.")

        df_fact_invoice_billing = pd.DataFrame({
            "supplier_id": invoice_billing_data['supplier_id'],
            "program_id": invoice_billing_data['program_id'],
            "date_id": invoice_billing_data['date_id'],
            "total_billed": invoice_billing_data['total_billed'],
            "discounts": invoice_billing_data['discounts'],
            "taxes": invoice_billing_data['taxes']
        })

        df_fact_invoice_billing.to_sql('Fact_Invoice_Billing', connection_sor, if_exists='append', index=False)
        print(f"Successfully loaded {df_fact_invoice_billing.shape[0]} rows into Fact_Invoice_Billing.")

    except Exception as e:
        print("Error during data loading:")
        traceback.print_exc()
    finally:
        if 'connection_stg' in locals():
            connection_stg.close()
        if 'connection_sor' in locals():
            connection_sor.close()

load_invoice_billing()
