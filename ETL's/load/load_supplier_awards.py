import traceback
import pandas as pd
from sqlalchemy import create_engine

def load_supplier_awards():
    try:
        engine_stg = create_engine('mysql+pymysql://dwh:elcaro_4U@192.168.10.193:3306/staging')
        connection_stg = engine_stg.connect()
        print("Connected successfully to staging database.")
        
        engine_sor = create_engine('mysql+pymysql://dwh:elcaro_4U@192.168.10.193:3306/sor')
        connection_sor = engine_sor.connect()
        print("Connected successfully to SOR database.")

        # Obtener IDs válidos de las tablas dimensionales
        valid_supplier_ids = pd.read_sql('SELECT supplier_id FROM Dim_Suppliers', engine_sor)['supplier_id'].tolist()
        valid_award_ids = pd.read_sql('SELECT award_id FROM Dim_Awards', engine_sor)['award_id'].tolist()
        valid_program_ids = pd.read_sql('SELECT program_id FROM Dim_Programs', engine_sor)['program_id'].tolist()
        valid_date_ids = pd.read_sql('SELECT date_id FROM Dim_Time', engine_sor)['date_id'].tolist()

        sql_query = f'''
        SELECT supplier_id, award_id, program_id, date_id, total_cost, awards_quantity
        FROM tra_fact_supplier_awards
        WHERE supplier_id IN ({','.join(['%s']*len(valid_supplier_ids))})
          AND award_id IN ({','.join(['%s']*len(valid_award_ids))})
          AND program_id IN ({','.join(['%s']*len(valid_program_ids))})
          AND date_id IN ({','.join(['%s']*len(valid_date_ids))})
        '''
        params = tuple(valid_supplier_ids + valid_award_ids + valid_program_ids + valid_date_ids)

        supplier_awards_data = pd.read_sql(sql_query, connection_stg, params=params)
        print(f"Retrieved {supplier_awards_data.shape[0]} rows from tra_fact_supplier_awards.")

        df_fact_supplier_awards = pd.DataFrame({
            "supplier_id": supplier_awards_data['supplier_id'],
            "award_id": supplier_awards_data['award_id'],
            "program_id": supplier_awards_data['program_id'],
            "date_id": supplier_awards_data['date_id'],
            "total_cost": supplier_awards_data['total_cost'],
            "awards_quantity": supplier_awards_data['awards_quantity']
        })

        df_fact_supplier_awards.to_sql('Fact_Supplier_Awards', connection_sor, if_exists='append', index=False)
        print(f"Successfully loaded {df_fact_supplier_awards.shape[0]} rows into Fact_Supplier_Awards.")

    except Exception as e:
        print("Error during data loading:")
        traceback.print_exc()
    finally:
        if 'connection_stg' in locals():
            connection_stg.close()
        if 'connection_sor' in locals():
            connection_sor.close()

load_supplier_awards()
