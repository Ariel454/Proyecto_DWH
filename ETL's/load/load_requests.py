import traceback
import pandas as pd
from sqlalchemy import create_engine

def load_requests():
    try:
        engine_stg = create_engine('mysql+pymysql://dwh:elcaro_4U@192.168.10.193:3306/staging')
        connection_stg = engine_stg.connect()
        print("Connected successfully to staging database.")
        
        engine_sor = create_engine('mysql+pymysql://dwh:elcaro_4U@192.168.10.193:3306/sor')
        connection_sor = engine_sor.connect()
        print("Connected successfully to SOR database.")

        # Obtener IDs válidos de programas, grupos y direcciones
        valid_program_ids = pd.read_sql('SELECT program_id FROM Dim_Programs', engine_sor)['program_id'].tolist()
        valid_group_ids = pd.read_sql('SELECT group_id FROM Dim_Groups', engine_sor)['group_id'].tolist()
        valid_address_ids = pd.read_sql('SELECT address_id FROM Dim_Address', engine_sor)['address_id'].tolist()

        sql_query = f'''
        SELECT program_id, group_id, address_id, date_id, quantity, used_cost, points
        FROM tra_fact_requests
        WHERE program_id IN ({','.join(['%s']*len(valid_program_ids))})
          AND group_id IN ({','.join(['%s']*len(valid_group_ids))})
          AND address_id IN ({','.join(['%s']*len(valid_address_ids))})
        '''
        params = tuple(valid_program_ids + valid_group_ids + valid_address_ids)

        requests_data = pd.read_sql(sql_query, connection_stg, params=params)
        print(f"Retrieved {requests_data.shape[0]} rows from tra_fact_requests.")

        df_fact_requests = pd.DataFrame({
            "program_id": requests_data['program_id'],
            "group_id": requests_data['group_id'],
            "address_id": requests_data['address_id'],
            "date_id": requests_data['date_id'],
            "quantity": requests_data['quantity'],
            "used_cost": requests_data['used_cost'],
            "points": requests_data['points']
        })

        df_fact_requests.to_sql('Fact_Requests', connection_sor, if_exists='append', index=False)
        print(f"Successfully loaded {df_fact_requests.shape[0]} rows into Fact_Requests.")

    except Exception as e:
        print("Error during data loading:")
        traceback.print_exc()
    finally:
        if 'connection_stg' in locals():
            connection_stg.close()
        if 'connection_sor' in locals():
            connection_sor.close()

load_requests()
