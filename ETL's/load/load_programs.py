import traceback
import pandas as pd
from sqlalchemy import create_engine

def load_programs():
    try:
        # Configuración de conexión a la base de datos de staging
        engine_stg = create_engine('mysql+pymysql://dwh:elcaro_4U@192.168.10.193:3306/staging')
        connection_stg = engine_stg.connect()
        print("Connected successfully to staging database.")
        
        # Configuración de conexión a la base de datos de almacenamiento en el área de presentación (SOR)
        engine_sor = create_engine('mysql+pymysql://dwh:elcaro_4U@192.168.10.193:3306/sor')
        connection_sor = engine_sor.connect()
        print("Connected successfully to SOR database.")

        # Extracción de datos desde la tabla tra_programs en staging
        sql_query = '''
        SELECT id, name, date_from, date_to, is_store_active, coin_name
        FROM tra_programs
        '''
        programs_tra = pd.read_sql(sql_query, connection_stg)
        print(f"Retrieved {programs_tra.shape[0]} rows from tra_programs.")

        # Creación del DataFrame para Dim_Programs
        df_dim_programs = pd.DataFrame({
            "program_id": programs_tra['id'],
            "name": programs_tra['name'],
            "date_from": programs_tra['date_from'],
            "date_to": programs_tra['date_to'],
            "is_store_active": programs_tra['is_store_active'],
            "coin_name": programs_tra['coin_name']
        })

        # Persistencia de los datos en la tabla dim_programs en SOR
        df_dim_programs.to_sql('Dim_Programs', connection_sor, if_exists='append', index=False)
        print(f"Successfully loaded {df_dim_programs.shape[0]} rows into Dim_Programs.")

    except Exception as e:
        print("Error during data loading:")
        traceback.print_exc()
    finally:
        if 'connection_stg' in locals():
            connection_stg.close()
            print("Staging database connection closed.")
        if 'connection_sor' in locals():
            connection_sor.close()
            print("SOR database connection closed.")

# Llamada a la función para cargar programas
load_programs()
