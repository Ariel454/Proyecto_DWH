import traceback
import pandas as pd
from sqlalchemy import create_engine

def load_groups():
    try:
        # Configuración de conexión a la base de datos de staging
        engine_stg = create_engine('mysql+pymysql://dwh:elcaro_4U@192.168.10.193:3306/staging')
        connection_stg = engine_stg.connect()
        print("Connected successfully to staging database.")
        
        # Configuración de conexión a la base de datos de almacenamiento en el área de presentación (SOR)
        engine_sor = create_engine('mysql+pymysql://dwh:elcaro_4U@192.168.10.193:3306/sor')
        connection_sor = engine_sor.connect()
        print("Connected successfully to SOR database.")

        # Extracción de datos desde la tabla tra_groups en staging
        sql_query = '''
        SELECT id, name, level, program_id
        FROM tra_groups
        '''
        groups_tra = pd.read_sql(sql_query, connection_stg)
        print(f"Retrieved {groups_tra.shape[0]} rows from tra_groups.")

        # Filtrar df_dim_groups para incluir solo program_id existentes en Dim_Programs
        valid_program_ids = pd.read_sql('SELECT program_id FROM Dim_Programs', engine_sor)['program_id'].tolist()
        df_dim_groups = groups_tra[groups_tra['program_id'].isin(valid_program_ids)]

        # Creación del DataFrame para Dim_Groups
        df_dim_groups_to_load = pd.DataFrame({
            "group_id": df_dim_groups['id'],
            "group_name": df_dim_groups['name'],
            "level": df_dim_groups['level'],
            "program_id": df_dim_groups['program_id']
        })

        # Persistencia de los datos filtrados en la tabla Dim_Groups en SOR
        df_dim_groups_to_load.to_sql('Dim_Groups', connection_sor, if_exists='append', index=False)
        print(f"Successfully loaded {df_dim_groups_to_load.shape[0]} rows into Dim_Groups.")

    except Exception as e:
        print("Error during data loading:")
        traceback.print_exc()
    finally:
        if 'connection_stg' in locals() and connection_stg is not None:
            connection_stg.close()
            print("Staging database connection closed.")
        if 'connection_sor' in locals() and connection_sor is not None:
            connection_sor.close()
            print("SOR database connection closed.")

# Llamada a la función para cargar grupos
load_groups()
