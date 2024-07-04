import traceback
import pandas as pd
from sqlalchemy import create_engine

def load_participants():
    try:
        # Configuración de conexión a la base de datos de staging
        engine_stg = create_engine('mysql+pymysql://dwh:elcaro_4U@192.168.10.193:3306/staging')
        connection_stg = engine_stg.connect()
        print("Connected successfully to staging database.")
        
        # Configuración de conexión a la base de datos de almacenamiento en el área de presentación (SOR)
        engine_sor = create_engine('mysql+pymysql://dwh:elcaro_4U@192.168.10.193:3306/sor')
        connection_sor = engine_sor.connect()
        print("Connected successfully to SOR database.")

        # Extracción de datos desde la tabla tra_participants en staging
        sql_query = '''
        SELECT id, username, first_name, last_name, city, email, mobile
        FROM tra_participants
        '''
        participants_tra = pd.read_sql(sql_query, connection_stg)
        print(f"Retrieved {participants_tra.shape[0]} rows from tra_participants.")

        # Creación del DataFrame para Dim_Participants
        df_dim_participants = pd.DataFrame({
            "participant_id": participants_tra['id'],
            "username": participants_tra['username'],
            "first_name": participants_tra['first_name'],
            "last_name": participants_tra['last_name'],
            "city": participants_tra['city'],
            "email": participants_tra['email'],
            "mobile": participants_tra['mobile']
        })

        # Persistencia de los datos en la tabla Dim_Participants en SOR
        df_dim_participants.to_sql('Dim_Participants', connection_sor, if_exists='append', index=False)
        print(f"Successfully loaded {df_dim_participants.shape[0]} rows into Dim_Participants.")

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

# Llamada a la función para cargar participantes
load_participants()
