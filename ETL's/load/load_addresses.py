import traceback
import pandas as pd
from sqlalchemy import create_engine

def load_addresses():
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
        SELECT id, city, sector, main_street, secondary_street
        FROM tra_address
        '''
        addresses_tra = pd.read_sql(sql_query, connection_stg)
        print(f"Retrieved {addresses_tra.shape[0]} rows from tra_address.")

        # Creación del DataFrame para Dim_Address
        df_dim_addresses = pd.DataFrame({
            "address_id": addresses_tra['id'],
            "city": addresses_tra['city'],
            "sector": addresses_tra['sector'],
            "main_street": addresses_tra['main_street'],
            "secondary_street": addresses_tra['secondary_street']
        })

        # Persistencia de los datos en la tabla Dim_Address en SOR
        df_dim_addresses.to_sql('Dim_Address', connection_sor, if_exists='append', index=False)
        print(f"Successfully loaded {df_dim_addresses.shape[0]} rows into Dim_Address.")

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

# Llamada a la función para cargar direcciones
load_addresses()
