import traceback
import pandas as pd
from sqlalchemy import create_engine

def load_awards():
    try:
        # Configuración de conexión a la base de datos de staging
        engine_stg = create_engine('mysql+pymysql://dwh:elcaro_4U@192.168.10.193:3306/staging')
        connection_stg = engine_stg.connect()
        print("Connected successfully to staging database.")
        
        # Configuración de conexión a la base de datos de almacenamiento en el área de presentación (SOR)
        engine_sor = create_engine('mysql+pymysql://dwh:elcaro_4U@192.168.10.193:3306/sor')
        connection_sor = engine_sor.connect()
        print("Connected successfully to SOR database.")

        # Extracción de datos desde la tabla de premios en staging
        sql_query = '''
        SELECT id AS award_id, name, model, current_cost, supplier_code
        FROM tra_awards
        '''
        awards_data = pd.read_sql(sql_query, connection_stg)
        print(f"Retrieved {awards_data.shape[0]} rows from tra_awards.")

        # Creación del DataFrame para Dim_Awards
        df_dim_awards = pd.DataFrame({
            "award_id": awards_data['award_id'],
            "name": awards_data['name'],
            "model": awards_data['model'],
            "current_cost": awards_data['current_cost'],
            "supplier_code": awards_data['supplier_code']
        })

        # Persistencia de los datos en la tabla Dim_Awards en SOR
        df_dim_awards.to_sql('Dim_Awards', connection_sor, if_exists='append', index=False)
        print(f"Successfully loaded {df_dim_awards.shape[0]} rows into Dim_Awards.")

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

# Llamada a la función para cargar premios
load_awards()
