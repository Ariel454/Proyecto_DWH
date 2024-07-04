import traceback
import pandas as pd
from sqlalchemy import create_engine

def load_time_dimension():
    try:
        # Configuración de conexión a la base de datos de staging
        engine_stg = create_engine('mysql+pymysql://dwh:elcaro_4U@192.168.10.193:3306/staging')
        connection_stg = engine_stg.connect()
        print("Connected successfully to staging database.")
        
        # Configuración de conexión a la base de datos de almacenamiento en el área de presentación (SOR)
        engine_sor = create_engine('mysql+pymysql://dwh:elcaro_4U@192.168.10.193:3306/sor')
        connection_sor = engine_sor.connect()
        print("Connected successfully to SOR database.")

        # Extracción de datos desde la tabla de fechas en staging
        sql_query = '''
        SELECT id AS date_id, date, DAY(date) AS day, MONTH(date) AS month, YEAR(date) AS year,
               QUARTER(date) AS quarter, WEEK(date) AS week
        FROM tra_time
        '''
        time_data = pd.read_sql(sql_query, connection_stg)
        print(f"Retrieved {time_data.shape[0]} rows from tra_time.")

        # Creación del DataFrame para Dim_Time
        df_dim_time = pd.DataFrame({
            "date_id": time_data['date_id'],
            "date": time_data['date'],
            "day": time_data['day'],
            "month": time_data['month'],
            "year": time_data['year'],
            "quarter": time_data['quarter'],
            "week": time_data['week']
        })

        # Persistencia de los datos en la tabla Dim_Time en SOR
        df_dim_time.to_sql('Dim_Time', connection_sor, if_exists='append', index=False)
        print(f"Successfully loaded {df_dim_time.shape[0]} rows into Dim_Time.")

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

# Llamada a la función para cargar la dimensión de tiempo
load_time_dimension()
