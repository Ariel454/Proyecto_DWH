import traceback
from sqlalchemy import create_engine
import pandas as pd

def persistir_staging(df_stg, tab_name):
    try:
        # Parámetros de conexión a la base de datos
        type = 'mysql'
        host = '192.168.10.193'
        port = '3306'
        user = 'dwh'
        pwd = 'elcaro_4U'
        db = 'staging'

        # Crear la conexión usando SQLAlchemy
        engine = create_engine(f'mysql+pymysql://{user}:{pwd}@{host}:{port}/{db}')
        
        # Iniciar la conexión
        with engine.connect() as connection:
            # Persistir el DataFrame en la tabla especificada
            df_stg.to_sql(tab_name, con=connection, if_exists='replace', index=False)
            print(f"Datos persistidos en la tabla {tab_name}")

    except Exception as e:
        traceback.print_exc()
        # Aquí podrías manejar el error de manera específica si es necesario
    finally:
        # Cerrar la conexión si es que se logró establecer
        if 'connection' in locals():
            connection.close()

