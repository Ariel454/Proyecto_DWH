import traceback
import pandas as pd
from sqlalchemy import create_engine
from per_staging import persistir_staging

def extract_requests():
    connection = None
    try:
        # Configuración de conexión a la base de datos de origen (oltp)
        engine = create_engine('mysql+pymysql://dwh:elcaro_4U@192.168.10.193:3306/oltp')
        connection = engine.connect()
        
        # Extracción de datos desde la tabla requests
        requests_data = pd.read_sql('SELECT * FROM requests', connection)

        return requests_data

    except Exception as e:
        print("Error durante la extracción de datos:")
        traceback.print_exc()
        return None
    finally:
        if connection:
            connection.close()

# Ejemplo de uso
requests_data = extract_requests()
if requests_data is not None:
    print(requests_data)
    # Persistir datos en la tabla de staging ext_requests
    persistir_staging(requests_data, 'ext_requests')
else:
    print("No se pudo extraer los datos de la tabla requests.")
