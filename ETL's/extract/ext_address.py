import traceback
import pandas as pd
from sqlalchemy import create_engine
from per_staging import persistir_staging

def extract_address():
    connection = None
    try:
        # Configuración de conexión a la base de datos de origen (oltp)
        engine = create_engine('mysql+pymysql://dwh:elcaro_4U@192.168.10.193:3306/oltp')
        connection = engine.connect()
        
        # Extracción de datos desde la tabla address
        address = pd.read_sql('SELECT * FROM address', connection)

        return address

    except Exception as e:
        print("Error durante la extracción de datos:")
        traceback.print_exc()
        return None
    finally:
        if connection:
            connection.close()

# Ejemplo de uso
address = extract_address()
if address is not None:
    print(address)
    # Persistir datos en la tabla de staging ext_address
    persistir_staging(address, 'ext_address')
else:
    print("No se pudo extraer los datos de la tabla address.")
