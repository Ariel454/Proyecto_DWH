import traceback
import pandas as pd
from sqlalchemy import create_engine
from per_staging import persistir_staging

def extract_participants():
    connection = None
    try:
        # Configuración de conexión a la base de datos de origen (oltp)
        engine = create_engine('mysql+pymysql://dwh:elcaro_4U@192.168.10.193:3306/oltp')
        connection = engine.connect()
        
        # Extracción de datos desde la tabla participants
        participants = pd.read_sql('SELECT * FROM participants', connection)

        return participants

    except Exception as e:
        print("Error durante la extracción de datos:")
        traceback.print_exc()
        return None
    finally:
        if connection:
            connection.close()

# Ejemplo de uso
participants = extract_participants()
if participants is not None:
    print(participants)
    # Persistir datos en la tabla de staging ext_participants
    persistir_staging(participants, 'ext_participants')
else:
    print("No se pudo extraer los datos de la tabla participants.")
