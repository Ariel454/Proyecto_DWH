import traceback
import pandas as pd
from sqlalchemy import create_engine
from per_staging import persistir_staging

def transform_participants():
    try:
        # Configuración de conexión a la base de datos de staging
        engine = create_engine('mysql+pymysql://dwh:elcaro_4U@192.168.10.193:3306/staging')
        connection = engine.connect()
        
        # Extracción de datos desde la tabla ext_participants en staging
        sql_query = '''
        SELECT id, username, first_name, last_name, city, email, mobile, is_deleted
        FROM ext_participants
        WHERE is_deleted = 0  -- Asegurarse de usar 0 para representar FALSE
        '''
        participants = pd.read_sql(sql_query, connection)

        # Aplicar transformaciones necesarias
        participants['username'] = participants['username'].str.strip()
        participants['first_name'] = participants['first_name'].str.strip()
        participants['last_name'] = participants['last_name'].str.strip()
        participants['city'] = participants['city'].str.title()  # Capitalizar ciudades
        participants['email'] = participants['email'].str.lower().str.strip()  # Convertir email a minúsculas y quitar espacios
        participants['mobile'] = participants['mobile'].str.replace(' ', '', regex=True)  # Quitar espacios en números de móvil

        return participants

    except Exception as e:
        print("Error during data transformation:")
        traceback.print_exc()
    finally:
        if 'connection' in locals():
            connection.close()

# Llamada a la función de transformación y asignación a la variable 'participants'
participants = transform_participants()

# Verificar los datos transformados
print(participants)

# Persistir datos en la tabla de staging tra_participants
if participants is not None:
    persistir_staging(participants, 'tra_participants')
