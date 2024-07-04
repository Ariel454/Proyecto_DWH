import traceback
import pandas as pd
from sqlalchemy import create_engine
from per_staging import persistir_staging

def transform_address():
    try:
        # Configuración de conexión a la base de datos de staging
        engine = create_engine('mysql+pymysql://dwh:elcaro_4U@192.168.10.193:3306/staging')
        connection = engine.connect()
        
        # Extracción de datos desde la tabla ext_address en staging
        sql_query = '''
        SELECT id, city, sector, main_street, secondary_street, is_deleted
        FROM ext_address
        WHERE is_deleted = 0  -- Asegurarse de usar 0 para representar FALSE
        '''
        addresses = pd.read_sql(sql_query, connection)

        # Aplicar transformaciones necesarias
        addresses['city'] = addresses['city'].str.title().str.strip()  # Normalizar y limpiar nombres de ciudades
        addresses['sector'] = addresses['sector'].str.strip()  # Limpiar espacios
        addresses['main_street'] = addresses['main_street'].str.strip()  # Limpiar espacios
        addresses['secondary_street'] = addresses['secondary_street'].str.strip()  # Limpiar espacios

        return addresses

    except Exception as e:
        print("Error during data transformation:")
        traceback.print_exc()
    finally:
        if 'connection' in locals():
            connection.close()

# Llamada a la función de transformación y asignación a la variable 'addresses'
addresses = transform_address()

# Verificar los datos transformados
print(addresses)

# Persistir datos en la tabla de staging tra_address
if addresses is not None:
    persistir_staging(addresses, 'tra_address')
