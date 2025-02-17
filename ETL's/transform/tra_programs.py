import traceback
import pandas as pd
from sqlalchemy import create_engine
from per_staging import persistir_staging

def transform_programs():
    try:
        # Configuración de conexión a la base de datos de staging
        engine = create_engine('mysql+pymysql://dwh:elcaro_4U@192.168.10.193:3306/staging')
        connection = engine.connect()
        
        # Extracción de datos desde la tabla ext_programs en staging
        sql_query = '''
        SELECT id, name, date_from, date_to, is_store_active, coin_name
        FROM ext_programs
        WHERE is_deleted = 0  -- Asegurarse de usar 0 para representar FALSE
        '''
        programs = pd.read_sql(sql_query, connection)

        # Aplicar transformaciones necesarias, por ejemplo, limpiar espacios en los nombres
        programs['name'] = programs['name'].str.strip()

        return programs

    except Exception as e:
        print("Error during data transformation:")
        traceback.print_exc()
    finally:
        if 'connection' in locals():
            connection.close()

# Llamada a la función de transformación y asignación a la variable 'programs'
programs = transform_programs()

# Verificar los datos transformados
print(programs)

# Persistir datos en la tabla de staging tra_programs
if programs is not None:
    persistir_staging(programs, 'tra_programs')
