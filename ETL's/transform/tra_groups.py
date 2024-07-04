import traceback
import pandas as pd
from sqlalchemy import create_engine
from per_staging import persistir_staging

def transform_groups():
    try:
        # Configuración de conexión a la base de datos de staging
        engine = create_engine('mysql+pymysql://dwh:elcaro_4U@192.168.10.193:3306/staging')
        connection = engine.connect()
        
        # Extracción de datos desde la tabla ext_groups en staging
        sql_query = '''
        SELECT id, name, level, program_id, is_deleted
        FROM ext_groups
        WHERE is_deleted = 0  -- Asegurarse de usar 0 para representar FALSE
        '''
        groups = pd.read_sql(sql_query, connection)

        # Aplicar transformaciones necesarias
        groups['name'] = groups['name'].str.strip()  # Limpiar espacios en los nombres de los grupos

        return groups

    except Exception as e:
        print("Error during data transformation:")
        traceback.print_exc()
    finally:
        if 'connection' in locals():
            connection.close()

# Llamada a la función de transformación y asignación a la variable 'groups'
groups = transform_groups()

# Verificar los datos transformados
print(groups)

# Persistir datos en la tabla de staging tra_groups
if groups is not None:
    persistir_staging(groups, 'tra_groups')
