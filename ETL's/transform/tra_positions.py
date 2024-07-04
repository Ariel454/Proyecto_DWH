import traceback
import pandas as pd
from sqlalchemy import create_engine
from per_staging import persistir_staging

def transform_positions():
    try:
        # Configuración de conexión a la base de datos de staging
        engine = create_engine('mysql+pymysql://dwh:elcaro_4U@192.168.10.193:3306/staging')
        connection = engine.connect()
        
        # Extracción de datos desde la tabla ext_positions en staging
        sql_query = '''
        SELECT id, name, max_points_per_month, program_id, is_deleted
        FROM ext_positions
        WHERE is_deleted = 0  -- Asegurarse de usar 0 para representar FALSE
        '''
        positions = pd.read_sql(sql_query, connection)

        # Aplicar transformaciones necesarias
        positions['name'] = positions['name'].str.strip()  # Limpiar espacios en los nombres

        return positions

    except Exception as e:
        print("Error during data transformation:")
        traceback.print_exc()
    finally:
        if 'connection' in locals():
            connection.close()

# Llamada a la función de transformación y asignación a la variable 'positions'
positions = transform_positions()

# Verificar los datos transformados
print(positions)

# Persistir datos en la tabla de staging tra_positions
if positions is not None:
    persistir_staging(positions, 'tra_positions')
