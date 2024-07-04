import traceback
import pandas as pd
from sqlalchemy import create_engine
from per_staging import persistir_staging

def transform_awards():
    try:
        # Configuración de conexión a la base de datos de staging
        engine = create_engine('mysql+pymysql://dwh:elcaro_4U@192.168.10.193:3306/staging')
        connection = engine.connect()
        
        # Extracción de datos desde la tabla ext_awards en staging
        sql_query = '''
        SELECT id, name, model, current_cost, supplier_code, is_active
        FROM ext_awards
        WHERE is_active = 1  -- Asegurarse de usar 1 para representar TRUE
        '''
        awards = pd.read_sql(sql_query, connection)

        # Aplicar transformaciones necesarias
        awards['name'] = awards['name'].str.strip()  # Limpiar espacios en los nombres
        awards['model'] = awards['model'].str.strip()  # Limpiar espacios en los modelos
        awards['supplier_code'] = awards['supplier_code'].str.strip()  # Limpiar espacios en los códigos de proveedor

        return awards

    except Exception as e:
        print("Error during data transformation:")
        traceback.print_exc()
    finally:
        if 'connection' in locals():
            connection.close()

# Llamada a la función de transformación y asignación a la variable 'awards'
awards = transform_awards()

# Verificar los datos transformados
print(awards)

# Persistir datos en la tabla de staging tra_awards
if awards is not None:
    persistir_staging(awards, 'tra_awards')
