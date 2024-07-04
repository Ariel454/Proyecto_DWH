import traceback
import pandas as pd
from sqlalchemy import create_engine
from per_staging import persistir_staging

def transform_suppliers():
    try:
        # Configuración de conexión a la base de datos de staging
        engine = create_engine('mysql+pymysql://dwh:elcaro_4U@192.168.10.193:3306/staging')
        connection = engine.connect()
        
        # Extracción de datos desde la tabla ext_suppliers en staging
        sql_query = '''
        SELECT id, name, contact_name, email, mobile, is_deleted
        FROM ext_suppliers
        WHERE is_deleted = 0  -- Asegurarse de usar 0 para representar FALSE
        '''
        suppliers = pd.read_sql(sql_query, connection)

        # Aplicar transformaciones necesarias
        suppliers['name'] = suppliers['name'].str.strip()  # Limpiar espacios en los nombres
        suppliers['contact_name'] = suppliers['contact_name'].str.strip()  # Limpiar espacios en nombres de contacto
        suppliers['email'] = suppliers['email'].str.lower().str.strip()  # Convertir email a minúsculas y quitar espacios
        suppliers['mobile'] = suppliers['mobile'].str.replace(' ', '', regex=True)  # Quitar espacios en números de móvil

        return suppliers

    except Exception as e:
        print("Error during data transformation:")
        traceback.print_exc()
    finally:
        if 'connection' in locals():
            connection.close()

# Llamada a la función de transformación y asignación a la variable 'suppliers'
suppliers = transform_suppliers()

# Verificar los datos transformados
print(suppliers)

# Persistir datos en la tabla de staging tra_suppliers
if suppliers is not None:
    persistir_staging(suppliers, 'tra_suppliers')
