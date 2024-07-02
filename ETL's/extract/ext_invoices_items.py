import traceback
import pandas as pd
from sqlalchemy import create_engine
from per_staging import persistir_staging

def extract_invoices_items():
    connection = None
    try:
        # Configuración de conexión a la base de datos de origen (oltp)
        engine = create_engine('mysql+pymysql://dwh:elcaro_4U@192.168.10.193:3306/oltp')
        connection = engine.connect()
        
        # Extracción de datos desde la tabla invoices_items
        invoices_items = pd.read_sql('SELECT * FROM invoices_items', connection)

        return invoices_items

    except Exception as e:
        print("Error durante la extracción de datos:")
        traceback.print_exc()
        return None
    finally:
        if connection:
            connection.close()

# Ejemplo de uso
invoices_items = extract_invoices_items()
if invoices_items is not None:
    print(invoices_items)
    # Persistir datos en la tabla de staging ext_invoices_items
    persistir_staging(invoices_items, 'ext_invoices_items')
else:
    print("No se pudo extraer los datos de la tabla invoices_items.")
