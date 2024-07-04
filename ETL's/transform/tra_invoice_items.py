import traceback
import pandas as pd
from sqlalchemy import create_engine
from per_staging import persistir_staging

def transform_invoice_items():
    try:
        engine = create_engine('mysql+pymysql://dwh:elcaro_4U@192.168.10.193:3306/staging')
        connection = engine.connect()
        
        # Carga los identificadores de fecha correctos desde tra_time usando 'id' como clave
        date_ids = pd.read_sql("SELECT id, date FROM tra_time", connection)
        date_ids['date'] = pd.to_datetime(date_ids['date']).dt.date
        date_ids.set_index('date', inplace=True)

        # Consulta SQL para obtener los datos necesarios de invoices_items y tablas relacionadas
        sql_query = '''
        SELECT 
            ii.id AS fact_invoice_item_id,
            ii.invoice_id,
            ii.name AS item_name,
            ii.quantity,
            ii.cost,
            ii.total,
            i.created_at AS date
        FROM ext_invoices_items ii
        JOIN ext_invoices i ON ii.invoice_id = i.id
        '''
        invoice_items = pd.read_sql(sql_query, connection)
        
        # Convertir created_at a 'id' de la tabla de tiempo
        invoice_items['date'] = pd.to_datetime(invoice_items['date']).dt.date
        invoice_items['date_id'] = invoice_items['date'].map(date_ids['id'])

        # Preparar el DataFrame para la inserción en la base de datos
        invoice_items = invoice_items[['fact_invoice_item_id', 'invoice_id', 'item_name', 'quantity', 'cost', 'total', 'date_id']]

        return invoice_items

    except Exception as e:
        print("Error during data transformation:")
        traceback.print_exc()
    finally:
        if 'connection' in locals():
            connection.close()

invoice_items = transform_invoice_items()
print(invoice_items)

if invoice_items is not None:
    persistir_staging(invoice_items, 'tra_invoice_items')
