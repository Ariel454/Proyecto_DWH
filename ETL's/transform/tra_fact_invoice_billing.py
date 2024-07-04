import traceback
import pandas as pd
from sqlalchemy import create_engine
from per_staging import persistir_staging

def transform_fact_invoice_billing():
    try:
        engine = create_engine('mysql+pymysql://dwh:elcaro_4U@192.168.10.193:3306/staging')
        connection = engine.connect()
        
        # Carga los identificadores de fecha correctos desde tra_time usando 'id' como clave
        date_ids = pd.read_sql("SELECT id, date FROM tra_time", connection)
        date_ids['date'] = pd.to_datetime(date_ids['date']).dt.date
        date_ids.set_index('date', inplace=True)

        # Consulta SQL para obtener los datos necesarios de invoices y tablas relacionadas
        sql_query = '''
        SELECT 
            i.id AS invoice_id,
            s.id AS supplier_id,
            p.id AS program_id,
            i.created_at AS date,
            i.subtotal AS total_billed,
            i.discount AS discounts,
            i.taxes AS taxes
        FROM ext_invoices i
        JOIN ext_suppliers s ON i.supplier_id = s.id
        JOIN ext_requests r ON i.id = r.invoice_id
        JOIN ext_address ad ON r.address_id = ad.id
        JOIN ext_participants pt ON ad.participant_id = pt.id
        JOIN ext_programs p ON pt.program_id = p.id
        WHERE i.is_deleted = 0 AND s.is_deleted = 0 AND r.is_deleted = 0
        '''
        invoice_billing = pd.read_sql(sql_query, connection)
        
        # Convertir created_at a 'id' de la tabla de tiempo
        invoice_billing['date'] = pd.to_datetime(invoice_billing['date']).dt.date
        invoice_billing['date_id'] = invoice_billing['date'].map(date_ids['id'])

        # Preparar el DataFrame para la inserción en la base de datos
        invoice_billing = invoice_billing[['invoice_id', 'supplier_id', 'program_id', 'date_id', 'total_billed', 'discounts', 'taxes']]

        return invoice_billing

    except Exception as e:
        print("Error during data transformation:")
        traceback.print_exc()
    finally:
        if 'connection' in locals():
            connection.close()

fact_invoice_billing = transform_fact_invoice_billing()
print(fact_invoice_billing)

if fact_invoice_billing is not None:
    persistir_staging(fact_invoice_billing, 'tra_fact_invoice_billing')
