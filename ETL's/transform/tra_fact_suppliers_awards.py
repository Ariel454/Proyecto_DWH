import traceback
import pandas as pd
from sqlalchemy import create_engine
from per_staging import persistir_staging

def transform_fact_supplier_awards():
    try:
        engine = create_engine('mysql+pymysql://dwh:elcaro_4U@192.168.10.193:3306/staging')
        connection = engine.connect()
        
        # Carga los identificadores de fecha correctos desde tra_time usando 'id' como clave
        date_ids = pd.read_sql("SELECT id, date FROM tra_time", connection)
        date_ids['date'] = pd.to_datetime(date_ids['date']).dt.date
        date_ids.set_index('date', inplace=True)

        # Consulta SQL para obtener los datos necesarios de suppliers, awards y tablas relacionadas
        sql_query = '''
        SELECT 
            s.id AS supplier_id,
            a.id AS award_id,
            p.id AS program_id,
            r.newer_at AS date,
            a.current_cost * r.quantity AS total_cost,
            r.quantity AS awards_quantity
        FROM ext_suppliers s
        JOIN ext_awards a ON s.id = a.brand_id
        JOIN ext_requests r ON a.id = r.award_id
        JOIN ext_address ad ON r.address_id = ad.id
        JOIN ext_participants pt ON ad.participant_id = pt.id
        JOIN ext_programs p ON pt.program_id = p.id
        WHERE s.is_deleted = 0 AND a.is_active = 1 AND r.is_deleted = 0
        '''
        supplier_awards = pd.read_sql(sql_query, connection)
        
        # Convertir newer_at a 'id' de la tabla de tiempo
        supplier_awards['date'] = pd.to_datetime(supplier_awards['date']).dt.date
        supplier_awards['date_id'] = supplier_awards['date'].map(date_ids['id'])

        # Preparar el DataFrame para la inserción en la base de datos
        supplier_awards = supplier_awards[['supplier_id', 'award_id', 'program_id', 'date_id', 'total_cost', 'awards_quantity']]

        return supplier_awards

    except Exception as e:
        print("Error during data transformation:")
        traceback.print_exc()
    finally:
        if 'connection' in locals():
            connection.close()

fact_supplier_awards = transform_fact_supplier_awards()
print(fact_supplier_awards)

if fact_supplier_awards is not None:
    persistir_staging(fact_supplier_awards, 'tra_fact_supplier_awards')
