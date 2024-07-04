import traceback
import pandas as pd
from sqlalchemy import create_engine
from per_staging import persistir_staging

def transform_fact_requests():
    try:
        engine = create_engine('mysql+pymysql://dwh:elcaro_4U@192.168.10.193:3306/staging')
        connection = engine.connect()
        
        # Carga los identificadores de fecha correctos desde tra_time usando 'id' como clave
        date_ids = pd.read_sql("SELECT id, date FROM tra_time", connection)
        date_ids['date'] = pd.to_datetime(date_ids['date']).dt.date
        date_ids.set_index('date', inplace=True)

        # Consulta SQL para obtener los datos necesarios de requests y tablas relacionadas
        sql_query = '''
        SELECT 
            r.id AS fact_request_id, 
            p.id AS program_id,
            r.address_id,
            pt.group_id,
            r.quantity, 
            r.used_cost,
            r.points,
            r.newer_at
        FROM ext_requests r
        JOIN ext_address a ON r.address_id = a.id
        JOIN ext_participants pt ON a.participant_id = pt.id
        JOIN ext_programs p ON pt.program_id = p.id
        WHERE r.is_deleted = 0
        '''
        requests = pd.read_sql(sql_query, connection)
        
        # Convertir newer_at a 'id' de la tabla de tiempo
        requests['date'] = pd.to_datetime(requests['newer_at']).dt.date

        # Manejar las fechas que no tienen un id correspondiente
        requests['date_id'] = requests['date'].map(date_ids['id'])

        # Verificar si hay fechas que no tienen id correspondiente
        missing_dates = requests[requests['date_id'].isnull()]['date'].unique()
        if len(missing_dates) > 0:
            print(f"Warning: There are dates in 'requests' that do not have corresponding ids in 'tra_time': {missing_dates}")

        # Preparar el DataFrame para la inserción en la base de datos
        requests = requests[['fact_request_id', 'program_id', 'group_id', 'address_id', 'date_id', 'quantity', 'used_cost', 'points']]

        return requests

    except Exception as e:
        print("Error during data transformation:")
        traceback.print_exc()
    finally:
        if 'connection' in locals():
            connection.close()

fact_requests = transform_fact_requests()
print(fact_requests)

if fact_requests is not None:
    persistir_staging(fact_requests, 'tra_fact_requests')
