import traceback
import pandas as pd
from sqlalchemy import create_engine
from per_staging import persistir_staging

def transform_fact_awards_claims():
    try:
        engine = create_engine('mysql+pymysql://dwh:elcaro_4U@192.168.10.193:3306/staging')
        connection = engine.connect()
        
        # Carga los identificadores de fecha correctos desde tra_time usando 'id' como clave
        date_ids = pd.read_sql("SELECT id, date FROM tra_time", connection)
        date_ids['date'] = pd.to_datetime(date_ids['date']).dt.date
        date_ids.set_index('date', inplace=True)

        # Consulta SQL para obtener los datos necesarios de awards y tablas relacionadas
        sql_query = '''
        SELECT 
            r.id AS fact_awards_claim_id, 
            pt.id AS participant_id,
            pt.position_id,
            pt.group_id,
            r.award_id,
            r.newer_at AS date,
            r.quantity AS number_of_awards
        FROM ext_requests r
        JOIN ext_address a ON r.address_id = a.id
        JOIN ext_participants pt ON a.participant_id = pt.id
        WHERE r.is_deleted = 0
        '''
        awards_claims = pd.read_sql(sql_query, connection)
        
        # Convertir newer_at a 'id' de la tabla de tiempo
        awards_claims['date'] = pd.to_datetime(awards_claims['date']).dt.date
        awards_claims['date_id'] = awards_claims['date'].map(date_ids['id'])

        # Preparar el DataFrame para la inserción en la base de datos
        awards_claims = awards_claims[['fact_awards_claim_id', 'participant_id', 'position_id', 'group_id', 'award_id', 'date_id', 'number_of_awards']]

        return awards_claims

    except Exception as e:
        print("Error during data transformation:")
        traceback.print_exc()
    finally:
        if 'connection' in locals():
            connection.close()

fact_awards_claims = transform_fact_awards_claims()
print(fact_awards_claims)

if fact_awards_claims is not None:
    persistir_staging(fact_awards_claims, 'tra_fact_awards_claims')
