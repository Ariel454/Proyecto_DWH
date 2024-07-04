import traceback
import pandas as pd
from sqlalchemy import create_engine

def load_awards_claims():
    try:
        engine_stg = create_engine('mysql+pymysql://dwh:elcaro_4U@192.168.10.193:3306/staging')
        connection_stg = engine_stg.connect()
        print("Connected successfully to staging database.")
        
        engine_sor = create_engine('mysql+pymysql://dwh:elcaro_4U@192.168.10.193:3306/sor')
        connection_sor = engine_sor.connect()
        print("Connected successfully to SOR database.")

        # Obtener IDs válidos de las tablas dimensionales
        valid_participant_ids = pd.read_sql('SELECT participant_id FROM Dim_Participants', engine_sor)['participant_id'].tolist()
        valid_position_ids = pd.read_sql('SELECT position_id FROM Dim_Positions', engine_sor)['position_id'].tolist()
        valid_group_ids = pd.read_sql('SELECT group_id FROM Dim_Groups', engine_sor)['group_id'].tolist()
        valid_award_ids = pd.read_sql('SELECT award_id FROM Dim_Awards', engine_sor)['award_id'].tolist()
        valid_date_ids = pd.read_sql('SELECT date_id FROM Dim_Time', engine_sor)['date_id'].tolist()

        sql_query = f'''
        SELECT participant_id, position_id, group_id, award_id, date_id, number_of_awards
        FROM tra_fact_awards_claims
        WHERE participant_id IN ({','.join(['%s']*len(valid_participant_ids))})
          AND position_id IN ({','.join(['%s']*len(valid_position_ids))})
          AND group_id IN ({','.join(['%s']*len(valid_group_ids))})
          AND award_id IN ({','.join(['%s']*len(valid_award_ids))})
          AND date_id IN ({','.join(['%s']*len(valid_date_ids))})
        '''
        params = tuple(valid_participant_ids + valid_position_ids + valid_group_ids + valid_award_ids + valid_date_ids)

        awards_claims_data = pd.read_sql(sql_query, connection_stg, params=params)
        print(f"Retrieved {awards_claims_data.shape[0]} rows from tra_fact_awards_claims.")

        df_fact_awards_claims = pd.DataFrame({
            "participant_id": awards_claims_data['participant_id'],
            "position_id": awards_claims_data['position_id'],
            "group_id": awards_claims_data['group_id'],
            "award_id": awards_claims_data['award_id'],
            "date_id": awards_claims_data['date_id'],
            "number_of_awards": awards_claims_data['number_of_awards']
        })

        df_fact_awards_claims.to_sql('Fact_Awards_Claims', connection_sor, if_exists='append', index=False)
        print(f"Successfully loaded {df_fact_awards_claims.shape[0]} rows into Fact_Awards_Claims.")

    except Exception as e:
        print("Error during data loading:")
        traceback.print_exc()
    finally:
        if 'connection_stg' in locals():
            connection_stg.close()
        if 'connection_sor' in locals():
            connection_sor.close()

load_awards_claims()
