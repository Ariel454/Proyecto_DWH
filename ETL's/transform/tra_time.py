import pandas as pd
from sqlalchemy import create_engine
import traceback

def create_time_dimension(start_date, end_date):
    try:
        # Generar un DataFrame con un rango de fechas
        dates = pd.date_range(start=start_date, end=end_date)
        time_df = pd.DataFrame(dates, columns=['date'])
        
        # Añadir columnas de año, mes, día, trimestre y semana
        time_df['day'] = time_df['date'].dt.day
        time_df['month'] = time_df['date'].dt.month
        time_df['year'] = time_df['date'].dt.year
        time_df['quarter'] = time_df['date'].dt.quarter
        time_df['week'] = time_df['date'].dt.isocalendar().week
        
        # Configuración de conexión a la base de datos de staging
        engine = create_engine('mysql+pymysql://dwh:elcaro_4U@192.168.10.193:3306/staging')
        connection = engine.connect()
        
        # Insertar datos en la tabla tra_time
        time_df.to_sql('tra_time', con=engine, index=False, if_exists='replace', method='multi')
        print("Time dimension table populated successfully.")

    except Exception as e:
        print("Error during time dimension creation:")
        traceback.print_exc()
    finally:
        if 'connection' in locals():
            connection.close()

# Llamar a la función con el rango de fechas deseado
create_time_dimension('2010-01-01', '2030-12-31')
