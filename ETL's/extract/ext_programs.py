import traceback
import pandas as pd
from sqlalchemy import create_engine
from per_staging import persistir_staging

def extract_programs():
    try:
        # Configuración de conexión a la base de datos de origen (oltp)
        engine = create_engine('mysql+pymysql://dwh:elcaro_4U@192.168.10.193:3306/oltp')
        connection = engine.connect()
        
        # Extracción de datos desde la tabla programs
        programs = pd.read_sql('SELECT * FROM programs', connection)

        return programs

    except:
        traceback.print_exc()
    finally:
        connection.close()

programs = extract_programs()
print(programs)

# Persistir datos en la tabla de staging ext_suppliers
persistir_staging(programs, 'ext_programs')
