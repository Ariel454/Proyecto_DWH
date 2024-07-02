import traceback
import pandas as pd
from per_staging import persistir_staging


def extract_positions ():

    try:
        filename = "D:/U/7 SEMESTER/Análisis y visualización de datos/Proyecto_DWH/ETL's/data/positions.csv"
        countries = pd.read_csv(filename)
        return countries

    except:
        traceback.print_exc()
    finally:
        pass
    
    
positions = extract_positions()
print(positions)

persistir_staging(positions, 'ext_positions')