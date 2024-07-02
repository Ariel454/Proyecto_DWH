import traceback
import pandas as pd
from per_staging import persistir_staging

def extract_suppliers():
    try:
        # Ruta al archivo Excel
        filename = "D:/U/7 SEMESTER/Análisis y visualización de datos/Proyecto_DWH/ETL's/data/suppliers.xlsx"
        
        # Leer el archivo Excel
        suppliers = pd.read_excel(filename)
        
        return suppliers

    except:
        traceback.print_exc()
    finally:
        pass

# Extraer datos de suppliers desde el archivo Excel
suppliers = extract_suppliers()
print(suppliers)

# Persistir datos en la tabla de staging ext_suppliers
persistir_staging(suppliers, 'ext_suppliers')
