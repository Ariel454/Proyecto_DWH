import traceback
import pandas as pd
from sqlalchemy import create_engine

def load_suppliers():
    try:
        # Configuración de conexión a la base de datos de staging
        engine_stg = create_engine('mysql+pymysql://dwh:elcaro_4U@192.168.10.193:3306/staging')
        connection_stg = engine_stg.connect()
        print("Connected successfully to staging database.")
        
        # Configuración de conexión a la base de datos de almacenamiento en el área de presentación (SOR)
        engine_sor = create_engine('mysql+pymysql://dwh:elcaro_4U@192.168.10.193:3306/sor')
        connection_sor = engine_sor.connect()
        print("Connected successfully to SOR database.")

        # Extracción de datos desde la tabla de proveedores en staging
        sql_query = '''
        SELECT id, name, contact_name, email, mobile
        FROM tra_suppliers
        '''
        suppliers_data = pd.read_sql(sql_query, connection_stg)
        print(f"Retrieved {suppliers_data.shape[0]} rows from tra_suppliers.")

        # Creación del DataFrame para Dim_Suppliers
        df_dim_suppliers = pd.DataFrame({
            "supplier_id": suppliers_data['id'],
            "name": suppliers_data['name'],
            "contact_name": suppliers_data['contact_name'],
            "email": suppliers_data['email'],
            "mobile": suppliers_data['mobile']
        })

        # Persistencia de los datos en la tabla Dim_Suppliers en SOR
        df_dim_suppliers.to_sql('Dim_Suppliers', connection_sor, if_exists='append', index=False)
        print(f"Successfully loaded {df_dim_suppliers.shape[0]} rows into Dim_Suppliers.")

    except Exception as e:
        print("Error during data loading:")
        traceback.print_exc()
    finally:
        if 'connection_stg' in locals():
            connection_stg.close()
            print("Staging database connection closed.")
        if 'connection_sor' in locals():
            connection_sor.close()
            print("SOR database connection closed.")

# Llamada a la función para cargar proveedores
load_suppliers()
