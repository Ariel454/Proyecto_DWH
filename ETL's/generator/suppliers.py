import pymysql
from sqlalchemy import create_engine, text
from faker import Faker
import random

# Conexión a la base de datos
engine = create_engine('mysql+pymysql://dwh:elcaro_4U@192.168.10.193:3306/oltp')
connection = engine.connect()

# Generador de datos falsos
faker = Faker()

# Sentencia SQL para insertar datos en la tabla suppliers
stmt_insert_suppliers = text("""
    INSERT INTO suppliers (code, name, mobile, contact_name, email, is_deleted, margin, created_at, updated_at)
    VALUES (:code, :name, :mobile, :contact_name, :email, :is_deleted, :margin, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
""")

try:
    # Inicia la transacción
    trans = connection.begin()

    # Inserta 120 registros aleatorios en la tabla suppliers
    for _ in range(120):
        supplier_data = {
            'code': faker.bothify(text='SUP-###-???'),
            'name': faker.company(),
            'mobile': faker.phone_number(),
            'contact_name': faker.name(),
            'email': faker.email(),
            'is_deleted': faker.boolean(),
            'margin': round(random.uniform(5.0, 20.0), 2)
        }
        result = connection.execute(stmt_insert_suppliers, supplier_data)
        print(f"Registro insertado en suppliers: {supplier_data['name']}")

    # Confirma la transacción
    trans.commit()
    print("Todos los registros han sido insertados correctamente en la tabla suppliers.")
except Exception as e:
    # Si hay un error, se revierte la transacción
    trans.rollback()
    print(f"Error durante la inserción en la tabla suppliers: {e}")
finally:
    # Cierra la conexión
    connection.close()
