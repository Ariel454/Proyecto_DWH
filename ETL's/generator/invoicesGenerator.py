import pymysql
from sqlalchemy import create_engine, text
from faker import Faker
import random

# Conexión a la base de datos
engine = create_engine('mysql+pymysql://dwh:elcaro_4U@192.168.10.193:3306/oltp')
connection = engine.connect()

# Generador de datos falsos
faker = Faker()

# Sentencia SQL para insertar datos en la tabla invoices
stmt_insert_invoices = text("""
    INSERT INTO invoices (attachment, has_individual_values, invoice_number, discount, delivery, taxes,
                          is_deleted, supplier_id, subtotal, created_at, updated_at)
    VALUES (:attachment, :has_individual_values, :invoice_number, :discount, :delivery, :taxes,
            :is_deleted, :supplier_id, :subtotal, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
""")

try:
    # Inicia la transacción
    trans = connection.begin()

    # Consulta para obtener todos los IDs de proveedores existentes
    stmt_select_suppliers = text("SELECT id FROM suppliers")
    result_suppliers = connection.execute(stmt_select_suppliers)

    # Extrae todos los IDs de proveedores de la consulta
    supplier_ids = [row[0] for row in result_suppliers.fetchall()]

    # Verificar que la lista de supplier_ids no esté vacía
    if not supplier_ids:
        raise Exception("No se encontraron registros en la tabla suppliers.")

    # Inserta 2000 registros aleatorios en la tabla invoices
    for _ in range(2000):
        supplier_id = random.choice(supplier_ids)
        invoice_data = {
            'attachment': faker.url(),
            'has_individual_values': faker.boolean(),
            'invoice_number': faker.bothify(text='INV-###-???'),
            'discount': round(random.uniform(0.0, 100.0), 2),
            'delivery': round(random.uniform(0.0, 50.0), 2),
            'taxes': round(random.uniform(0.0, 200.0), 2),
            'is_deleted': faker.boolean(),
            'supplier_id': supplier_id,
            'subtotal': round(random.uniform(100.0, 1000.0), 2)
        }
        result = connection.execute(stmt_insert_invoices, invoice_data)
        print(f"Registro insertado en invoices: {invoice_data['invoice_number']}")

    # Confirma la transacción
    trans.commit()
    print("Todos los registros han sido insertados correctamente en la tabla invoices.")
except Exception as e:
    # Si hay un error, se revierte la transacción
    trans.rollback()
    print(f"Error durante la inserción en la tabla invoices: {e}")
finally:
    # Cierra la conexión
    connection.close()
