import pymysql
from sqlalchemy import create_engine, text
from faker import Faker
import random

# Conexión a la base de datos
engine = create_engine('mysql+pymysql://dwh:elcaro_4U@192.168.10.193:3306/oltp')
connection = engine.connect()

# Generador de datos falsos
faker = Faker()

# Sentencia SQL para insertar datos en la tabla invoices_items
stmt_insert_invoices_items = text("""
    INSERT INTO invoices_items (quantity, name, supplier_code, cost, delivery, is_gift, has_special_cost, 
                                invoice_id, total, type, reference_unit_cost, comment, created_at, updated_at)
    VALUES (:quantity, :name, :supplier_code, :cost, :delivery, :is_gift, :has_special_cost, 
            :invoice_id, :total, :type, :reference_unit_cost, :comment, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
""")

try:
    # Inicia la transacción
    trans = connection.begin()

    # Consulta para obtener todos los IDs de facturas existentes
    stmt_select_invoices = text("SELECT id FROM invoices")
    result_invoices = connection.execute(stmt_select_invoices)

    # Extrae todos los IDs de facturas de la consulta
    invoice_ids = [row[0] for row in result_invoices.fetchall()]

    # Verificar que la lista de invoice_ids no esté vacía
    if not invoice_ids:
        raise Exception("No se encontraron registros en la tabla invoices.")

    # Inserta 6000 registros aleatorios en la tabla invoices_items
    for _ in range(6000):
        invoice_id = random.choice(invoice_ids)
        item_data = {
            'quantity': random.randint(1, 10),
            'name': faker.word(),
            'supplier_code': faker.bothify(text='SUP-###'),
            'cost': round(random.uniform(10.0, 100.0), 2),
            'delivery': round(random.uniform(0.0, 20.0), 2),
            'is_gift': faker.boolean(),
            'has_special_cost': faker.boolean(),
            'invoice_id': invoice_id,
            'total': round(random.uniform(50.0, 500.0), 2),
            'type': faker.word(),
            'reference_unit_cost': round(random.uniform(5.0, 50.0), 2),
            'comment': faker.text()
        }
        result = connection.execute(stmt_insert_invoices_items, item_data)
        print(f"Registro insertado en invoices_items: {item_data['name']}")

    # Confirma la transacción
    trans.commit()
    print("Todos los registros han sido insertados correctamente en la tabla invoices_items.")
except Exception as e:
    # Si hay un error, se revierte la transacción
    trans.rollback()
    print(f"Error durante la inserción en la tabla invoices_items: {e}")
finally:
    # Cierra la conexión
    connection.close()
