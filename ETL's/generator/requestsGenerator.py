import pymysql
from sqlalchemy import create_engine, text
from faker import Faker
import random

# Conexión a la base de datos
engine = create_engine('mysql+pymysql://dwh:elcaro_4U@192.168.10.193:3306/oltp')
connection = engine.connect()

# Generador de datos falsos
faker = Faker()

# Sentencia SQL para insertar datos en la tabla requests
stmt_insert_requests = text("""
    INSERT INTO requests (code, quantity, used_cost, margin, points, approved_at, downloaded_at, newer_at, 
                          delivered_at, warehouse_at, dispatched_at, canceled_at, cancelation_reason, 
                          shipping_guide, award_id, address_id, type, status, invoice_id, courier, special_at, 
                          invoice_number, invoice_cost, billing_status, invoice_discount, invoice_interest, 
                          invoice_delivery, is_deleted, created_at, updated_at)
    VALUES (:code, :quantity, :used_cost, :margin, :points, :approved_at, :downloaded_at, :newer_at, 
            :delivered_at, :warehouse_at, :dispatched_at, :canceled_at, :cancelation_reason, 
            :shipping_guide, :award_id, :address_id, :type, :status, :invoice_id, :courier, :special_at, 
            :invoice_number, :invoice_cost, :billing_status, :invoice_discount, :invoice_interest, 
            :invoice_delivery, :is_deleted, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
""")

try:
    # Inicia la transacción
    trans = connection.begin()

    # Consultas para obtener todos los IDs de premios, direcciones y facturas existentes
    stmt_select_awards = text("SELECT id FROM awards")
    stmt_select_addresses = text("SELECT id FROM address")
    stmt_select_invoices = text("SELECT id FROM invoices")
    result_awards = connection.execute(stmt_select_awards)
    result_addresses = connection.execute(stmt_select_addresses)
    result_invoices = connection.execute(stmt_select_invoices)

    # Extrae todos los IDs de premios, direcciones y facturas de las consultas
    award_ids = [row[0] for row in result_awards.fetchall()]
    address_ids = [row[0] for row in result_addresses.fetchall()]
    invoice_ids = [row[0] for row in result_invoices.fetchall()]

    # Verificar que las listas de IDs no estén vacías
    if not award_ids or not address_ids or not invoice_ids:
        raise Exception("No se encontraron registros en las tablas awards, address o invoices.")

    # Inserta 12000 registros aleatorios en la tabla requests
    for _ in range(12000):
        request_data = {
            'code': faker.bothify(text='REQ-#####'),
            'quantity': random.randint(1, 20),
            'used_cost': round(random.uniform(10.0, 500.0), 2),
            'margin': round(random.uniform(0.0, 100.0), 2),
            'points': random.randint(0, 1000),
            'approved_at': faker.date_time_between(start_date='-2y', end_date='now'),
            'downloaded_at': faker.date_time_between(start_date='-2y', end_date='now'),
            'newer_at': faker.date_time_between(start_date='-2y', end_date='now'),
            'delivered_at': faker.date_time_between(start_date='-2y', end_date='now'),
            'warehouse_at': faker.date_time_between(start_date='-2y', end_date='now'),
            'dispatched_at': faker.date_time_between(start_date='-2y', end_date='now'),
            'canceled_at': faker.date_time_between(start_date='-2y', end_date='now'),
            'cancelation_reason': faker.text(),
            'shipping_guide': faker.text(),
            'award_id': random.choice(award_ids),
            'address_id': random.choice(address_ids),
            'type': faker.word(),
            'status': faker.word(),
            'invoice_id': random.choice(invoice_ids),
            'courier': faker.company(),
            'special_at': faker.date_time_between(start_date='-2y', end_date='now'),
            'invoice_number': faker.bothify(text='INV-#####'),
            'invoice_cost': round(random.uniform(50.0, 1000.0), 2),
            'billing_status': faker.word(),
            'invoice_discount': round(random.uniform(0.0, 100.0), 2),
            'invoice_interest': round(random.uniform(0.0, 50.0), 2),
            'invoice_delivery': round(random.uniform(0.0, 50.0), 2),
            'is_deleted': faker.boolean()
        }
        result = connection.execute(stmt_insert_requests, request_data)
        print(f"Registro insertado en requests: {request_data['code']}")

    # Confirma la transacción
    trans.commit()
    print("Todos los registros han sido insertados correctamente en la tabla requests.")
except Exception as e:
    # Si hay un error, se revierte la transacción
    trans.rollback()
    print(f"Error durante la inserción en la tabla requests: {e}")
finally:
    # Cierra la conexión
    connection.close()
