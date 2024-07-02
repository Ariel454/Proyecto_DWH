import pymysql
from sqlalchemy import create_engine, text
from faker import Faker
import random

# Conexión a la base de datos
engine = create_engine('mysql+pymysql://dwh:elcaro_4U@192.168.10.193:3306/oltp')
connection = engine.connect()

# Generador de datos falsos
faker = Faker()

# Sentencia SQL para insertar datos en la tabla address
stmt_insert_address = text("""
    INSERT INTO address (alias, sector, main_street, house_number, secondary_street, reference,
                         contact_name, contact_phone, is_deleted, city, created_at, updated_at,
                         participant_id)
    VALUES (:alias, :sector, :main_street, :house_number, :secondary_street, :reference,
            :contact_name, :contact_phone, :is_deleted, :city, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP,
            :participant_id)
""")

try:
    # Inicia la transacción
    trans = connection.begin()

    # Consulta para obtener todos los IDs de participantes existentes
    stmt_select_participants = text("SELECT id FROM participants")
    result_participants = connection.execute(stmt_select_participants)

    # Extrae todos los IDs de participantes de la consulta
    participant_ids = [row[0] for row in result_participants.fetchall()]

    # Verificar que la lista de participant_ids no esté vacía
    if not participant_ids:
        raise Exception("No se encontraron registros en la tabla participants.")

    # Inserta 3000 registros aleatorios en la tabla address
    for _ in range(3000):
        participant_id = random.choice(participant_ids)
        address_data = {
            'alias': faker.word(),
            'sector': faker.word(),
            'main_street': faker.street_name(),
            'house_number': faker.building_number(),
            'secondary_street': faker.street_name(),
            'reference': faker.text(),
            'contact_name': faker.name(),
            'contact_phone': faker.phone_number(),
            'is_deleted': faker.boolean(),
            'city': faker.city(),
            'participant_id': participant_id
        }
        result = connection.execute(stmt_insert_address, address_data)
        print(f"Registro insertado en address para participant_id {participant_id}")

    # Confirma la transacción
    trans.commit()
    print("Todos los registros han sido insertados correctamente en la tabla address.")
except Exception as e:
    # Si hay un error, se revierte la transacción
    trans.rollback()
    print(f"Error durante la inserción en la tabla address: {e}")
finally:
    # Cierra la conexión
    connection.close()
