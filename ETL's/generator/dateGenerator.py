from faker import Faker
import random
from sqlalchemy import text

faker = Faker()

# Función para generar datos del programa
def generate_program_data():
    return {
        'name': faker.company(),
        'date_from': faker.date_between(start_date='-2y', end_date='today'),
        'date_to': faker.date_between(start_date='today', end_date='+1y'),
        'logo': faker.image_url(),
        'is_store_active': faker.boolean(),
        'main_banner': faker.image_url(),
        'faq': faker.text(),
        'rules': faker.text(),
        'how_to_earn_points': faker.text(),
        'support_phone': faker.phone_number(),
        'terms_and_conditions': faker.text(),
        'coin_name': faker.currency_name(),
        'is_demo': faker.boolean(),
        'has_academy': faker.boolean(),
        'academy_url': faker.url(),
        'is_deleted': faker.boolean(),
        'initial_points_bag': random.randint(0, 1000)
    }

# Función para insertar datos en la tabla programs
def insert_program_data(connection, num_records=10):
    stmt = text("""
        INSERT INTO programs (name, date_from, date_to, logo, is_store_active,
                              main_banner, faq, rules, how_to_earn_points, support_phone,
                              terms_and_conditions, coin_name, is_demo, has_academy,
                              academy_url, is_deleted, created_at, updated_at,
                              initial_points_bag)
        VALUES (:name, :date_from, :date_to, :logo, :is_store_active,
                :main_banner, :faq, :rules, :how_to_earn_points, :support_phone,
                :terms_and_conditions, :coin_name, :is_demo, :has_academy,
                :academy_url, :is_deleted, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP,
                :initial_points_bag)
    """)

    program_ids = []

    try:
        # Inicia la transacción
        trans = connection.begin()

        # Inserta los registros
        for _ in range(num_records):
            program_data = generate_program_data()
            result = connection.execute(stmt, **program_data)
            program_ids.append(result.lastrowid)
            print(f"Registro insertado: {program_data['name']}")

        # Confirma la transacción
        trans.commit()
        print("Todos los registros han sido insertados correctamente.")
    except Exception as e:
        # Si hay un error, se revierte la transacción
        trans.rollback()
        print(f"Error durante la inserción: {e}")

    return program_ids
