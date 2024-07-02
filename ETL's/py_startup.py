import pymysql
from sqlalchemy import create_engine, text
from faker import Faker
import random

# Conexión a la base de datos
engine = create_engine('mysql+pymysql://dwh:elcaro_4U@192.168.10.193:3306/oltp')
connection = engine.connect()

# Generador de datos falsos
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

# Sentencia SQL para insertar datos
stmt_insert_programs = text("""
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

try:
    # Inicia la transacción
    trans = connection.begin()
    
    # Inserta 10 registros en la tabla programs
    for _ in range(10):
        program_data = generate_program_data()
        connection.execute(stmt_insert_programs, program_data)
        print(f"Registro insertado: {program_data['name']}")
    
    # Confirma la transacción
    trans.commit()
    print("Todos los registros han sido insertados correctamente en la tabla programs.")
except Exception as e:
    # Si hay un error, se revierte la transacción
    trans.rollback()
    print(f"Error durante la inserción en la tabla programs: {e}")
finally:
    # Cierra la conexión
    connection.close()

# Conexión a la base de datos para la tabla positions
connection = engine.connect()

# Función para generar datos de posición
def generate_position_data(program_id):
    return {
        'name': faker.job(),
        'max_points_per_month': random.randint(100, 1000),
        'is_deleted': faker.boolean(),
        'program_id': program_id,
        'crp': faker.boolean()
    }

# Sentencia SQL para insertar datos en la tabla positions
stmt_insert_positions = text("""
    INSERT INTO positions (name, max_points_per_month, is_deleted,
                           created_at, updated_at, program_id, crp)
    VALUES (:name, :max_points_per_month, :is_deleted,
            CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, :program_id, :crp)
""")

try:
    # Inicia la transacción
    trans = connection.begin()

    # Consulta para obtener todos los IDs de programas existentes en la tabla programs
    stmt_select_programs = text("SELECT id FROM programs")
    result = connection.execute(stmt_select_programs)

    # Extrae todos los IDs de programas de la consulta
    program_ids = [row[0] for row in result.fetchall()]

    # Inserta 40 registros aleatorios en la tabla positions
    for _ in range(40):
        program_id = random.choice(program_ids)
        position_data = generate_position_data(program_id)
        connection.execute(stmt_insert_positions, position_data)
        print(f"Registro insertado para el programa ID {program_id}: {position_data['name']}")

    # Confirma la transacción
    trans.commit()
    print("Todos los registros han sido insertados correctamente en la tabla positions.")
except Exception as e:
    # Si hay un error, se revierte la transacción
    trans.rollback()
    print(f"Error durante la inserción en la tabla positions: {e}")
finally:
    # Cierra la conexión
    connection.close()

# Conexión a la base de datos para la tabla groups
connection = engine.connect()

# Función para generar datos de grupo
def generate_group_data(program_id, parent_id, supervisor_id):
    return {
        'name': faker.company(),
        'level': random.randint(1, 5),
        'is_deleted': faker.boolean(),
        'program_id': program_id,
        'registered_by_participant_id': random.randint(1, 1000), # assuming participant_id range
        'parent_id': parent_id,
        'supervisor_id': supervisor_id,
        'code': faker.bothify(text='???-####'),
        'mobile': faker.phone_number(),
        'can_upload_snaps': faker.boolean(),
        'can_upload_extra_snaps': faker.boolean()
    }

# Sentencia SQL para insertar datos en la tabla groups
stmt_insert_groups = text("""
    INSERT INTO `groups` (name, level, is_deleted, created_at, updated_at,
                          program_id, registered_by_participant_id, parent_id,
                          supervisor_id, code, mobile, can_upload_snaps, can_upload_extra_snaps)
    VALUES (:name, :level, :is_deleted, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP,
            :program_id, :registered_by_participant_id, :parent_id,
            :supervisor_id, :code, :mobile, :can_upload_snaps, :can_upload_extra_snaps)
""")

try:
    # Inicia la transacción
    trans = connection.begin()

    # Consulta para obtener todos los IDs de programas existentes en la tabla programs
    result = connection.execute(stmt_select_programs)

    # Extrae todos los IDs de programas de la consulta
    program_ids = [row[0] for row in result.fetchall()]

    # Inserta 1000 registros aleatorios en la tabla groups
    for _ in range(1000):
        program_id = random.choice(program_ids)
        parent_id = random.randint(1, 100)  # assuming parent_id range
        supervisor_id = random.randint(1, 100)  # assuming supervisor_id range
        group_data = generate_group_data(program_id, parent_id, supervisor_id)
        connection.execute(stmt_insert_groups, group_data)
        print(f"Registro insertado para el programa ID {program_id}: {group_data['name']}")

    # Confirma la transacción
    trans.commit()
    print("Todos los registros han sido insertados correctamente en la tabla groups.")
except Exception as e:
    # Si hay un error, se revierte la transacción
    trans.rollback()
    print(f"Error durante la inserción en la tabla groups: {e}")
finally:
    # Cierra la conexión
    connection.close()
