import pymysql
from sqlalchemy import create_engine, text
from faker import Faker
import random

# Conexión a la base de datos
engine = create_engine('mysql+pymysql://dwh:elcaro_4U@192.168.10.193:3306/oltp')
connection = engine.connect()

# Generador de datos falsos
faker = Faker()

# Función para generar datos de participantes sin grupo
def generate_participant_data_without_group(program_id, position_id, supervisor_id):
    return {
        'username': faker.user_name(),
        'identifier': faker.ssn(),
        'password': faker.password(),
        'avatar': faker.image_url(),
        'first_name': faker.first_name(),
        'last_name': faker.last_name(),
        'mobile': faker.phone_number(),
        'email': faker.email(),
        'is_active': faker.boolean(),
        'is_a_consumer_owner': faker.boolean(),
        'is_a_consumer_registrar': faker.boolean(),
        'is_deleted': faker.boolean(),
        'program_id': program_id,
        'position_id': position_id,
        'participant_supervisor_id': supervisor_id,
        'document': faker.ssn(),
        'date_of_birth': faker.date_of_birth(),
        'is_approved': faker.boolean(),
        'city': faker.city(),
        'created_from': faker.ipv4(),
        'is_vip': faker.boolean(),
        'is_for_test': faker.boolean(),
        'approved_terms_and_conditions': faker.text(),
        'segment_id': random.randint(1, 10),
        'can_make_requests': faker.boolean(),
        'cannot_make_requests_reason': faker.text(),
        'is_v5': faker.boolean()
    }

# Función para generar datos de grupos
def generate_group_data(program_id, registered_by_participant_id, parent_id, supervisor_id):
    return {
        'name': faker.company(),
        'level': random.randint(1, 5),
        'is_deleted': faker.boolean(),
        'program_id': program_id,
        'registered_by_participant_id': registered_by_participant_id,
        'parent_id': parent_id,
        'supervisor_id': supervisor_id,
        'code': faker.bothify(text='???-####'),
        'mobile': faker.phone_number(),
        'can_upload_snaps': faker.boolean(),
        'can_upload_extra_snaps': faker.boolean()
    }

# Sentencias SQL para insertar datos
stmt_insert_participants_without_group = text("""
    INSERT INTO participants (username, identifier, password, avatar, first_name, last_name,
                              mobile, email, is_active, is_a_consumer_owner, is_a_consumer_registrar,
                              is_deleted, created_at, updated_at, program_id, position_id, participant_supervisor_id,
                              document, date_of_birth, is_approved, city, created_from, is_vip, is_for_test,
                              approved_terms_and_conditions, segment_id, can_make_requests, cannot_make_requests_reason, is_v5)
    VALUES (:username, :identifier, :password, :avatar, :first_name, :last_name, :mobile, :email,
            :is_active, :is_a_consumer_owner, :is_a_consumer_registrar, :is_deleted, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP,
            :program_id, :position_id, :participant_supervisor_id, :document, :date_of_birth, :is_approved, :city,
            :created_from, :is_vip, :is_for_test, :approved_terms_and_conditions, :segment_id, :can_make_requests,
            :cannot_make_requests_reason, :is_v5)
""")

stmt_insert_groups = text("""
    INSERT INTO `groups` (name, level, is_deleted, created_at, updated_at,
                          program_id, registered_by_participant_id, parent_id,
                          supervisor_id, code, mobile, can_upload_snaps, can_upload_extra_snaps)
    VALUES (:name, :level, :is_deleted, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP,
            :program_id, :registered_by_participant_id, :parent_id,
            :supervisor_id, :code, :mobile, :can_upload_snaps, :can_upload_extra_snaps)
""")

stmt_update_participant_group = text("""
    UPDATE participants
    SET group_id = :group_id
    WHERE id = :participant_id
""")

try:
    # Inicia la transacción
    trans = connection.begin()

    # Consulta para obtener todos los IDs de programas y posiciones existentes
    stmt_select_programs = text("SELECT id FROM programs")
    stmt_select_positions = text("SELECT id FROM positions")
    result_programs = connection.execute(stmt_select_programs)
    result_positions = connection.execute(stmt_select_positions)

    # Extrae todos los IDs de programas y posiciones de la consulta
    program_ids = [row[0] for row in result_programs.fetchall()]
    position_ids = [row[0] for row in result_positions.fetchall()]

    # Verificar que las listas de program_ids y position_ids no estén vacías
    if not program_ids or not position_ids:
        raise Exception("No se encontraron registros en las tablas programs o positions.")

    participant_ids = []

    # Inserta 3000 registros aleatorios en la tabla participants sin la referencia a grupos
    for _ in range(3000):
        program_id = random.choice(program_ids)
        position_id = random.choice(position_ids)
        supervisor_id = random.choice(participant_ids) if participant_ids else None
        participant_data = generate_participant_data_without_group(program_id, position_id, supervisor_id)
        result = connection.execute(stmt_insert_participants_without_group, participant_data)
        participant_ids.append(result.lastrowid)
        print(f"Registro insertado en participants: {participant_data['username']}")

    # Inserta 1000 registros aleatorios en la tabla groups
    for _ in range(1000):
        program_id = random.choice(program_ids)
        registered_by_participant_id = random.choice(participant_ids)
        parent_id = random.choice(participant_ids)
        supervisor_id = random.choice(participant_ids)
        group_data = generate_group_data(program_id, registered_by_participant_id, parent_id, supervisor_id)
        result = connection.execute(stmt_insert_groups, group_data)
        group_id = result.lastrowid
        print(f"Registro insertado en groups: {group_data['name']}")

        # Actualiza los participantes con el grupo recién creado
        connection.execute(stmt_update_participant_group, {'group_id': group_id, 'participant_id': registered_by_participant_id})
        connection.execute(stmt_update_participant_group, {'group_id': group_id, 'participant_id': parent_id})
        connection.execute(stmt_update_participant_group, {'group_id': group_id, 'participant_id': supervisor_id})

    # Confirma la transacción
    trans.commit()
    print("Todos los registros han sido insertados correctamente en las tablas participants y groups.")
except Exception as e:
    # Si hay un error, se revierte la transacción
    trans.rollback()
    print(f"Error durante la inserción en las tablas participants o groups: {e}")
finally:
    # Cierra la conexión
    connection.close()
