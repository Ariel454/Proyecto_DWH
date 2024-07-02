import pymysql
from sqlalchemy import create_engine, text
from faker import Faker
import random

# Conexión a la base de datos
engine = create_engine('mysql+pymysql://dwh:elcaro_4U@192.168.10.193:3306/oltp')
connection = engine.connect()

# Generador de datos falsos
faker = Faker()

# Sentencia SQL para insertar datos en la tabla awards
stmt_insert_awards = text("""
    INSERT INTO awards (code, name, model, description, main_image, current_cost, last_cost_updated_date,
                        is_active, brand_id, supplier_code, created_at, updated_at)
    VALUES (:code, :name, :model, :description, :main_image, :current_cost, :last_cost_updated_date,
            :is_active, :brand_id, :supplier_code, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
""")

try:
    # Inicia la transacción
    trans = connection.begin()

    # Consulta para obtener todos los IDs de marcas existentes
    stmt_select_brands = text("SELECT id FROM suppliers")
    result_brands = connection.execute(stmt_select_brands)

    # Extrae todos los IDs de marcas de la consulta
    brand_ids = [row[0] for row in result_brands.fetchall()]

    # Verificar que la lista de brand_ids no esté vacía
    if not brand_ids:
        raise Exception("No se encontraron registros en la tabla suppliers.")

    # Inserta 4000 registros aleatorios en la tabla awards
    for _ in range(4000):
        brand_id = random.choice(brand_ids)
        award_data = {
            'code': faker.bothify(text='AWD-###-???'),
            'name': faker.word(),
            'model': faker.word(),
            'description': faker.text(),
            'main_image': faker.image_url(),
            'current_cost': round(random.uniform(100.0, 1000.0), 2),
            'last_cost_updated_date': faker.date_this_year(),
            'is_active': faker.boolean(),
            'brand_id': brand_id,
            'supplier_code': faker.bothify(text='SUP-###')
        }
        result = connection.execute(stmt_insert_awards, award_data)
        print(f"Registro insertado en awards: {award_data['name']}")

    # Confirma la transacción
    trans.commit()
    print("Todos los registros han sido insertados correctamente en la tabla awards.")
except Exception as e:
    # Si hay un error, se revierte la transacción
    trans.rollback()
    print(f"Error durante la inserción en la tabla awards: {e}")
finally:
    # Cierra la conexión
    connection.close()
