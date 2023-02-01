from app.main import db
from app.main.seeders import (
    add_anonymization_records,
    add_anonymization_types,
    add_database,
    add_database_keys,
    add_sql_logs,
    add_user,
    add_valid_database,
    create_test_frida_db,
)


def create_seed():
    # To seed tables
    add_user()
    add_valid_database()
    add_database()
    add_database_keys()
    add_anonymization_types()
    add_anonymization_records()
    add_sql_logs()
    db.session.commit()

    # Create Test Database
    try:
        create_test_frida_db(
            USER="root",
            DB_PW="larces132",
            HOST="localhost",
            DB_NAME="test_frida_db",
            TABLE_NAME="clientes",
        )
    except:
        print("==== Log Create DB Flask ====")
        print("Teste database not created")
