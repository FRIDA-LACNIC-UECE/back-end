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


def create_seed(env_name: str):
    # To seed tables
    add_user()
    add_valid_database()
    add_database(env_name=env_name)
    add_database_keys()
    add_anonymization_types()
    add_anonymization_records()
    add_sql_logs()
    db.session.commit()

    # Create Test Database
    try:
        if env_name == "dev":
            create_test_frida_db(
                USER="root",
                DB_PW="larces132",
                HOST="localhost",
                DB_NAME="test_frida_db",
            )
        else:
            create_test_frida_db(
                USER="root",
                DB_PW="",
                HOST="db",
                DB_NAME="test_frida_db",
            )
    except:
        print("==== Log Create DB Flask ====")
        print("Teste database not created")
